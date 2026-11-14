"""增量采集服务：HTTP Range 续采 + 磁盘暂存未配对读段 + SQLite 原子提交。

内存模型（这是本服务在"R1 领先数万条、R2 尚未写入"时不会 MemoryError 的
关键）：

* 采集进程**不在内存里囤积未配对读段**。每解析出一条完整 FASTQ 记录，
  就立即在单个事务中写入 SQLite ``pending`` 表，并前推该端的采集进度
  ``r*_fetched``；
* 内存中每个端只有两样东西：一个``buffer``（仅保存当前文件末尾尚未凑齐
  四行的残缺字节，天然受单条记录长度约束）和当前已确认字节偏移；
* 配对时只按批（默认 500 条）从 ``pending`` 顺序读取，配成一对即提交并
  删除对应 pending 行，内存占用与已积压的未配对读段数量无关。

持久化边界：

1. ``append_pending``：记录落盘与该端采集偏移前推同一事务；
2. ``commit_pairs``：配对内容、两端已配对偏移、配对数、pending 删除同一事务。

任何一步失败整体回滚，进程重启后从数据库中已提交的偏移与 pending 行恢复，
保持"不漏对、不重对"；磁盘上已暂存的 R1 读段在 R2 追上后仍按原顺序配对。

错误分类：
* 瞬时问题（连接失败、流中断、源站暂时 5xx、暂时读到文件末尾）——不改变
  running 状态，已收字节（完整记录已落盘、残条留在 buffer）保留，下一次
  调用按 Range 断点续采；
* 数据问题（结构错误、名称不匹配、完成后残条/孤读）——进入 error 终态，
  停止采集并回报两端字节位置。
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field

import httpx

from . import db
from .parser import FastqFormatError, Record, split_fastq, validate  # noqa: F401

DONE_NAME = "DONE"
R1_NAME = "R1.fastq"
R2_NAME = "R2.fastq"
IDLE_DEADLINE = 4          # 一轮采集中两端连续没有新字节的轮次上限
PAIR_BATCH = int(os.environ.get("COLLECTOR_PAIR_BATCH", "500"))  # 单事务最大配对数
READ_CHUNK = 64 * 1024     # HTTP 流式读取块大小
MAX_PARTIAL = int(                      # 单条残记录字节上限，防止畸形输入撑爆内存
    os.environ.get("COLLECTOR_MAX_PARTIAL", str(64 * 1024 * 1024))
)


class CollectorError(RuntimeError):
    """采集进入不可恢复的数据错误终态。"""

    def __init__(self, code: str, message: str, detail: dict):
        super().__init__(message)
        self.code = code
        self.detail = detail


@dataclass
class SideState:
    """单个端的轻量内存状态：只有已确认偏移与末尾残条。"""

    fetched: int = 0                               # 已落盘的采集字节进度
    buffer: bytearray = field(default_factory=bytearray)  # 仅末尾残缺记录

    def advance(self, delta: int) -> None:
        if delta < 0 or delta > len(self.buffer):
            raise AssertionError(f"非法的缓冲裁剪: delta={delta}")
        del self.buffer[:delta]
        self.fetched += delta


class CollectorService:
    def __init__(self, conn, timeout: float = 10.0):
        self.conn = conn
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._locks: dict[str, asyncio.Lock] = {}
        self._sides: dict[str, dict[int, SideState]] = {}

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _lock(self, run_id: str) -> asyncio.Lock:
        lock = self._locks.get(run_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[run_id] = lock
        return lock

    def _live(self, run_id: str) -> dict[int, SideState]:
        """内存状态；进程重启后从 SQLite 的采集进度重建（buffer 为空）。

        重启时尚未配对的完整记录都在 pending 表中，无需重建队列；文件末尾
        的残缺字节不在 fetched 计数内，会从 fetched 处重新 Range 拉取。
        """
        sides = self._sides.get(run_id)
        if sides is None:
            row = db.get_run(self.conn, run_id)
            sides = {
                1: SideState(fetched=row["r1_fetched"]),
                2: SideState(fetched=row["r2_fetched"]),
            }
            self._sides[run_id] = sides
        return sides

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout, follow_redirects=True)
        return self._client

    # ------------------------------------------------------------------ 主流程
    async def collect(self, run_id: str, base_url: str) -> dict:
        async with self._lock(run_id):
            now = time.time()
            db.create_run(self.conn, run_id, base_url, now)
            row = db.get_run(self.conn, run_id)
            if row["status"] in ("completed", "error"):
                return db.snapshot(self.conn, row)

            sides = self._live(run_id)
            notices: list[str] = []
            idle_rounds = 0
            try:
                while True:
                    done = await self._source_done(base_url)

                    advanced = False
                    problems = False
                    eof: dict[int, bool] = {}
                    for side in (1, 2):
                        got, problem, at_eof = await self._fetch_side(
                            run_id, base_url, sides, side
                        )
                        if got:
                            advanced = True
                        if problem:
                            problems = True
                            notices.append(problem)
                        eof[side] = at_eof
                    # 先把两端已落盘的记录尽量配对，再判断终态
                    self._pair(run_id)

                    if done and eof[1] and eof[2] and not problems:
                        # DONE 后两端都确认到真实末尾（416/404 或读满 total），
                        # 且本轮没有传输问题，才允许判定 completed / 残条 / 孤读
                        self._finalize(run_id, sides)
                        return db.snapshot(self.conn, db.get_run(self.conn, run_id))

                    if not advanced:
                        idle_rounds += 1
                        if idle_rounds >= IDLE_DEADLINE:
                            notices.append("源端暂无可采集的新字节，等待后续追加")
                            break
                        await asyncio.sleep(0.2)
                    else:
                        idle_rounds = 0
            except CollectorError as exc:
                db.mark_status(
                    self.conn, run_id, "error", time.time(),
                    {"code": exc.code, "message": str(exc), **exc.detail},
                )
            snap = db.snapshot(self.conn, db.get_run(self.conn, run_id))
            if notices:
                # 去重后透传瞬时情况，状态仍为 running
                snap["notices"] = list(dict.fromkeys(notices))
            return snap

    # ------------------------------------------------------------- 单端 Range 拉取
    async def _fetch_side(
        self, run_id: str, base_url: str, sides: dict[int, SideState], side: int
    ) -> tuple[bool, str | None, bool]:
        """从"已落盘偏移 + 末尾残条长度"处续采。

        返回 (本轮是否收到新字节, 瞬时问题描述, 是否确认到 EOF)。
        EOF 仅在源站明确返回 416/404、或本次响应读满 Content-Range 报告的
        总长度时成立；传输中断/连接失败一律不成立，残条与断点保留。
        """
        st = sides[side]
        client = await self._http()
        url = f"{base_url.rstrip('/')}/{R1_NAME if side == 1 else R2_NAME}"
        start = st.fetched + len(st.buffer)
        try:
            req = client.build_request(
                "GET", url, headers={"Range": f"bytes={start}-"}
            )
            resp = await client.send(req, stream=True)
        except httpx.HTTPError as exc:
            return False, f"R{side} 连接源站失败（保留断点 {start}）: {exc}", False

        problem: str | None = None
        eof = False
        got = False
        try:
            if resp.status_code in (404, 416):
                return False, None, True  # 文件尚未创建 / 已到 EOF
            if resp.status_code >= 500:
                # 源站瞬时故障/中断：保留断点，下轮按 Range 重试
                word = "中断" if resp.status_code == 503 else "故障"
                return False, (
                    f"R{side} 源站连接{word}（HTTP {resp.status_code}），"
                    f"保留断点 {start}，等待重试"
                ), False
            if resp.status_code != 206 and not (resp.status_code == 200 and start == 0):
                raise CollectorError(
                    "source_http_error",
                    f"源站返回不可恢复的 HTTP {resp.status_code}",
                    {"url": url, "r1_offset": self._committed(run_id, 1),
                     "r2_offset": self._committed(run_id, 2)},
                )
            if resp.status_code == 200 and start != 0:
                # 源站不支持 Range：继续会破坏"已有字节不改写"的前提
                raise CollectorError(
                    "range_unsupported",
                    "源站未按 Range 请求返回 206，无法保证增量字节一致性",
                    {"url": url, "requested_offset": start,
                     "r1_offset": self._committed(run_id, 1),
                     "r2_offset": self._committed(run_id, 2)},
                )
            try:
                async for chunk in resp.aiter_bytes(READ_CHUNK):
                    if not chunk:
                        continue
                    got = True
                    st.buffer.extend(chunk)
                    # 边收边解析落盘：内存里永远只留下末尾凑不齐四行的残条
                    self._consume(run_id, side, st)
            except httpx.HTTPError as exc:
                problem = (
                    f"R{side} 传输中断，已保留 {st.fetched + len(st.buffer)} 字节"
                    f"断点，下次续采: {exc}"
                )
            if not problem:
                # 完整读完本次 Range 响应：读满 Content-Range 报告的总长即到
                # 请求时刻的 EOF（文件后续若再追加，下轮 Range 会继续取到）
                content_range = resp.headers.get("content-range", "")
                if content_range and "/" in content_range:
                    total = int(content_range.rsplit("/", 1)[1])
                    eof = st.fetched + len(st.buffer) >= total
                elif resp.status_code == 200:
                    eof = True
            return got, problem, eof
        finally:
            await resp.aclose()

    def _committed(self, run_id: str, side: int) -> int:
        row = db.get_run(self.conn, run_id)
        return int(row["r1_offset"] if side == 1 else row["r2_offset"]) if row else 0

    def _consume(self, run_id: str, side: int, st: SideState) -> int:
        """把 buffer 中的完整四行记录校验后原子落盘，裁剪已消费字节。"""
        try:
            records, leftover, _parsed, _partial, _poff = split_fastq(
                bytes(st.buffer), 0, st.fetched
            )
            for rec in records:
                validate(rec, side)
        except FastqFormatError as exc:
            other = 2 if side == 1 else 1
            raise CollectorError(
                "fastq_structure_error", f"R{side} FASTQ 结构错误",
                {
                    "reason": str(exc),
                    "side": side,
                    f"r{side}_committed_offset": self._committed(run_id, side),
                    f"r{other}_committed_offset": self._committed(run_id, other),
                    f"r{side}_error_offset": exc.offset,
                },
            ) from exc

        if not records:
            # 没有任何完整记录可落盘：残条等待后续字节
            if len(st.buffer) > MAX_PARTIAL:
                other = 2 if side == 1 else 1
                raise CollectorError(
                    "fastq_structure_error",
                    f"R{side} 单条记录超过 {MAX_PARTIAL} 字节仍无结尾换行",
                    {
                        "reason": "partial record too large",
                        "side": side,
                        f"r{side}_committed_offset": self._committed(run_id, side),
                        f"r{other}_committed_offset": self._committed(run_id, other),
                        f"r{side}_error_offset": st.fetched,
                    },
                )
            return 0

        consumed = len(st.buffer) - len(leftover)
        new_fetched = st.fetched + consumed
        db.append_pending(
            self.conn, run_id, side,
            [(rec.name, rec.raw, rec.end) for rec in records],
            new_fetched, time.time(),
        )
        st.advance(consumed)  # 裁剪已落盘字节，只保留末尾残条
        return len(records)

    # ------------------------------------------------------------------ 配对状态机
    def _pair(self, run_id: str) -> None:
        """按 pending 自增顺序逐批配对并单事务提交。"""
        while True:
            row = db.get_run(self.conn, run_id)
            seq = row["pairs"]
            q1 = db.read_pending(self.conn, run_id, 1, PAIR_BATCH)
            q2 = db.read_pending(self.conn, run_id, 2, PAIR_BATCH)
            n = min(len(q1), len(q2))
            if not q1 or not q2:
                # 任一端暂无未配对记录：等待另一端追上（或终结时判孤读）
                return

            batch: list[tuple[str, bytes, bytes]] = []
            for i in range(n):
                r1, r2 = q1[i], q2[i]
                if r1["name"] != r2["name"]:
                    # 先提交已确认匹配的前缀，再报错：保证错误终态下已配对
                    # 记录不丢、两端偏移准确（与逐条提交的语义一致）
                    r1_off, r2_off = row["r1_offset"], row["r2_offset"]
                    if batch:
                        r1_off, r2_off = q1[i - 1]["end_offset"], q2[i - 1]["end_offset"]
                        db.commit_pairs(
                            self.conn, run_id, seq, batch,
                            r1_offset=r1_off, r2_offset=r2_off,
                            r1_last_id=q1[i - 1]["id"],
                            r2_last_id=q2[i - 1]["id"],
                            now=time.time(),
                        )
                        seq += len(batch)
                    raise CollectorError(
                        "name_mismatch",
                        f"读段名不匹配: R1={r1['name']!r} 与 R2={r2['name']!r}",
                        {
                            "pair_index": seq,
                            "r1_name": r1["name"],
                            "r2_name": r2["name"],
                            "r1_record_start": r1["end_offset"] - len(r1["record"]),
                            "r2_record_start": r2["end_offset"] - len(r2["record"]),
                            "r1_committed_offset": r1_off,
                            "r2_committed_offset": r2_off,
                        },
                    )
                batch.append((r1["name"], r1["record"], r2["record"]))

            db.commit_pairs(
                self.conn, run_id, seq, batch,
                r1_offset=q1[n - 1]["end_offset"],
                r2_offset=q2[n - 1]["end_offset"],
                r1_last_id=q1[n - 1]["id"],
                r2_last_id=q2[n - 1]["id"],
                now=time.time(),
            )
            if len(q1) < PAIR_BATCH and len(q2) < PAIR_BATCH:
                return  # 两端都已读空，暂无更多可配

    # ------------------------------------------------------------------ 终态校验
    def _finalize(self, run_id: str, sides: dict[int, SideState]) -> None:
        row = db.get_run(self.conn, run_id)
        # 1) 残条：文件已到真实末尾，仍有写不完的残缺记录
        for side in (1, 2):
            st = sides[side]
            other = 2 if side == 1 else 1
            residual = len(st.buffer)
            if residual:
                raise CollectorError(
                    "residual_records",
                    f"源端完成后 R{side} 仍有未写完的残缺记录",
                    {
                        "side": side,
                        f"r{side}_committed_offset": row[f"r{side}_offset"],
                        f"r{other}_committed_offset": row[f"r{other}_offset"],
                        "residual_bytes": residual,
                    },
                )
        # 2) 孤读：文件已到真实末尾，仍有完整记录没等到另一端
        for side in (1, 2):
            first = db.first_pending(self.conn, run_id, side)
            if first is None:
                continue
            other = 2 if side == 1 else 1
            raise CollectorError(
                "orphan_read",
                f"源端完成后 R{side} 存在没有另一端配对的孤读: {first['name']!r}",
                {
                    "side": side,
                    "name": first["name"],
                    "record_start": first["end_offset"] - len(first["record"]),
                    f"r{side}_committed_offset": row[f"r{side}_offset"],
                    f"r{other}_committed_offset": row[f"r{other}_offset"],
                },
            )
        db.mark_status(self.conn, run_id, "completed", time.time())

    async def _source_done(self, base_url: str) -> bool:
        client = await self._http()
        url = f"{base_url.rstrip('/')}/{DONE_NAME}"
        try:
            resp = await client.head(url)
        except httpx.HTTPError:
            return False
        try:
            return resp.status_code == 200
        finally:
            await resp.aclose()
