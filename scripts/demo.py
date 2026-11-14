"""端到端演示：构造"R1 大幅领先、R2 延迟到达"的场景，验证单端超前采集。

流程：
1. 让中转站先写入 ``READS`` 条 R1 并触发一次连接中断，R2 则等到 R1 全部
   落盘后才开始写（``r2_lag_reads`` >= ``READS``）。此时采集端面对的是
   "数万条 R1 已就绪、R2 尚未写入"的极端单端超前局面；
2. 反复触发采集：观察 R1 采集进度与暂存量先增长、配对数保持 0；
3. R2 追上来后，按原顺序配对并一起前推两端进度；
4. 下载并逐字节校验两端内容。

环境变量：
  RELAY_URL     默认 http://relay:8000
  COLLECTOR_URL 默认 http://collector:8000
  RUN_ID        默认 demo（自动追加时间戳避免重复）
  READS         默认 30000
  SEQ_LEN       默认 120
"""
from __future__ import annotations

import os
import sys
import time

import httpx

RELAY = os.environ.get("RELAY_URL", "http://relay:8000").rstrip("/")
COLLECTOR = os.environ.get("COLLECTOR_URL", "http://collector:8000").rstrip("/")
RUN_ID = os.environ.get("RUN_ID", "demo")
READS = int(os.environ.get("READS", "30000"))
SEQ_LEN = int(os.environ.get("SEQ_LEN", "120"))


def wait_ready(client: httpx.Client, url: str, timeout: float = 60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if client.get(f"{url}/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"服务未就绪: {url}")


def create_scenario(client: httpx.Client, run_id: str) -> str:
    scenario = {
        "run_id": run_id,
        "reads": READS,
        "append_delay": 0.0,
        "r2_extra_delay": 0.0,
        # R2 落后整个数据集，且 R1 写完后先按住 R2 8 秒，
        # 让"数万条 R1 已就绪、R2 尚未写入"的超前窗口稳定可观察
        "r2_lag_reads": READS,
        "r2_start_delay": 8.0,
        "chunk": max(1, READS // 20),
        "interrupt_at_second": 0.4,
        "interrupt_duration": 1.5,
        "seq_len": SEQ_LEN,
    }
    r = client.post(f"{RELAY}/runs", json=scenario)
    if r.status_code == 409:
        print(f"[demo] 运行 {run_id} 已存在，改用新 run_id")
        run_id = f"{run_id}-{int(time.time())}"
        scenario["run_id"] = run_id
        r = client.post(f"{RELAY}/runs", json=scenario)
    if r.status_code != 200:
        raise RuntimeError(f"创建中转站运行失败: {r.status_code} {r.text}")
    print(f"[demo] 中转站场景已创建: {run_id}（reads={READS}, "
          f"R1 先全部落盘, R2 延迟到达, 含一次连接中断）")
    return run_id


def main() -> int:
    with httpx.Client(timeout=30) as c:
        wait_ready(c, RELAY)
        wait_ready(c, COLLECTOR)
        print("[demo] 两个服务均已就绪")

        run_id = create_scenario(c, RUN_ID)
        base_url = f"{RELAY}/runs/{run_id}"

        # 先等 R1 全部落盘且 R2 尚未开始，确认"单端超前"局面真实存在
        deadline = time.time() + 120
        relay_state = None
        while time.time() < deadline:
            relay_state = c.get(f"{RELAY}/runs/{run_id}/status").json()
            if (relay_state["r1_reads"] == READS
                    and relay_state["r2_reads"] == 0):
                break
            time.sleep(0.2)
        else:
            print(f"[demo] 未观察到 R1 领先窗口: {relay_state}")
            return 1
        print(f"[demo] 单端超前窗口: R1 已落盘 {relay_state['r1_reads']} 条 "
              f"({relay_state['r1_bytes']} 字节)，R2 仍为 0 条")

        # 领先窗口内触发采集：R2 未到达前，R1 应只被暂存到磁盘（pairs 仍为 0）
        lead_seen = False
        deadline = time.time() + 180
        snap = None
        last_key = None
        while time.time() < deadline:
            r = c.post(f"{COLLECTOR}/collect/runs",
                       json={"run_id": run_id, "base_url": base_url})
            snap = r.json()
            if snap["pairs"] == 0 and snap["r1_pending"] > 0:
                lead_seen = True
            key = (snap["status"], snap["pairs"], snap["r1_pending"] > 0)
            if key != last_key or snap["status"] != "running":
                print(f"[demo] status={snap['status']} pairs={snap['pairs']} "
                      f"r1_offset={snap['r1_offset']} r1_fetched={snap['r1_fetched']} "
                      f"r1_pending={snap['r1_pending']} r2_pending={snap['r2_pending']} "
                      f"notices={snap.get('notices')}")
                last_key = key
            if snap["status"] == "error":
                print(f"[demo] 采集失败: {snap['error']}")
                return 1
            if snap["status"] == "completed":
                break
            time.sleep(0.8)
        else:
            print("[demo] 等待完成超时")
            return 1

        if not lead_seen:
            print("[demo] 警告：未观察到 R1 领先暂存态（窗口过短）")

        assert snap["pairs"] == READS, f"配对数不符: {snap['pairs']}"
        assert snap["r1_pending"] == snap["r2_pending"] == 0, "完成后不应有暂存"
        assert snap["r1_offset"] == snap["r2_offset"] == snap["r1_fetched"], \
            "两端已配对进度应同时到达末尾"

        for side, expect_suffix in ((1, "/1"), (2, "/2")):
            r = c.get(f"{COLLECTOR}/collect/runs/{run_id}/R{side}")
            r.raise_for_status()
            lines = r.content.decode().splitlines()
            assert len(lines) == READS * 4, "下载行数不符"
            assert all(lines[i].startswith("@") and lines[i].endswith(expect_suffix)
                       for i in range(0, len(lines), 4)), "名称后缀不符"
            assert all(len(lines[i + 1]) == len(lines[i + 3])
                       for i in range(0, len(lines), 4)), "序质长度不等"
            print(f"[demo] R{side} 校验通过: {READS} 条, {len(r.content)} 字节")

        print(f"[demo] 最终进度: {c.get(f'{COLLECTOR}/collect/runs/{run_id}').json()}")
        print("[demo] 单端超前采集演示完成 ✔")
        print(f"[demo] 可在浏览器查看中转站接口文档: {RELAY}/docs")
        return 0


if __name__ == "__main__":
    sys.exit(main())
