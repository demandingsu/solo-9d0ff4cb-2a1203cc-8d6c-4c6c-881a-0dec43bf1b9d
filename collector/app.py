"""采集端 REST API。

端点：
* POST /collect/runs            提交/触发一次增量采集（幂等，可重复调用）
* GET  /collect/runs            列出全部运行
* GET  /collect/runs/{id}       查询单次运行进度与错误详情
* GET  /collect/runs/{id}/R1    下载已配对的 R1（仅 completed 允许）
* GET  /collect/runs/{id}/R2    下载已配对的 R2（仅 completed 允许）
* GET  /healthz                 存活探针
"""
from __future__ import annotations

import os
from urllib.parse import urlparse

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import db
from .service import CollectorService

DB_PATH = os.environ.get("COLLECTOR_DB", "/data/collector.db")
HTTP_TIMEOUT = float(os.environ.get("COLLECTOR_TIMEOUT", "10"))

_conn = db.connect(DB_PATH)
db.init_db(_conn)
_service = CollectorService(_conn, timeout=HTTP_TIMEOUT)


def _read_conn():
    """每次只读查询用独立短连接。

    采集循环在事件循环线程里长期持有 ``_conn``；若同步端点（FastAPI 会放到
    线程池执行）复用同一连接，可能触发 sqlite3 的跨线程并发错误。WAL 模式
    下独立连接能读到已提交快照，开销可忽略。
    """
    return db.connect(DB_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await _service.aclose()


app = FastAPI(
    title="双端 FASTQ 增量采集服务",
    version="1.1.0",
    description=(
        "通过 HTTP Range 增量拉取 R1/R2，按读段名配对并持久化到 SQLite。\n\n"
        "未配对读段写入 `pending` 表（磁盘）而非堆在内存，因此在受限内存下"
        "也能承受\"R1 领先数万条、R2 尚未写入\"的单端超前；R2 追上后按原顺序"
        "配对，两端进度在同一事务中一起前推。\n\n"
        "建议先用中转站的 `POST /demo/large-lead` 创建场景，再把其返回的 "
        "`absolute_base_url` 填入本服务 `POST /collect/runs` 的 body。"
    ),
    lifespan=lifespan,
)


class CollectRequest(BaseModel):
    run_id: str = Field(..., min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    base_url: str = Field(..., min_length=1, description="运行目录 URL，如 http://relay:8000/runs/demo")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/collect/runs")
async def collect(req: CollectRequest):
    parsed = urlparse(req.base_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=422, detail="base_url 必须是合法的 http(s) URL")
    snap = await _service.collect(req.run_id, req.base_url)
    # 错误终态用 422 表达，方便调用方区分；进度内容仍在响应体中
    status_code = 422 if snap["status"] == "error" else 200
    return JSONResponse(snap, status_code=status_code)


@app.get("/collect/runs")
def list_runs():
    conn = _read_conn()
    try:
        return {"runs": [db.snapshot(conn, r) for r in db.list_runs(conn)]}
    finally:
        conn.close()


@app.get("/collect/runs/{run_id}")
def get_run(run_id: str):
    conn = _read_conn()
    try:
        row = db.get_run(conn, run_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"运行 {run_id} 不存在")
        return db.snapshot(conn, row)
    finally:
        conn.close()


@app.get("/collect/runs/{run_id}/R{side:int}")
def download(run_id: str, side: int):
    if side not in (1, 2):
        raise HTTPException(status_code=404, detail="仅支持 R1 / R2")
    peek = _read_conn()
    try:
        row = db.get_run(peek, run_id)
    finally:
        peek.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"运行 {run_id} 不存在")
    if row["status"] != "completed":
        raise HTTPException(
            status_code=409,
            detail={
                "message": "只有全部读段成功配对（status=completed）后才能下载",
                "run_id": run_id,
                "status": row["status"],
                "pairs": row["pairs"],
                "r1_offset": row["r1_offset"],
                "r2_offset": row["r2_offset"],
                "r1_fetched": row["r1_fetched"],
                "r2_fetched": row["r2_fetched"],
            },
        )
    # 独立连接，避免流式输出期间与采集写事务共用同一连接
    stream_conn = db.connect(DB_PATH)

    def generate():
        try:
            for chunk in db.iter_records(stream_conn, run_id, side):
                yield chunk
        finally:
            stream_conn.close()

    filename = f"{run_id}_R{side}.fastq"
    return StreamingResponse(
        generate(),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
