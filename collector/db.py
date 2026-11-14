"""SQLite 持久层。

两类持久化边界，彼此独立、都可以在进程重启后恢复：

1. **未配对读段落盘（pending 表）**：某端每解析出一条完整 FASTQ 记录，
   立即在单事务中写入 ``pending`` 并前推该端的**采集进度**
   （``r*_fetched``）。因此即使 R1 比 R2 领先数万条，未配对读段占用的是
   磁盘而不是采集进程内存；R2 追上后按写入顺序（pending 自增 id 升序）
   逐条配对。
2. **配对提交（pairs 表 + runs 偏移）**：每批配对在单事务中写入
   ``pairs``、删除对应 ``pending`` 行，并**同时**前推两端的
   ``r1_offset`` / ``r2_offset``（已配对字节进度）与 ``pairs`` 计数。
   任一语句失败整体回滚，重启后从旧偏移重新拉取，不会漏对或重对。

旧库兼容：``init_db`` 会给已有的 ``runs`` 表补上 ``r1_fetched`` /
``r2_fetched`` 列并把它们抬到不小于已配对偏移，历史数据与配对数保持原样。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    base_url      TEXT NOT NULL,
    status        TEXT NOT NULL,            -- running|completed|error
    r1_offset     INTEGER NOT NULL DEFAULT 0,   -- 已配对 R1 字节进度
    r2_offset     INTEGER NOT NULL DEFAULT 0,   -- 已配对 R2 字节进度
    r1_fetched    INTEGER NOT NULL DEFAULT 0,   -- 已落盘 R1 采集进度
    r2_fetched    INTEGER NOT NULL DEFAULT 0,   -- 已落盘 R2 采集进度
    pairs         INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pairs (
    run_id     TEXT NOT NULL,
    seq        INTEGER NOT NULL,             -- 配对顺序号，从 0 开始
    name       TEXT NOT NULL,
    r1_record  BLOB NOT NULL,
    r2_record  BLOB NOT NULL,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, name)
);
CREATE TABLE IF NOT EXISTS pending (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,
    side        INTEGER NOT NULL,            -- 1=R1, 2=R2
    name        TEXT NOT NULL,               -- 去掉 /1 /2 后缀的读段名
    record      BLOB NOT NULL,               -- 完整四行原始字节
    end_offset  INTEGER NOT NULL             -- 该记录之后的源文件绝对字节偏移
);
CREATE INDEX IF NOT EXISTS idx_pending_run_side ON pending(run_id, side, id);
"""

_FETCH_COLUMNS = ("r1_fetched", "r2_fetched")


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _migrate(conn)
    conn.commit()


def _migrate(conn: sqlite3.Connection) -> None:
    """给旧库补列/补表，保留已有运行与配对数据。"""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
    for col in _FETCH_COLUMNS:
        if col not in cols:
            conn.execute(
                f"ALTER TABLE runs ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0"
            )
    # 旧库没有采集进度的概念：以已配对偏移作为起点，保证不会回退重复拉取
    conn.execute(
        "UPDATE runs SET r1_fetched=MAX(r1_fetched, r1_offset),"
        " r2_fetched=MAX(r2_fetched, r2_offset)"
    )


def create_run(conn: sqlite3.Connection, run_id: str, base_url: str, now: float) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO runs(run_id, base_url, status, created_at, updated_at)"
        " VALUES (?, ?, 'running', ?, ?)",
        (run_id, base_url, now, now),
    )
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()


def list_runs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM runs ORDER BY created_at").fetchall()


def mark_status(
    conn: sqlite3.Connection, run_id: str, status: str, now: float, error: dict | None = None
) -> None:
    conn.execute(
        "UPDATE runs SET status=?, error=?, updated_at=? WHERE run_id=?",
        (status, json.dumps(error, ensure_ascii=False) if error else None, now, run_id),
    )
    conn.commit()


def append_pending(
    conn: sqlite3.Connection,
    run_id: str,
    side: int,
    records: Iterable[tuple[str, bytes, int]],
    fetched_offset: int,
    now: float,
) -> None:
    """把一条或多条完整记录落盘，并在同一事务里前推该端采集进度。

    ``records`` 为 ``(name, raw, end_offset)``，顺序与源文件一致。
    """
    records = list(records)
    if not records:
        return
    if side not in (1, 2):
        raise AssertionError(f"非法 side={side}")
    col = f"r{side}_fetched"
    with conn:  # 原子：记录落盘与采集偏移要么都生效，要么都回滚
        conn.executemany(
            "INSERT INTO pending(run_id, side, name, record, end_offset)"
            " VALUES (?, ?, ?, ?, ?)",
            [(run_id, side, name, raw, end) for name, raw, end in records],
        )
        conn.execute(
            f"UPDATE runs SET {col}=?, updated_at=? WHERE run_id=?",
            (fetched_offset, now, run_id),
        )


def read_pending(
    conn: sqlite3.Connection, run_id: str, side: int, limit: int
) -> list[sqlite3.Row]:
    """按写入顺序读取该端最早的至多 ``limit`` 条未配对记录。"""
    return conn.execute(
        "SELECT id, name, record, end_offset FROM pending"
        " WHERE run_id=? AND side=? ORDER BY id LIMIT ?",
        (run_id, side, limit),
    ).fetchall()


def first_pending(conn: sqlite3.Connection, run_id: str, side: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, name, record, end_offset FROM pending"
        " WHERE run_id=? AND side=? ORDER BY id LIMIT 1",
        (run_id, side),
    ).fetchone()


def count_pending(conn: sqlite3.Connection, run_id: str, side: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM pending WHERE run_id=? AND side=?",
        (run_id, side),
    ).fetchone()
    return int(row["n"])


def commit_pairs(
    conn: sqlite3.Connection,
    run_id: str,
    start_seq: int,
    pairs: Iterable[tuple[str, bytes, bytes]],
    r1_offset: int,
    r2_offset: int,
    r1_last_id: int,
    r2_last_id: int,
    now: float,
) -> int:
    """单事务提交一批配对。

    ``pairs`` 为 ``(name, r1_record, r2_record)``，序号从 ``start_seq`` 起。
    同一事务内：插入 pairs 行、删除已消费的 pending 行、同时前推两端
    已配对偏移与配对数。返回本批提交的配对数。
    """
    items = list(pairs)
    if not items:
        return 0
    with conn:
        conn.executemany(
            "INSERT INTO pairs(run_id, seq, name, r1_record, r2_record)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (run_id, start_seq + i, name, r1_raw, r2_raw)
                for i, (name, r1_raw, r2_raw) in enumerate(items)
            ],
        )
        # 同端 id 单调递增：本批读到的前 N 条即该端 id <= last_id 的全部行
        conn.execute(
            "DELETE FROM pending WHERE run_id=? AND side=1 AND id<=?",
            (run_id, r1_last_id),
        )
        conn.execute(
            "DELETE FROM pending WHERE run_id=? AND side=2 AND id<=?",
            (run_id, r2_last_id),
        )
        conn.execute(
            "UPDATE runs SET r1_offset=?, r2_offset=?, pairs=pairs+?, updated_at=?"
            " WHERE run_id=?",
            (r1_offset, r2_offset, len(items), now, run_id),
        )
    return len(items)


def iter_records(conn: sqlite3.Connection, run_id: str, side: str):
    """按配对顺序流式产出某一端的完整 FASTQ 记录字节。"""
    col = "r1_record" if side == 1 else "r2_record"
    cur = conn.execute(
        f"SELECT {col} AS rec FROM pairs WHERE run_id=? ORDER BY seq", (run_id,)
    )
    for row in cur:
        yield row["rec"]


def snapshot(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    error = json.loads(row["error"]) if row["error"] else None
    return {
        "run_id": row["run_id"],
        "base_url": row["base_url"],
        "status": row["status"],
        # 已配对字节进度（沿用原有含义，两段在配对事务中一起前推）
        "r1_offset": row["r1_offset"],
        "r2_offset": row["r2_offset"],
        # 已落盘采集进度：单端领先时它会先于已配对进度前进
        "r1_fetched": row["r1_fetched"],
        "r2_fetched": row["r2_fetched"],
        "pairs": row["pairs"],
        "r1_pending": count_pending(conn, row["run_id"], 1),
        "r2_pending": count_pending(conn, row["run_id"], 2),
        "error": error,
    }
