"""单端超前采集专项测试。

重点覆盖"R1 已有数万条、R2 尚未写入时不得把 R1 堆进内存"：
* 受限内存下大量 R1 领先只落盘、不撑爆进程；
* 读取窗口把一条记录切成两半时不丢记录；
* R2 追上后按原顺序配对、两端进度一起前推；
* 重复调用、进程重启后不漏对、不重对；
* DONE 出现后必须读到两端真实末尾才判定 completed / 残条 / 孤读。
"""
import os
import time

from collector import db
from collector.service import CollectorService

from tests.conftest import Server


def make_scenario(run_id, reads=10, **over):
    sc = {
        "run_id": run_id,
        "reads": reads,
        "append_delay": 0.05,
        "r2_extra_delay": 0.1,
        "chunk": 2,
        "interrupt_at_second": 0,
        "interrupt_duration": 0,
        "seq_len": 12,
    }
    sc.update(over)
    return sc


def poll_completed(client, collector, run_id, base_url, timeout=90):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = client.post(
            f"{collector}/collect/runs",
            json={"run_id": run_id, "base_url": base_url},
        ).json()
        if last["status"] in ("completed", "error"):
            return last
        time.sleep(0.2)
    raise AssertionError(f"未在 {timeout}s 内结束: {last}")


def test_large_r1_lead_is_staged_on_disk_not_in_memory(servers, client, tmp_path):
    """R1 领先数万条时，内存在收集过程中保持有界，且配对进度到磁盘。

    直接在进程内构造服务与 DB，避免 HTTP 进程边界掩盖采集端自身的内存占用。
    """
    # --- 单元级：_consume 把完整记录落盘后，内存只留末尾残条 ---
    conn = db.connect(str(tmp_path / "lead.db"))
    db.init_db(conn)
    svc = CollectorService(conn)
    db.create_run(conn, "lead", "http://x", time.time())

    st = svc._live("lead")[1]
    line = b"x" * 100
    blob = b"".join(
        b"@r%05d/1\n%s\n+\n%s\n" % (i, line, line) for i in range(20_000)
    )
    st.buffer.extend(blob)
    svc._consume("lead", 1, st)

    assert len(st.buffer) == 0, "已落盘的完整记录不应继续留在内存"
    assert db.count_pending(conn, "lead", 1) == 20_000
    row = db.get_run(conn, "lead")
    assert row["r1_fetched"] == len(blob) > 0
    assert row["r1_offset"] == 0 and row["pairs"] == 0, "R2 未到时不得配对"

    # --- 端到端：真实 relay 让 R1 先写好、R2 滞后 ---
    relay, _collector, _meta = servers
    run_id = "lead_e2e"
    client.post(f"{relay}/runs", json=make_scenario(
        run_id, reads=4000, r2_extra_delay=3.0, chunk=4000, seq_len=24,
    ))
    base_url = f"{relay}/runs/{run_id}"

    # 等 R1 全部写完而 R2 尚未开始
    deadline = time.time() + 30
    while time.time() < deadline:
        stt = client.get(f"{relay}/runs/{run_id}/status").json()
        if stt["r1_reads"] == 4000 and stt["r2_reads"] == 0:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("relay 未按预期先写完 R1")

    r = client.post(f"{_collector}/collect/runs",
                    json={"run_id": run_id, "base_url": base_url})
    snap = r.json()
    assert snap["status"] == "running"
    assert snap["pairs"] == 0
    assert snap["r1_pending"] == 4000, "领先的 R1 应全部暂存到磁盘"
    assert snap["r1_fetched"] > snap["r1_offset"] == 0
    assert snap["r2_pending"] == 0

    final = poll_completed(client, _collector, run_id, base_url)
    assert final["status"] == "completed" and final["pairs"] == 4000
    assert final["r1_pending"] == final["r2_pending"] == 0
    assert final["r1_offset"] == final["r2_offset"] == final["r1_fetched"]

    src1 = client.get(f"{relay}/runs/{run_id}/R1.fastq").content
    got1 = client.get(f"{_collector}/collect/runs/{run_id}/R1").content
    assert got1 == src1, "R2 追上后应按原顺序配对，内容逐字节一致"


def test_record_split_across_read_windows_is_not_lost(tmp_path):
    """一条记录被读取窗口切成两半（甚至切在行中间）时仍能正确配对。

    直接驱动 CollectorService 的解析/落盘路径，按 3 字节切块喂入，确保
    "暂时读到文件末尾/窗口切开记录"不会丢记录或产生半对。
    """
    conn = db.connect(str(tmp_path / "split.db"))
    db.init_db(conn)
    svc = CollectorService(conn)
    db.create_run(conn, "split", "http://x", time.time())

    sides = svc._live("split")
    r1 = b"".join(b"@read%02d/1\nACGTACGT\n+\nIIIIIIII\n" % i for i in range(25))
    r2 = b"".join(b"@read%02d/2\nACGTACGT\n+\nIIIIIIII\n" % i for i in range(25))

    def feed(blob, side):
        st = sides[side]
        for i in range(0, len(blob), 3):  # 3 字节一切，故意切到行中间
            st.buffer.extend(blob[i:i + 3])
            svc._consume("split", side, st)
            svc._pair("split")
            assert len(st.buffer) < 64, "残条窗口应有界"

    # R1 先全部到达并暂存，R2 完全没有
    feed(r1, 1)
    row = db.get_run(conn, "split")
    assert db.count_pending(conn, "split", 1) == 25
    assert row["pairs"] == 0 and row["r1_fetched"] == len(r1)

    # R2 追赶，逐块喂入；配对过程中不得出现半对或错序
    feed(r2, 2)
    row = db.get_run(conn, "split")
    assert row["pairs"] == 25
    assert db.count_pending(conn, "split", 1) == 0
    assert db.count_pending(conn, "split", 2) == 0
    assert row["r1_offset"] == len(r1) and row["r2_offset"] == len(r2)

    got1 = b"".join(db.iter_records(conn, "split", 1))
    got2 = b"".join(db.iter_records(conn, "split", 2))
    assert got1 == r1 and got2 == r2
    conn.close()


def test_read_window_split_over_http(servers, client):
    """端到端：真实 HTTP Range 下按小 read 窗口反复续采，不丢不重。"""
    relay, collector, _ = servers
    run_id = "splitwin"
    client.post(f"{relay}/runs", json=make_scenario(
        run_id, reads=40, chunk=1, append_delay=0.01, r2_extra_delay=0.0,
    ))
    base_url = f"{relay}/runs/{run_id}"
    snap = poll_completed(client, collector, run_id, base_url)
    assert snap["status"] == "completed" and snap["pairs"] == 40

    src1 = client.get(f"{relay}/runs/{run_id}/R1.fastq").content
    got1 = client.get(f"{collector}/collect/runs/{run_id}/R1").content
    assert got1 == src1


def test_demo_endpoint_exposes_large_lead(servers, client):
    """中转站 /demo/large-lead 应能一键产出可被采集的绝对 base_url。"""
    relay, _collector, _meta = servers
    d = client.post(f"{relay}/demo/large-lead",
                    params={"reads": 300, "seq_len": 20}).json()
    assert d["run_id"].startswith("lead-")
    assert d["absolute_base_url"].endswith(f"/runs/{d['run_id']}")
    # 该场景应出现"R1 全部落盘、R2 仍为 0"的超前窗口
    deadline = time.time() + 20
    while time.time() < deadline:
        st = client.get(f"{relay}/runs/{d['run_id']}/status").json()
        if st["r1_reads"] == 300 and st["r2_reads"] == 0:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("未出现 R1 领先窗口")
    snap = client.post(f"{_collector}/collect/runs",
                       json={"run_id": d["run_id"],
                             "base_url": d["absolute_base_url"]}).json()
    assert snap["status"] == "running" and snap["r1_pending"] == 300
    assert snap["pairs"] == 0


def test_repeated_calls_after_complete_are_idempotent(servers, client):
    relay, collector, _ = servers
    run_id = "idem"
    client.post(f"{relay}/runs", json=make_scenario(run_id, reads=5, chunk=1))
    base_url = f"{relay}/runs/{run_id}"
    first = poll_completed(client, collector, run_id, base_url)
    assert first["pairs"] == 5
    for _ in range(3):
        again = client.post(f"{collector}/collect/runs",
                            json={"run_id": run_id, "base_url": base_url}).json()
        assert again["pairs"] == 5 and again["status"] == "completed"


def test_restart_with_committed_lead_does_not_duplicate(servers, client, tmp_path):
    """领先的 R1 已落盘后重启：新进程从 DB 恢复，不重拉已落盘字节。"""
    relay, _collector, _meta = servers
    run_id = "restart_lead"
    client.post(f"{relay}/runs", json=make_scenario(
        run_id, reads=300, r2_extra_delay=4.0, chunk=300, seq_len=16,
    ))
    base_url = f"{relay}/runs/{run_id}"
    deadline = time.time() + 30
    while time.time() < deadline:
        stt = client.get(f"{relay}/runs/{run_id}/status").json()
        if stt["r1_reads"] == 300 and stt["r2_reads"] == 0:
            break
        time.sleep(0.05)

    # 进程 A 只把 R1 落盘
    srv_a = Server("collector.app:app",
                   {"COLLECTOR_DB": str(tmp_path / "restart.db")})
    srv_a.start()
    try:
        a = client.post(f"{srv_a.base_url}/collect/runs",
                        json={"run_id": run_id, "base_url": base_url}).json()
        assert a["r1_pending"] == 300 and a["pairs"] == 0
        fetched_after_a = a["r1_fetched"]
    finally:
        srv_a.stop()

    # 进程 B：同一 DB，应从 fetched 处继续而不是从 0 重拉
    srv_b = Server("collector.app:app",
                   {"COLLECTOR_DB": str(tmp_path / "restart.db")})
    srv_b.start()
    try:
        b = client.get(f"{srv_b.base_url}/collect/runs/{run_id}").json()
        assert b["r1_pending"] == 300 and b["r1_fetched"] == fetched_after_a
        final = poll_completed(client, srv_b.base_url, run_id, base_url, timeout=90)
        assert final["status"] == "completed" and final["pairs"] == 300
        src1 = client.get(f"{relay}/runs/{run_id}/R1.fastq").content
        got1 = client.get(f"{srv_b.base_url}/collect/runs/{run_id}/R1").content
        assert got1 == src1, "重启后不得漏对或重对"
    finally:
        srv_b.stop()


def test_done_with_unconfirmable_eof_waits_then_completes(servers, client):
    """DONE 已在但端点正处中断窗口（无法确认 EOF）时不得提前判完成/孤读。

    先等 relay 写完并放 DONE，再打开中断窗口使采集端拿不到真实末尾；
    此时 status 必须保持 running 且不进入孤读/残条错误；恢复链路后才完成。
    """
    relay, collector, _ = servers
    run_id = "donewait"
    client.post(f"{relay}/runs", json=make_scenario(
        run_id, reads=4, chunk=2, append_delay=0.02, r2_extra_delay=0.0,
    ))
    base_url = f"{relay}/runs/{run_id}"
    # 等 relay 完全写完（DONE 已放）
    deadline = time.time() + 30
    while time.time() < deadline:
        st = client.get(f"{relay}/runs/{run_id}/status").json()
        if st["done"]:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("relay 未完成")

    # 打开一个足够长的中断窗口：此刻 DONE 可见，但 GET 会 503
    client.post(f"{relay}/runs/{run_id}/interrupt", json={"duration": 5})
    snap = client.post(f"{collector}/collect/runs",
                       json={"run_id": run_id, "base_url": base_url}).json()
    assert snap["status"] == "running", f"DONE 但 EOF 未确认时不得终态化: {snap}"
    assert snap["error"] is None, "不得因无法确认 EOF 而报残条/孤读"

    # 关闭中断窗口后应能读到真实末尾并完成
    client.post(f"{relay}/runs/{run_id}/resume")
    final = poll_completed(client, collector, run_id, base_url, timeout=30)
    assert final["status"] == "completed" and final["pairs"] == 4


def test_done_with_short_r2_reports_orphan(servers, client):
    """两端都到真实末尾且 DONE 在：缺一端才算孤读。"""
    relay, collector, meta = servers
    run_id = "orphan2"
    rdir = os.path.join(meta["relay_data"], run_id)
    os.makedirs(rdir, exist_ok=True)
    with open(os.path.join(rdir, "R1.fastq"), "wb") as f:
        f.write(b"@a/1\nACGT\n+\nIIII\n@b/1\nACGT\n+\nIIII\n")
    with open(os.path.join(rdir, "R2.fastq"), "wb") as f:
        f.write(b"@a/2\nACGT\n+\nIIII\n")
    open(os.path.join(rdir, "DONE"), "w").close()
    snap = client.post(
        f"{collector}/collect/runs",
        json={"run_id": run_id, "base_url": f"{relay}/runs/{run_id}"},
    ).json()
    assert snap["status"] == "error" and snap["error"]["code"] == "orphan_read"
    assert snap["error"]["name"] == "b"
