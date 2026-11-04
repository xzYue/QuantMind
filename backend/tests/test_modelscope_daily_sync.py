"""ModelScope 每日增量同步的单元测试。

覆盖：diff 报告、只下载 pending、dry-run 零写入、护栏拦截、哈希直验自愈、
状态库多副本登记、脚本层下游阶段隔离与退出码。网络全部用 fake httpx.Client
拦截（与 test_modelscope_dataset_sync 同款手法），远端枚举 monkeypatch
ms.list_remote_files，聚焦增量逻辑本身。
"""

import hashlib
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.scripts import modelscope_daily_sync as mds
from backend.services.engine.data_platform import modelscope_dataset_sync as ms

DF = "1_kline_data/daily_forward"


# ---------------------------------------------------------------------------
# fake httpx（下载走 .stream，枚举被 monkeypatch 不经过 Client.get）
# ---------------------------------------------------------------------------
class _StreamResp:
    def __init__(self, content: bytes):
        self._content = content

    def raise_for_status(self):
        return None

    def iter_bytes(self, chunk_size=0):
        step = chunk_size or len(self._content) or 1
        for i in range(0, len(self._content), step):
            yield self._content[i : i + step]


class _StreamCtx:
    def __init__(self, resp):
        self._resp = resp

    def __enter__(self):
        return self._resp

    def __exit__(self, *exc):
        return False


class _FakeClient:
    payloads: dict = {}
    stream_calls: int = 0

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def stream(self, method, url):
        path = parse_qs(urlparse(url).query)["FilePath"][0]
        type(self).stream_calls += 1
        return _StreamCtx(_StreamResp(self.payloads[path]))


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _rf(path: str, content: bytes, dataset: str = "daily_forward"):
    return ms.RemoteFile(
        path=path,
        size=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        dataset=dataset,
        layout="v2_daily_partition",
    )


def _write_local(root: Path, rf, content: bytes) -> None:
    p = root / rf.path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)


def _state_db_paths(root: Path) -> list[Path]:
    return [
        root / "quantdb_sync.sqlite",
        root / ".sync_state" / "quantdb_sync_data_quantdb.sqlite",
    ]


def _seed_state(root: Path, rfs: list) -> None:
    """模拟初始化完成后的两个状态库副本（容器命名口径）。"""
    rows = [
        (f.path, f.sha256, f.sha256, f.size, str(root / f.path), f.layout, f.dataset)
        for f in rfs
    ]
    for db in _state_db_paths(root):
        db.parent.mkdir(parents=True, exist_ok=True)
        conn = ms._open_state_db(db)
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO objects(key, etag, sha256, size, path, layout, dataset)"
                " VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()


def _state_rows(db: Path) -> dict:
    conn = sqlite3.connect(str(db))
    try:
        return {r[0]: r[1] for r in conn.execute("SELECT key, sha256 FROM objects")}
    finally:
        conn.close()


@pytest.fixture
def env_root(tmp_path, monkeypatch):
    root = tmp_path / "quantdb"
    root.mkdir()
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(root))
    monkeypatch.delenv("QUANTDB_STATE_DIR", raising=False)
    monkeypatch.setenv("MODELSCOPE_SYNC_WORKERS", "2")
    _FakeClient.payloads = {}
    _FakeClient.stream_calls = 0
    monkeypatch.setattr(ms.httpx, "Client", _FakeClient)
    return root


def _seed_aligned(env_root, monkeypatch):
    """本地+状态库与远端一致的 1 个文件，外加 1 个变更 + 1 个新增。"""
    old = b"old-content"
    keep = _rf(f"{DF}/dt=20260101/data.parquet", old)
    v1 = _rf(f"{DF}/dt=20260102/data.parquet", b"v1")
    _write_local(env_root, keep, old)
    _write_local(env_root, v1, b"v1")
    _seed_state(env_root, [keep, v1])
    changed = _rf(f"{DF}/dt=20260102/data.parquet", b"v2-longer")
    new = _rf(f"{DF}/dt=20260103/data.parquet", b"brand-new")
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [keep, changed, new])
    return keep, changed, new


# ---------------------------------------------------------------------------
# diff 报告
# ---------------------------------------------------------------------------
def test_diff_report_detects_new_and_changed(env_root, monkeypatch):
    keep, changed, new = _seed_aligned(env_root, monkeypatch)

    report = ms.diff_from_modelscope(["daily_forward"])

    ds = report["datasets"]["daily_forward"]
    assert ds["remote_files"] == 3
    assert ds["skippable"] == 1
    assert ds["pending_files"] == 2
    assert ds["pending_bytes"] == len(b"v2-longer") + len(b"brand-new")
    assert ds["pending_missing_sha"] == 0
    assert ds["latest_remote_dt"] == "20260103"
    assert ds["latest_local_dt"] == "20260102"
    assert sorted(ds["pending_samples"]) == sorted([changed.path, new.path])
    assert report["total_pending_files"] == 2
    # 状态库副本发现：SDK 库 + .sync_state 容器命名副本
    assert any(p.endswith("quantdb_sync.sqlite") for p in report["state_dbs"])
    assert any(".sync_state" in p for p in report["state_dbs"])
    assert report["state_keys"] == 2


def test_diff_unknown_dataset_raises(env_root, monkeypatch):
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [])
    with pytest.raises(ValueError, match="未知数据集"):
        ms.diff_from_modelscope(["not_a_dataset"])


# ---------------------------------------------------------------------------
# 增量下载
# ---------------------------------------------------------------------------
def test_incremental_downloads_only_pending(env_root, monkeypatch):
    keep, changed, new = _seed_aligned(env_root, monkeypatch)
    _FakeClient.payloads = {changed.path: b"v2-longer", new.path: b"brand-new"}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["status"] == "ok"
    assert summary["downloaded"] == 2
    assert summary["errors"] == 0
    assert _FakeClient.stream_calls == 2  # 已对齐文件不重下
    assert (env_root / keep.path).read_bytes() == b"old-content"
    assert (env_root / changed.path).read_bytes() == b"v2-longer"
    assert (env_root / new.path).read_bytes() == b"brand-new"
    assert summary["touched_partitions"]["daily_forward"] == ["20260102", "20260103"]
    # 两个状态库副本都登记了新 sha256
    for db in _state_db_paths(env_root):
        rows = _state_rows(db)
        assert rows[new.path] == new.sha256
        assert rows[changed.path] == changed.sha256
        assert rows[keep.path] == keep.sha256


def test_dry_run_writes_nothing(env_root, monkeypatch):
    keep, changed, new = _seed_aligned(env_root, monkeypatch)
    before = _state_rows(env_root / "quantdb_sync.sqlite")

    summary = ms.sync_incremental_from_modelscope(["daily_forward"], dry_run=True)

    assert summary["status"] == "checked"
    assert summary["dry_run"] is True
    assert summary["total_pending_files"] == 2
    assert _FakeClient.stream_calls == 0
    assert not (env_root / new.path).exists()
    assert (env_root / changed.path).read_bytes() == b"v1"
    assert _state_rows(env_root / "quantdb_sync.sqlite") == before


def test_up_to_date_when_fully_aligned(env_root, monkeypatch):
    old = b"same"
    f1 = _rf(f"{DF}/dt=20260101/data.parquet", old)
    _write_local(env_root, f1, old)
    _seed_state(env_root, [f1])
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["status"] == "up_to_date"
    assert summary["downloaded"] == 0
    assert _FakeClient.stream_calls == 0


def test_download_failure_marks_partial_and_keeps_others(env_root, monkeypatch):
    keep, changed, new = _seed_aligned(env_root, monkeypatch)
    # changed 的载荷与远端 sha 不符 → 校验失败；new 正常
    _FakeClient.payloads = {changed.path: b"CORRUPTED!!", new.path: b"brand-new"}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["status"] == "partial"
    assert summary["downloaded"] == 1
    assert summary["errors"] == 1
    assert any(changed.path in s for s in summary["error_samples"])
    # 失败对象不得把新 sha 登记进状态库（防止陈旧文件被误判为已同步）：
    # seed 登记的是 v1 内容指纹，失败后必须保持 v1 指纹而非远端新指纹
    rows = _state_rows(env_root / "quantdb_sync.sqlite")
    assert rows[changed.path] != changed.sha256
    assert rows[new.path] == new.sha256
    # 损坏内容不得落到目标路径（.part 清理 + 原子替换语义）
    assert (env_root / changed.path).read_bytes() == b"v1"


# ---------------------------------------------------------------------------
# 护栏与哈希直验
# ---------------------------------------------------------------------------
def test_guard_blocks_large_pending(env_root, monkeypatch):
    remote = [_rf(f"{DF}/dt=2026010{i}/data.parquet", b"x" * 100) for i in (1, 2)]
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: remote)
    _FakeClient.payloads = {f.path: b"x" * 100 for f in remote}

    with pytest.raises(ms.PendingTooLargeError) as ei:
        ms.sync_incremental_from_modelscope(["daily_forward"], max_pending_bytes=50)
    assert ei.value.pending_files == 2
    assert ei.value.pending_bytes == 200
    assert "quantdb_local_scan" in str(ei.value)
    assert _FakeClient.stream_calls == 0  # 拦截发生在任何下载之前

    summary = ms.sync_incremental_from_modelscope(
        ["daily_forward"], max_pending_bytes=50, allow_large=True
    )
    assert summary["downloaded"] == 2


def test_hash_verify_recovers_missing_state(env_root, monkeypatch):
    """状态库全丢但本地文件完好：哈希直验免下载，并重建登记。"""
    c1 = b"payload-1"
    f1 = _rf(f"{DF}/dt=20260101/data.parquet", c1)
    _write_local(env_root, f1, c1)
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["downloaded"] == 0
    assert summary["hash_verified"] == 1
    assert summary["status"] == "ok"
    assert _FakeClient.stream_calls == 0
    rows = _state_rows(env_root / "quantdb_sync.sqlite")
    assert rows[f1.path] == f1.sha256


def test_hash_verify_mismatch_still_downloads(env_root, monkeypatch):
    """同尺寸但内容损坏（无状态登记）：直验不通过，照常重下。"""
    f1 = _rf(f"{DF}/dt=20260101/data.parquet", b"remote-data")
    _write_local(env_root, f1, b"corrupt-dta")  # 等长不同内容
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])
    _FakeClient.payloads = {f1.path: b"remote-data"}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["hash_verified"] == 0
    assert summary["downloaded"] == 1
    assert (env_root / f1.path).read_bytes() == b"remote-data"


def test_hash_verify_disabled(env_root, monkeypatch):
    c1 = b"payload-1"
    f1 = _rf(f"{DF}/dt=20260101/data.parquet", c1)
    _write_local(env_root, f1, c1)
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])
    _FakeClient.payloads = {f1.path: c1}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"], hash_verify=False)

    assert summary["hash_verified"] == 0
    assert summary["downloaded"] == 1  # 关闭直验 → 无登记就重下
    assert _FakeClient.stream_calls == 1


# ---------------------------------------------------------------------------
# 状态库副本
# ---------------------------------------------------------------------------
def test_discover_state_dbs_dedupes_and_orders(env_root, tmp_path, monkeypatch):
    (env_root / ".sync_state").mkdir()
    (env_root / ".sync_state" / "quantdb_sync_data_quantdb.sqlite").write_bytes(b"")
    monkeypatch.setenv("QUANTDB_STATE_DIR", str(tmp_path / "state"))

    dbs = ms.discover_state_dbs(env_root)

    # env 派生库排最前，其后 .sync_state 副本，最后 root SDK 库；无重复
    from backend.scripts.quantdb_daily_sync import _state_path

    assert dbs[0] == _state_path(env_root)
    assert any(".sync_state" in str(p) for p in dbs)
    assert dbs[-1] == env_root / "quantdb_sync.sqlite"
    assert len({str(p.resolve()) for p in dbs}) == len(dbs)


def test_env_state_db_written_when_env_set(env_root, tmp_path, monkeypatch):
    state_dir = tmp_path / "state"
    monkeypatch.setenv("QUANTDB_STATE_DIR", str(state_dir))
    f1 = _rf(f"{DF}/dt=20260105/data.parquet", b"hello-env")
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])
    _FakeClient.payloads = {f1.path: b"hello-env"}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["downloaded"] == 1
    from backend.scripts.quantdb_daily_sync import _state_path

    env_db = _state_path(env_root)
    assert env_db.exists()
    assert _state_rows(env_db)[f1.path] == f1.sha256
    assert _state_rows(env_root / "quantdb_sync.sqlite")[f1.path] == f1.sha256


def test_native_run_does_not_create_env_db_when_unset(env_root, tmp_path, monkeypatch):
    """未设 QUANTDB_STATE_DIR 时不得在 ~/.quantdb_state 凭空建第三份库。"""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    f1 = _rf(f"{DF}/dt=20260106/data.parquet", b"no-env")
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])
    _FakeClient.payloads = {f1.path: b"no-env"}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["downloaded"] == 1
    assert list(summary["state_dbs"]) == [str(env_root / "quantdb_sync.sqlite")]
    assert not (tmp_path / "home" / ".quantdb_state").exists()


# ---------------------------------------------------------------------------
# 脚本层：下游隔离 / 退出码 / 离线 status
# ---------------------------------------------------------------------------
def _fake_sync_summary(env_root, **over):
    base = {
        "status": "ok",
        "downloaded": 3,
        "errors": 0,
        "downloaded_bytes": 30,
        "hash_verified": 0,
        "error_samples": [],
        "per_dataset": {},
        "touched_partitions": {"daily_forward": ["20261103"]},
        "state_dbs": [],
        "elapsed_sec": 0.5,
        "diff": {
            "root": str(env_root),
            "repo_id": "r/r",
            "revision": "master",
            "total_remote_files": 10,
            "total_pending_files": 3,
            "total_pending_bytes": 30,
            "datasets": {},
        },
    }
    base.update(over)
    return base


def test_run_skips_downstream_when_up_to_date(env_root, monkeypatch):
    calls = []
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(env_root, status="up_to_date", downloaded=0),
    )
    monkeypatch.setattr(mds, "_phase_pg_refresh", lambda days: calls.append("pg") or {})
    monkeypatch.setattr(mds, "_phase_publish", lambda: calls.append("pub") or {})

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "up_to_date"
    assert calls == []
    assert "phases" not in summary
    assert mds._exit_code(summary) == 0


def test_run_guard_maps_to_exit_2(env_root, monkeypatch):
    def _raise(*a, **k):
        raise ms.PendingTooLargeError(74936, 78 * 1024**3, 10 * 1024**3)

    monkeypatch.setattr(ms, "sync_incremental_from_modelscope", _raise)

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "guard"
    assert summary["guard"]["pending_files"] == 74936
    assert mds._exit_code(summary) == 2


def test_run_downstream_errors_isolated(env_root, monkeypatch):
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(env_root),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (True, "db:5432"))
    events = []
    monkeypatch.setattr(
        mds, "_record_event", lambda s, d: events.append((s["status"], d))
    )

    def _boom_pg(days):
        raise RuntimeError("pg down")

    def _boom_pub():
        raise RuntimeError("publish down")

    monkeypatch.setattr(mds, "_phase_pg_refresh", _boom_pg)
    monkeypatch.setattr(mds, "_phase_publish", _boom_pub)
    monkeypatch.setattr(mds, "_phase_backfill_l1", lambda root: {"patched": 1})

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "partial"
    assert summary["phases"]["pg_fill"]["status"] == "error"
    assert "pg down" in summary["phases"]["pg_fill"]["reason"]
    assert summary["phases"]["publish_baseline"]["status"] == "error"
    assert summary["phases"]["l1_ohlcv_backfill"]["status"] == "ok"
    assert summary["phases"]["qlib_cache"]["status"] == "skipped"
    assert mds._exit_code(summary) == 1
    assert events == [("partial", 3)]


def test_run_db_unreachable_skips_pg_phases(env_root, monkeypatch):
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(env_root),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (False, "db:5432"))
    monkeypatch.setattr(mds, "_phase_backfill_l1", lambda root: {"patched": 1})
    called = []
    monkeypatch.setattr(
        mds, "_phase_pg_refresh", lambda days: called.append("pg") or {}
    )

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "ok"
    assert summary["phases"]["pg_fill"]["status"] == "skipped"
    assert "不可达" in summary["phases"]["pg_fill"]["reason"]
    assert summary["phases"]["publish_baseline"]["status"] == "skipped"
    assert called == []


def test_run_missing_dependency_phase_skipped(env_root, monkeypatch):
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(env_root),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (False, "db:5432"))

    def _no_dep(root):
        raise ModuleNotFoundError("No module named 'duckdb'", name="duckdb")

    monkeypatch.setattr(mds, "_phase_backfill_l1", _no_dep)

    summary = mds.run_modelscope_daily_sync()

    assert summary["phases"]["l1_ohlcv_backfill"]["status"] == "skipped"
    assert "duckdb" in summary["phases"]["l1_ohlcv_backfill"]["reason"]
    assert summary["status"] == "ok"


def test_run_plain_importerror_phase_skipped(env_root, monkeypatch):
    """pandas「无可用引擎」类普通 ImportError（非 ModuleNotFoundError）也应跳过。"""
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(env_root),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (False, "db:5432"))

    def _no_engine(root):
        raise ImportError(
            "Unable to find a usable engine; tried using: 'pyarrow', 'fastparquet'."
        )

    monkeypatch.setattr(mds, "_phase_backfill_l1", _no_engine)

    summary = mds.run_modelscope_daily_sync()

    assert summary["phases"]["l1_ohlcv_backfill"]["status"] == "skipped"
    assert "pyarrow" in summary["phases"]["l1_ohlcv_backfill"]["reason"]
    assert summary["status"] == "ok"  # 原生依赖缺失不拉低退出码


def test_db_reachable_rejects_fake_ip(monkeypatch):
    """代理 fake-IP DNS 把 db 解析进 198.18.0.0/15：判不可达且不发起 TCP。"""
    import socket as _socket

    monkeypatch.setenv("DB_HOST", "db")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        _socket,
        "getaddrinfo",
        lambda host, port, **kw: [
            (_socket.AF_INET, _socket.SOCK_STREAM, 6, "", ("198.18.1.91", port))
        ],
    )
    connected = []

    def _conn(addr, timeout=None):
        connected.append(addr)
        raise AssertionError("fake-ip 不应发起 TCP 连接")

    monkeypatch.setattr(_socket, "create_connection", _conn)

    ok, target = mds._db_reachable()

    assert ok is False
    assert "fake-ip" in target
    assert connected == []


def test_db_reachable_real_ip(monkeypatch):
    import socket as _socket

    monkeypatch.setenv("DB_HOST", "127.0.0.1")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        _socket,
        "getaddrinfo",
        lambda host, port, **kw: [
            (_socket.AF_INET, _socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))
        ],
    )

    class _FakeSock:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        _socket, "create_connection", lambda addr, timeout=None: _FakeSock()
    )

    ok, target = mds._db_reachable()

    assert ok is True
    assert target == "127.0.0.1:5432"


def test_local_status_offline(env_root, monkeypatch):
    f1 = _rf(f"{DF}/dt=20260102/data.parquet", b"zz")
    _write_local(env_root, f1, b"zz")
    _seed_state(env_root, [f1])

    def _no_net(**kw):
        raise AssertionError("local_status 不应联网")

    monkeypatch.setattr(ms, "list_remote_files", _no_net)

    st = mds.local_status()

    assert st["root"] == str(env_root)
    assert st["datasets"]["daily_forward"]["latest_local_dt"] == "20260102"
    sdk = [db for db in st["state_dbs"] if db["path"].endswith("quantdb_sync.sqlite")]
    assert sdk and sdk[0]["exists"] and sdk[0]["object_rows"] == 1
    assert st["disk_free_gb"] is not None


def test_main_check_mode_prints_report(env_root, monkeypatch, capsys):
    keep, changed, new = _seed_aligned(env_root, monkeypatch)

    rc = mds.main(["--check", "--datasets", "daily_forward"])

    assert rc == 0
    import json as _json

    out = capsys.readouterr().out
    # 日志在前、JSON 在后：取第一个 { 起的块
    payload = _json.loads(out[out.index("{") :])
    assert payload["status"] == "checked"
    assert payload["datasets"]["daily_forward"]["pending_files"] == 2
    assert _FakeClient.stream_calls == 0
    assert not (env_root / new.path).exists()


# ---------------------------------------------------------------------------
# 回归测试（提交前评审发现：MAJOR-1/MAJOR-2/minor-1/3/nit-3）
# ---------------------------------------------------------------------------
def test_failed_download_only_cleans_own_part(env_root, monkeypatch):
    """.part 带 pid 隔离：失败清理不得误删/覆写其它进程的同目标 part（MAJOR-2）。"""
    f1 = _rf(f"{DF}/dt=20260109/data.parquet", b"abc")
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f1])
    _FakeClient.payloads = {f1.path: b"WRONG"}  # sha 不符 → 重试后失败
    monkeypatch.setattr(ms.time, "sleep", lambda _s: None)
    d = env_root / DF / "dt=20260109"
    d.mkdir(parents=True, exist_ok=True)
    # 旧命名的确定性 part（视为另一进程正在写入的文件）
    sentinel = d / "data.parquet.part"
    sentinel.write_bytes(b"other-process-inflight")

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["status"] == "partial"
    assert _FakeClient.stream_calls == 3  # DOWNLOAD_RETRIES
    assert sentinel.read_bytes() == b"other-process-inflight"  # 未被截断/误删
    assert not (d / "data.parquet").exists()  # 损坏内容不落目标路径
    assert not list(d.glob("*.part-*"))  # 本进程 pid-part 已清理


def test_empty_remote_sha_preserves_registration(env_root, monkeypatch):
    """远端 sha 为空的 blob 下载后不得把既有好登记降级为空（minor-3）。"""
    content = b"good-content"
    good = _rf(f"{DF}/dt=20260101/data.parquet", content)
    _write_local(env_root, good, content)
    _seed_state(env_root, [good])
    nosha = ms.RemoteFile(
        path=good.path,
        size=len(b"new-content"),
        sha256="",
        dataset="daily_forward",
        layout="v2_daily_partition",
    )
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [nosha])
    _FakeClient.payloads = {good.path: b"new-content"}

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["downloaded"] == 1
    assert (env_root / good.path).read_bytes() == b"new-content"
    for db in _state_db_paths(env_root):
        assert _state_rows(db)[good.path] == good.sha256  # 未被空 sha 覆盖


def test_hash_verify_partial_budget(env_root, monkeypatch):
    """哈希直验预算不足时部分直验（而非整批放弃后全量重下）（minor-1）。"""
    files = []
    for i in range(5):
        content = f"c{i}".encode() * 10
        f = _rf(f"{DF}/dt=2026020{i}/data.parquet", content)
        _write_local(env_root, f, content)
        files.append((f, content))
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: [f for f, _ in files])
    _FakeClient.payloads = {f.path: c for f, c in files}
    monkeypatch.setattr(ms, "HASH_VERIFY_MAX_FILES", 2)

    summary = ms.sync_incremental_from_modelscope(["daily_forward"])

    assert summary["hash_verified"] == 2  # 预算内直验免下载
    assert summary["downloaded"] == 3  # 超预算部分照常下载（幂等覆盖）
    assert summary["status"] == "ok"
    assert summary["state_dbs_written"] == [str(env_root / "quantdb_sync.sqlite")]


def test_hash_verify_respects_cancel(env_root, monkeypatch):
    """直验与下载循环均响应取消，取消时零下载（nit-3）。"""
    files = []
    for i in range(17):
        content = f"x{i:02d}".encode()
        f = _rf(f"{DF}/dt=202603{i:02d}/data.parquet", content)
        _write_local(env_root, f, content)
        files.append(f)
    monkeypatch.setattr(ms, "list_remote_files", lambda **kw: files)
    polls = {"n": 0}

    def _cancel():
        polls["n"] += 1
        return polls["n"] > 1

    summary = ms.sync_incremental_from_modelscope(
        ["daily_forward"], should_cancel=_cancel
    )

    assert summary["status"] == "cancelled"
    assert _FakeClient.stream_calls == 0
    assert summary["hash_verified"] <= 16


def test_run_all_failed_reports_partial_not_ok(env_root, monkeypatch):
    """MAJOR-1：全部下载失败必须 partial/exit 1 并落事件，不得伪装 ok。"""
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(
            env_root,
            status="partial",
            downloaded=0,
            errors=2,
            error_samples=["a: boom", "b: boom"],
        ),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (True, "db:5432"))
    events = []
    monkeypatch.setattr(
        mds, "_record_event", lambda s, d: events.append((s["status"], d))
    )
    called = []
    monkeypatch.setattr(
        mds, "_phase_backfill_l1", lambda root: called.append("l1") or {}
    )

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "partial"
    assert "phases" not in summary  # 无新数据不跑下游
    assert called == []
    assert mds._exit_code(summary) == 1
    assert "哈希" not in summary.get("note", "")  # 不得谎称哈希验证一致
    assert events == [("partial", 0)]  # 全失败日必须留痕


def test_run_all_cancelled_reports_cancelled(env_root, monkeypatch):
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(
            env_root, status="cancelled", downloaded=0, errors=0, cancelled=True
        ),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (False, "db:5432"))
    called = []
    monkeypatch.setattr(
        mds, "_phase_backfill_l1", lambda root: called.append("l1") or {}
    )

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "cancelled"
    assert called == []
    assert mds._exit_code(summary) == 1


def test_run_hash_only_early_exit_ok(env_root, monkeypatch):
    """哈希直验免下载（无失败）→ ok/exit 0，note 如实说明。"""
    monkeypatch.setattr(
        ms,
        "sync_incremental_from_modelscope",
        lambda *a, **k: _fake_sync_summary(
            env_root, downloaded=0, hash_verified=5, touched_partitions={}
        ),
    )
    monkeypatch.setattr(mds, "_db_reachable", lambda timeout=3.0: (False, "db:5432"))
    called = []
    monkeypatch.setattr(
        mds, "_phase_backfill_l1", lambda root: called.append("l1") or {}
    )

    summary = mds.run_modelscope_daily_sync()

    assert summary["status"] == "ok"
    assert "哈希" in summary["note"]
    assert "phases" not in summary
    assert called == []
    assert mds._exit_code(summary) == 0


def test_main_empty_datasets_usage_error(capsys):
    """--datasets 解析后为空 → 用法错误 exit 2，而非静默 no-op（minor-4）。"""
    rc = mds.main(["--datasets", " , "])

    assert rc == 2
    out = capsys.readouterr().out
    assert '"fatal"' in out
