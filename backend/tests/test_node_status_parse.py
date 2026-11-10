"""NodeStatus 远程采集解析：静态方法误用 cls 会导致采集被吞掉、前端显示未连接。"""

from backend.services.engine.training.node_manager import NodeStatus


def test_parse_gpu_sample_sets_ready():
    sample = """
===SYS===
16
 10:00:00 up 1 day,  1:00,  1 user,  load average: 0.10, 0.20, 0.30
mem:64000 8000
disk:104857600 10485760
rx1:100
tx1:200
===GPU===
12, 1024, 24576, 45, NVIDIA GeForce RTX 4090
===DOCKER===
no-docker
===NET===
0.12
"""
    parsed = NodeStatus._parse(sample, {"id": "autodl-rtx4090", "name": "AutoDL", "host": "x", "online": False})
    assert parsed["online"] is True
    assert parsed["gpus"][0]["name"] == "NVIDIA GeForce RTX 4090"
    assert parsed["readiness"] == "ready"
    assert parsed["readiness_label"] == "已就绪"


def test_collect_all_keeps_stub_when_remote_raises():
    import asyncio

    class Boom(NodeStatus):
        @classmethod
        async def collect_local(cls):
            return {"id": "local", "online": True, "readiness": "ready"}

        @classmethod
        async def collect(cls, node):
            raise NameError("cls is not defined")

    async def _run():
        # 绕过真实 SSH：直接测 gather 异常不会把远程节点丢掉
        from unittest.mock import patch

        with patch(
            "backend.services.engine.training.node_manager.load_training_nodes",
            return_value=[{"id": "autodl-rtx4090", "name": "AutoDL", "host": "h"}],
        ):
            return await Boom.collect_all()

    out = asyncio.run(_run())
    ids = [s["id"] for s in out]
    assert "local" in ids
    assert "autodl-rtx4090" in ids
    remote = next(s for s in out if s["id"] == "autodl-rtx4090")
    assert remote["online"] is False
    assert "cls is not defined" in str(remote.get("error"))


_PARSE_OK_SAMPLE = b"""
===SYS===
4
  10:00:00 up 1 day,  1:00,  1 user,  load average: 0.10, 0.20, 0.30
mem:64000 8000
disk:104857600 10485760
rx1:100
tx1:200
===GPU===
12, 1024, 24576, 45, NVIDIA GeForce RTX 4090
===DOCKER===
no-docker
===NET===
0.12
"""


def test_collect_retries_signal_killed_ssh():
    """SSH 子进程被信号杀死（如 rc=-11）时重试一次，成功则恢复在线。"""
    import asyncio
    from unittest.mock import patch

    calls = {"n": 0}

    async def fake_run(cls, node):
        calls["n"] += 1
        if calls["n"] == 1:
            return -11, b"", b""
        return 0, _PARSE_OK_SAMPLE, b""

    async def _run():
        with patch.object(NodeStatus, "_run_collect_cmd", classmethod(fake_run)):
            return await NodeStatus.collect({"id": "autodl-1", "host": "h"})

    out = asyncio.run(_run())
    assert out["online"] is True
    assert out["readiness"] == "ready"
    assert calls["n"] == 2


def test_collect_signal_kill_twice_reports_offline():
    """两次都被信号杀死则上报离线，且只尝试两次（不死循环）。"""
    import asyncio
    from unittest.mock import patch

    calls = {"n": 0}

    async def fake_run(cls, node):
        calls["n"] += 1
        return -11, b"", b""

    async def _run():
        with patch.object(NodeStatus, "_run_collect_cmd", classmethod(fake_run)):
            return await NodeStatus.collect({"id": "autodl-1", "host": "h"})

    out = asyncio.run(_run())
    assert out["online"] is False
    assert "code=-11" in str(out.get("error"))
    assert calls["n"] == 2


def _probe_with_fake_client(fake_client):
    """跑一次 GPU 容器探针（重置 60s TTL 缓存，避免测试间互相污染）。"""
    import asyncio

    NodeStatus._gpu_probe_cache = None
    try:
        return asyncio.run(
            NodeStatus._probe_gpu_via_docker(fake_client, "quantmind-oss:latest")
        )
    finally:
        NodeStatus._gpu_probe_cache = None


def test_probe_gpu_parses_output_and_removes_container():
    """成功路径：解析 nvidia-smi 输出，且探针容器被删除。"""
    from unittest.mock import MagicMock

    container = MagicMock()
    container.logs.return_value = b"12, 1024, 24576, 45, NVIDIA GeForce RTX 4090\n"
    client = MagicMock()
    client.containers.create.return_value = container

    gpus = _probe_with_fake_client(client)

    assert gpus == [
        {
            "util": 12,
            "mem_used_mb": 1024,
            "mem_total_mb": 24576,
            "temp_c": 45,
            "name": "NVIDIA GeForce RTX 4090",
        }
    ]
    container.start.assert_called_once()
    container.remove.assert_called_once_with(force=True)
    kwargs = client.containers.create.call_args.kwargs
    assert kwargs["name"].startswith("qm-gpu-probe-")
    assert kwargs["labels"] == {"qm.gpu-probe": "1"}


def test_probe_gpu_removes_container_when_start_fails():
    """回归：start 失败（无 NVIDIA CDI）时不能残留 Created 空壳，且不抛异常。

    2026-11 本机 Docker Desktop 上每次打开节点状态页都会漏一个 Created 容器，
    根因是 containers.run(remove=True) 只在容器正常退出后才删。
    """
    from unittest.mock import MagicMock

    container = MagicMock()
    container.start.side_effect = RuntimeError(
        "failed to discover GPU vendor from CDI: no known GPU vendor found"
    )
    client = MagicMock()
    client.containers.create.return_value = container

    assert _probe_with_fake_client(client) == []
    container.remove.assert_called_once_with(force=True)


def test_probe_gpu_swallows_cleanup_failure():
    """清理自身失败时只告警，不影响返回值。"""
    from unittest.mock import MagicMock

    container = MagicMock()
    container.start.side_effect = RuntimeError("no gpu")
    container.remove.side_effect = RuntimeError("already gone")
    client = MagicMock()
    client.containers.create.return_value = container

    assert _probe_with_fake_client(client) == []
