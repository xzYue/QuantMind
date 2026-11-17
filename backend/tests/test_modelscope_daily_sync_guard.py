"""`backend/scripts/modelscope_daily_sync_guard.sh` 的行为测试。

守卫是纯 shell 脚本：用 stub `docker` 驱动，注入日期/时刻，验证
「当日去重、执行窗口、目录锁（含失效回收）、失败计数上限、护栏当日暂停、
成功标记原子写」等语义。所有状态都写在 pytest 的临时目录里，不触碰真实环境。
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

GUARD = (
    Path(__file__).resolve().parents[1] / "scripts" / "modelscope_daily_sync_guard.sh"
)

DOCKER_STUB = """#!/bin/sh
printf "%s\\n" "$*" >> "$STUB_DOCKER_CALLS"
if [ "$1" = "ps" ]; then
  if [ "${STUB_RUNNING:-1}" = "1" ]; then printf "abc123\\n"; fi
  exit 0
fi
if [ "$1" = "exec" ]; then
  if [ -n "${STUB_EXEC_SLEEP:-}" ]; then sleep "$STUB_EXEC_SLEEP"; fi
  if [ -n "${STUB_EXEC_STDOUT:-}" ]; then printf "%s\\n" "$STUB_EXEC_STDOUT"; fi
  if [ -n "${STUB_EXEC_EXTRA:-}" ]; then printf "%s\\n" "$STUB_EXEC_EXTRA"; fi
  exit "${STUB_EXEC_RC:-0}"
fi
exit 0
"""

# 与 modelscope_daily_sync.py 的真实输出同构（indent=2、顶层 status、sync 段）
UP_TO_DATE_JSON = """{
  "status": "up_to_date",
  "repo_id": "qusong0627/LightGBM_Alpha300",
  "sync": {
    "downloaded": 7,
    "downloaded_bytes": 4096,
    "elapsed_sec": 12.5
  },
  "finished": "2026-11-16T07:00:21.123456"
}"""

GUARD_JSON = """{
  "status": "guard",
  "error": "待下载 21988 个文件 / 68.0 GiB 超过护栏 30.0 GiB：状态库可能丢失或损坏",
  "guard": {
    "pending_files": 21988,
    "pending_bytes": 73014444032,
    "limit_bytes": 32212254720
  },
  "finished": "2026-11-16T07:00:21.123456"
}"""


class GuardRun:
    def __init__(self, proc: subprocess.CompletedProcess[str], tmp_path: Path) -> None:
        self.proc = proc
        self.rc = proc.returncode
        self.stdout = proc.stdout
        self.stderr = proc.stderr
        self.state_dir = tmp_path / "state"
        self.state_file = self.state_dir / "modelscope_daily_sync.last_success"
        self.attempts_file = self.state_dir / "modelscope_daily_sync.attempts"
        self.log = tmp_path / "sync.log"
        self.calls = tmp_path / "docker.calls"

    @property
    def state_text(self) -> str:
        return self.state_file.read_text() if self.state_file.exists() else ""

    @property
    def attempts_text(self) -> str:
        return self.attempts_file.read_text() if self.attempts_file.exists() else ""

    @property
    def log_text(self) -> str:
        return self.log.read_text() if self.log.exists() else ""

    @property
    def docker_calls(self) -> list[str]:
        if not self.calls.exists():
            return []
        return [line for line in self.calls.read_text().splitlines() if line]

    @property
    def exec_calls(self) -> list[str]:
        return [c for c in self.docker_calls if c.startswith("exec ")]


def guard_env(tmp_path: Path, **env: object) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(DOCKER_STUB, encoding="utf-8")
    docker.chmod(0o755)

    # 显式白名单：不继承 os.environ，避免开发者环境里的 QM_MODELSCOPE_* 污染用例
    base_env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": str(tmp_path / "home"),
        "TZ": "Asia/Shanghai",
        "STUB_DOCKER_CALLS": str(tmp_path / "docker.calls"),
        "STUB_EXEC_STDOUT": UP_TO_DATE_JSON,
        "QM_MODELSCOPE_GUARD_STATE_DIR": str(tmp_path / "state"),
        "QM_MODELSCOPE_GUARD_LOG": str(tmp_path / "sync.log"),
        "QM_MODELSCOPE_GUARD_TODAY": "2026-11-16",
    }
    base_env.update({k: str(v) for k, v in env.items()})
    return base_env


def run_guard(tmp_path: Path, cwd: Path | None = None, **env: object) -> GuardRun:
    proc = subprocess.run(
        ["bash", str(GUARD)],
        env=guard_env(tmp_path, **env),
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return GuardRun(proc, tmp_path)


def write_state(tmp_path: Path, text: str) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "modelscope_daily_sync.last_success").write_text(
        text, encoding="utf-8"
    )


def write_attempts(tmp_path: Path, text: str) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "modelscope_daily_sync.attempts").write_text(text, encoding="utf-8")


def test_guard_runs_when_window_reached(tmp_path: Path) -> None:
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0, res.stdout + res.stderr
    assert res.state_text.startswith("date=2026-11-16 "), res.state_text
    assert "status=up_to_date" in res.state_text
    assert "files=7" in res.state_text
    assert "bytes=4096" in res.state_text
    assert "elapsed=12.5" in res.state_text
    assert "exit=0" in res.state_text
    assert "同步成功" in res.log_text


def test_guard_passes_expected_sync_args(tmp_path: Path) -> None:
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0701")
    assert len(res.exec_calls) == 1, res.docker_calls
    call = res.exec_calls[0]
    assert call.startswith(
        "exec quantmind python /app/backend/scripts/modelscope_daily_sync.py"
    )
    assert "--skip-datasets factor_dictionary,l1_l2_factors" in call
    assert "--max-pending-gb 30" in call


def test_guard_skips_before_window(tmp_path: Path) -> None:
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0630")
    assert res.rc == 0
    assert res.docker_calls == []
    assert "未到当日执行窗口" in res.log_text
    assert not res.state_file.exists()


def test_guard_invalid_not_before_falls_back_to_0700(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0630", QM_MODELSCOPE_GUARD_NOT_BEFORE="7:00"
    )
    assert res.rc == 0
    assert res.docker_calls == []
    assert "非法 NOT_BEFORE" in res.log_text
    assert "未到当日执行窗口" in res.log_text


def test_guard_force_ignores_window(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0630", QM_MODELSCOPE_GUARD_FORCE="1"
    )
    assert res.rc == 0
    assert res.state_text.startswith("date=2026-11-16 ")


def test_guard_skips_when_already_done_today(tmp_path: Path) -> None:
    write_state(
        tmp_path,
        "date=2026-11-16 time=07:00:12 status=ok files=3 bytes=10 elapsed=1.0 exit=0\n",
    )
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="1100")
    assert res.rc == 0
    assert res.docker_calls == []
    assert "今日（2026-11-16）已完成" in res.log_text


def test_guard_force_reruns_despite_today_marker(tmp_path: Path) -> None:
    # FORCE=1 是手动强制补跑：当日已有标记、且失败次数已满，也必须真的跑一次
    write_state(
        tmp_path,
        "date=2026-11-16 time=07:00:12 status=ok files=3 bytes=10 elapsed=1.0 exit=0\n",
    )
    write_attempts(tmp_path, "date=2026-11-16 attempts=6 last=12:00:00 rc=1\n")
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0600", QM_MODELSCOPE_GUARD_FORCE="1"
    )
    assert res.rc == 0
    assert len(res.exec_calls) == 1, res.docker_calls
    assert res.state_text.startswith("date=2026-11-16 ")


def test_guard_runs_again_next_day(tmp_path: Path) -> None:
    write_state(
        tmp_path,
        "date=2026-11-15 time=07:00:12 status=ok files=3 bytes=10 elapsed=1.0 exit=0\n",
    )
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert res.state_text.startswith("date=2026-11-16 ")


def test_guard_skips_when_container_not_running(tmp_path: Path) -> None:
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", STUB_RUNNING="0")
    assert res.rc == 75
    assert not res.state_file.exists()
    assert "未运行" in res.log_text


def test_guard_failure_does_not_mark_day_done(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="1",
        STUB_EXEC_STDOUT="",
        STUB_EXEC_EXTRA="partial: 3 个文件下载失败",
    )
    assert res.rc == 1
    assert not res.state_file.exists()
    assert "同步失败" in res.log_text
    assert res.attempts_text.startswith("date=2026-11-16 attempts=1"), res.attempts_text


def test_guard_success_clears_attempts(tmp_path: Path) -> None:
    write_attempts(tmp_path, "date=2026-11-16 attempts=2 last=08:00:00 rc=1\n")
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0900")
    assert res.rc == 0
    assert res.state_text.startswith("date=2026-11-16 ")
    assert not res.attempts_file.exists()


def test_guard_stops_after_max_attempts(tmp_path: Path) -> None:
    write_attempts(tmp_path, "date=2026-11-16 attempts=6 last=12:00:00 rc=1\n")
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="1300")
    assert res.rc == 0
    assert res.docker_calls == []
    assert "今日已失败 6 次" in res.log_text


def test_guard_attempts_from_previous_day_are_ignored(tmp_path: Path) -> None:
    write_attempts(tmp_path, "date=2026-11-15 attempts=6 last=12:00:00 rc=1\n")
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert res.state_text.startswith("date=2026-11-16 ")


def test_guard_guardrail_blocks_rest_of_day(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT=GUARD_JSON,
    )
    assert res.rc == 2
    assert "status=pending_too_large" in res.state_text
    assert "bytes=73014444032" in res.state_text
    assert "护栏触发" in res.log_text

    # 同一天第二次调用不应再跑（先清空第一次留下的 docker 记录）
    res.calls.unlink()
    again = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0900",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT=GUARD_JSON,
    )
    assert again.rc == 0
    assert again.exec_calls == []


def test_guard_detects_guardrail_from_status_only(tmp_path: Path) -> None:
    # 只靠结构化字段 "status": "guard"（没有任何中文文案）也必须能识别
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT=('{\n  "status": "guard",\n  "error": "pending too large"\n}'),
    )
    assert res.rc == 2
    assert "status=pending_too_large" in res.state_text


def test_guard_fatal_without_guardrail_marker_retries(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT='{\n  "status": "fatal",\n  "error": "枚举失败"\n}',
    )
    assert res.rc == 2
    assert not res.state_file.exists(), res.state_text
    assert "同步失败" in res.log_text


def test_guard_lock_held_by_live_process(tmp_path: Path) -> None:
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    lock.mkdir(parents=True)
    (lock / "pid").write_text(str(os.getpid()), encoding="utf-8")
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert res.docker_calls == []
    assert "WARN 锁被 pid=" in res.log_text


def test_guard_reclaims_stale_lock(tmp_path: Path) -> None:
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    lock.mkdir(parents=True)
    (lock / "pid").write_text("999999", encoding="utf-8")
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert res.state_text.startswith("date=2026-11-16 ")
    assert "回收失效锁" in res.log_text
    assert not lock.exists()


def test_guard_reclaims_lock_when_pid_reused(tmp_path: Path) -> None:
    # 锁登记的 pid 仍存活，但那个进程启动于锁创建之后 → 是 pid 复用而非持锁者，必须回收
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    lock.mkdir(parents=True)
    (lock / "pid").write_text(str(os.getpid()), encoding="utf-8")
    old = 1_600_000_000
    os.utime(lock, (old, old))
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert "pid 复用" in res.log_text, res.log_text
    assert "回收失效锁" in res.log_text
    assert res.state_text.startswith("date=2026-11-16 ")


def test_guard_custom_sync_args(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        QM_MODELSCOPE_GUARD_SYNC_ARGS="--datasets l1_factors --allow-large",
    )
    assert res.exec_calls[0].endswith("--datasets l1_factors --allow-large")


# ---------------------------------------------------------------- 配置健壮性

# 生产护栏路径在 modelscope_daily_sync.py 里提前 return，报告里**没有** skipped_by_config；
# 守卫仍取「最后一处 pending_bytes」（guard 块最靠后），避免被 sync 段或未来新增段抢走。
GUARD_TOTAL_JSON = """{
  "status": "guard",
  "error": "待下载 100 个文件 / 68.0 GiB 超过护栏 30.0 GiB",
  "guard": {
    "pending_files": 100,
    "pending_bytes": 73014444032,
    "limit_bytes": 32212254720
  }
}"""


def test_guard_rejects_non_numeric_max_attempts(tmp_path: Path) -> None:
    # 非数字会让 (( )) 在 set -u 下直接终止脚本（表现为静默不同步），必须回退默认值继续跑
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", QM_MODELSCOPE_MAX_ATTEMPTS="abc"
    )
    assert res.rc == 0, res.stdout + res.stderr
    assert "WARN 非法 MAX_ATTEMPTS=abc" in res.log_text
    assert res.state_text.startswith("date=2026-11-16 "), res.state_text


def test_guard_rejects_non_numeric_lock_max_age(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", QM_MODELSCOPE_LOCK_MAX_AGE="abc"
    )
    assert res.rc == 0, res.stdout + res.stderr
    assert "WARN 非法 LOCK_MAX_AGE=abc" in res.log_text
    assert len(res.exec_calls) == 1, res.docker_calls


def test_guard_requires_home_without_state_dir(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(DOCKER_STUB, encoding="utf-8")
    docker.chmod(0o755)
    res = subprocess.run(
        ["bash", str(GUARD)],
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "STUB_DOCKER_CALLS": str(tmp_path / "calls"),
        },
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert res.returncode == 75, res.stdout + res.stderr
    assert "HOME 未设置" in res.stderr
    assert "unbound variable" not in res.stderr


def test_guard_accepts_compact_json_status(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_STDOUT='{"status":"ok","sync":{"downloaded":5,"downloaded_bytes":10}}',
    )
    assert "status=ok" in res.state_text, res.state_text
    assert "files=5" in res.state_text
    assert "bytes=10" in res.state_text


# ---------------------------------------------------------------- 锁的边界


def test_guard_treats_fresh_lock_without_pid_as_held(tmp_path: Path) -> None:
    # mkdir 成功但 pid 文件还没写下时，另一个实例不能把锁抢走
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    lock.mkdir(parents=True)
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert res.docker_calls == []
    assert "pid 文件缺失" in res.log_text


def test_guard_reclaims_old_lock_without_pid(tmp_path: Path) -> None:
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    lock.mkdir(parents=True)
    old = 1_600_000_000
    os.utime(lock, (old, old))
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.rc == 0
    assert res.state_text.startswith("date=2026-11-16 ")


# ---------------------------------------------------------------- 参数与护栏


def test_guard_does_not_glob_sync_args(tmp_path: Path) -> None:
    (tmp_path / "aaa.txt").write_text("x", encoding="utf-8")
    res = run_guard(
        tmp_path,
        cwd=tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        QM_MODELSCOPE_GUARD_SYNC_ARGS="--datasets *",
    )
    assert res.exec_calls[0].endswith("--datasets *"), res.exec_calls[0]


def test_guard_reports_guardrail_total_not_per_dataset(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT=GUARD_TOTAL_JSON,
    )
    assert res.rc == 2
    assert "bytes=73014444032" in res.state_text, res.state_text
    assert "elapsed=?" in res.state_text


# ---------------------------------------------------------------- 锁：长跑与归属


def _spawn_sleeper(seconds: int = 60) -> subprocess.Popen[bytes]:
    return subprocess.Popen(["sleep", str(seconds)])  # noqa: S603, S607


def test_guard_keeps_lock_of_live_long_running_owner(tmp_path: Path) -> None:
    # MAJOR 回归：持锁进程仍存活且启动于锁创建之前（= 真持锁者，可能是 >6h 的长跑），
    # 即便锁龄超过 LOCK_MAX_AGE 也不能回收，否则会与长跑并发
    sleeper = _spawn_sleeper()
    try:
        lock = tmp_path / "state" / "modelscope_daily_sync.lock"
        lock.mkdir(parents=True)
        (lock / "pid").write_text(str(sleeper.pid), encoding="utf-8")
        res = run_guard(
            tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", QM_MODELSCOPE_LOCK_MAX_AGE="1"
        )
        assert res.rc == 0
        assert res.docker_calls == [], res.docker_calls
        assert "WARN 锁被 pid=" in res.log_text, res.log_text
        assert "回收失效锁" not in res.log_text
        assert (lock / "pid").read_text(encoding="utf-8").strip() == str(sleeper.pid)
    finally:
        sleeper.kill()
        sleeper.wait()


def test_guard_warns_when_lock_held_suspiciously_long(tmp_path: Path) -> None:
    sleeper = _spawn_sleeper()
    try:
        lock = tmp_path / "state" / "modelscope_daily_sync.lock"
        lock.mkdir(parents=True)
        (lock / "pid").write_text(str(sleeper.pid), encoding="utf-8")
        # 锁龄 5s（仍早于 STUCK_WARN 判据与 pid 复用容差），触发「疑似卡死」告警
        five_ago = time.time() - 5
        os.utime(lock, (five_ago, five_ago))
        res = run_guard(
            tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", QM_MODELSCOPE_GUARD_STUCK_WARN="1"
        )
        assert res.rc == 0
        assert res.docker_calls == []
        assert "疑似卡死" in res.log_text, res.log_text
    finally:
        sleeper.kill()
        sleeper.wait()


def test_guard_does_not_delete_foreign_lock_on_exit(tmp_path: Path) -> None:
    # MAJOR 回归：锁被回收后由别人重建，先前的持锁进程退出时不得删掉别人的锁
    env = guard_env(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", STUB_EXEC_SLEEP="2")
    proc = subprocess.Popen(  # noqa: S603
        ["bash", str(GUARD)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    deadline = time.time() + 30
    while time.time() < deadline and not (lock / "pid").exists():
        time.sleep(0.05)
    assert (lock / "pid").exists(), "守卫没有拿到锁"
    # 模拟「锁被回收 + 新持有者重建」：把原锁挪走，重建一把属于本测试进程的锁
    lock.rename(tmp_path / "state" / "stolen.lock")
    lock.mkdir()
    (lock / "pid").write_text(str(os.getpid()), encoding="utf-8")
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == 0, out + err
    assert (lock / "pid").exists(), "退出时把别人的锁删了"
    assert (lock / "pid").read_text(encoding="utf-8").strip() == str(os.getpid())


# TERM/INT 都要释放自己的锁并以 128+信号 退出：被 kill 后继续跑完会写成功标记，掩盖真实状态
@pytest.mark.parametrize(
    ("signum", "expected"),
    [(signal.SIGTERM, 143), (signal.SIGINT, 130)],
    ids=["term", "int"],
)
def test_guard_signal_exits_128_plus_and_only_drops_own_lock(
    tmp_path: Path, signum: int, expected: int
) -> None:
    env = guard_env(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", STUB_EXEC_SLEEP="2")
    proc = subprocess.Popen(  # noqa: S603
        ["bash", str(GUARD)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    lock = tmp_path / "state" / "modelscope_daily_sync.lock"
    deadline = time.time() + 30
    while time.time() < deadline and not (lock / "pid").exists():
        time.sleep(0.05)
    assert (lock / "pid").exists(), "守卫没有拿到锁"
    proc.send_signal(signum)
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == expected, f"{proc.returncode}: {out}{err}"
    assert not lock.exists(), "收到信号后应释放自己的锁"
    assert not (tmp_path / "state" / "modelscope_daily_sync.last_success").exists()


# ---------------------------------------------------------------- 日志段与原子写


OLD_SEGMENT = """{
  "status": "ok",
  "sync": {
    "downloaded": 111,
    "downloaded_bytes": 999,
    "elapsed_sec": 88.8
  }
}"""

NEW_SEGMENT = """{
  "status": "partial",
  "sync": {
    "downloaded": 7,
    "downloaded_bytes": 4096,
    "elapsed_sec": 3.0
  }
}"""


def test_guard_parses_only_new_log_segment(tmp_path: Path) -> None:
    (tmp_path / "sync.log").write_text(OLD_SEGMENT + "\n", encoding="utf-8")
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", STUB_EXEC_STDOUT=NEW_SEGMENT
    )
    assert "status=partial" in res.state_text, res.state_text
    assert "files=7" in res.state_text
    assert "bytes=4096" in res.state_text
    assert "elapsed=3.0" in res.state_text


def test_guard_attempts_write_is_atomic(tmp_path: Path) -> None:
    # 失败计数同样是「临时文件 + mv」：直接 `>` 覆盖时若被截断成空文件，当日失败计数归零，
    # 守卫就会一直每小时重试（MAX_ATTEMPTS 失效）。用 inode 变化断言。
    first = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="1",
        STUB_EXEC_STDOUT="",
    )
    assert first.rc == 1, first.stdout + first.stderr
    attempts = tmp_path / "state" / "modelscope_daily_sync.attempts"
    assert "attempts=1" in attempts.read_text(encoding="utf-8")
    inode_before = attempts.stat().st_ino
    second = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0800",
        STUB_EXEC_RC="1",
        STUB_EXEC_STDOUT="",
    )
    assert second.rc == 1, second.stdout + second.stderr
    assert "attempts=2" in attempts.read_text(encoding="utf-8")
    assert attempts.stat().st_ino != inode_before, (
        "失败计数应被 mv 原子替换（inode 变化）"
    )


def test_guard_state_write_is_atomic(tmp_path: Path) -> None:
    # 原子写是「写临时文件 + mv -f」，mv 会换 inode；直接 `>` 覆盖则原地截断、inode 不变。
    # 只断言无临时文件残留挡不住退化成直接覆盖，因此必须比对 inode。
    write_state(
        tmp_path,
        "date=2026-11-15 time=07:00:12 status=ok files=3 bytes=10 elapsed=1.0 exit=0\n",
    )
    marker = tmp_path / "state" / "modelscope_daily_sync.last_success"
    inode_before = marker.stat().st_ino
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    assert res.state_text.startswith("date=2026-11-16 ")
    assert marker.stat().st_ino != inode_before, (
        "标记文件应被 mv 原子替换（inode 变化）"
    )
    assert list((tmp_path / "state").glob("*.tmp.*")) == [], "原子写残留临时文件"


# ---------------------------------------------------------------- docker 契约与路径


def test_guard_checks_container_with_strict_filters(tmp_path: Path) -> None:
    res = run_guard(tmp_path, QM_MODELSCOPE_GUARD_NOW="0700")
    ps_calls = [c for c in res.docker_calls if c.startswith("ps ")]
    assert len(ps_calls) == 1, res.docker_calls
    assert "name=^quantmind$" in ps_calls[0], ps_calls[0]
    assert "status=running" in ps_calls[0], ps_calls[0]


def test_guard_respects_custom_container_name(tmp_path: Path) -> None:
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        QM_MODELSCOPE_CONTAINER="quantmind-alt",
    )
    assert res.exec_calls[0].startswith("exec quantmind-alt "), res.exec_calls[0]
    assert "name=^quantmind-alt$" in res.docker_calls[0], res.docker_calls[0]


def test_guard_exits_75_when_log_unwritable(tmp_path: Path) -> None:
    log_dir = tmp_path / "log-is-a-dir"
    log_dir.mkdir()
    res = run_guard(
        tmp_path, QM_MODELSCOPE_GUARD_NOW="0700", QM_MODELSCOPE_GUARD_LOG=str(log_dir)
    )
    assert res.rc == 75, res.stdout + res.stderr
    assert "日志文件不可写" in res.stderr
    assert res.docker_calls == []


def test_guard_state_dir_with_spaces_and_chinese(tmp_path: Path) -> None:
    state_dir = tmp_path / "状态 目录"
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        QM_MODELSCOPE_GUARD_STATE_DIR=str(state_dir),
    )
    assert res.rc == 0, res.stdout + res.stderr
    marker = state_dir / "modelscope_daily_sync.last_success"
    assert marker.exists() and marker.read_text(encoding="utf-8").startswith("date=")


def test_guard_detects_guardrail_from_message_only(tmp_path: Path) -> None:
    # 护栏判定不能只依赖结构化 status 字段：PendingTooLargeError 抛的是中文 error 文案，
    # 日志里只有这句话时也要按护栏处理（当日暂停），而不是当成普通失败每小时重撞一次。
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT=(
            '{"error": "待下载 100 个文件 / 68.0 GiB 超过护栏 30.0 GiB", '
            '"guard": {"pending_bytes": 73014444032}}'
        ),
    )
    assert res.rc == 2, res.stdout + res.stderr
    assert "status=pending_too_large" in res.state_text, res.state_text
    assert "bytes=73014444032" in res.state_text, res.state_text


def test_guard_detects_compact_json_guardrail(tmp_path: Path) -> None:
    # 紧凑 JSON 且无中文文案：只能靠结构化 status 识别护栏
    res = run_guard(
        tmp_path,
        QM_MODELSCOPE_GUARD_NOW="0700",
        STUB_EXEC_RC="2",
        STUB_EXEC_STDOUT='{"status":"guard","guard":{"pending_bytes":73014444032}}',
    )
    assert res.rc == 2
    assert "status=pending_too_large" in res.state_text, res.state_text
