#!/usr/bin/env python3
"""ModelScope QuantDB 每日增量同步 — 免 QuantDB API key 的数据源通道
=====================================================================

背景：本机 data/quantdb 的全量数据即来自魔搭（ModelScope）公开数据集仓库
（默认 qusong0627/LightGBM_Alpha300，与管理台「初始化数据」同一来源）。
本脚本按使用者配置的 cron 运行（示例为每天 07:00）：枚举远端清单 → 与本地
同步状态库（多副本合并）diff → 只下载新增/内容变化的 parquet，然后走与
quantdb_daily_sync 相同的下游修复链路。

数据流:
  1. 增量下载：逐文件 sha256+size 校验、.part 原子替换；每批完成即把登记
     写入全部状态库副本（root/quantdb_sync.sqlite、root/.sync_state/*、
     QUANTDB_STATE_DIR 派生库），quantdb_daily_sync(SDK) 与管理台初始化的
     fast-path 均可命中，原生/容器交叉运行互不误判
  2. 护栏：待下载量异常（默认 >10GiB）时拒绝执行——通常是状态库丢失，
     防止无人值守触发 72.8GiB 全量重拉；小缺口先做本地 sha256 直验，
     内容一致免下载只补登记（自愈）
  3. l1_factors OHLCV 回填（backfill_l1_ohlcv，防训练标签为空）
  4. PG stock_daily_latest 滚动刷新（refresh_stock_daily_latest，
     与每日同步同一口径；DB 不可达自动跳过——本机 PG 只在 compose 内网）
  5. 用户因子基线发布（与管理台初始化 _publish_baseline_after_init 同款）
  6. 可选 Qlib 缓存重建（--with-qlib；本机只读复用 qlib 数据卷，默认关闭）

用法:
  # 预检：只枚举远端差异并输出报告，不下载不写盘（建议首次手动跑一遍）
  python3 backend/scripts/modelscope_daily_sync.py --check

  # 每日增量（推荐容器内执行：下游 PG/基线发布依赖齐全）
  docker exec quantmind python /app/backend/scripts/modelscope_daily_sync.py

  # 原生执行（下载+状态库正常；PG/基线发布因 DB 不可达自动跳过）
  PATH="/opt/homebrew/bin:$PATH" no_proxy='*' NO_PROXY='*' \
      python3 backend/scripts/modelscope_daily_sync.py

  # 仅指定数据集 / 调整护栏 / 确认大批量补齐
  python3 backend/scripts/modelscope_daily_sync.py --datasets daily_forward,l1_l2_factors
  python3 backend/scripts/modelscope_daily_sync.py --max-pending-gb 20
  python3 backend/scripts/modelscope_daily_sync.py --allow-large

  # 只同步 L1 所需数据集：排除 L2 宽表（单次全历史重算可省几十 GB）
  python3 backend/scripts/modelscope_daily_sync.py \
      --skip-datasets factor_dictionary,l1_l2_factors --max-pending-gb 30
  # 被排除的数据集不计入 pending / 不参与护栏 / 不下载，
  # 只在报告里单列为 skipped_by_config（含「本来要下多少」的统计）。
  # 注意：仅影响增量同步，不影响管理台「ModelScope 初始化」（init 仍是全量对齐）；
  # 上游每次发布可能做全历史重算，跳过后剩余量仍可能超过默认 10GiB 护栏，
  # 需要显式调高上限（示例见下）或用 --allow-large 放行。

  # 本地状态（不联网）：各数据集最新分区、状态库副本、磁盘余量
  python3 backend/scripts/modelscope_daily_sync.py --status

crontab 示例（每天 07:00 容器内执行；上游未发布时自动 no-op）:
  0 7 * * * PATH="/opt/homebrew/bin:/usr/local/bin:$PATH" no_proxy='*' NO_PROXY='*' docker exec quantmind python /app/backend/scripts/modelscope_daily_sync.py --skip-datasets factor_dictionary,l1_l2_factors --max-pending-gb 30 >> /tmp/modelscope_daily_sync.log 2>&1

  注：全历史重算的那一周，跳过后剩余量实测约 12.2GiB，超过默认 10GiB 护栏会
  exit 2（一个字节都不下），因此示例显式给 --max-pending-gb 30。

注：遵循仓库约定不内置默认调度（AGENTS.md「市场数据同步不内置默认调度」），
是否挂 cron 由使用者决定。退出码：0=成功/无更新/预检，1=部分失败（下载或
下游阶段出错，含全部下载失败），2=致命错误（远端枚举失败/护栏拦截/参数非法）。

状态库口径：合并读取数据根下全部可发现副本（root/quantdb_sync.sqlite、
root/.sync_state/*、显式设置 QUANTDB_STATE_DIR 时的派生库），下载成功按批
upsert 全部副本、绝不删除既有登记。原生未设 QUANTDB_STATE_DIR 时不会创建
~/.quantdb_state 第三副本；SDK 路径（quantdb_daily_sync）原生 fast-path 只读
它自己的默认库，看不到这里的增量登记——如需互通请在容器内运行或显式导出
QUANTDB_STATE_DIR 指向同一目录。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("modelscope_daily_sync")

DEFAULT_MAX_PENDING_GB = 10.0
DEFAULT_PG_DAYS = 30


# ---------------------------------------------------------------------------
# 下游阶段（与 quantdb_daily_sync.run_daily_sync 同款口径，全部惰性导入）
# ---------------------------------------------------------------------------
def _phase_backfill_l1(root: Path) -> dict:
    """l1_factors 历史分区缺 OHLCV，同步后回填最近窗口（防训练标签为空）。"""
    from backend.scripts.backfill_l1_ohlcv import backfill_l1_ohlcv

    return backfill_l1_ohlcv(root, start=date.today() - timedelta(days=10))


def _phase_pg_refresh(days: int) -> dict:
    """滚动刷新 PG stock_daily_latest（与每日同步 Phase 2 相同入口）。"""
    from backend.scripts.stock_daily_latest_refresh import refresh_stock_daily_latest

    return refresh_stock_daily_latest(days=days)


def _phase_publish() -> dict:
    """用户因子基线发布（与管理台初始化完成后的动作一致；自身永不抛错）。"""
    from backend.shared.user_factor_publish import (
        publish_baseline_after_data_init_sync,
    )

    return publish_baseline_after_data_init_sync(created_by="modelscope-daily-sync")


def _phase_qlib() -> dict:
    """重建 Qlib 二进制缓存（与每日同步 Phase 3 相同入口）。"""
    from backend.scripts.quantdb_daily_sync import update_qlib_cache

    return update_qlib_cache()


def _db_reachable(timeout: float = 3.0) -> tuple[bool, str]:
    """探测 PG 可达性（env 口径与 stock_daily_latest_refresh 一致）。

    本机 PG 刻意不发布宿主端口（只在 compose 内网 db:5432），原生跑时据此
    优雅跳过 PG/基线发布阶段而非报错。另拒绝解析进 198.18.0.0/15 的地址：
    该段是 RFC2544 基准测试保留段（真实 PG 不会在此），代理软件的 fake-IP
    DNS 会把任意域名解析进此段且 TCP 握手假成功，不排除会让原生运行误判
    可达、进入真实连接后以混乱报错收场。
    """
    import ipaddress
    import socket
    from urllib.parse import urlparse

    host = os.getenv("DB_HOST", "db")
    try:
        port = int(os.getenv("DB_PORT", "5432"))
    except ValueError:
        port = 5432
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        try:
            parsed = urlparse(url.replace("+asyncpg", "").replace("+psycopg2", ""))
            host = parsed.hostname or host
            port = parsed.port or port
        except Exception:  # noqa: BLE001 - URL 异常时退回 env 口径
            pass
    target = f"{host}:{port}"
    fake_ip_net = ipaddress.ip_network("198.18.0.0/15")
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError:
        return False, target
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except (ValueError, IndexError):
            continue
        if ip in fake_ip_net:
            return False, f"{target} (fake-ip {ip})"
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, target
    except OSError:
        return False, target


def _run_phase(name: str, fn, *, skip: bool, skip_reason: str) -> dict:
    """执行单个下游阶段：跳过/依赖缺失/失败都不向外抛，统一返回状态字典。"""
    if skip:
        log.info("[phase] %s 跳过: %s", name, skip_reason)
        return {"status": "skipped", "reason": skip_reason}
    log.info("[phase] %s 开始", name)
    try:
        out = fn()
        log.info("[phase] %s 完成: %s", name, str(out.get("result"))[:200])
        return out
    except ImportError as exc:
        # ModuleNotFoundError 及 pandas「无可用 parquet 引擎」一类 ImportError
        # 都属于运行环境依赖缺失：跳过而非报错（原生 cron 保持退出码 0）
        reason = f"依赖不可用: {getattr(exc, 'name', None) or str(exc)[:120]}"
        log.info("[phase] %s 跳过（%s）", name, reason)
        return {"status": "skipped", "reason": reason}
    except Exception as exc:  # noqa: BLE001 - 单阶段失败不拖垮整体
        log.warning("[phase] %s 失败: %s", name, exc)
        return {"status": "error", "reason": str(exc)[:300]}


def _record_event(summary: dict, downloaded: int) -> None:
    """落一条数据同步系统事件（管理台可见；失败只告警）。"""
    try:
        from backend.shared.system_events import record_system_event

        sync = summary.get("sync", {})
        ok = summary.get("status") == "ok"
        record_system_event(
            event_type="data_sync",
            level="info" if ok else "warning",
            source="sync",
            title="ModelScope QuantDB 增量同步完成"
            if ok
            else "ModelScope QuantDB 增量同步部分失败",
            message=(
                f"下载 {downloaded} 个文件 / "
                f"{sync.get('downloaded_bytes', 0) / 1024**2:.1f} MiB，"
                f"哈希验证免下载 {sync.get('hash_verified', 0)}，"
                f"失败 {sync.get('errors', 0)}；涉及分区: "
                + json.dumps(sync.get("touched_partitions", {}), ensure_ascii=False)[
                    :400
                ]
            ),
            meta={
                "market": "quantdb",
                "channel": "modelscope",
                "downloaded": downloaded,
                "errors": sync.get("errors", 0),
                "status": summary.get("status"),
            },
        )
    except Exception as exc:  # noqa: BLE001 - 事件记录非关键路径
        log.warning("记录系统事件失败（忽略）: %s", exc)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def run_modelscope_daily_sync(
    *,
    datasets: list[str] | None = None,
    skip_datasets: list[str] | None = None,
    check: bool = False,
    max_pending_gb: float = DEFAULT_MAX_PENDING_GB,
    allow_large: bool = False,
    hash_verify: bool = True,
    skip_backfill: bool = False,
    skip_pg: bool = False,
    skip_publish: bool = False,
    with_qlib: bool = False,
    pg_days: int | None = None,
    force_downstream: bool = False,
    repo_id: str | None = None,
    revision: str | None = None,
    endpoint: str | None = None,
) -> dict:
    """执行 ModelScope 每日增量同步；返回可 JSON 序列化的 summary。

    check=True 时只输出远端差异报告（不下载不写盘）。
    """
    from backend.services.engine.data_platform.modelscope_dataset_sync import (
        PendingTooLargeError,
        resolve_data_root,
        sync_incremental_from_modelscope,
    )

    summary: dict = {
        "started": datetime.now().isoformat(),
        "mode": "check" if check else "sync",
    }

    def _cb(event: str, **kw) -> None:
        if event == "phase":
            log.info("[phase] %s", kw.get("message") or kw.get("phase"))
        elif event == "dataset_start":
            log.info(
                "[下载] %s: 待下载 %s 个 / %.1f MiB",
                kw.get("dataset"),
                kw.get("pending"),
                (kw.get("bytes") or 0) / 1024**2,
            )
        elif event == "dataset_done":
            log.info(
                "[下载] %s: 完成，下载 %s 失败 %s",
                kw.get("dataset"),
                kw.get("downloaded"),
                kw.get("errors"),
            )
        # enumerate/file 事件太密，不逐条打日志

    try:
        sync = sync_incremental_from_modelscope(
            datasets,
            skip_datasets=skip_datasets,
            dry_run=check,
            max_pending_bytes=int(max_pending_gb * 1024**3),
            allow_large=allow_large,
            hash_verify=hash_verify,
            repo_id=repo_id,
            revision=revision,
            endpoint=endpoint,
            progress_cb=_cb,
        )
    except PendingTooLargeError as exc:
        summary.update(
            status="guard",
            error=str(exc),
            guard={
                "pending_files": exc.pending_files,
                "pending_bytes": exc.pending_bytes,
                "limit_bytes": exc.limit,
            },
            finished=datetime.now().isoformat(),
        )
        # 仅在实际生效（且与 --datasets 求交后非空）时附带跳过信息，避免误报
        _eff_skip = {str(s).strip() for s in (skip_datasets or ()) if str(s).strip()}
        if datasets is not None:
            _eff_skip &= set(datasets)
        if _eff_skip:
            summary["skipped_datasets"] = sorted(_eff_skip)
            summary["hint"] = (
                "被 --skip-datasets 排除的数据集已不计入该 pending；剩余量仍按 "
                "--max-pending-gb 判定，确认后可调高上限或用 --allow-large 放行"
            )
        return summary

    # dry_run 的 summary 本身就是对账报告（字段在顶层）；正式同步嵌在 "diff"
    diff = sync if check else (sync.pop("diff", {}) or {})
    summary["sync"] = {
        "status": sync.get("status"),
        "downloaded": sync.get("downloaded", 0),
        "hash_verified": sync.get("hash_verified", 0),
        "errors": sync.get("errors", 0),
        "downloaded_bytes": sync.get("downloaded_bytes", 0),
        "elapsed_sec": sync.get("elapsed_sec"),
        "touched_partitions": sync.get("touched_partitions", {}),
        "per_dataset": sync.get("per_dataset", {}),
        "error_samples": sync.get("error_samples", []),
        "state_dbs": sync.get("state_dbs", []),
        "state_dbs_written": sync.get("state_dbs_written", []),
        "state_write_failures": sync.get("state_write_failures", []),
        "remote": {
            "repo_id": diff.get("repo_id"),
            "revision": diff.get("revision"),
            "total_remote_files": diff.get("total_remote_files"),
            "total_pending_files": diff.get("total_pending_files"),
            "total_pending_bytes": diff.get("total_pending_bytes"),
        },
    }

    summary["skipped_by_config"] = diff.get("skipped_by_config") or {}

    if check:
        summary["status"] = "checked"
        summary["datasets"] = diff.get("datasets", {})
        summary["root"] = diff.get("root")
        summary["state_dbs"] = diff.get("state_dbs", [])
        summary["state_keys"] = diff.get("state_keys")
        summary["finished"] = datetime.now().isoformat()
        return summary

    if sync.get("status") == "up_to_date" and not force_downstream:
        summary["status"] = "up_to_date"
        log.info("远端无增量，跳过下游阶段（--force-downstream 可强制执行）")
        summary["finished"] = datetime.now().isoformat()
        return summary

    downloaded = sync.get("downloaded", 0)
    sync_failed = sync.get("errors", 0) > 0 or sync.get("status") in (
        "partial",
        "cancelled",
    )
    if downloaded == 0 and not force_downstream:
        # 无新数据落盘 → 不跑下游；但必须区分「全部失败/取消」与「哈希直验免下载」：
        # 前者如实上报 partial/cancelled（退出码 1），否则 cron 会把全失败日当成功
        db_ok, db_target = _db_reachable()
        if sync_failed:
            summary["status"] = (
                "cancelled" if sync.get("status") == "cancelled" else "partial"
            )
            summary["note"] = "增量下载全部失败或取消，未执行下游阶段"
        else:
            summary["status"] = "ok"
            summary["note"] = (
                "增量全部经本地哈希验证一致（仅补登记状态库），跳过下游阶段"
                if sync.get("hash_verified", 0) > 0
                else "无新数据落盘，跳过下游阶段"
            )
        if db_ok and summary["status"] != "ok":
            _record_event(summary, downloaded)
        summary["finished"] = datetime.now().isoformat()
        return summary

    root = Path(diff.get("root") or str(resolve_data_root()))
    db_ok, db_target = _db_reachable()
    log.info("PG 可达性: %s (%s)", "可达" if db_ok else "不可达", db_target)

    # 以报告里的实际同步范围为准；diff 缺 datasets（如测试桩）时退回按参数推断
    _sync_ds = diff.get("datasets") or {}
    if _sync_ds:
        includes_l1 = "l1_factors" in _sync_ds
    else:
        _skip_set = set(skip_datasets or ())
        includes_l1 = (
            datasets is None or "l1_factors" in datasets
        ) and "l1_factors" not in _skip_set
    days = (
        pg_days
        if pg_days is not None
        else int(os.getenv("PG_REFRESH_DAYS", str(DEFAULT_PG_DAYS)))
    )
    db_skip = f"数据库不可达: {db_target}"

    phases: dict = {}
    phases["l1_ohlcv_backfill"] = _run_phase(
        "l1_ohlcv_backfill",
        lambda: {"status": "ok", "result": _phase_backfill_l1(root)},
        skip=skip_backfill or not includes_l1,
        skip_reason="--skip-backfill" if skip_backfill else "l1_factors 不在同步范围",
    )
    phases["pg_fill"] = _run_phase(
        "pg_fill",
        lambda: {"status": "ok", "result": _phase_pg_refresh(days)},
        skip=skip_pg or not db_ok,
        skip_reason="--skip-pg" if skip_pg else db_skip,
    )
    phases["publish_baseline"] = _run_phase(
        "publish_baseline",
        lambda: {"status": "ok", "result": _phase_publish()},
        skip=skip_publish or not db_ok,
        skip_reason="--skip-publish" if skip_publish else db_skip,
    )
    phases["qlib_cache"] = _run_phase(
        "qlib_cache",
        lambda: {"status": "ok", "result": _phase_qlib()},
        skip=not with_qlib,
        skip_reason="默认关闭（本机只读复用 qlib 数据卷；--with-qlib 开启）",
    )
    summary["phases"] = phases

    phase_failed = any(p.get("status") == "error" for p in phases.values())
    summary["status"] = "partial" if (sync_failed or phase_failed) else "ok"

    if db_ok and (downloaded > 0 or summary["status"] != "ok"):
        _record_event(summary, downloaded)

    summary["finished"] = datetime.now().isoformat()
    return summary


def local_status() -> dict:
    """离线概况：各数据集最新本地分区、状态库副本、磁盘余量（不联网）。"""
    import sqlite3

    from backend.services.engine.data_platform.modelscope_dataset_sync import (
        _latest_local_dt,
        discover_state_dbs,
        resolve_data_root,
    )
    from backend.shared.quantdb_datasets import DATASETS

    root = resolve_data_root()
    dbs = []
    for p in discover_state_dbs(root):
        info: dict = {"path": str(p), "exists": p.exists()}
        if p.exists():
            st = p.stat()
            info["bytes"] = st.st_size
            info["modified"] = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.localtime(st.st_mtime)
            )
            try:
                conn = sqlite3.connect(str(p), timeout=5)
                try:
                    info["object_rows"] = conn.execute(
                        "SELECT COUNT(*) FROM objects"
                    ).fetchone()[0]
                finally:
                    conn.close()
            except Exception:  # noqa: BLE001 - 损坏副本只报 None
                info["object_rows"] = None
        dbs.append(info)

    datasets = {}
    for spec in DATASETS:
        if not (root / spec.rel_dir).is_dir():
            continue
        datasets[spec.dataset] = {
            "name": spec.name,
            "layout": spec.layout,
            "latest_local_dt": _latest_local_dt(root, spec.rel_dir),
        }

    disk = shutil.disk_usage(str(root)) if root.exists() else None
    return {
        "root": str(root),
        "datasets": datasets,
        "state_dbs": dbs,
        "disk_free_gb": round(disk.free / 1024**3, 1) if disk else None,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _exit_code(summary: dict) -> int:
    status = summary.get("status")
    if status in ("guard", "fatal"):
        return 2
    if status in ("partial", "cancelled"):
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="ModelScope QuantDB 每日增量同步（免 API key）",
    )
    parser.add_argument(
        "--check", action="store_true", help="只枚举远端差异并输出报告，不下载不写盘"
    )
    parser.add_argument(
        "--datasets", type=str, default=None, help="逗号分隔数据集名（默认=仓库内全部）"
    )
    parser.add_argument(
        "--skip-datasets",
        type=str,
        default=None,
        help=(
            "逗号分隔、要从本次同步中排除的数据集名（如 factor_dictionary,l1_l2_factors）；"
            "被排除项不计入 pending、不参与护栏、不下载，只单列在 skipped_by_config"
        ),
    )
    parser.add_argument("--repo", dest="repo_id", default=None, help="覆盖魔搭仓库 id")
    parser.add_argument("--revision", default=None, help="覆盖魔搭 revision")
    parser.add_argument("--endpoint", default=None, help="覆盖魔搭 endpoint")
    parser.add_argument(
        "--max-pending-gb",
        type=float,
        default=DEFAULT_MAX_PENDING_GB,
        help=f"护栏：待下载超过该体积拒绝执行（默认 {DEFAULT_MAX_PENDING_GB}GiB）",
    )
    parser.add_argument(
        "--allow-large", action="store_true", help="放行超过护栏的大批量待下载"
    )
    parser.add_argument(
        "--no-hash-verify",
        action="store_true",
        help="关闭本地 sha256 直验（默认开启：小缺口先哈希比对，一致免下载只补登记）",
    )
    parser.add_argument(
        "--skip-backfill", action="store_true", help="跳过 l1_factors OHLCV 回填"
    )
    parser.add_argument(
        "--skip-pg", action="store_true", help="跳过 PG stock_daily_latest 刷新"
    )
    parser.add_argument(
        "--skip-publish", action="store_true", help="跳过用户因子基线发布"
    )
    parser.add_argument(
        "--with-qlib", action="store_true", help="同步后重建 Qlib 缓存（默认关闭）"
    )
    parser.add_argument(
        "--pg-days",
        type=int,
        default=None,
        help=f"PG 滚动刷新天数（默认 env PG_REFRESH_DAYS 或 {DEFAULT_PG_DAYS}）",
    )
    parser.add_argument(
        "--force-downstream",
        action="store_true",
        help="即使远端无增量也执行下游阶段",
    )
    parser.add_argument(
        "--status", action="store_true", help="离线查看本地数据/状态库概况（不联网）"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.status:
        print(json.dumps(local_status(), ensure_ascii=False, indent=2, default=str))
        return 0

    datasets: list[str] | None = None
    if args.datasets is not None:
        datasets = [s.strip() for s in args.datasets.split(",") if s.strip()]
        if not datasets:
            log.error(
                "--datasets 为空：请提供逗号分隔的数据集名（如 daily_forward,l1_factors）"
            )
            print(
                json.dumps(
                    {"status": "fatal", "error": "--datasets 为空"},
                    ensure_ascii=False,
                )
            )
            return 2

    skip_datasets: list[str] | None = None
    if args.skip_datasets is not None:
        skip_datasets = [s.strip() for s in args.skip_datasets.split(",") if s.strip()]
        if not skip_datasets:
            log.error("--skip-datasets 为空：请提供逗号分隔的数据集名")
            print(
                json.dumps(
                    {"status": "fatal", "error": "--skip-datasets 为空"},
                    ensure_ascii=False,
                )
            )
            return 2
        from backend.shared.quantdb_datasets import DATASETS as _DATASETS

        _known = {d.dataset for d in _DATASETS}
        _unknown = sorted(set(skip_datasets) - _known)
        if _unknown:
            _msg = f"未知数据集: {', '.join(_unknown)}"
            log.error("--skip-datasets %s", _msg)
            print(json.dumps({"status": "fatal", "error": _msg}, ensure_ascii=False))
            return 2
        if datasets is not None:
            _left = [d for d in datasets if d not in set(skip_datasets)]
            if not _left:
                _msg = "--datasets 与 --skip-datasets 组合后为空"
                log.error(_msg)
                print(
                    json.dumps({"status": "fatal", "error": _msg}, ensure_ascii=False)
                )
                return 2

    try:
        summary = run_modelscope_daily_sync(
            datasets=datasets,
            skip_datasets=skip_datasets,
            check=args.check,
            max_pending_gb=args.max_pending_gb,
            allow_large=args.allow_large,
            hash_verify=not args.no_hash_verify,
            skip_backfill=args.skip_backfill,
            skip_pg=args.skip_pg,
            skip_publish=args.skip_publish,
            with_qlib=args.with_qlib,
            pg_days=args.pg_days,
            force_downstream=args.force_downstream,
            repo_id=args.repo_id,
            revision=args.revision,
            endpoint=args.endpoint,
        )
    except Exception as exc:  # noqa: BLE001 - 顶层兜底：枚举失败等致命错误
        log.error("FATAL: %s", exc, exc_info=True)
        print(
            json.dumps(
                {"status": "fatal", "error": str(exc)[:500]},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2

    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return _exit_code(summary)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
