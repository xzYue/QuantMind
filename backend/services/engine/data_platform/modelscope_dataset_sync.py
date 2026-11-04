#!/usr/bin/env python3
"""魔搭（ModelScope）QuantDB 数据集 → 本地数据目录一键初始化。

从魔搭公开数据集仓库（默认 ``qusong0627/LightGBM_Alpha300``）分页枚举远端
parquet 清单（含 sha256 / size），并发流式下载到 ``QM_QUANTDB_DATA_DIR``，
逐文件 sha256 校验后原子覆盖，并按需重建同步状态库，使后续
``quantdb_daily_sync`` 的增量 fast-path 直接命中，避免全量重拉。

远端仓库根目录与本地数据集 ``rel_dir`` 一一对应（如
``6_ml_datasets/l1_l2_factors/dt=20260918/data.parquet``），无需映射表。

设计要点：
  - 只依赖 stdlib + httpx（requirements 已含），不引入 modelscope SDK。
  - 下载走 302 → CDN 签名 URL，httpx 自动跟随重定向。
  - 全量覆盖：逐文件下载并原地替换（os.replace），与魔搭社区完全对齐。
  - 断点续传：仅当本地存在 + size 一致 + 状态库登记 sha256 == 远端 sha256
    才跳过；否则重下覆盖。每批完成即增量登记状态，中断后重跑只补未完成项。
  - 状态库：用远端 sha256 写 objects（免 56GB 重哈希）。
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Optional
from collections.abc import Callable
from urllib.parse import quote

import httpx

log = logging.getLogger("modelscope_dataset_sync")

DEFAULT_ENDPOINT = "https://www.modelscope.cn"
DEFAULT_REPO_ID = "qusong0627/LightGBM_Alpha300"
DEFAULT_REVISION = "master"

TREE_PAGE_SIZE = 1000
TREE_WORKERS = 6
DEFAULT_WORKERS = 6
DOWNLOAD_RETRIES = 3
FILE_PROGRESS_STEP = 25
BATCH_FACTOR = 8  # 每批提交 workers * BATCH_FACTOR 个任务，便于响应取消

_USER_AGENT = "QuantMind-ModelScopeSync/1.0"


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
def _endpoint() -> str:
    return (os.getenv("MODELSCOPE_ENDPOINT") or DEFAULT_ENDPOINT).rstrip("/")


def _repo_id() -> str:
    return os.getenv("MODELSCOPE_DATASET_REPO") or DEFAULT_REPO_ID


def _revision() -> str:
    return os.getenv("MODELSCOPE_DATASET_REVISION") or DEFAULT_REVISION


def _token() -> str | None:
    token = os.getenv("MODELSCOPE_TOKEN") or os.getenv("MODELSCOPE_API_TOKEN")
    return token or None


def _workers() -> int:
    try:
        return max(1, int(os.getenv("MODELSCOPE_SYNC_WORKERS", str(DEFAULT_WORKERS))))
    except ValueError:
        return DEFAULT_WORKERS


def _tree_workers() -> int:
    """清单分页并发数。远端 tree API 为只读查询，6 路并发足够且不易触发限流。"""
    try:
        return max(1, int(os.getenv("MODELSCOPE_TREE_WORKERS", str(TREE_WORKERS))))
    except ValueError:
        return TREE_WORKERS


def resolve_data_root() -> Path:
    """目标数据目录：与 quantdb_daily_sync / 本地扫描同源（QM_QUANTDB_DATA_DIR）。"""
    from backend.scripts.quantdb_local_scan import _default_root

    return Path(os.path.abspath(_default_root()))


# ---------------------------------------------------------------------------
# 远端枚举
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RemoteFile:
    path: str  # 仓库内 posix 相对路径
    size: int
    sha256: str
    dataset: str | None  # 映射到的本地数据集；未知顶层目录为 None
    layout: str  # v2_daily_partition / v1_symbol


def _classify(rel_path: str) -> tuple[str | None, str]:
    """把仓库相对路径归类到数据集。返回 (dataset, layout_col)。"""
    from backend.shared.quantdb_datasets import DATASETS

    for spec in DATASETS:
        if rel_path.startswith(spec.rel_dir + "/"):
            layout = "v2_daily_partition" if spec.layout == "partition" else "v1_symbol"
            return spec.dataset, layout
    return None, "v2_manifest"


def _headers() -> dict[str, str]:
    headers = {"User-Agent": _USER_AGENT}
    token = _token()
    if token:
        # 私有仓库 / 提高限流阈值；公开数据集留空即可
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _file_url(endpoint: str, repo_id: str, revision: str, rel_path: str) -> str:
    return (
        f"{endpoint}/api/v1/datasets/{repo_id}/repo"
        f"?Revision={quote(revision, safe='')}&FilePath={quote(rel_path, safe='')}"
    )


def _fetch_tree_page(
    client: httpx.Client, url: str, revision: str, page: int
) -> tuple[list[dict[str, Any]], int | None]:
    """拉取单页 tree；失败重试，返回 (entries, TotalCount)。"""
    last_exc: Exception | None = None
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            resp = client.get(
                url,
                params={
                    "Revision": revision,
                    "Recursive": "true",
                    "PageNumber": page,
                    "PageSize": TREE_PAGE_SIZE,
                },
            )
            resp.raise_for_status()
            body = resp.json()
            if body.get("Code") != 200:
                raise RuntimeError(
                    f"ModelScope tree API 失败: {body.get('Message') or body.get('Code')}"
                )
            payload = body.get("Data") or {}
            total = payload.get("TotalCount")
            return (
                payload.get("Files") or [],
                int(total) if total is not None else None,
            )
        except Exception as exc:  # noqa: BLE001 - 429/网络抖动都值得重试
            last_exc = exc
            if attempt < DOWNLOAD_RETRIES - 1:
                time.sleep(1.0 * (attempt + 1))
    raise last_exc if last_exc else RuntimeError("ModelScope tree API 失败")


def list_remote_files(
    *,
    endpoint: str | None = None,
    repo_id: str | None = None,
    revision: str | None = None,
    should_cancel: Callable[[], bool] | None = None,
    progress_cb: Callable[..., None] | None = None,
) -> list[RemoteFile]:
    """分页枚举魔搭仓库全部 blob（递归），返回带 sha256/size 的清单。

    首屏拿到 TotalCount 后，其余页并发拉取（默认 6 路）：远端 10 万条
    清单约 106 页，串行按单页 0.2~0.6s 要 30~60s，并发后通常 5~10s。
    """
    ep = (endpoint or _endpoint()).rstrip("/")
    repo = repo_id or _repo_id()
    rev = revision or _revision()
    url = f"{ep}/api/v1/datasets/{repo}/repo/tree"

    files: dict[str, RemoteFile] = {}
    seen_entries = 0
    total: int | None = None

    def _absorb(entries: list[dict[str, Any]]) -> None:
        nonlocal seen_entries
        seen_entries += len(entries)
        for ent in entries:
            if ent.get("Type") != "blob":
                continue
            raw_path = ent.get("Path") or ent.get("Name") or ""
            if not raw_path:
                continue
            dataset, layout = _classify(raw_path)
            files[raw_path] = RemoteFile(
                path=raw_path,
                size=int(ent.get("Size") or 0),
                sha256=(ent.get("Sha256") or "").lower(),
                dataset=dataset,
                layout=layout,
            )

    timeout = httpx.Timeout(30.0, read=120.0)
    with httpx.Client(
        follow_redirects=True, timeout=timeout, headers=_headers()
    ) as client:
        entries, total = _fetch_tree_page(client, url, rev, 1)
        _absorb(entries)
        page = 1
        if progress_cb:
            progress_cb("enumerate", done=seen_entries, total=total or seen_entries)

        if total is not None and total > TREE_PAGE_SIZE and entries:
            # 已知总页数：其余页并发拉取。httpx.Client 线程安全。
            page = (total + TREE_PAGE_SIZE - 1) // TREE_PAGE_SIZE
            pages = list(range(2, page + 1))
            with ThreadPoolExecutor(
                max_workers=min(_tree_workers(), len(pages))
            ) as pool:
                futures = {
                    pool.submit(_fetch_tree_page, client, url, rev, p): p for p in pages
                }
                for fut in as_completed(futures):
                    if should_cancel is not None and should_cancel():
                        for pending in futures:
                            pending.cancel()
                        break
                    page_entries, _page_total = fut.result()
                    _absorb(page_entries)
                    if progress_cb:
                        progress_cb("enumerate", done=seen_entries, total=total)
        else:
            # 小仓库（或 API 未返回 TotalCount）：保持逐页串行，直到取空/取够。
            while entries and (total is None or seen_entries < total):
                if should_cancel is not None and should_cancel():
                    break
                page += 1
                entries, page_total = _fetch_tree_page(client, url, rev, page)
                if total is None:
                    total = page_total
                _absorb(entries)
                if progress_cb:
                    progress_cb(
                        "enumerate", done=seen_entries, total=total or seen_entries
                    )

    log.info("[MODELSCOPE] 枚举 %d 个文件（%d 页）", len(files), page)
    return list(files.values())


# ---------------------------------------------------------------------------
# 本地落盘
# ---------------------------------------------------------------------------
def _target_path(root: Path, rel_path: str) -> Path:
    """把仓库相对路径安全地映射到本地绝对路径（防目录穿越）。

    这里刻意不做逐文件 ``Path.resolve()``：Windows 上一次约 0.7ms，7.5 万
    文件 × 两轮会吃掉 100s+。远端路径的绝对路径/".." 已被 parts 校验排除，
    词法拼接必然落在 root 之下；root 由调用方保证为绝对路径。
    远端仓库无法在本地创建符号链接，故不存在经符号链接逃逸的路径。
    """
    pure = PurePosixPath(rel_path)
    if pure.is_absolute() or ".." in pure.parts:
        raise ValueError(f"非法远端路径: {rel_path}")
    return root.joinpath(*pure.parts)


def _download_one(
    client: httpx.Client, url: str, remote: RemoteFile, target: Path
) -> str:
    """下载单个文件：.part → sha256 校验 → 原子覆盖。失败抛异常。

    .part 名带 pid 后缀，隔离跨进程并发写同一目标（管理台 init × cron 增量）：
    确定性 .part 名会被后开进程 truncate 出零洞，先完成进程以内存哈希过校验
    后 os.replace，落下 size+登记齐全的损坏文件且此后永远被跳过。
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(f"{target.name}.part-{os.getpid()}")
    last_exc: Exception | None = None
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                h = hashlib.sha256()
                size = 0
                with open(part, "wb") as fh:
                    for chunk in resp.iter_bytes(1 << 20):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        h.update(chunk)
                        size += len(chunk)
            if remote.size and size != remote.size:
                raise OSError(f"size 不符: 期望 {remote.size} 实得 {size}")
            if remote.sha256 and h.hexdigest() != remote.sha256:
                raise OSError("sha256 校验失败")
            os.replace(part, target)
            return "downloaded"
        except Exception as exc:  # noqa: BLE001 - 重试后统一抛出
            last_exc = exc
            try:
                if part.exists():
                    part.unlink()
            except OSError:
                pass
            if attempt < DOWNLOAD_RETRIES - 1:
                time.sleep(1.5 * (attempt + 1))
    raise last_exc if last_exc else RuntimeError("下载失败")


def _local_breakdown(root: Path, files: list[RemoteFile]) -> tuple[int, int, int]:
    """按本地状态拆分远端文件字节数，返回 (已存在, 缺失, 存在但size不同)。

    「缺失」才真正需要新增磁盘空间；「存在但size不同」是原地覆盖（os.replace），
    基本不产生净增量，「已存在」直接跳过。
    """
    present = missing = changed = 0
    for f in files:
        try:
            target = _target_path(root, f.path)
        except ValueError:
            missing += f.size
            continue
        try:
            if not target.is_file():
                missing += f.size
            elif f.size and target.stat().st_size != f.size:
                changed += f.size
            else:
                present += f.size
        except OSError:
            changed += f.size
    return present, missing, changed


def _scan_local(
    root: Path, files: list[RemoteFile], state_shas: dict[str, str]
) -> tuple[list[RemoteFile], int, int, int]:
    """一次遍历同时完成续传判定与本地空间拆分。

    返回 (可跳过文件, 已存在字节, 缺失字节, 存在但大小不同字节)。
    原先 preflight 对同一批文件跑 ``_split_resumable`` + ``_local_breakdown``
    两遍（每遍还各自 stat），合并后 Windows 上 7.5 万文件可省约一半时间。
    """
    skippable: list[RemoteFile] = []
    present = missing = changed = 0
    for f in files:
        try:
            target = _target_path(root, f.path)
        except ValueError:
            missing += f.size
            continue
        try:
            if not target.is_file():
                missing += f.size
                continue
            same_size = (not f.size) or target.stat().st_size == f.size
        except OSError:
            changed += f.size
            continue
        if not same_size:
            changed += f.size
            continue
        present += f.size
        if f.size and f.sha256 and state_shas.get(f.path) == f.sha256:
            skippable.append(f)
    return skippable, present, missing, changed


def _split_resumable(
    root: Path, files: list[RemoteFile], state_shas: dict[str, str]
) -> tuple[list[RemoteFile], list[RemoteFile]]:
    """按「本地已完整下载」拆分为 (可跳过, 待下载)。

    判据：本地文件存在、size 与远端一致，且状态库登记的同 key sha256 == 远端 sha256。
    任一不满足则重下并原地覆盖（保证与魔搭对齐）。
    """
    skippable: list[RemoteFile] = []
    pending: list[RemoteFile] = []
    for f in files:
        try:
            target = _target_path(root, f.path)
            if (
                f.size
                and f.sha256
                and target.is_file()
                and target.stat().st_size == f.size
                and state_shas.get(f.path) == f.sha256
            ):
                skippable.append(f)
                continue
        except (OSError, ValueError):
            pass
        pending.append(f)
    return skippable, pending


# ---------------------------------------------------------------------------
# 状态库重建
# ---------------------------------------------------------------------------
def _open_state_db(path: Path):
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS objects ("
        "key TEXT PRIMARY KEY, etag TEXT, sha256 TEXT, size INTEGER,"
        " path TEXT, layout TEXT, dataset TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS releases (dataset TEXT PRIMARY KEY, release_id TEXT NOT NULL)"
    )
    return conn


def _write_state_meta(
    root: Path, per_dataset: dict[str, list[RemoteFile]]
) -> tuple[dict[str, str], int]:
    """用远端元数据直接写 objects 表（免重哈希），使增量 fast-path 命中。"""
    from backend.scripts.quantdb_daily_sync import _state_path

    targets: list[tuple[str, Path]] = [
        ("quantmind", _state_path(root)),
        ("sdk", root / "quantdb_sync.sqlite"),
    ]
    written = 0
    for _, db_path in targets:
        conn = _open_state_db(db_path)
        try:
            for dataset, files in per_dataset.items():
                conn.execute("DELETE FROM objects WHERE dataset=?", (dataset,))
                rows = [
                    (
                        f.path,
                        f.sha256,
                        f.sha256,
                        f.size,
                        str(_target_path(root, f.path)),
                        f.layout,
                        dataset,
                    )
                    for f in files
                ]
                conn.executemany(
                    "INSERT OR REPLACE INTO objects(key, etag, sha256, size, path, layout, dataset)"
                    " VALUES(?,?,?,?,?,?,?)",
                    rows,
                )
                written += len(rows)
            conn.commit()
        finally:
            conn.close()
    return {label: str(p) for label, p in targets}, written


def _state_db_paths(root: Path) -> tuple[Path, ...]:
    from backend.scripts.quantdb_daily_sync import _state_path

    return (_state_path(root), root / "quantdb_sync.sqlite")


def _load_state_shas(root: Path, datasets: list[str]) -> dict[str, str]:
    """读同步状态库：key → 已登记的 sha256（用于断点续传跳过已下载文件）。"""
    import sqlite3

    if not datasets:
        return {}
    db_path = _state_db_paths(root)[0]
    if not db_path.exists():
        return {}
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
    except sqlite3.Error:
        return {}
    try:
        placeholders = ",".join("?" for _ in datasets)
        rows = conn.execute(
            f"SELECT key, sha256 FROM objects WHERE dataset IN ({placeholders})",
            datasets,
        )
        return {key: sha for key, sha in rows if sha}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def _upsert_state_rows(root: Path, rows: list[tuple]) -> None:
    """增量登记已下载对象到两个状态库（中断后续传据此跳过）。"""
    if not rows:
        return
    for db_path in _state_db_paths(root):
        conn = _open_state_db(db_path)
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO objects(key, etag, sha256, size, path, layout, dataset)"
                " VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def _progress(progress_cb: Callable[..., None] | None, event: str, **kw: Any) -> None:
    if progress_cb:
        try:
            progress_cb(event, **kw)
        except Exception:  # noqa: BLE001 - 进度回调异常不得中断同步
            log.debug("progress_cb 异常", exc_info=True)


def _group_remote(
    remote: list[RemoteFile], datasets: list[str] | None
) -> dict[str, list[RemoteFile]]:
    grouped: dict[str, list[RemoteFile]] = {}
    for f in remote:
        if f.dataset is None or not f.path.endswith(".parquet"):
            continue
        if datasets is not None and f.dataset not in datasets:
            continue
        grouped.setdefault(f.dataset, []).append(f)
    return grouped


def init_from_modelscope(
    datasets: list[str] | None = None,
    *,
    repo_id: str | None = None,
    revision: str | None = None,
    endpoint: str | None = None,
    progress_cb: Callable[..., None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """从魔搭全量拉取 QuantDB 数据并原地覆盖本地目录。

    目标是与魔搭社区数据集完全对齐：不比对、不增量，逐文件下载并覆盖
    已存在的文件；完成后用远端 sha256 重建同步状态库。
    datasets: 数据集名列表（DATASETS 规格）；None=仓库内可识别的全部。
    """
    from backend.shared.quantdb_datasets import DATASETS

    known = {spec.dataset for spec in DATASETS}
    if datasets is not None:
        unknown = set(datasets) - known
        if unknown:
            raise ValueError(f"未知数据集: {', '.join(sorted(unknown))}")

    root = resolve_data_root()
    root.mkdir(parents=True, exist_ok=True)
    started = time.time()

    _progress(progress_cb, "phase", phase="enumerate", message="枚举魔搭远端清单")
    remote = list_remote_files(
        endpoint=endpoint,
        repo_id=repo_id,
        revision=revision,
        should_cancel=should_cancel,
        progress_cb=progress_cb,
    )
    grouped = _group_remote(remote, datasets)

    if datasets is not None:
        missing = [d for d in datasets if d not in grouped]
        if missing:
            raise ValueError(
                f"魔搭仓库中不存在这些数据集: {', '.join(sorted(missing))}"
            )

    total_files = sum(len(v) for v in grouped.values())
    total_bytes = sum(f.size for v in grouped.values() for f in v)
    _progress(
        progress_cb,
        "enumerate_done",
        datasets=len(grouped),
        files=total_files,
        bytes=total_bytes,
    )

    workers = _workers()
    batch = max(workers, workers * BATCH_FACTOR)
    timeout = httpx.Timeout(30.0, read=300.0, write=120.0, pool=60.0)

    # 断点续传：已完整下载（size 一致且状态库 sha256 == 远端 sha256）的文件跳过
    state_shas = _load_state_shas(root, list(grouped))
    downloaded = skipped = errors = downloaded_bytes = 0
    processed_bytes = 0
    error_samples: list[str] = []
    per_dataset_result: dict[str, dict[str, Any]] = {}
    cancelled = False
    ep = endpoint or _endpoint()
    repo = repo_id or _repo_id()
    rev = revision or _revision()

    with httpx.Client(
        follow_redirects=True, timeout=timeout, headers=_headers()
    ) as client:
        for idx, (dataset, files) in enumerate(grouped.items()):
            if should_cancel is not None and should_cancel():
                cancelled = True
                break

            skippable, pending = _split_resumable(root, files, state_shas)
            skipped += len(skippable)
            processed_bytes += sum(f.size for f in skippable)
            _progress(
                progress_cb,
                "dataset_start",
                dataset=dataset,
                index=idx,
                total=len(grouped),
                files=len(files),
                pending=len(pending),
                skipped=len(skippable),
            )
            ds_downloaded = ds_errors = done_in_ds = 0

            for start in range(0, len(pending), batch):
                if should_cancel is not None and should_cancel():
                    cancelled = True
                    break
                window = pending[start : start + batch]
                done_rows: list[tuple] = []
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {
                        pool.submit(
                            _download_one,
                            client,
                            _file_url(ep, repo, rev, f.path),
                            f,
                            _target_path(root, f.path),
                        ): f
                        for f in window
                    }
                    for fut in as_completed(futures):
                        f = futures[fut]
                        try:
                            fut.result()
                            ds_downloaded += 1
                            downloaded_bytes += f.size
                            processed_bytes += f.size
                            done_rows.append(
                                (
                                    f.path,
                                    f.sha256,
                                    f.sha256,
                                    f.size,
                                    str(_target_path(root, f.path)),
                                    f.layout,
                                    dataset,
                                )
                            )
                        except Exception as exc:  # noqa: BLE001
                            ds_errors += 1
                            if len(error_samples) < 20:
                                error_samples.append(f"{f.path}: {exc}")
                            log.warning("[MODELSCOPE] %s 下载失败: %s", f.path, exc)
                        done_in_ds += 1
                        if done_in_ds % FILE_PROGRESS_STEP == 0:
                            _progress(
                                progress_cb,
                                "file",
                                dataset=dataset,
                                done=done_in_ds,
                                total=len(pending),
                                processed=processed_bytes,
                                downloaded=downloaded_bytes,
                                errors=errors + ds_errors,
                            )
                # 每批完成即增量登记状态，保证中断后重跑可跳过
                _upsert_state_rows(root, done_rows)
                if cancelled:
                    break

            downloaded += ds_downloaded
            errors += ds_errors
            per_dataset_result[dataset] = {
                "files": len(files),
                "downloaded": ds_downloaded,
                "skipped": len(skippable),
                "errors": ds_errors,
                "bytes": sum(f.size for f in files),
            }
            _progress(
                progress_cb,
                "dataset_done",
                dataset=dataset,
                downloaded=ds_downloaded,
                skipped=len(skippable),
                errors=ds_errors,
            )
            log.info(
                "[MODELSCOPE] %s: 下载 %d 跳过 %d (共 %d) 失败 %d",
                dataset,
                ds_downloaded,
                len(skippable),
                len(files),
                ds_errors,
            )
            if cancelled:
                break

    # 用远端元数据重建同步状态库（本地已是魔搭内容，直接登记 sha256）
    state_info: dict[str, Any] = {"mode": "meta", "status": "skipped"}
    if not cancelled:
        _progress(progress_cb, "phase", phase="state", message="写入同步状态库")
        try:
            per_ds_files = {
                d: [f for f in grouped[d] if _target_path(root, f.path).is_file()]
                for d in grouped
            }
            dbs, written = _write_state_meta(root, per_ds_files)
            state_info = {
                "mode": "meta",
                "status": "ok",
                "objects": written,
                "state_dbs": dbs,
            }
        except Exception as exc:  # noqa: BLE001
            state_info = {"mode": "meta", "status": "failed", "reason": str(exc)}
            log.error("[MODELSCOPE] 状态库写入失败: %s", exc, exc_info=True)

    return {
        "root": str(root),
        "repo_id": repo,
        "cancelled": cancelled,
        "datasets": per_dataset_result,
        "total_files": total_files,
        "downloaded": downloaded,
        "skipped": skipped,
        "errors": errors,
        "downloaded_bytes": downloaded_bytes,
        "error_samples": error_samples,
        "state": state_info,
        "elapsed_sec": round(time.time() - started, 1),
    }


def preflight_modelscope(
    datasets: list[str] | None = None,
    *,
    repo_id: str | None = None,
    revision: str | None = None,
    endpoint: str | None = None,
) -> dict[str, Any]:
    """初始化前预检：远端各数据集文件数/字节、本地目录与磁盘余量。"""
    from backend.shared.quantdb_datasets import DATASETS

    root = resolve_data_root()
    root.mkdir(parents=True, exist_ok=True)

    remote = list_remote_files(endpoint=endpoint, repo_id=repo_id, revision=revision)
    grouped = _group_remote(remote, datasets)

    # 续传判据（size 一致 + 状态库 sha256 一致）决定「已就绪(将跳过)」，
    # 与 init_from_modelscope 的 `_split_resumable` 保持同一套口径。
    state_shas = _load_state_shas(root, list(grouped))

    # 按 DATASETS 规格顺序输出（天然按 6 大类分组），不按标识字母排序
    items = []
    total_bytes = 0
    skip_files = 0
    skip_bytes = 0
    missing_bytes = 0
    changed_bytes = 0
    for spec in DATASETS:
        files = grouped.get(spec.dataset)
        if not files:
            continue
        ds_bytes = sum(f.size for f in files)
        # 缺失 = 路径不存在（真正占新增空间）；变更 = 存在但大小不同（原地覆盖）
        skippable, _present, ds_missing, ds_changed = _scan_local(
            root, files, state_shas
        )
        ds_skip = sum(f.size for f in skippable)
        total_bytes += ds_bytes
        skip_files += len(skippable)
        skip_bytes += ds_skip
        missing_bytes += ds_missing
        changed_bytes += ds_changed
        items.append(
            {
                "dataset": spec.dataset,
                "name": spec.name,
                "group": spec.group,
                "layout": spec.layout,
                "rel_dir": spec.rel_dir,
                "files": len(files),
                "bytes": ds_bytes,
                "skip_bytes": ds_skip,
            }
        )

    try:
        usage = shutil.disk_usage(str(root))
        disk = {"total": usage.total, "used": usage.used, "free": usage.free}
    except OSError:
        disk = {"total": 0, "used": 0, "free": 0}

    # 全量覆盖下载：已存在的文件原地替换（同目录 .part → os.replace），几乎不占净增量；
    # 只有本地缺失的文件才真正需要新增磁盘空间。
    warnings: list[str] = []
    if disk["free"] and missing_bytes and disk["free"] < missing_bytes * 1.1:
        warnings.append(
            f"磁盘余量不足：本地缺失约 {missing_bytes / 1024**3:.1f} GB 需新增空间，"
            f"当前可用 {disk['free'] / 1024**3:.1f} GB。请先清理磁盘再执行。"
        )

    ep = (endpoint or _endpoint()).rstrip("/")
    repo = repo_id or _repo_id()
    return {
        "repo_id": repo,
        "repo_url": f"{ep}/datasets/{repo}",
        "revision": revision or _revision(),
        "root": str(root),
        "datasets": items,
        "total_files": sum(it["files"] for it in items),
        "total_bytes": total_bytes,
        "skip_files": skip_files,
        "skip_bytes": skip_bytes,
        "missing_bytes": missing_bytes,
        "changed_bytes": changed_bytes,
        "disk": disk,
        "warnings": warnings,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# 每日增量同步（diff → 只下载新增/变更 → 全部状态库副本登记）
# ---------------------------------------------------------------------------
# 与 init_from_modelscope（全量对齐、末尾整段重写状态库）不同，增量模式面向
# 每日 cron：只下载远端新出现或内容变化的对象，成功后把 sha256 upsert 进所有
# 被发现的状态库副本，不删除、不整段重写既有登记。
#
# 状态库副本背景：同一份 objects 登记在本机/容器存在多份命名不同的拷贝——
#   1. env 派生库：QUANTDB_STATE_DIR/quantdb_sync_<数据根路径>.sqlite
#      （容器内 root=/data/quantdb，即 .sync_state/quantdb_sync_data_quantdb.sqlite）
#   2. root/.sync_state/ 下的历史副本（跨运行环境命名口径不同，如原生跑时
#      root=/Users/.../data/quantdb，与容器命名不一致）
#   3. root/quantdb_sync.sqlite（SDK 库，root 相对，原生/容器路径一致）
# 增量读取时合并全部副本（任一副本 sha256 命中即可跳过——sha256 相等即内容
# 相等，旧副本的陈旧 sha 不会导致误跳过）；写入时同步 upsert 全部副本，保证
# 无论增量在哪里跑，quantdb_daily_sync（SDK）与管理台初始化的 fast-path 都命中，
# 不会把已下载的几十 GiB 误判为缺失而全量重拉。

DEFAULT_MAX_PENDING_BYTES = 10 * 1024**3  # 护栏：待下载超过 10GiB 视为状态库异常
HASH_VERIFY_MAX_FILES = 2000  # 状态登记缺失时，允许本地哈希直验免下载的文件数上限
HASH_VERIFY_MAX_BYTES = 4 * 1024**3  # ……及字节上限（更大缺口应交护栏拦截后人工处理）

_DT_RE = re.compile(r"/dt=(\d{8})(?:/|$)")


class PendingTooLargeError(RuntimeError):
    """待下载量超过护栏——通常意味着状态库丢失/损坏，无人值守执行有全量重拉风险。

    恢复指引：先跑 backend/scripts/quantdb_local_scan.py 用本地已有文件重建
    状态库，或确认确需大批量下载后显式传 allow_large=True（CLI --allow-large）。
    """

    def __init__(self, pending_files: int, pending_bytes: int, limit: int):
        self.pending_files = pending_files
        self.pending_bytes = pending_bytes
        self.limit = limit
        super().__init__(
            f"待下载 {pending_files} 个文件 / {pending_bytes / 1024**3:.1f} GiB "
            f"超过护栏 {limit / 1024**3:.1f} GiB：状态库可能丢失或损坏，"
            f"继续执行有全量重拉风险。可先运行 quantdb_local_scan.py 重建状态库，"
            f"或确认后用 --allow-large 放行。"
        )


def discover_state_dbs(root: Path) -> list[Path]:
    """列出应读/写的全部同步状态库副本（去重；顺序：env 派生 > .sync_state > SDK 库）。

    root/quantdb_sync.sqlite 恒在列表中（不存在时写入方会建库，与
    _state_db_paths 口径一致）；env 派生库仅在显式设置 QUANTDB_STATE_DIR
    时参与，避免原生运行时在 ~/.quantdb_state 凭空多出第三份拷贝。
    """
    found: list[Path] = []
    seen: set[str] = set()

    def _add(p: Path) -> None:
        try:
            key = str(p.resolve())
        except OSError:
            return
        if key not in seen:
            seen.add(key)
            found.append(p)

    if os.getenv("QUANTDB_STATE_DIR"):
        try:
            from backend.scripts.quantdb_daily_sync import _state_path

            _add(_state_path(root))
        except Exception:  # noqa: BLE001 - 无 pandas 等环境：退化到可发现副本
            log.debug("[MODELSCOPE] env 状态库路径解析失败（忽略）", exc_info=True)
    sync_state = root / ".sync_state"
    if sync_state.is_dir():
        for p in sorted(sync_state.glob("quantdb_sync_*.sqlite")):
            _add(p)
    _add(root / "quantdb_sync.sqlite")
    return found


def _load_state_sha_sets(root: Path) -> dict[str, set[str]]:
    """合并全部状态库副本的 objects 表：key → 已登记 sha256 集合。"""
    import sqlite3

    merged: dict[str, set[str]] = {}
    for db in discover_state_dbs(root):
        if not db.exists():
            continue
        try:
            conn = sqlite3.connect(str(db), timeout=10)
        except sqlite3.Error:
            log.warning("[MODELSCOPE] 状态库无法打开（跳过该副本）: %s", db)
            continue
        try:
            for key, sha in conn.execute("SELECT key, sha256 FROM objects"):
                if key and sha:
                    merged.setdefault(key, set()).add(str(sha).lower())
        except sqlite3.Error:
            log.warning(
                "[MODELSCOPE] 状态库 objects 读取失败（跳过该副本）: %s",
                db,
                exc_info=True,
            )
        finally:
            conn.close()
    return merged


def _split_pending(
    root: Path, files: list[RemoteFile], sha_sets: dict[str, set[str]]
) -> tuple[list[RemoteFile], list[RemoteFile]]:
    """按「本地已完整且与远端一致」拆分 (可跳过, 待下载)。

    判据与 _split_resumable 相同，但 sha 登记允许命中任一状态库副本；
    远端缺 sha256 的 blob（个别对象可能没有）一律进入待下载。
    """
    skippable: list[RemoteFile] = []
    pending: list[RemoteFile] = []
    for f in files:
        try:
            target = _target_path(root, f.path)
            if (
                f.size
                and f.sha256
                and target.is_file()
                and target.stat().st_size == f.size
                and f.sha256 in sha_sets.get(f.path, ())
            ):
                skippable.append(f)
                continue
        except (OSError, ValueError):
            pass
        pending.append(f)
    return skippable, pending


def _latest_remote_dt(files: list[RemoteFile]) -> str | None:
    """远端清单中最新的 dt=YYYYMMDD 分区（symbol/single 布局返回 None）。"""
    latest: str | None = None
    for f in files:
        m = _DT_RE.search(f.path)
        if m and (latest is None or m.group(1) > latest):
            latest = m.group(1)
    return latest


def _latest_local_dt(root: Path, rel_dir: str) -> str | None:
    """本地数据集目录下已落地 data.parquet 的最新 dt 分区。"""
    base = root / rel_dir
    if not base.is_dir():
        return None
    latest: str | None = None
    try:
        entries = list(base.iterdir())
    except OSError:
        return None
    for part in entries:
        if not (part.is_dir() and part.name.startswith("dt=")):
            continue
        digits = part.name[3:]
        if len(digits) != 8 or not digits.isdigit():
            continue
        if not (part / "data.parquet").is_file():
            continue
        if latest is None or digits > latest:
            latest = digits
    return latest


def _sha256_file(path: Path) -> str:
    """分块读文件计算 sha256（内存友好）。"""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _hash_verify_pending(
    root: Path,
    pending: list[RemoteFile],
    *,
    should_cancel: Callable[[], bool] | None = None,
    max_files: int | None = None,
    max_bytes: int | None = None,
) -> tuple[list[RemoteFile], list[RemoteFile]]:
    """对本地已存在的待下载文件直接算 sha256，与远端一致则免下载。

    用于状态库副本陈旧/缺失时的自愈（内容一致就不重下，只补登记）。
    哈希预算（默认 HASH_VERIFY_MAX_FILES/BYTES）按实际哈希的文件逐个扣减，
    耗尽后其余文件直接转入待下载——部分直验优于整批放弃后重下；
    should_cancel 每 16 个文件轮询一次，取消时剩余全部转入待下载。
    返回 (验证通过免下载, 仍需下载)。
    """
    if max_files is None:
        max_files = HASH_VERIFY_MAX_FILES
    if max_bytes is None:
        max_bytes = HASH_VERIFY_MAX_BYTES
    verified: list[RemoteFile] = []
    rest: list[RemoteFile] = []
    for i, f in enumerate(pending):
        if should_cancel is not None and i % 16 == 0 and should_cancel():
            rest.extend(pending[i:])
            break
        try:
            target = _target_path(root, f.path)
            if (
                f.sha256
                and target.is_file()
                and (not f.size or target.stat().st_size == f.size)
                and max_files > 0
                and f.size <= max_bytes
            ):
                max_files -= 1
                max_bytes -= f.size
                if _sha256_file(target) == f.sha256:
                    verified.append(f)
                    continue
        except (OSError, ValueError):
            pass
        rest.append(f)
    return verified, rest


def _upsert_state_rows_multi(
    root: Path, rows: list[tuple], *, state_dbs: list[Path] | None = None
) -> list[Path]:
    """把 objects 行增量登记进全部状态库副本；返回实际写入成功的库列表。

    单个副本写失败（如被容器进程长锁）只告警不中断：其余副本仍能支撑
    fast-path，且下次运行会再次尝试补齐。
    """
    written: list[Path] = []
    if not rows:
        return written
    for db_path in state_dbs if state_dbs is not None else discover_state_dbs(root):
        try:
            conn = _open_state_db(db_path)
        except Exception as exc:  # noqa: BLE001 - sqlite3.Error 及路径权限问题
            log.warning(
                "[MODELSCOPE] 状态库打开失败（跳过该副本）: %s (%s)", db_path, exc
            )
            continue
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO objects(key, etag, sha256, size, path, layout, dataset)"
                " VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
            written.append(db_path)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "[MODELSCOPE] 状态库写入失败（跳过该副本）: %s (%s)", db_path, exc
            )
        finally:
            conn.close()
    return written


def _enumerate_and_diff(
    datasets: list[str] | None,
    *,
    repo_id: str | None,
    revision: str | None,
    endpoint: str | None,
    should_cancel: Callable[[], bool] | None,
    progress_cb: Callable[..., None] | None,
) -> tuple[
    Path, dict[str, list[RemoteFile]], dict[str, list[RemoteFile]], dict[str, Any]
]:
    """枚举远端并与本地对账（只读）。

    返回 (数据根, 按数据集分组的远端清单, 按数据集分组的待下载清单, 报告)。
    """
    from backend.shared.quantdb_datasets import DATASETS

    known = {spec.dataset for spec in DATASETS}
    if datasets is not None:
        unknown = set(datasets) - known
        if unknown:
            raise ValueError(f"未知数据集: {', '.join(sorted(unknown))}")

    root = resolve_data_root()
    _progress(progress_cb, "phase", phase="enumerate", message="枚举魔搭远端清单")
    remote = list_remote_files(
        endpoint=endpoint,
        repo_id=repo_id,
        revision=revision,
        should_cancel=should_cancel,
        progress_cb=progress_cb,
    )
    grouped = _group_remote(remote, datasets)
    if datasets is not None:
        missing = [d for d in datasets if d not in grouped]
        if missing:
            raise ValueError(
                f"魔搭仓库中不存在这些数据集: {', '.join(sorted(missing))}"
            )

    sha_sets = _load_state_sha_sets(root)
    pending_by_ds: dict[str, list[RemoteFile]] = {}
    per_dataset: dict[str, Any] = {}
    total_pending_files = 0
    total_pending_bytes = 0
    for spec in DATASETS:  # 固定规格顺序，报告可读且稳定
        files = grouped.get(spec.dataset)
        if not files:
            continue
        skippable, pending = _split_pending(root, files, sha_sets)
        pending_bytes = sum(f.size for f in pending)
        total_pending_files += len(pending)
        total_pending_bytes += pending_bytes
        pending_by_ds[spec.dataset] = pending
        per_dataset[spec.dataset] = {
            "name": spec.name,
            "rel_dir": spec.rel_dir,
            "layout": spec.layout,
            "remote_files": len(files),
            "skippable": len(skippable),
            "pending_files": len(pending),
            "pending_bytes": pending_bytes,
            "pending_missing_sha": sum(1 for f in pending if not f.sha256),
            "latest_remote_dt": _latest_remote_dt(files),
            "latest_local_dt": _latest_local_dt(root, spec.rel_dir),
            "pending_samples": [f.path for f in pending[:5]],
        }
    report = {
        "root": str(root),
        "repo_id": repo_id or _repo_id(),
        "revision": revision or _revision(),
        "endpoint": (endpoint or _endpoint()).rstrip("/"),
        "state_dbs": [str(p) for p in discover_state_dbs(root)],
        "state_keys": len(sha_sets),
        "datasets": per_dataset,
        "total_remote_files": sum(len(v) for v in grouped.values()),
        "total_pending_files": total_pending_files,
        "total_pending_bytes": total_pending_bytes,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    return root, grouped, pending_by_ds, report


def diff_from_modelscope(
    datasets: list[str] | None = None,
    *,
    repo_id: str | None = None,
    revision: str | None = None,
    endpoint: str | None = None,
    should_cancel: Callable[[], bool] | None = None,
    progress_cb: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """枚举魔搭远端清单并与本地状态库/磁盘对账，全程只读（不下载不写库）。

    返回按数据集的 pending_files/pending_bytes/latest_remote_dt/latest_local_dt
    等，用于每日任务预检（--check）与增量护栏判断。
    """
    _root, _grouped, _pending, report = _enumerate_and_diff(
        datasets,
        repo_id=repo_id,
        revision=revision,
        endpoint=endpoint,
        should_cancel=should_cancel,
        progress_cb=progress_cb,
    )
    return report


def sync_incremental_from_modelscope(
    datasets: list[str] | None = None,
    *,
    dry_run: bool = False,
    max_pending_bytes: int = DEFAULT_MAX_PENDING_BYTES,
    allow_large: bool = False,
    hash_verify: bool = True,
    repo_id: str | None = None,
    revision: str | None = None,
    endpoint: str | None = None,
    progress_cb: Callable[..., None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """每日增量同步：只下载远端新增/内容变化的文件，校验后登记全部状态库副本。

    - dry_run=True 仅预检（等价 diff_from_modelscope），不写盘；
    - 待下载字节超过 max_pending_bytes 且未 allow_large 时抛 PendingTooLargeError
      （状态库丢失时防无人值守全量重拉）；
    - 小缺口（<= HASH_VERIFY_MAX_*）先做本地 sha256 直验，一致则免下载只补登记；
    - 下载沿用 init 的机制：.part → sha256+size 校验 → os.replace 原子覆盖，
      每批完成即 upsert 状态库，中断重跑只补未完成项。
    """
    started = time.time()
    root, _grouped, pending_by_ds, report = _enumerate_and_diff(
        datasets,
        repo_id=repo_id,
        revision=revision,
        endpoint=endpoint,
        should_cancel=should_cancel,
        progress_cb=progress_cb,
    )
    if dry_run:
        return {"status": "checked", "dry_run": True, **report}

    pending_files = report["total_pending_files"]
    pending_bytes = report["total_pending_bytes"]
    if pending_files == 0:
        log.info(
            "[MODELSCOPE] 远端无增量（%d 个文件全部已对齐）",
            report["total_remote_files"],
        )
        return {
            "status": "up_to_date",
            "downloaded": 0,
            "errors": 0,
            "downloaded_bytes": 0,
            "hash_verified": 0,
            "diff": report,
            "elapsed_sec": round(time.time() - started, 1),
        }
    if pending_bytes > max_pending_bytes and not allow_large:
        raise PendingTooLargeError(pending_files, pending_bytes, max_pending_bytes)

    state_dbs = discover_state_dbs(root)
    touched_partitions: dict[str, set[str]] = {}

    def _note_dt(dataset: str, path: str) -> None:
        m = _DT_RE.search(path)
        if m:
            touched_partitions.setdefault(dataset, set()).add(m.group(1))

    # 小缺口先做本地哈希直验（状态库副本陈旧/缺失时自愈，避免无谓重下）；
    # 预算在 _hash_verify_pending 内逐文件扣减，超出部分照常转入下载
    hash_verified = 0
    state_written: set[str] = set()
    upsert_attempted = False
    if hash_verify:
        verified_rows: list[tuple] = []
        for ds, files in list(pending_by_ds.items()):
            if should_cancel is not None and should_cancel():
                break
            verified, rest = _hash_verify_pending(
                root, files, should_cancel=should_cancel
            )
            pending_by_ds[ds] = rest
            if not verified:
                continue
            hash_verified += len(verified)
            for f in verified:
                verified_rows.append(
                    (
                        f.path,
                        f.sha256,
                        f.sha256,
                        f.size,
                        str(_target_path(root, f.path)),
                        f.layout,
                        ds,
                    )
                )
                _note_dt(ds, f.path)
            log.info(
                "[MODELSCOPE] %s: %d 个文件本地哈希与远端一致，免下载补登记",
                ds,
                len(verified),
            )
        if verified_rows:
            upsert_attempted = True
            state_written.update(
                str(p)
                for p in _upsert_state_rows_multi(
                    root, verified_rows, state_dbs=state_dbs
                )
            )

    remaining = sum(len(v) for v in pending_by_ds.values())
    if remaining == 0:
        log.info("[MODELSCOPE] 增量全部经本地哈希验证一致，无需下载")
        return {
            "status": "ok",
            "downloaded": 0,
            "errors": 0,
            "downloaded_bytes": 0,
            "hash_verified": hash_verified,
            "touched_partitions": {k: sorted(v) for k, v in touched_partitions.items()},
            "state_dbs": [str(p) for p in state_dbs],
            "state_dbs_written": sorted(state_written),
            "diff": report,
            "elapsed_sec": round(time.time() - started, 1),
        }

    workers = _workers()
    batch = max(workers, workers * BATCH_FACTOR)
    timeout = httpx.Timeout(30.0, read=300.0, write=120.0, pool=60.0)
    downloaded = errors = downloaded_bytes = 0
    error_samples: list[str] = []
    per_dataset: dict[str, dict[str, Any]] = {}
    cancelled = False
    ep, repo, rev = report["endpoint"], report["repo_id"], report["revision"]

    with httpx.Client(
        follow_redirects=True, timeout=timeout, headers=_headers()
    ) as client:
        for ds, pending in pending_by_ds.items():
            if not pending:
                continue
            if should_cancel is not None and should_cancel():
                cancelled = True
                break
            _progress(
                progress_cb,
                "dataset_start",
                dataset=ds,
                pending=len(pending),
                bytes=sum(f.size for f in pending),
            )
            ds_downloaded = ds_errors = done_in_ds = 0
            for start in range(0, len(pending), batch):
                if should_cancel is not None and should_cancel():
                    cancelled = True
                    break
                window = pending[start : start + batch]
                done_rows: list[tuple] = []
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {}
                    for f in window:
                        try:
                            target = _target_path(root, f.path)
                        except ValueError as exc:
                            # 投毒/非法路径按单文件错误记录，不 fatal 整次运行
                            ds_errors += 1
                            if len(error_samples) < 20:
                                error_samples.append(f"{f.path}: {exc}")
                            continue
                        futures[
                            pool.submit(
                                _download_one,
                                client,
                                _file_url(ep, repo, rev, f.path),
                                f,
                                target,
                            )
                        ] = (f, target)
                    for fut in as_completed(futures):
                        f, target = futures[fut]
                        try:
                            fut.result()
                            ds_downloaded += 1
                            downloaded_bytes += f.size
                            if f.sha256:
                                # 空 sha 不登记：避免把既有好登记降级为空
                                done_rows.append(
                                    (
                                        f.path,
                                        f.sha256,
                                        f.sha256,
                                        f.size,
                                        str(target),
                                        f.layout,
                                        ds,
                                    )
                                )
                            _note_dt(ds, f.path)
                        except Exception as exc:  # noqa: BLE001
                            ds_errors += 1
                            if len(error_samples) < 20:
                                error_samples.append(f"{f.path}: {exc}")
                            log.warning("[MODELSCOPE] %s 下载失败: %s", f.path, exc)
                        done_in_ds += 1
                        if done_in_ds % FILE_PROGRESS_STEP == 0:
                            _progress(
                                progress_cb,
                                "file",
                                dataset=ds,
                                done=done_in_ds,
                                total=len(pending),
                                downloaded=downloaded_bytes,
                                errors=errors + ds_errors,
                            )
                # 每批完成即登记全部状态库副本，中断重跑据此跳过
                if done_rows:
                    upsert_attempted = True
                    state_written.update(
                        str(p)
                        for p in _upsert_state_rows_multi(
                            root, done_rows, state_dbs=state_dbs
                        )
                    )
            downloaded += ds_downloaded
            errors += ds_errors
            per_dataset[ds] = {
                "pending": len(pending),
                "downloaded": ds_downloaded,
                "errors": ds_errors,
                "bytes": sum(f.size for f in pending),
            }
            _progress(
                progress_cb,
                "dataset_done",
                dataset=ds,
                downloaded=ds_downloaded,
                errors=ds_errors,
            )
            log.info(
                "[MODELSCOPE] %s: 下载 %d 失败 %d (待下载 %d)",
                ds,
                ds_downloaded,
                ds_errors,
                len(pending),
            )
            if cancelled:
                break

    status = "cancelled" if cancelled else ("partial" if errors else "ok")
    result: dict[str, Any] = {
        "status": status,
        "cancelled": cancelled,
        "downloaded": downloaded,
        "errors": errors,
        "downloaded_bytes": downloaded_bytes,
        "hash_verified": hash_verified,
        "error_samples": error_samples,
        "per_dataset": per_dataset,
        "touched_partitions": {k: sorted(v) for k, v in touched_partitions.items()},
        "state_dbs": [str(p) for p in state_dbs],
        "state_dbs_written": sorted(state_written),
        "diff": report,
        "elapsed_sec": round(time.time() - started, 1),
    }
    if upsert_attempted:
        failures = sorted({str(p) for p in state_dbs} - state_written)
        if failures:
            # 副本被锁不致命：其余副本仍支撑 fast-path，下次运行自动重试
            result["state_write_failures"] = failures
            log.warning(
                "[MODELSCOPE] 状态库副本写入失败（下次运行自动重试）: %s",
                ", ".join(failures),
            )
    return result
