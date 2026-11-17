#!/usr/bin/env bash
# ModelScope 每日增量同步 —— 唤醒/登录补跑守卫
#
# 背景：macOS 的 cron 不会唤醒机器，也不会在唤醒后补跑错过的时刻。
# 本脚本被 launchd（StartCalendarInterval 07:00 + StartInterval 每小时 + RunAtLoad）
# 与 cron 同时调用，用「当日完成标记 + 目录锁」保证：
#   1) 每天最多成功跑一次；
#   2) 07:00 机器在睡，则唤醒后第一个 tick 立即补跑；
#   3) cron 与 launchd 同时触发时只有一个真正执行；
#   4) 容器/Docker 没起来时不写完成标记，留给下个 tick 重试；
#   5) 同一天连续失败达到上限后停手，不再每小时空转。
#
# 退出码：0 = 已是最新/今日已完成/未到时间/今日失败已达上限/锁被他人持有（本次跳过）；
#         1 = 同步部分失败（下个 tick 重试）；
#         2 = 护栏触发（当日暂停，需人工确认）或同步脚本致命错误（按失败计数重试）；
#         75 = 环境未就绪（docker/容器/日志/锁不可用，稍后重试）；
#         130/143 = 收到 INT/TERM（先释放自己的锁再以 130/143 退出，不吞信号）。
#
# 环境变量（默认值见下）：
#   QM_MODELSCOPE_GUARD_STATE_DIR   完成标记、锁与尝试计数目录（默认 ~/.quantmind/state）
#   QM_MODELSCOPE_GUARD_LOG         日志文件（默认 /tmp/modelscope_daily_sync.log）
#   QM_MODELSCOPE_CONTAINER         容器名（默认 quantmind）
#   QM_MODELSCOPE_GUARD_SYNC_ARGS   传给同步脚本的参数
#   QM_MODELSCOPE_GUARD_NOT_BEFORE  当日最早可执行时刻 HHMM（默认 0700）
#   QM_MODELSCOPE_GUARD_FORCE=1     手动强制补跑：忽略执行窗口、当日完成标记与失败次数上限
#   QM_MODELSCOPE_LOCK_MAX_AGE      锁 pid 文件缺失时的保守回收秒数（默认 21600）；
#                                   持锁进程存活时永不回收（不管多久），超 QM_MODELSCOPE_GUARD_STUCK_WARN 告警。
#   QM_MODELSCOPE_GUARD_STUCK_WARN  持锁时长超过该秒数即告警「疑似卡死」（默认 10800）
#   QM_MODELSCOPE_MAX_ATTEMPTS      同日最多失败次数，达到后当日停手（默认 6）
#   QM_MODELSCOPE_GUARD_TODAY / _NOW  时间注入（仅供测试）
set -uo pipefail

if [[ -z "${QM_MODELSCOPE_GUARD_STATE_DIR:-}" && -z "${HOME:-}" ]]; then
  printf "%s [guard] ERROR HOME 未设置且未提供 QM_MODELSCOPE_GUARD_STATE_DIR\n" "$(date "+%F %T")" >&2
  exit 75
fi
STATE_DIR="${QM_MODELSCOPE_GUARD_STATE_DIR:-${HOME}/.quantmind/state}"
LOG_FILE="${QM_MODELSCOPE_GUARD_LOG:-/tmp/modelscope_daily_sync.log}"
CONTAINER="${QM_MODELSCOPE_CONTAINER:-quantmind}"
SYNC_ARGS="${QM_MODELSCOPE_GUARD_SYNC_ARGS:---skip-datasets factor_dictionary,l1_l2_factors --max-pending-gb 30}"
NOT_BEFORE="${QM_MODELSCOPE_GUARD_NOT_BEFORE:-0700}"
FORCE="${QM_MODELSCOPE_GUARD_FORCE:-0}"
LOCK_MAX_AGE="${QM_MODELSCOPE_LOCK_MAX_AGE:-21600}"
LOCK_STUCK_WARN="${QM_MODELSCOPE_GUARD_STUCK_WARN:-10800}"
MAX_ATTEMPTS="${QM_MODELSCOPE_MAX_ATTEMPTS:-6}"

export TZ="${TZ:-Asia/Shanghai}"

state_file="${STATE_DIR}/modelscope_daily_sync.last_success"
lock_dir="${STATE_DIR}/modelscope_daily_sync.lock"
attempts_file="${STATE_DIR}/modelscope_daily_sync.attempts"

mkdir -p "$STATE_DIR" 2>/dev/null || true

log() {
  local ts line
  ts="$(date "+%F %T")"
  line="$ts [guard] $*"
  printf "%s\n" "$line"
  printf "%s\n" "$line" >>"$LOG_FILE" 2>/dev/null || true
}

# 日志必须可写：否则后面 docker exec 的 >> 重定向会让命令根本不执行（还会被误报成同步失败）
if ! : >>"$LOG_FILE" 2>/dev/null; then
  printf "%s [guard] ERROR 日志文件不可写：%s\n" "$(date "+%F %T")" "$LOG_FILE" >&2
  exit 75
fi

today="${QM_MODELSCOPE_GUARD_TODAY:-$(date +%F)}"
now_hhmm="${QM_MODELSCOPE_GUARD_NOW:-$(date +%H%M)}"
if [[ ! "$now_hhmm" =~ ^[0-9]{4}$ ]]; then
  log "WARN 非法时刻 now=${now_hhmm}，回退当前时间"
  now_hhmm="$(date +%H%M)"
fi
if [[ ! "$NOT_BEFORE" =~ ^[0-9]{4}$ ]]; then
  log "WARN 非法 NOT_BEFORE=${NOT_BEFORE}，回退 0700"
  NOT_BEFORE=0700
fi
# 数值型配置必须是纯数字：否则 (( )) 在 set -u 下报 unbound variable 直接终止脚本（表现为静默不同步）
if [[ ! "$LOCK_MAX_AGE" =~ ^[0-9]+$ ]] || (( 10#${LOCK_MAX_AGE:-0} <= 0 )); then
  log "WARN 非法 LOCK_MAX_AGE=${LOCK_MAX_AGE}，回退 21600"
  LOCK_MAX_AGE=21600
fi
if [[ ! "$MAX_ATTEMPTS" =~ ^[0-9]+$ ]] || (( 10#${MAX_ATTEMPTS:-0} <= 0 )); then
  log "WARN 非法 MAX_ATTEMPTS=${MAX_ATTEMPTS}，回退 6"
  MAX_ATTEMPTS=6
fi
if [[ ! "$LOCK_STUCK_WARN" =~ ^[0-9]+$ ]] || (( 10#${LOCK_STUCK_WARN:-0} <= 0 )); then
  log "WARN 非法 LOCK_STUCK_WARN=${LOCK_STUCK_WARN}，回退 10800"
  LOCK_STUCK_WARN=10800
fi

# 1) 今天已经成功过？
last_date=""
if [[ -f "$state_file" ]]; then
  last_date="$(sed -n "s/^date=\([0-9][0-9-]*\).*/\1/p" "$state_file" 2>/dev/null | head -1)"
fi
if [[ "$FORCE" != "1" && "$last_date" == "$today" ]]; then
  log "今日（${today}）已完成，跳过"
  exit 0
fi

# 2) 今天失败次数已达上限？
attempts=0
if [[ -f "$attempts_file" ]]; then
  a_date="$(sed -n "s/^date=\([0-9][0-9-]*\).*/\1/p" "$attempts_file" 2>/dev/null | head -1)"
  a_n="$(sed -n "s/.*attempts=\([0-9]*\).*/\1/p" "$attempts_file" 2>/dev/null | head -1)"
  if [[ "$a_date" == "$today" ]]; then
    attempts="${a_n:-0}"
  fi
fi
if [[ "$FORCE" != "1" ]] && (( attempts >= MAX_ATTEMPTS )); then
  log "今日已失败 ${attempts} 次（上限 ${MAX_ATTEMPTS}），暂停重试到下一天"
  exit 0
fi

# 3) 到点了吗？
if [[ "$FORCE" != "1" ]] && (( 10#$now_hhmm < 10#$NOT_BEFORE )); then
  log "未到当日执行窗口（now=${now_hhmm} < ${NOT_BEFORE}），跳过"
  exit 0
fi

# 4) 抢锁（mkdir 原子；失效锁用「原子改名 + 只让改名成功者清理」回收）
#    回收判据只看「锁登记的进程是否仍然存在、且确实是当初持锁的那个人」：
#      - 进程存活且启动时间不晚于锁创建 → 真持锁者，永不回收（长时间跑也不会被抢；疑似卡死只告警等人工）；
#      - 进程已不存在 → 直接回收（pid 复用与此无关）；
#      - 进程存活但启动时间晚于锁创建 +60s → pid 被复用，按失效锁回收。
lock_owner() { # 锁目录里登记的 pid（缺失时输出空）
  [[ -f "$lock_dir/pid" ]] || return 0
  tr -cd "0-9" <"$lock_dir/pid" 2>/dev/null | head -c 20
}

lock_mtime() { # 锁目录 mtime；BSD 与 GNU 的 stat 取值参数不同（GNU 的 -f 不失败，会给出文件系统信息）
  local mtime=0
  if [[ "$(uname -s)" == "Darwin" ]]; then
    mtime="$(stat -f %m "$lock_dir" 2>/dev/null || echo 0)"
  else
    mtime="$(stat -c %Y "$lock_dir" 2>/dev/null || echo 0)"
  fi
  [[ "${mtime:-0}" =~ ^[0-9]+$ ]] || mtime=0
  (( mtime > 0 )) || mtime="$(date +%s)" # 取不到 mtime 时按「刚创建」处理，不回收
  printf "%s" "$mtime"
}

path_inode() { # $1=路径 → inode 号（取不到输出空）
  # 锁的身份令牌用 inode：pid 可能还没写进去（mkdir 与 printf 之间），空 pid 会让
  # 「搬走的是不是刚才那把锁」校验双方都是空串而失效
  local p="$1" ino=""
  case "$(uname -s)" in
    Darwin) ino="$(stat -f %i "$p" 2>/dev/null || true)" ;;
    *) ino="$(stat -c %i "$p" 2>/dev/null || true)" ;;
  esac
  ino="${ino//[^0-9]/}"
  printf "%s" "$ino"
}

proc_start_epoch() { # $1=pid → 进程启动时间 epoch（取不到输出空）
  # LC_ALL=C 钉住 locale：日期解析依赖英文月份/星期缩写，本地化环境下会静默解析失败
  local pid="$1" lstart="" e=""
  lstart="$(LC_ALL=C ps -o lstart= -p "$pid" 2>/dev/null | sed -e "s/^[[:space:]]*//" -e "s/[[:space:]]*$//")"
  [[ -n "$lstart" ]] || return 0
  if [[ "$(uname -s)" == "Darwin" ]]; then
    e="$(LC_ALL=C date -j -f "%a %b %d %T %Y" "$lstart" +%s 2>/dev/null || true)"
  else
    e="$(LC_ALL=C date -d "$lstart" +%s 2>/dev/null || true)"
  fi
  [[ "$e" =~ ^[0-9]+$ ]] && printf "%s" "$e"
}

# 只删属于自己的锁：锁被回收后重建的锁属于别人（否则会连环抢锁）
release_lock() {
  # 被 TERM/INT 打断时也会走到这里：顺手清掉本次的 mktemp 输出文件，避免每次都留一个
  [[ -n "${tmp_out:-}" ]] && rm -f "$tmp_out" 2>/dev/null
  [[ "$(lock_owner)" == "$$" ]] || return 0
  rm -rf "$lock_dir" 2>/dev/null || true
}

take_lock() {
  mkdir "$lock_dir" 2>/dev/null || return 1
  printf "%s\n" "$$" >"$lock_dir/pid" 2>/dev/null || true
  trap "release_lock" EXIT
  trap "release_lock; exit 130" INT
  trap "release_lock; exit 143" TERM
  return 0
}

acquire_lock() {
  local owner="" age=0 mtime=0 pstart="" moved_pid="" lock_ino="" moved_ino="" replaced=0
  if take_lock; then
    return 0
  fi
  owner="$(lock_owner)"
  mtime="$(lock_mtime)"
  age=$(( $(date +%s) - mtime ))
  if [[ -n "$owner" ]] && kill -0 "$owner" 2>/dev/null; then
    pstart="$(proc_start_epoch "$owner")"
    if [[ ! "$pstart" =~ ^[0-9]+$ ]]; then
      # 解析失败时无法判断 pid 复用，只能保守当作真持锁者（否则可能抢真持锁者的锁）
      log "WARN 无法解析 pid=${owner} 的启动时间（ps/date 不可用？），保守按持锁处理，本次跳过"
      return 1
    fi
    if (( pstart > mtime + 60 )); then
      log "WARN 锁登记的 pid=${owner} 启动于锁创建之后（pid 复用），按失效锁处理"
    else
      if (( age >= LOCK_STUCK_WARN )); then
        log "WARN 锁被 pid=${owner} 持有 ${age}s（>=${LOCK_STUCK_WARN}s，疑似卡死；持锁进程仍存活故不回收，请人工确认 ${lock_dir}）"
      else
        log "WARN 锁被 pid=${owner} 持有（已 ${age}s），本次跳过"
      fi
      return 1
    fi
  elif [[ -d "$lock_dir" ]] && [[ -z "$owner" ]] && (( age < LOCK_MAX_AGE )); then
    # pid 文件缺失但锁很新：可能刚 mkdir 完还没写 pid，也可能是持锁进程被 SIGKILL 前一刻，
    # 一律按「被持有」保守处理，等锁龄超过 LOCK_MAX_AGE 再回收
    log "WARN 锁目录存在但 pid 文件缺失（age=${age}s），按被持有处理，本次跳过"
    return 1
  fi
  if [[ ! -d "$lock_dir" ]]; then
    # 目录已不存在（刚被持有者释放，或状态目录压根建不出来）：能建就建，建不出来按环境故障返回
    if take_lock; then
      return 0
    fi
    return 1
  fi
  lock_ino="$(path_inode "$lock_dir")"
  if ! mv "$lock_dir" "${lock_dir}.dead.$$" 2>/dev/null; then
    return 1
  fi
  # 搬走的必须就是刚才判定失效的那把锁：并发者可能在这几毫秒内回收旧锁并重建新锁，
  # 若把别人的新锁搬走，就会出现两个同步同时跑（TOCTOU）
  moved_ino="$(path_inode "${lock_dir}.dead.$$")"
  if [[ -n "$lock_ino" ]] && [[ -n "$moved_ino" ]]; then
    [[ "$lock_ino" == "$moved_ino" ]] || replaced=1
  else
    # inode 取不到（极少数文件系统）：退回 pid 比较；两者都为空时无法判别，按下同处理
    if [[ -f "${lock_dir}.dead.$$/pid" ]]; then
      moved_pid="$(tr -cd "0-9" <"${lock_dir}.dead.$$/pid" 2>/dev/null | head -c 20)"
    fi
    [[ "$moved_pid" == "${owner:-}" ]] || replaced=1
  fi
  if (( replaced )); then
    if [[ ! -e "$lock_dir" ]] && mv "${lock_dir}.dead.$$" "$lock_dir" 2>/dev/null; then
      log "WARN 回收瞬间锁被替换（搬走的 inode=${moved_ino:-?}/pid=${moved_pid:-none}，预期 inode=${lock_ino:-?}/pid=${owner:-none}），已放回并跳过"
    else
      log "ERROR 回收瞬间锁被替换且无法放回，请人工检查并清理 ${lock_dir}.dead.$$"
    fi
    return 1
  fi
  rm -rf "${lock_dir}.dead.$$"
  if ! take_lock; then
    log "WARN 锁在回收后被另一个进程抢先占用，本次跳过"
    return 1
  fi
  log "回收失效锁（pid=${owner:-unknown}，age=${age}s）"
  return 0
}

if ! acquire_lock; then
  if [[ -d "$lock_dir" ]]; then
    log "已有另一个同步进程在跑，本次跳过"
    exit 0
  fi
  log "ERROR 无法创建锁 ${lock_dir}（权限或磁盘空间？），稍后重试"
  exit 75
fi

# 5) 环境检查：docker 可用 + 容器在跑
if ! command -v docker >/dev/null 2>&1; then
  log "找不到 docker 命令，跳过（稍后重试）"
  exit 75
fi
if [[ -z "$(docker ps --filter "name=^${CONTAINER}$" --filter status=running -q 2>/dev/null || true)" ]]; then
  log "容器 ${CONTAINER} 未运行（Docker Desktop 可能未启动），稍后重试"
  exit 75
fi

# 6) 执行同步，只解析本次新增的日志段
tmp_out="$(mktemp "${TMPDIR:-/tmp}/qm-modelscope-guard.XXXXXX" 2>/dev/null)" || tmp_out=""
if [[ -z "$tmp_out" || ! -f "$tmp_out" ]]; then
  log "ERROR mktemp 失败，无法解析本次同步输出"
  exit 75
fi
offset=0
if [[ -f "$LOG_FILE" ]]; then
  offset="$(wc -c <"$LOG_FILE" | tr -d " ")"
fi
log "开始同步：docker exec $CONTAINER python /app/backend/scripts/modelscope_daily_sync.py $SYNC_ARGS"
set -f # SYNC_ARGS 需要按空格拆分（SC2086），但不能被当前目录的通配符展开
# shellcheck disable=SC2086
docker exec "$CONTAINER" python /app/backend/scripts/modelscope_daily_sync.py $SYNC_ARGS >>"$LOG_FILE" 2>&1
rc=$?
set +f

if [[ -f "$LOG_FILE" ]]; then
  tail -c +"$((offset + 1))" "$LOG_FILE" >"$tmp_out" 2>/dev/null || true
fi
if [[ ! -s "$tmp_out" ]]; then
  log "WARN 未取到本次同步的日志段（日志可能被轮转/清空），状态字段解析将为空"
fi

pick() { # $1 = 正则，$2 = first/last（从本次日志段里取）
  local pat="$1" which="${2:-first}" out=""
  if [[ "$which" == "last" ]]; then
    out="$(grep -oE "$pat" "$tmp_out" 2>/dev/null | tail -1)"
  else
    out="$(grep -oE "$pat" "$tmp_out" 2>/dev/null | head -1)"
  fi
  # 键值切分同时兼容 `"key": 1`（indent=2）与 `"key":1`（紧凑 JSON）
  out="$(printf "%s" "$out" | sed -E 's/^[^:]*:[[:space:]]*//; s/^"//; s/"$//')"
  printf "%s" "$out"
}

# 顶层 status（缩进两格），避免误取 phases.* 里的同名字段
status="$(pick '^  "status": *"[a-z_]*"|^\{"status": *"[a-z_]*"' last)"
files="$(pick '"downloaded": *[0-9]*' first)"
bytes="$(pick '"downloaded_bytes": *[0-9]*' first)"
elapsed="$(pick '"elapsed_sec": *[0-9.]*' first)"
pending_bytes="$(pick '"pending_bytes": *[0-9]*' first)"

write_state() { # $1=status $2=exit $3=files $4=bytes $5=elapsed
  local tmp="${state_file}.tmp.$$"
  if printf "date=%s time=%s status=%s files=%s bytes=%s elapsed=%s exit=%s\n" \
      "$today" "$(date +%T)" "$1" "$3" "$4" "${5:-?}" "$2" >"$tmp" 2>/dev/null; then
    if mv -f "$tmp" "$state_file" 2>/dev/null; then
      return 0
    fi
  fi
  rm -f "$tmp" 2>/dev/null || true
  return 1
}

if [[ $rc -eq 0 ]]; then
  if ! write_state "${status:-unknown}" 0 "${files:-0}" "${bytes:-0}" "${elapsed:-?}"; then
    log "ERROR 无法写入当日完成标记 ${state_file}，稍后重试"
    rm -f "$tmp_out"
    exit 75
  fi
  rm -f "$attempts_file" 2>/dev/null || true
  log "同步成功：status=${status:-unknown} files=${files:-0} bytes=${bytes:-0} elapsed=${elapsed:-?}"
  rm -f "$tmp_out"
  exit 0
fi

# 护栏（待下载过大）需要人工确认 --allow-large：当日不再重试，避免每小时刷屏
# 结构化信号优先（同步脚本在护栏时输出顶层 "status": "guard" 并以 exit 2 退出）
guard_pending="$(pick '"pending_bytes": *[0-9]*' last)" # guard 块在报告里最靠后，取最后一处=总量
if [[ -n "$guard_pending" ]]; then pending_bytes="$guard_pending"; fi
if [[ $rc -eq 2 ]] && { [[ "$status" == "guard" ]] || grep -qE '"status":[[:space:]]*"guard"|超过护栏' "$tmp_out" 2>/dev/null; }; then
  if ! write_state "pending_too_large" 2 "${files:-0}" "${pending_bytes:-${bytes:-0}}" "${elapsed:-?}"; then
    log "ERROR 无法写入当日标记 ${state_file}（无标记会每小时重跑并再次撞护栏）"
    rm -f "$tmp_out"
    exit 75
  fi
  log "护栏触发：待下载 ${pending_bytes:-${bytes:-?}} 字节超过上限，今日暂停；如需强制拉取请手动执行并加 --allow-large"
  rm -f "$tmp_out"
  exit 2
fi

n=$(( attempts + 1 ))
# 原子写：直接 `>` 覆盖时若被 truncate 会把 attempts 归零，当天重试超过上限
if ! { printf "date=%s attempts=%s last=%s rc=%s\n" "$today" "$n" "$(date +%T)" "$rc" >"${attempts_file}.tmp.$$" 2>/dev/null && mv -f "${attempts_file}.tmp.$$" "$attempts_file" 2>/dev/null; }; then
  rm -f "${attempts_file}.tmp.$$" 2>/dev/null || true
  log "WARN 无法写入失败计数 ${attempts_file}（下次失败仍从当前计数开始）"
fi
log "同步失败：exit=${rc}（今日第 ${n}/${MAX_ATTEMPTS} 次），不写当日完成标记，下个 tick 重试"
rm -f "$tmp_out"
exit "$rc"
