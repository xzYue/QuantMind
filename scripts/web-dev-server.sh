#!/bin/bash
# QuantMind Web 开发服务器启动器（供 launchd 调用）
#
# 为什么要由 launchd 托管：以前用 nohup 从 DSH 会话里起 dev server，
# 会话结束时整个进程组被回收，nohup 挡不住，于是 localhost:3001 隔几小时
# 就静默消失。launchd 把进程挂到用户域，脱离会话生命周期，并负责崩溃重启。
#
# 端口：electron/vite.config.ts 的默认值（3001）。宿主 3000 已被 grafana 占用。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/electron"

exec /usr/local/bin/npm run dev:react -- --host 0.0.0.0
