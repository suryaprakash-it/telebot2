#!/usr/bin/env bash
set -u

/app/run-telegram-bot-api.sh &
api_pid=$!
python /app/app.py &
app_pid=$!

shutdown() {
  kill "$api_pid" "$app_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
  wait "$app_pid" 2>/dev/null || true
}
trap shutdown INT TERM

wait -n "$api_pid" "$app_pid"
status=$?
shutdown
exit "$status"
