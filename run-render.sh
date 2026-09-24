#!/usr/bin/env bash
set -u

api_port="${TELEGRAM_API_PORT:-8081}"
/app/run-telegram-bot-api.sh &
api_pid=$!
app_pid=""

shutdown() {
  if [ -n "$app_pid" ]; then
    kill "$app_pid" 2>/dev/null || true
    wait "$app_pid" 2>/dev/null || true
  fi
  kill "$api_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
}
trap shutdown EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Waiting for the local Telegram Bot API on 127.0.0.1:${api_port}..."
ready=0
for _ in {1..90}; do
  if ! kill -0 "$api_pid" 2>/dev/null; then
    wait "$api_pid"
    api_status=$?
    echo "Telegram Bot API exited during startup (status ${api_status}). Check TELEGRAM_API_ID, TELEGRAM_API_HASH, and the Bot API startup log above." >&2
    exit 1
  fi
  if python -c 'import socket,sys; s=socket.socket(); s.settimeout(0.5); result=s.connect_ex(("127.0.0.1", int(sys.argv[1]))); s.close(); sys.exit(0 if result == 0 else 1)' "$api_port" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "Telegram Bot API did not listen on 127.0.0.1:${api_port} within 90 seconds; refusing to start the app without it." >&2
  exit 1
fi

echo "Telegram Bot API is listening; starting the download app."
python /app/app.py &
app_pid=$!

wait -n "$api_pid" "$app_pid"
status=$?
echo "A required service exited (status ${status}); stopping the other process." >&2
exit "$status"