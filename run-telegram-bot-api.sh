#!/bin/sh
set -eu

exec /usr/local/bin/telegram-bot-api \
  --api-id="${TELEGRAM_API_ID:?TELEGRAM_API_ID is required}" \
  --api-hash="${TELEGRAM_API_HASH:?TELEGRAM_API_HASH is required}" \
  --local \
  --http-port="${TELEGRAM_API_PORT:-8081}" \
  --dir="${BOT_API_DATA_DIR:-/var/lib/telegram-bot-api}"
