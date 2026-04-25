#!/bin/bash
# Recall launcher — reindex, start the server, open the browser.
#
# Usage:   ./launch.sh                # default port 8765
#          ./launch.sh 9000           # custom port
#          PORT=9000 ./launch.sh
#          ./launch.sh --no-index     # skip reindex (server starts immediately)
#          ./launch.sh --demo         # use demo/chat.db instead of live
#
# Ctrl+C stops the server.

set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8765}"
SKIP_INDEX=0
DEMO=0

for a in "$@"; do
  case "$a" in
    --no-index|-n)  SKIP_INDEX=1 ;;
    --demo|-d)      DEMO=1 ;;
    --help|-h)
      sed -n '2,11p' "$0"; exit 0 ;;
    [0-9]*)         PORT="$a" ;;
    *)              echo "unknown arg: $a" >&2; exit 2 ;;
  esac
done

if [ "$DEMO" = "1" ]; then
  export RECALL_CHAT_DB="demo/chat.db"
  export RECALL_INDEX_DB="demo/index.db"
  if [ ! -f "$RECALL_CHAT_DB" ] || [ ! -f "$RECALL_INDEX_DB" ]; then
    echo "→ building demo data…"
    python3 demo/build_demo.py
  fi
  echo "→ demo mode (chat=$RECALL_CHAT_DB index=$RECALL_INDEX_DB)"
fi

if [ "$SKIP_INDEX" = "0" ] && [ "$DEMO" = "0" ]; then
  echo "→ reindexing from ~/Library/Messages/chat.db…"
  python3 -m recall.cli index
fi

# Free the port if a previous server is still bound.
if lsof -ti tcp:"$PORT" >/dev/null 2>&1; then
  echo "→ port $PORT in use, killing old listener…"
  lsof -ti tcp:"$PORT" | xargs kill 2>/dev/null || true
  sleep 0.3
fi

URL="http://127.0.0.1:$PORT"
echo "→ starting server at $URL"
python3 -m recall.cli serve --port "$PORT" &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null || true" EXIT INT TERM

# Wait until the server actually answers, then open the browser.
for _ in $(seq 1 30); do
  if curl -fsS -o /dev/null "$URL/health" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

open "$URL"
echo "→ opened $URL · ctrl+c to stop"
wait "$SERVER_PID"
