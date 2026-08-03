#!/bin/sh
set -e

MIRURO_PROXY_PORT=${MIRURO_PROXY_PORT:-8099}
export ANIRAKU_MIRURO_PROXY_URL="http://127.0.0.1:$MIRURO_PROXY_PORT"

python3 /app/proxy.py &
PROXY_PID=$!

for i in $(seq 1 60); do
    if curl -sf http://127.0.0.1:$MIRURO_PROXY_PORT/health > /dev/null 2>&1; then
        break
    fi
    sleep 2
done

/app/aniraku-server &
GO_PID=$!

trap "kill $GO_PID $PROXY_PID 2>/dev/null; wait" EXIT INT TERM

wait $GO_PID
