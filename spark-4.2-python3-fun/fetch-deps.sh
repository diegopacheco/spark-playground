#!/bin/bash
set -e
cd "$(dirname "$0")"

URL="https://files.pythonhosted.org/packages/source/p/pyspark/pyspark-4.2.0.tar.gz"
DEST="vendor/pyspark-4.2.0.tar.gz"
EXPECTED=450129423

mkdir -p vendor

if [ -f "$DEST" ] && [ "$(wc -c < "$DEST" | tr -d '[:space:]')" = "$EXPECTED" ]; then
  echo "pyspark tarball already present"
  exit 0
fi

echo "fetching pyspark 4.2.0 (450 MB, resumable) ..."
for a in $(seq 1 30); do
  curl -sS -C - -L --connect-timeout 20 --max-time 3600 -o "$DEST" "$URL" && break
  echo "retry $a ($(du -h "$DEST" 2>/dev/null | cut -f1) so far)"
  sleep 1
done

SIZE="$(wc -c < "$DEST" | tr -d '[:space:]')"
if [ "$SIZE" != "$EXPECTED" ]; then
  echo "FAIL: expected $EXPECTED bytes, got $SIZE"
  exit 1
fi
echo "pyspark tarball ready ($SIZE bytes)"
