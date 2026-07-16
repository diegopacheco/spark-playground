#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

UI=http://localhost:8174

echo "[links] spark 4.2 python3 ui -> $UI"

open "$UI"
