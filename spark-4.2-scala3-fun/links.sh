#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

UI=http://localhost:8173

echo "[links] spark 4.2 scala3 ui -> $UI"

open "$UI"
