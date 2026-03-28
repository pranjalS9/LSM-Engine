#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
MAIN_OUT="$BUILD_DIR/classes/java/main"

rm -rf "$BUILD_DIR"
mkdir -p "$MAIN_OUT"

find "$ROOT_DIR/src/main/java" -name "*.java" > "$BUILD_DIR/sources_main.txt"
javac -d "$MAIN_OUT" @"$BUILD_DIR/sources_main.txt"

java -cp "$MAIN_OUT" lsm.engine.Bench "$@"

