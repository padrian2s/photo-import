#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

# Install dependencies and run photo-import
uv run photo-import "$@"
