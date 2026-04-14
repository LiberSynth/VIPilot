#!/bin/bash
set -e

uv sync
playwright install chromium || true

bash scripts/check_conventions.sh
