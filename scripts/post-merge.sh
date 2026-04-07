#!/bin/bash
set -e

uv sync
playwright install chromium --with-deps 2>/dev/null || playwright install chromium || true
