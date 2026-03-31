#!/bin/bash
set -e

# Install Python dependencies if pyproject.toml is present
if [ -f pyproject.toml ]; then
  uv sync --frozen 2>/dev/null || uv sync || true
fi
