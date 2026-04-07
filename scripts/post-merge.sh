#!/bin/bash
set -e

uv sync
playwright install chromium || true
