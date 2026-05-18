# _build.py - pinned app build number.
#
# Purpose: keep BUILD between runs when git is unavailable on prod.
# Rewritten by scripts/set_build_number.py during deployment.
# Used in upgrade.py to compare against DB build_number.
BUILD = "2398"
