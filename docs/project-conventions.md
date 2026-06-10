# VIPilot — Автоматическая публикация видеоисторий

Flask-приложение для автоматической публикации видеоисторий в VK, Дзен и Рутьюб через систему из шести пайплайнов.

## Run & Operate

*   **Run application:** `python main.py`
*   **Env vars:** `FAL_API_KEY`, `OPENROUTER_API_KEY`, `VK_USER_TOKEN`, `DZEN_OAUTH_TOKEN`, `DATABASE_URL`
*   **Post-task check:** Always run `bash scripts/check_conventions.sh` before reporting task completion. Fix any violations.

## Stack

*   **Framework:** Flask
*   **ORM:** _Populate as you build_
*   **Validation:** _Populate as you build_
*   **Build Tool:** _Populate as you build_
*   **Runtime:** Python

## Where things live

*   **Pipelined Workflow:** `pipelines/` (modules: `planning.py`, `story.py`, `video.py`, `transcode.py`, `publish.py`, `cleanup.py`)
*   **Web Routes:** `routes/web.py` (frontend), `routes/api.py` (backend API)
*   **Database Schema:** Defined implicitly by `db/migrations.py` and `db/seed.py`
*   **Configuration:** `settings` and `cycle_config` tables in DB via `db/cycle_config.py`
*   **AI Models:** `ai_models` table in DB, managed by `routes/api.py`
*   **Logging:** `log/log.py`
*   **Frontend Assets:** `static/` (CSS, JS, images)
*   **HTML Templates:** `templates/` (Jinja2)
*   **Core Application Logic:** `main.py`, `common/`, `utils/`
*   **API Clients:** `clients/` (VK, Dzen, RuTube)
*   **Browser Automation Services:** `services/` (`dzen_browser.py`, `rutube_browser.py`, `vkvideo_browser.py`)

## Architecture decisions

*   **Pipeline-based processing:** A 6-stage pipeline (planning, story, video, transcode, publish, cleanup) handles video story generation and publication, with each stage running in its own thread.
*   **Centralized error handling:** Custom `AppException` and `FatalError` classes are used for predictable and invariant-breaking errors, caught by the main loop.
*   **Strict environment access:** Environment variables and settings are read into a snapshot at the beginning of each pipeline run to ensure consistency and prevent race conditions.
*   **Browser automation for complex platforms:** Dzen and RuTube (and VK Video) publication is handled via Playwright-driven browser automation due to complex UI interactions.
*   **Single point of DB access:** All database interactions are routed through `db/db.py` to ensure consistent connection management and transaction handling.
*   **All JS dialogs and modals inherit from `Dialog`.**
*   **Styling for `<textarea>` is done via tag-selector in `common.css`, not inline or via class.**

## Product

*   Automated scheduling and batching of video story generation.
*   AI-powered text story generation (OpenRouter) and video generation (fal.ai).
*   Video transcoding for platform compatibility (ffmpeg).
*   Multi-platform publishing to VK (stories/wall), Dzen, and RuTube.
*   Monitoring interface for pipeline status and logs.
*   User-configurable production cycle settings (prompts, duration, approval flows).

## User preferences

- **Strict adherence to conventions:** `scripts/check_conventions.sh` must pass before reporting task completion. Disabling or bypassing the `pre-commit` hook is forbidden.
- **Error handling in pipelines:** Exceptions should bubble up; direct `try-except` around DB operations is forbidden **inside pipelines**.
- **Logging:** Use `write_log_entry(batch_id, category, message, level)` and `app_log(...)` at the call site; logging wrappers (`log_*()` helpers, message-builder functions for log calls) are forbidden outside `log/log.py`.
- **Environment variables:** Read via `refresh_environment()` only; direct `db_get`/`env_get` for specific keys outside this function is forbidden.
- **Thread environment:** Threads must read environment settings from a snapshot (`snap = environment.snapshot()`).
- **GUID/UUID formatting:** Use `fmt_id_msg` for all GUID/UUID in log messages.

## Gotchas

*   `pre-commit` hook: The `.git/hooks/pre-commit` hook is mandatory and should never be disabled or bypassed. Violations must be fixed.
*   Direct DB access: Do not directly access environment variables or settings from the DB; use `environment.*` or `snap.*`.
*   Logging bypass: Do not use `print()` for logging or directly write to log tables; use the provided `log/log.py` functions.
*   `notify_failure` exceptions: `notify_failure` handles its own exceptions; do not wrap its calls in external `try-except` blocks.

## Pointers

*   **Code Conventions:** `docs/conventions.md`
*   **Flask Documentation:** [https://flask.palletsprojects.com/](https://flask.palletsprojects.com/)
*   **OpenRouter API:** [https://openrouter.ai/docs](https://openrouter.ai/docs)
*   **Fal.ai API:** [https://docs.fal.ai/](https://docs.fal.ai/)
*   **FFmpeg Documentation:** [https://ffmpeg.org/documentation.html](https://ffmpeg.org/documentation.html)
*   **Playwright Documentation:** [https://playwright.dev/python/docs/intro](https://playwright.dev/python/docs/intro)
