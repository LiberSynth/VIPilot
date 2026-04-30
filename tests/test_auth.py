"""
Tests that protected API endpoints return HTTP 401 for unauthenticated requests.

This catches bugs where the auth guard is accidentally removed or calls an
undefined function (which would produce a 500 instead of a 401).
"""
import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_401(response):
    """Assert a response is 401 Unauthorized (not 500 or anything else)."""
    assert response.status_code == 401, (
        f"Expected 401 Unauthorized, got {response.status_code}. "
        "The auth guard may be missing or broken."
    )


# ---------------------------------------------------------------------------
# /api/* endpoints (bp blueprint)
# ---------------------------------------------------------------------------

class TestApiBlueprintAuth:
    """All routes registered under the /api prefix must reject unauthenticated requests."""

    def test_post_run_now(self, client):
        _assert_401(client.post("/api/run-now"))

    def test_get_monitor(self, client):
        _assert_401(client.get("/api/monitor"))

    def test_get_schedule(self, client):
        _assert_401(client.get("/api/schedule"))

    def test_post_schedule(self, client):
        _assert_401(client.post("/api/schedule", json={"time": "12:00"}))

    def test_delete_schedule_slot(self, client):
        _assert_401(client.delete("/api/schedule/1"))

    def test_get_models(self, client):
        _assert_401(client.get("/api/models"))

    def test_post_model_activate(self, client):
        _assert_401(client.post("/api/models/some-model/activate"))

    def test_post_models_reorder(self, client):
        _assert_401(client.post("/api/models/reorder", json={"ids": ["a"]}))

    def test_get_text_models(self, client):
        _assert_401(client.get("/api/text-models"))

    def test_post_text_model_activate(self, client):
        _assert_401(client.post("/api/text-models/some-model/activate"))

    def test_post_text_model_note(self, client):
        _assert_401(client.post("/api/text-models/some-model/note", json={"note": "x"}))

    def test_post_text_model_body(self, client):
        _assert_401(client.post("/api/text-models/some-model/body", json={"body": {}}))

    def test_post_text_model_grade(self, client):
        _assert_401(client.post("/api/text-models/some-model/grade", json={"grade": "good"}))

    def test_post_text_models_reorder(self, client):
        _assert_401(client.post("/api/text-models/reorder", json={"ids": ["a"]}))

    def test_post_text_model_probe(self, client):
        _assert_401(client.post("/api/text-models/some-model/probe"))

    def test_post_video_model_grade(self, client):
        _assert_401(client.post("/api/video-models/some-model/grade", json={"grade": "good"}))

    def test_post_video_model_note(self, client):
        _assert_401(client.post("/api/video-models/some-model/note", json={"note": "x"}))

    def test_post_video_model_body(self, client):
        _assert_401(client.post("/api/video-models/some-model/body", json={"body": {}}))

    def test_post_video_model_probe(self, client):
        _assert_401(client.post("/api/video-models/some-model/probe"))

    def test_post_target_config(self, client):
        _assert_401(client.post("/api/targets/some-target/targets-config", json={"targets_config": {}}))

    def test_get_batch_logs(self, client):
        _assert_401(client.get("/api/batch/some-batch/logs"))

    def test_post_reseed(self, client):
        _assert_401(client.post("/api/reseed"))

    def test_post_clear_history(self, client):
        _assert_401(client.post("/api/clear_history"))

    def test_get_workflow_state(self, client):
        _assert_401(client.get("/api/workflow/state"))

    def test_post_workflow_start(self, client):
        _assert_401(client.post("/api/workflow/start"))

    def test_post_workflow_pause(self, client):
        _assert_401(client.post("/api/workflow/pause"))

    def test_post_workflow_deep_debugging(self, client):
        _assert_401(client.post("/api/workflow/deep_debugging"))

    def test_post_workflow_use_donor(self, client):
        _assert_401(client.post("/api/workflow/use_donor"))

    def test_get_donors_count(self, client):
        _assert_401(client.get("/api/donors/count"))

    def test_post_workflow_approve_movies(self, client):
        _assert_401(client.post("/api/workflow/approve_movies"))

    def test_post_publication_counter_reset(self, client):
        """The endpoint that previously called an undefined function (_auth_required)
        and returned 500 instead of 401."""
        _assert_401(client.post("/api/publication-counter/reset"))

    def test_post_cycle_config_words_per_second(self, client):
        _assert_401(client.post("/api/cycle-config/words-per-second"))

    def test_post_cycle_config_good_samples_count(self, client):
        _assert_401(client.post("/api/cycle-config/good-samples-count"))

    def test_post_workflow_emulation(self, client):
        _assert_401(client.post("/api/workflow/emulation"))

    def test_get_monitor_batch_entries(self, client):
        _assert_401(client.get("/api/monitor/batch/some-batch/entries"))

    def test_get_monitor_log_entries(self, client):
        _assert_401(client.get("/api/monitor/log/some-log/entries"))

    def test_get_monitor_orphan_entries(self, client):
        _assert_401(client.get("/api/monitor/orphan-entries"))

    def test_post_monitor_batch_delete(self, client):
        _assert_401(client.post("/api/monitor/batch/some-batch/delete"))

    def test_post_batch_reset_pipeline(self, client):
        _assert_401(client.post("/api/batch/some-batch/reset/story"))

    def test_post_workflow_restart(self, client):
        _assert_401(client.post("/api/workflow/restart"))

    def test_get_batch_video(self, client):
        _assert_401(client.get("/api/batch/some-batch/video"))

    def test_get_story(self, client):
        _assert_401(client.get("/api/story/some-story"))

    def test_get_batch_publish_frame(self, client):
        _assert_401(client.get("/api/batch/some-batch/publish-frame"))

    def test_post_import_update_package(self, client):
        _assert_401(client.post("/api/import-update-package"))

    def test_get_export_update_package(self, client):
        _assert_401(client.get("/api/export-update-package"))


# ---------------------------------------------------------------------------
# /production/* endpoints (production_bp blueprint)
# ---------------------------------------------------------------------------

class TestProductionBlueprintAuth:
    """All routes on the production blueprint must reject unauthenticated requests."""

    def test_get_stories(self, client):
        _assert_401(client.get("/production/stories"))

    def test_post_story_pin(self, client):
        _assert_401(client.post("/production/story/1/pin"))

    def test_get_stories_filter_ids(self, client):
        _assert_401(client.get("/production/stories/filter-ids"))

    def test_get_stories_pool(self, client):
        _assert_401(client.get("/production/stories/pool"))

    def test_get_stories_good_pool_count(self, client):
        _assert_401(client.get("/production/stories/good_pool_count"))

    def test_post_env(self, client):
        _assert_401(client.post("/production/env"))

    def test_get_movies_filter_ids(self, client):
        _assert_401(client.get("/production/movies/filter-ids"))

    def test_get_movies(self, client):
        _assert_401(client.get("/production/movies"))

    def test_get_movie_video(self, client):
        _assert_401(client.get("/production/movie/1/video"))

    def test_post_movie_grade(self, client):
        _assert_401(client.post("/production/movie/1/grade"))

    def test_post_story_grade(self, client):
        _assert_401(client.post("/production/story/1/grade"))

    def test_delete_story(self, client):
        _assert_401(client.delete("/production/story/1/delete"))

    def test_delete_movie(self, client):
        _assert_401(client.delete("/production/movie/1/delete"))

    def test_post_story_draft(self, client):
        _assert_401(client.post("/production/story/draft"))

    def test_post_story_content(self, client):
        _assert_401(client.post("/production/story/1/content"))

    def test_post_stories_delete_bad(self, client):
        _assert_401(client.post("/production/stories/delete_bad"))

    def test_post_movies_delete_bad(self, client):
        _assert_401(client.post("/production/movies/delete_bad"))

    def test_get_movies_good_meta(self, client):
        _assert_401(client.get("/production/movies/good_meta"))

    def test_get_movie_download(self, client):
        _assert_401(client.get("/production/movies/1/download"))

    def test_post_movies_upload(self, client):
        _assert_401(client.post("/production/movies/upload"))

    def test_post_video_generate(self, client):
        _assert_401(client.post("/production/video/generate"))

    def test_post_story_generate(self, client):
        _assert_401(client.post("/production/story/generate"))
