import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import timedelta
from flask import Flask

os.environ.setdefault("FLASK_SECRET", "test-secret-for-tests")


@pytest.fixture
def app():
    """Minimal Flask app with API blueprints, no real DB connections."""
    flask_app = Flask("test", static_folder=None)
    flask_app.secret_key = "test-secret-for-tests"
    flask_app.permanent_session_lifetime = timedelta(days=7)
    flask_app.config["TESTING"] = True

    noop = MagicMock(return_value=None)

    with (
        patch("utils.limiter.limiter.init_app", noop),
        patch("log.log.write_log_entry", noop),
        patch("log.log.write_log", noop),
    ):
        from routes.api import bp, production_bp

        flask_app.register_blueprint(bp)
        flask_app.register_blueprint(production_bp)

    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()
