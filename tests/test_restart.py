import os
from unittest.mock import MagicMock, patch

import pytest

from common import restart as restart_mod


class TestWindowsServiceName:
    def test_explicit_env(self, monkeypatch):
        monkeypatch.setenv("VIPILOT_SERVICE_NAME", "CustomService")
        monkeypatch.delenv("SESSIONNAME", raising=False)
        assert restart_mod._windows_service_name() == "CustomService"

    def test_session_services_default(self, monkeypatch):
        monkeypatch.delenv("VIPILOT_SERVICE_NAME", raising=False)
        monkeypatch.setenv("SESSIONNAME", "Services")
        assert restart_mod._windows_service_name() == "VIPilotService"

    def test_console_session_no_default(self, monkeypatch):
        monkeypatch.delenv("VIPILOT_SERVICE_NAME", raising=False)
        monkeypatch.setenv("SESSIONNAME", "Console")
        assert restart_mod._windows_service_name() is None


class TestDoRestart:
    def test_windows_service_path(self, monkeypatch):
        monkeypatch.setattr(restart_mod.platform, "system", lambda: "Windows")
        monkeypatch.setattr(restart_mod, "_windows_service_name", lambda: "VIPilotService")
        with patch.object(restart_mod, "_restart_via_windows_service") as svc:
            with patch.object(restart_mod, "_restart_via_windows_spawn") as spawn:
                restart_mod._do_restart()
        svc.assert_called_once_with("VIPilotService")
        spawn.assert_not_called()

    def test_windows_spawn_path(self, monkeypatch):
        monkeypatch.setattr(restart_mod.platform, "system", lambda: "Windows")
        monkeypatch.setattr(restart_mod, "_windows_service_name", lambda: None)
        with patch.object(restart_mod, "_restart_via_windows_service") as svc:
            with patch.object(restart_mod, "_restart_via_windows_spawn") as spawn:
                restart_mod._do_restart()
        spawn.assert_called_once()
        svc.assert_not_called()

    def test_unix_execv_path(self, monkeypatch):
        monkeypatch.setattr(restart_mod.platform, "system", lambda: "Linux")
        with patch.object(restart_mod, "_restart_via_execv") as execv:
            restart_mod._do_restart()
        execv.assert_called_once()


class TestScheduleAppRestart:
    def test_starts_daemon_thread(self):
        with patch.object(restart_mod.threading, "Thread") as thread_cls:
            thread_cls.return_value = MagicMock()
            restart_mod.schedule_app_restart()
        thread_cls.assert_called_once()
        kwargs = thread_cls.call_args.kwargs
        assert kwargs["target"] is restart_mod._do_restart
        assert kwargs["daemon"] is True
