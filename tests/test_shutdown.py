"""Тесты кооперативной остановки приложения."""

from common.exceptions import ShutdownRequested
from common.shutdown import is_shutting_down, request_shutdown


def test_shutdown_requested_is_base_exception():
    assert issubclass(ShutdownRequested, BaseException)
    assert not issubclass(ShutdownRequested, Exception)


def test_request_shutdown_sets_flag():
    request_shutdown()
    try:
        assert is_shutting_down()
    finally:
        import common.shutdown as mod
        mod._shutdown_requested = False
        mod._interrupt_count = 0
