"""Unit tests for publish auth detection (URL + Rutube studio login modal)."""

from unittest.mock import MagicMock, patch

import pytest

from services.publish_auth_check import (
    login_screen_visible,
    raise_if_login_required,
    wait_raise_if_login_required,
)
from clients.rutube import RutubeCsrfExpired


def _mock_page(*, url: str = "", visible_texts: set[str] | None = None, input_visible: bool = False):
    visible_texts = visible_texts or set()
    page = MagicMock()
    page.url = url

    def _get_by_text(text, exact=False):
        loc = MagicMock()
        key = text if exact else text
        loc.first.is_visible.side_effect = lambda timeout=200: key in visible_texts
        return loc

    page.get_by_text.side_effect = _get_by_text

    input_loc = MagicMock()
    input_loc.first.is_visible.side_effect = lambda timeout=200: input_visible
    page.locator.return_value = input_loc
    return page


class TestLoginScreenVisible:
    def test_rutube_passport_url(self):
        page = _mock_page(url="https://passport.rutube.ru/auth")
        assert login_screen_visible(page, "rutube") is True

    def test_rutube_studio_login_modal(self):
        page = _mock_page(
            url="https://studio.rutube.ru/",
            visible_texts={"Вход", "Телефон или почта"},
        )
        assert login_screen_visible(page, "rutube") is True

    def test_rutube_studio_without_login_modal(self):
        page = _mock_page(url="https://studio.rutube.ru/", visible_texts=set())
        assert login_screen_visible(page, "rutube") is False

    def test_rutube_studio_vhod_only_not_enough(self):
        page = _mock_page(url="https://studio.rutube.ru/", visible_texts={"Вход"})
        assert login_screen_visible(page, "rutube") is False

    def test_rutube_studio_login_via_input_placeholder(self):
        page = _mock_page(
            url="https://studio.rutube.ru/",
            visible_texts={"Вход"},
            input_visible=True,
        )
        assert login_screen_visible(page, "rutube") is True


class TestRaiseIfLoginRequired:
    def test_rutube_studio_modal_raises(self):
        page = _mock_page(
            url="https://studio.rutube.ru/",
            visible_texts={"Вход", "Зарегистрироваться"},
        )
        with pytest.raises(RutubeCsrfExpired):
            raise_if_login_required(page, "rutube")

    def test_rutube_studio_ok_no_raise(self):
        page = _mock_page(url="https://studio.rutube.ru/")
        raise_if_login_required(page, "rutube")


class TestWaitRaiseIfLoginRequired:
    def test_raises_as_soon_as_login_appears(self):
        page = MagicMock()
        page.wait_for_timeout.side_effect = lambda ms: None
        visible = iter((False, False, True))

        with patch(
            "services.publish_auth_check.login_screen_visible",
            side_effect=lambda *_a, **_k: next(visible),
        ):
            with pytest.raises(RutubeCsrfExpired):
                wait_raise_if_login_required(
                    page, "rutube", timeout_ms=3000, poll_ms=100,
                )
        assert page.wait_for_timeout.call_count == 2

    def test_stops_when_authenticated(self):
        page = MagicMock()
        page.wait_for_timeout.side_effect = lambda ms: None
        auth = iter((False, True))

        with patch("services.publish_auth_check.raise_if_login_required"):
            wait_raise_if_login_required(
                page,
                "rutube",
                timeout_ms=3000,
                poll_ms=100,
                is_authenticated=lambda: next(auth),
            )
        assert page.wait_for_timeout.call_count == 1
