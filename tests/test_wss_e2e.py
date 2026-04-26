"""End-to-end browser tests for TLS (wss://) UI elements.

Uses Playwright to test the JavaScript behavior of the TLS toggle,
port auto-switching, and form submission across all three pages:
- Settings page (api/templates/settings.html)
- Setup page (provision/templates/setup.html)
- Reconfigure page (provision/templates/reconfigure.html)

Prerequisites:
    pip install pytest-playwright
    playwright install chromium
"""

import json
import threading
import time

import httpx
import pytest
import uvicorn


# ── Server fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def provision_server(tmp_path_factory):
    """Start the provision app on a local port for Playwright tests.

    Isolates ``provision.app.PERSIST_DIR`` to a fresh tmp path so the
    test run is independent of any persisted ``cms_config.json`` that
    may exist on the developer/CI machine (e.g. from a prior provisioning
    session). Without this, ``/api/cms/config`` leaks real state into the
    reconfigure page and pre-checks the TLS checkbox.
    """
    import provision.app as provision_app

    persist = tmp_path_factory.mktemp("provision_persist")
    original_persist = provision_app.PERSIST_DIR
    provision_app.PERSIST_DIR = persist

    config = uvicorn.Config(provision_app.app, host="127.0.0.1", port=18081, log_level="error", ws="none")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    for _ in range(50):
        try:
            httpx.get("http://127.0.0.1:18081/", timeout=1)
            break
        except (httpx.ConnectError, httpx.ReadTimeout):
            time.sleep(0.2)

    try:
        yield "http://127.0.0.1:18081"
    finally:
        server.should_exit = True
        provision_app.PERSIST_DIR = original_persist


@pytest.fixture(scope="module")
def api_server(tmp_path_factory):
    """Start the main API app on a local port for Playwright tests."""
    from api.config import Settings
    from api.main import app

    tmp = tmp_path_factory.mktemp("agora")
    settings = Settings(
        agora_base=tmp,
        api_key="test-key",
        web_username="admin",
        web_password="testpass",
        secret_key="test-secret",
        device_name="test-node",
    )
    settings.ensure_dirs()
    app.state.settings = settings

    config = uvicorn.Config(app, host="127.0.0.1", port=18082, log_level="error", ws="none")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    for _ in range(50):
        try:
            httpx.get("http://127.0.0.1:18082/login", timeout=1)
            break
        except (httpx.ConnectError, httpx.ReadTimeout):
            time.sleep(0.2)

    yield "http://127.0.0.1:18082", settings
    server.should_exit = True


def _login(page, base_url):
    """Log into the web UI via the login form."""
    page.goto(f"{base_url}/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "testpass")
    page.click('button[type="submit"]')
    page.wait_for_load_state("networkidle")


# ── Setup page (provision portal) ────────────────────────────────────────────


class TestSetupPageTlsToggle:
    """Test TLS toggle JavaScript behavior on the provisioning setup page."""

    def test_tls_checkbox_present_and_unchecked(self, page, provision_server):
        page.goto(provision_server)
        checkbox = page.locator("#cms-tls")
        # Native input is visually hidden by the slider CSS — assert the
        # slider wrapper is visible instead.
        assert page.locator(".toggle-switch").first.is_visible()
        assert not checkbox.is_checked()

    def test_enable_tls_switches_port_to_443(self, page, provision_server):
        """Enabling TLS should auto-switch port from 8080 to 443."""
        page.goto(provision_server)
        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        port.fill("8080")
        assert port.input_value() == "8080"

        tls.check(force=True)
        assert port.input_value() == "443"

    def test_disable_tls_clears_port(self, page, provision_server):
        """Disabling TLS should clear port back to empty (placeholder 8080)."""
        page.goto(provision_server)
        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        tls.check(force=True)
        assert port.input_value() == "443"

        tls.uncheck(force=True)
        assert port.input_value() == ""
        assert port.get_attribute("placeholder") == "8080"

    def test_custom_port_not_overwritten(self, page, provision_server):
        """A custom port (not 8080/443) should NOT be auto-switched."""
        page.goto(provision_server)
        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        port.fill("9443")
        tls.check(force=True)
        assert port.input_value() == "9443"

    def test_empty_port_becomes_443_on_tls(self, page, provision_server):
        """Empty port should become 443 when TLS is enabled."""
        page.goto(provision_server)
        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        port.fill("")
        tls.check(force=True)
        assert port.input_value() == "443"


# ── Reconfigure page ─────────────────────────────────────────────────────────


class TestReconfigurePageTlsToggle:
    """Test TLS toggle JavaScript behavior on the reconfigure page."""

    def test_tls_checkbox_present_and_unchecked(self, page, provision_server):
        page.goto(f"{provision_server}/reconfigure")
        checkbox = page.locator("#cms-tls")
        assert page.locator(".toggle-switch").first.is_visible()
        assert not checkbox.is_checked()

    def test_enable_tls_switches_port(self, page, provision_server):
        page.goto(f"{provision_server}/reconfigure")
        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        port.fill("8080")
        tls.check(force=True)
        assert port.input_value() == "443"

    def test_disable_tls_clears_port(self, page, provision_server):
        page.goto(f"{provision_server}/reconfigure")
        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        tls.check(force=True)
        assert port.input_value() == "443"

        tls.uncheck(force=True)
        assert port.input_value() == ""

    def test_form_submits_tls_flag(self, page, provision_server):
        """Submitting the reconfigure form should include cms_tls in the payload."""
        page.goto(f"{provision_server}/reconfigure")
        page.fill("#cms-host", "cms.example.com")
        page.check("#cms-tls", force=True)

        with page.expect_request("**/api/reconfigure") as req_info:
            page.click("#submit-btn")

        body = req_info.value.post_data_json
        assert body["cms_tls"] is True
        assert body["cms_port"] == 443


# ── Settings page (main API) ─────────────────────────────────────────────────


class TestSettingsPageTlsToggle:
    """Test TLS toggle JavaScript behavior on the settings page."""

    def test_tls_checkbox_present(self, page, api_server):
        base_url, _ = api_server
        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        checkbox = page.locator("#cms-tls")
        assert checkbox.is_visible()
        assert not checkbox.is_checked()

    def test_enable_tls_switches_port(self, page, api_server):
        base_url, _ = api_server
        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        port = page.locator("#cms-port")
        tls = page.locator("#cms-tls")

        port.fill("8080")
        tls.check()
        assert port.input_value() == "443"

    def test_tls_checked_when_configured(self, page, api_server):
        """When TLS is saved in config, the checkbox should be pre-checked."""
        base_url, settings = api_server
        config = {
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
        }
        settings.cms_config_path.write_text(json.dumps(config))

        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        checkbox = page.locator("#cms-tls")
        assert checkbox.is_checked()

        # Clean up
        settings.cms_config_path.unlink(missing_ok=True)

    def test_dashboard_link_uses_https_when_tls(self, page, api_server):
        """CMS dashboard link should use https:// when TLS is enabled."""
        base_url, settings = api_server
        config = {
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
        }
        settings.cms_config_path.write_text(json.dumps(config))

        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        link = page.locator("a.cms-link").first
        href = link.get_attribute("href")
        assert href.startswith("https://cms.example.com")

        settings.cms_config_path.unlink(missing_ok=True)

    def test_form_submits_tls_flag(self, page, api_server):
        """Submitting the settings form should include cms_tls in the payload."""
        base_url, settings = api_server
        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        page.fill("#cms-host", "cms.example.com")
        page.check("#cms-tls")

        with page.expect_request("**/cms/config") as req_info:
            page.click("#cms-connect-btn")

        body = req_info.value.post_data_json
        assert body["cms_tls"] is True


class TestSettingsPageCmsReconnect:
    """Test that changing the CMS host on the Settings page writes config
    and returns success (the client-side reconnect is tested separately
    in test_cms_url_reconnect.py)."""

    def test_change_cms_host_saves_config(self, page, api_server):
        """Submitting a new CMS host should write cms_config.json on disk."""
        base_url, settings = api_server
        # Clean any leftover config from prior tests
        settings.cms_config_path.unlink(missing_ok=True)
        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        page.fill("#cms-host", "new-cms.example.com")
        page.fill("#cms-port", "8080")
        # Ensure TLS is unchecked (may be checked from prior test)
        if page.locator("#cms-tls").is_checked():
            page.uncheck("#cms-tls")

        with page.expect_request("**/cms/config") as req_info:
            page.click("#cms-connect-btn")

        body = req_info.value.post_data_json
        assert body["cms_host"] == "new-cms.example.com"
        assert body["cms_port"] == 8080

        # Wait for the response to be processed
        page.wait_for_timeout(1000)

        # Config file should be updated on disk
        config = json.loads(settings.cms_config_path.read_text())
        assert config["cms_url"] == "ws://new-cms.example.com:8080/ws/device"

    def test_change_cms_host_with_tls_saves_wss_url(self, page, api_server):
        """Submitting with TLS enabled should write a wss:// URL."""
        base_url, settings = api_server
        _login(page, base_url)
        page.goto(f"{base_url}/settings")

        page.fill("#cms-host", "cloud.example.com")
        page.check("#cms-tls")

        with page.expect_request("**/cms/config") as req_info:
            page.click("#cms-connect-btn")

        body = req_info.value.post_data_json
        assert body["cms_host"] == "cloud.example.com"
        assert body["cms_tls"] is True

        page.wait_for_timeout(1000)

        config = json.loads(settings.cms_config_path.read_text())
        assert config["cms_url"].startswith("wss://cloud.example.com")

        # Clean up
        settings.cms_config_path.unlink(missing_ok=True)
