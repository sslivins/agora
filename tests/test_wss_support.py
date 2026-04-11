"""Tests for TLS (wss://) support in CMS configuration.

Covers:
- _build_ws_url() URL construction (ws vs wss, port omission)
- POST /cms/config: protocol stripping, TLS auto-detect, port auto-switch
- GET /cms/config: cms_tls field in response
- Settings page rendering: TLS checkbox, dashboard link protocol
- Provision app: /api/provision and /api/reconfigure TLS handling
- Setup and reconfigure page rendering: TLS checkbox presence
"""

import json
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.routers.cms import (
    DEFAULT_CMS_PORT,
    DEFAULT_CMS_TLS_PORT,
    _build_ws_url,
)


# ── Unit tests: _build_ws_url ────────────────────────────────────────────────


class TestBuildWsUrl:
    """Test WebSocket URL construction with TLS support."""

    def test_ws_with_explicit_port(self):
        assert _build_ws_url("192.168.1.100", 8080) == "ws://192.168.1.100:8080/ws/device"

    def test_ws_default_port_80_omitted(self):
        """Port 80 is the default for ws://, should be omitted."""
        assert _build_ws_url("192.168.1.100", 80) == "ws://192.168.1.100/ws/device"

    def test_ws_non_default_port(self):
        assert _build_ws_url("192.168.1.100", 9090) == "ws://192.168.1.100:9090/ws/device"

    def test_wss_with_default_port_443_omitted(self):
        """Port 443 is the default for wss://, should be omitted."""
        assert _build_ws_url("cms.example.com", 443, tls=True) == "wss://cms.example.com/ws/device"

    def test_wss_with_custom_port(self):
        assert _build_ws_url("cms.example.com", 8443, tls=True) == "wss://cms.example.com:8443/ws/device"

    def test_wss_with_port_8080(self):
        """Non-standard TLS port should be included."""
        assert _build_ws_url("cms.example.com", 8080, tls=True) == "wss://cms.example.com:8080/ws/device"

    def test_ws_default_tls_false(self):
        """TLS defaults to False."""
        assert _build_ws_url("host", 8080).startswith("ws://")

    def test_hostname_preserved(self):
        url = _build_ws_url("my-cms.example.com", 443, tls=True)
        assert "my-cms.example.com" in url

    def test_path_always_ws_device(self):
        """All URLs must end with /ws/device."""
        assert _build_ws_url("h", 80).endswith("/ws/device")
        assert _build_ws_url("h", 443, tls=True).endswith("/ws/device")
        assert _build_ws_url("h", 9999, tls=True).endswith("/ws/device")


# ── API tests: POST /cms/config ──────────────────────────────────────────────


@pytest.mark.asyncio
class TestCmsConfigPostTls:
    """Test CMS config endpoint TLS handling."""

    @patch("api.routers.cms.subprocess")
    async def test_tls_flag_saved(self, mock_sp, client, settings):
        """Posting cms_tls=true should persist it in cms_config.json."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_tls"] is True

        config = json.loads(settings.cms_config_path.read_text())
        assert config["cms_tls"] is True
        assert config["cms_url"].startswith("wss://")

    @patch("api.routers.cms.subprocess")
    async def test_tls_false_uses_ws(self, mock_sp, client, settings):
        """Posting cms_tls=false should use ws:// scheme."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "192.168.1.100",
            "cms_port": 8080,
            "cms_tls": False,
        })
        assert resp.status_code == 200
        config = json.loads(settings.cms_config_path.read_text())
        assert config["cms_url"].startswith("ws://")
        assert config["cms_tls"] is False

    @patch("api.routers.cms.subprocess")
    async def test_wss_prefix_auto_enables_tls(self, mock_sp, client, settings):
        """Pasting wss://host should auto-enable TLS even if cms_tls=false."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "wss://cms.example.com",
            "cms_port": 8080,
            "cms_tls": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_tls"] is True
        assert data["cms_host"] == "cms.example.com"

    @patch("api.routers.cms.subprocess")
    async def test_https_prefix_auto_enables_tls(self, mock_sp, client, settings):
        """Pasting https://host should auto-enable TLS."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "https://cms.example.com",
            "cms_port": 8080,
            "cms_tls": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_tls"] is True

    @patch("api.routers.cms.subprocess")
    async def test_ws_prefix_stripped_no_tls(self, mock_sp, client, settings):
        """Pasting ws://host should strip prefix without enabling TLS."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "ws://192.168.1.100",
            "cms_port": 8080,
            "cms_tls": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_tls"] is False
        assert data["cms_host"] == "192.168.1.100"

    @patch("api.routers.cms.subprocess")
    async def test_http_prefix_stripped_no_tls(self, mock_sp, client, settings):
        """Pasting http://host should strip prefix without enabling TLS."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "http://192.168.1.100",
            "cms_port": 8080,
            "cms_tls": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_tls"] is False

    @patch("api.routers.cms.subprocess")
    async def test_port_auto_switch_to_443_when_tls(self, mock_sp, client, settings):
        """When TLS is enabled and port is the default 8080, auto-switch to 443."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "cms.example.com",
            "cms_port": DEFAULT_CMS_PORT,
            "cms_tls": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_port"] == DEFAULT_CMS_TLS_PORT

    @patch("api.routers.cms.subprocess")
    async def test_custom_port_preserved_with_tls(self, mock_sp, client, settings):
        """A non-default port should NOT be auto-switched when enabling TLS."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "cms.example.com",
            "cms_port": 9443,
            "cms_tls": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_port"] == 9443

    @patch("api.routers.cms.subprocess")
    async def test_host_with_embedded_port_extracted(self, mock_sp, client, settings):
        """Host like 'cms.example.com:9443' should extract port."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "cms.example.com:9443",
            "cms_port": 8080,
            "cms_tls": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_host"] == "cms.example.com"
        assert data["cms_port"] == 9443

    @patch("api.routers.cms.subprocess")
    async def test_host_with_trailing_path_stripped(self, mock_sp, client, settings):
        """Trailing paths in the host should be stripped."""
        resp = await client.post("/api/v1/cms/config", json={
            "cms_host": "https://cms.example.com/some/path",
            "cms_port": 443,
            "cms_tls": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_host"] == "cms.example.com"


# ── API tests: GET /cms/config ───────────────────────────────────────────────


@pytest.mark.asyncio
class TestCmsConfigGetTls:
    """Test GET /cms/config returns TLS field."""

    async def test_returns_tls_field_default_false(self, client, settings):
        """GET /cms/config should include cms_tls, defaulting to False."""
        resp = await client.get("/api/v1/cms/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "cms_tls" in data
        assert data["cms_tls"] is False

    async def test_returns_tls_true_when_configured(self, client, settings):
        """GET /cms/config should return cms_tls=true when saved."""
        config = {
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
            "cms_url": "wss://cms.example.com/ws/device",
        }
        settings.cms_config_path.write_text(json.dumps(config))

        resp = await client.get("/api/v1/cms/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cms_tls"] is True
        assert data["cms_host"] == "cms.example.com"
        assert data["cms_port"] == 443


# ── Settings page rendering ─────────────────────────────────────────────────


@pytest.mark.asyncio
class TestSettingsPageTls:
    """Test that the settings page renders TLS elements correctly."""

    async def test_settings_page_renders_tls_checkbox(self, web_client, settings):
        """The settings page should contain the TLS checkbox."""
        resp = await web_client.get("/settings", follow_redirects=True)
        assert resp.status_code == 200
        assert 'id="cms-tls"' in resp.text
        assert "Use TLS (wss://)" in resp.text

    async def test_settings_page_tls_unchecked_by_default(self, web_client, settings):
        """TLS checkbox should not be checked when cms_tls is false/absent."""
        resp = await web_client.get("/settings", follow_redirects=True)
        assert resp.status_code == 200
        assert 'id="cms-tls"' in resp.text
        # No "checked" attribute should appear on the checkbox line
        for line in resp.text.splitlines():
            if 'id="cms-tls"' in line:
                assert "checked" not in line.replace("unchecked", "")
                break

    async def test_settings_page_tls_checked_when_enabled(self, web_client, settings):
        """TLS checkbox should be checked when cms_tls is saved as true."""
        config = {
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
        }
        settings.cms_config_path.write_text(json.dumps(config))

        resp = await web_client.get("/settings", follow_redirects=True)
        assert resp.status_code == 200
        # The Jinja template renders "checked" on the line after id="cms-tls"
        # so look in the full HTML block around the checkbox
        html = resp.text
        tls_start = html.find('id="cms-tls"')
        assert tls_start != -1, "TLS checkbox not found in settings page"
        # Look in a window around the checkbox element for the checked attribute
        tls_block = html[max(0, tls_start - 100):tls_start + 200]
        assert "checked" in tls_block

    async def test_settings_page_dashboard_link_uses_https(self, web_client, settings):
        """When TLS is enabled, the CMS dashboard link should use https://."""
        config = {
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
        }
        settings.cms_config_path.write_text(json.dumps(config))

        resp = await web_client.get("/settings", follow_redirects=True)
        assert resp.status_code == 200
        assert "https://cms.example.com" in resp.text

    async def test_settings_page_dashboard_link_uses_http(self, web_client, settings):
        """When TLS is disabled, the CMS dashboard link should use http://."""
        config = {
            "cms_host": "192.168.1.100",
            "cms_port": 8080,
            "cms_tls": False,
        }
        settings.cms_config_path.write_text(json.dumps(config))

        resp = await web_client.get("/settings", follow_redirects=True)
        assert resp.status_code == 200
        assert "http://192.168.1.100:8080/" in resp.text

    async def test_settings_page_port_placeholder_443_when_tls(self, web_client, settings):
        """Port placeholder should be 443 when TLS is enabled."""
        config = {
            "cms_host": "cms.example.com",
            "cms_port": 443,
            "cms_tls": True,
        }
        settings.cms_config_path.write_text(json.dumps(config))

        resp = await web_client.get("/settings", follow_redirects=True)
        assert resp.status_code == 200
        assert 'placeholder="443"' in resp.text


# ── Provision app tests ─────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestProvisionTls:
    """Test TLS handling in the provisioning portal."""

    @pytest_asyncio.fixture
    async def prov_client(self):
        """Client for the provision app (no auth required)."""
        from provision.app import app
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    async def test_provision_saves_tls_config(self, prov_client, tmp_path):
        """POST /api/provision with cms_tls=true should save TLS config."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.portal_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/provision", json={
                "wifi_ssid": "TestNetwork",
                "wifi_password": "pass123",
                "cms_host": "cms.example.com",
                "cms_port": 443,
                "cms_tls": True,
            })
            assert resp.status_code == 200
            data = resp.json()
            assert data["success"] is True

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_tls"] is True
            assert config["cms_url"].startswith("wss://")

    async def test_provision_wss_prefix_auto_enables_tls(self, prov_client, tmp_path):
        """Pasting wss://host in provision should auto-enable TLS."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.portal_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/provision", json={
                "wifi_ssid": "TestNetwork",
                "wifi_password": "pass123",
                "cms_host": "wss://cms.example.com",
                "cms_port": 8080,
                "cms_tls": False,
            })
            assert resp.status_code == 200

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_tls"] is True
            assert config["cms_host"] == "cms.example.com"

    async def test_provision_default_port_switch(self, prov_client, tmp_path):
        """TLS with default port 8080 should auto-switch to 443."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.portal_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/provision", json={
                "wifi_ssid": "TestNetwork",
                "wifi_password": "pass123",
                "cms_host": "cms.example.com",
                "cms_port": 8080,
                "cms_tls": True,
            })
            assert resp.status_code == 200

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_port"] == 443

    async def test_provision_no_tls_uses_ws(self, prov_client, tmp_path):
        """POST /api/provision without TLS should use ws:// scheme."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.portal_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/provision", json={
                "wifi_ssid": "TestNetwork",
                "wifi_password": "pass123",
                "cms_host": "192.168.1.100",
                "cms_port": 8080,
                "cms_tls": False,
            })
            assert resp.status_code == 200

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_tls"] is False
            assert config["cms_url"].startswith("ws://")

    async def test_reconfigure_saves_tls(self, prov_client, tmp_path):
        """POST /api/reconfigure with TLS should save TLS config."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.reconfigure_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/reconfigure", json={
                "cms_host": "cms.example.com",
                "cms_port": 443,
                "cms_tls": True,
            })
            assert resp.status_code == 200

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_tls"] is True
            assert config["cms_url"].startswith("wss://")

    async def test_reconfigure_https_prefix_auto_tls(self, prov_client, tmp_path):
        """Pasting https://host in reconfigure should auto-enable TLS."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.reconfigure_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/reconfigure", json={
                "cms_host": "https://cms.example.com",
                "cms_port": 8080,
                "cms_tls": False,
            })
            assert resp.status_code == 200

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_tls"] is True
            assert config["cms_port"] == 443

    async def test_reconfigure_port_443_omitted_in_url(self, prov_client, tmp_path):
        """wss:// URL with port 443 should omit the port."""
        with patch("provision.app.PERSIST_DIR", tmp_path), \
             patch("provision.app.reconfigure_events") as mock_events:
            mock_events.put_nowait = lambda x: None

            resp = await prov_client.post("/api/reconfigure", json={
                "cms_host": "cms.example.com",
                "cms_port": 443,
                "cms_tls": True,
            })
            assert resp.status_code == 200

            config = json.loads((tmp_path / "cms_config.json").read_text())
            assert config["cms_url"] == "wss://cms.example.com/ws/device"

    async def test_get_cms_config_returns_tls(self, prov_client, tmp_path):
        """GET /api/cms/config should return cms_tls field."""
        config = {"cms_host": "x.com", "cms_port": 443, "cms_tls": True}
        (tmp_path / "cms_config.json").write_text(json.dumps(config))

        with patch("provision.app.PERSIST_DIR", tmp_path):
            resp = await prov_client.get("/api/cms/config")
            assert resp.status_code == 200
            data = resp.json()
            assert data["cms_tls"] is True

    async def test_get_cms_config_defaults_tls_false(self, prov_client, tmp_path):
        """GET /api/cms/config should default cms_tls to false."""
        with patch("provision.app.PERSIST_DIR", tmp_path):
            resp = await prov_client.get("/api/cms/config")
            assert resp.status_code == 200
            data = resp.json()
            assert data["cms_tls"] is False


# ── Provision page rendering ────────────────────────────────────────────────


@pytest.mark.asyncio
class TestProvisionPagesTls:
    """Test that provision pages contain TLS elements."""

    @pytest_asyncio.fixture
    async def prov_client(self):
        from provision.app import app
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    async def test_reconfigure_page_has_tls_checkbox(self, prov_client):
        resp = await prov_client.get("/reconfigure")
        assert resp.status_code == 200
        assert 'id="cms-tls"' in resp.text
        assert "Use TLS (wss://)" in resp.text

    async def test_setup_page_has_tls_checkbox(self, prov_client):
        resp = await prov_client.get("/")
        assert resp.status_code == 200
        assert 'id="cms-tls"' in resp.text
        assert "Use TLS (wss://)" in resp.text

    async def test_setup_page_has_tls_toggle_js(self, prov_client):
        """Setup page should include the onTlsToggle JavaScript function."""
        resp = await prov_client.get("/")
        assert resp.status_code == 200
        assert "onTlsToggle" in resp.text

    async def test_reconfigure_page_has_tls_toggle_js(self, prov_client):
        """Reconfigure page should include the onTlsToggle JavaScript function."""
        resp = await prov_client.get("/reconfigure")
        assert resp.status_code == 200
        assert "onTlsToggle" in resp.text

    async def test_reconfigure_page_tls_hint_text(self, prov_client):
        """Reconfigure page should mention TLS for cloud-hosted servers."""
        resp = await prov_client.get("/reconfigure")
        assert resp.status_code == 200
        assert "cloud-hosted" in resp.text.lower()
