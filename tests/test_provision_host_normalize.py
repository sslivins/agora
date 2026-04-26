"""Unit tests for ``provision.app._normalize_cms_host``.

The captive portal must defensively strip URL prefixes, smart quotes
(autocorrect victims), and validate the result against RFC 1123 so that
malformed hostnames don't get silently IDN-encoded into garbage that
fails DNS in mysterious ways.
"""

from provision.app import (
    CMS_DEFAULT_PORT,
    CMS_MDNS_HOST,
    _normalize_cms_host,
)


# ── Empty / trivial input ────────────────────────────────────────────────────


def test_empty_string_falls_back_to_mdns_default():
    host, port, tls, err = _normalize_cms_host("", False, CMS_DEFAULT_PORT)
    assert host == CMS_MDNS_HOST
    assert port == CMS_DEFAULT_PORT
    assert tls is False
    assert err is None


def test_whitespace_only_falls_back_to_mdns_default():
    host, port, tls, err = _normalize_cms_host("   ", False, CMS_DEFAULT_PORT)
    assert host == CMS_MDNS_HOST
    assert err is None


def test_plain_hostname_passes_through():
    host, port, tls, err = _normalize_cms_host(
        "agora-cms.local", False, CMS_DEFAULT_PORT,
    )
    assert host == "agora-cms.local"
    assert port == CMS_DEFAULT_PORT
    assert tls is False
    assert err is None


# ── Smart-quote stripping ────────────────────────────────────────────────────


def test_right_single_quote_is_stripped():
    """U+2019 is what iOS autocorrects ' into."""
    host, _, _, err = _normalize_cms_host(
        "kennan\u2019s.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "kennans.com"
    assert err is None


def test_left_single_quote_is_stripped():
    host, _, _, err = _normalize_cms_host(
        "\u2018foo.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "foo.com"
    assert err is None


def test_double_smart_quotes_are_stripped():
    host, _, _, err = _normalize_cms_host(
        "\u201Cfoo.com\u201D", False, CMS_DEFAULT_PORT,
    )
    assert host == "foo.com"
    assert err is None


def test_acute_accent_is_stripped():
    host, _, _, err = _normalize_cms_host(
        "fo\u00B4o.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "foo.com"
    assert err is None


# ── URL prefix stripping ─────────────────────────────────────────────────────


def test_https_prefix_implies_tls_and_strips():
    host, _, tls, err = _normalize_cms_host(
        "https://cms.example.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert tls is True
    assert err is None


def test_wss_prefix_implies_tls_and_strips():
    host, _, tls, err = _normalize_cms_host(
        "wss://cms.example.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert tls is True
    assert err is None


def test_http_prefix_does_not_force_tls():
    host, _, tls, err = _normalize_cms_host(
        "http://cms.example.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert tls is False
    assert err is None


def test_ws_prefix_strips_without_changing_tls():
    host, _, tls, err = _normalize_cms_host(
        "ws://cms.example.com", True, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    # ws:// does not downgrade an explicit TLS=True setting.
    assert tls is True
    assert err is None


def test_uppercase_prefix_is_handled():
    host, _, tls, _ = _normalize_cms_host(
        "HTTPS://cms.example.com", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert tls is True


# ── Path / query / fragment stripping ────────────────────────────────────────


def test_path_is_stripped():
    host, _, _, err = _normalize_cms_host(
        "cms.example.com/some/path", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert err is None


def test_query_is_stripped():
    host, _, _, err = _normalize_cms_host(
        "cms.example.com?foo=bar", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert err is None


def test_fragment_is_stripped():
    host, _, _, err = _normalize_cms_host(
        "cms.example.com#anchor", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert err is None


def test_full_url_with_prefix_path_and_query():
    host, _, tls, err = _normalize_cms_host(
        "https://cms.example.com/admin?token=x", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert tls is True
    assert err is None


# ── Port parsing ─────────────────────────────────────────────────────────────


def test_port_extracted_from_host_string():
    host, port, _, err = _normalize_cms_host(
        "cms.example.com:9090", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert port == 9090
    assert err is None


def test_invalid_port_value_falls_back_to_default():
    host, port, _, err = _normalize_cms_host(
        "cms.example.com:notaport", False, 1234,
    )
    # Bad port suffix should fail validation, not silently work
    assert err is not None


def test_host_with_prefix_and_port():
    host, port, tls, err = _normalize_cms_host(
        "https://cms.example.com:8443", False, CMS_DEFAULT_PORT,
    )
    assert host == "cms.example.com"
    assert port == 8443
    assert tls is True
    assert err is None


# ── Validation rejects garbage ───────────────────────────────────────────────


def test_space_in_hostname_rejected():
    _, _, _, err = _normalize_cms_host(
        "cms example.com", False, CMS_DEFAULT_PORT,
    )
    assert err is not None
    assert "Invalid CMS host" in err


def test_underscore_rejected():
    _, _, _, err = _normalize_cms_host(
        "cms_server.local", False, CMS_DEFAULT_PORT,
    )
    # Underscores are not valid in RFC 1123 hostnames.
    assert err is not None


def test_leading_hyphen_rejected():
    _, _, _, err = _normalize_cms_host(
        "-cms.local", False, CMS_DEFAULT_PORT,
    )
    assert err is not None


def test_unicode_letters_rejected():
    _, _, _, err = _normalize_cms_host(
        "kennan\u00e9.com", False, CMS_DEFAULT_PORT,
    )
    assert err is not None


def test_smart_quote_only_garbage_after_strip_rejected():
    """If the string was entirely smart quotes, the post-strip empty
    string falls back to mDNS default — that's fine. But mixed garbage
    that survives stripping but isn't a valid hostname must be rejected.
    """
    _, _, _, err = _normalize_cms_host(
        "kennan\u2019s house.com", False, CMS_DEFAULT_PORT,
    )
    # After stripping U+2019 we still have "kennans house.com" with a
    # literal space, which is invalid.
    assert err is not None
