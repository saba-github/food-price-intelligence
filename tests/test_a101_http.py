from scraper.a101.http import build_session, should_bypass_env_proxy


def test_a101_should_bypass_dead_loopback_proxy_from_env():
    env = {
        "HTTP_PROXY": "http://127.0.0.1:9",
        "HTTPS_PROXY": "",
        "ALL_PROXY": "",
    }

    assert should_bypass_env_proxy(env) is True


def test_a101_should_not_bypass_valid_proxy_configuration():
    env = {
        "HTTP_PROXY": "http://10.0.0.5:8080",
        "HTTPS_PROXY": "",
        "ALL_PROXY": "",
    }

    assert should_bypass_env_proxy(env) is False


def test_a101_build_session_disables_trust_env_for_dead_proxy(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "")
    monkeypatch.setenv("ALL_PROXY", "")

    session = build_session()
    try:
        assert session.trust_env is False
    finally:
        session.close()
