import os
from urllib.parse import urlparse

import requests


PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
LOOPBACK_PROXY_HOSTS = {"127.0.0.1", "localhost", "::1"}
DEAD_PROXY_PORTS = {9}


def _is_dead_loopback_proxy(proxy_value: str | None) -> bool:
    if not proxy_value:
        return False

    candidate = proxy_value.strip()
    if not candidate:
        return False

    if "://" not in candidate:
        candidate = f"http://{candidate}"

    try:
        parsed = urlparse(candidate)
    except ValueError:
        return False

    return (
        parsed.hostname in LOOPBACK_PROXY_HOSTS
        and parsed.port in DEAD_PROXY_PORTS
    )


def should_bypass_env_proxy(env: dict[str, str] | None = None) -> bool:
    active_env = env or os.environ
    return any(
        _is_dead_loopback_proxy(active_env.get(key))
        for key in PROXY_ENV_KEYS
    )


def build_session() -> requests.Session:
    session = requests.Session()
    if should_bypass_env_proxy():
        session.trust_env = False
    return session
