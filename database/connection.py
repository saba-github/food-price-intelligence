from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping
from urllib.parse import quote_plus

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as PGConnection


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = ROOT_DIR / ".env"

_ENV_LOADED = False
_DATABASE_URL_KEYS = ("DATABASE_URL",)
_DB_CONFIG_KEYS = {
    "host": ("DB_HOST", "DATABASE_HOST"),
    "port": ("DB_PORT", "DATABASE_PORT"),
    "name": ("DB_NAME", "DATABASE_NAME"),
    "user": ("DB_USER", "DATABASE_USER"),
    "password": ("DB_PASSWORD", "DATABASE_PASSWORD"),
    "sslmode": ("DB_SSLMODE", "DATABASE_SSLMODE"),
}


def load_database_env(dotenv_path: Path | None = None) -> None:
    global _ENV_LOADED

    if _ENV_LOADED:
        return

    load_dotenv(dotenv_path or DEFAULT_ENV_PATH, override=False)
    _ENV_LOADED = True


def _first_non_empty(*values: object) -> str | None:
    for value in values:
        if value is None:
            continue

        string_value = str(value).strip()
        if string_value:
            return string_value

    return None


def _get_setting(
    key_group: tuple[str, ...],
    *,
    settings: Mapping[str, object] | None = None,
    env: Mapping[str, str] | None = None,
) -> str | None:
    if settings:
        setting_value = _first_non_empty(*(settings.get(key) for key in key_group))
        if setting_value:
            return setting_value

    environment = env or os.environ
    return _first_non_empty(*(environment.get(key) for key in key_group))


def build_database_url_from_settings(settings: Mapping[str, object]) -> str:
    host = _first_non_empty(settings.get("host"), *(settings.get(key) for key in _DB_CONFIG_KEYS["host"]))
    port = _first_non_empty(settings.get("port"), *(settings.get(key) for key in _DB_CONFIG_KEYS["port"])) or "5432"
    name = _first_non_empty(settings.get("name"), *(settings.get(key) for key in _DB_CONFIG_KEYS["name"]))
    user = _first_non_empty(settings.get("user"), *(settings.get(key) for key in _DB_CONFIG_KEYS["user"]))
    password = _first_non_empty(
        settings.get("password"),
        *(settings.get(key) for key in _DB_CONFIG_KEYS["password"])
    )
    sslmode = _first_non_empty(
        settings.get("sslmode"),
        *(settings.get(key) for key in _DB_CONFIG_KEYS["sslmode"])
    )

    missing_fields = [
        field_name
        for field_name, field_value in (
            ("host", host),
            ("name", name),
            ("user", user),
            ("password", password),
        )
        if not field_value
    ]

    if missing_fields:
        raise ValueError(
            "Incomplete database configuration. Missing: "
            + ", ".join(missing_fields)
        )

    database_url = (
        "postgresql://"
        f"{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{quote_plus(name)}"
    )

    if sslmode:
        database_url = f"{database_url}?sslmode={quote_plus(sslmode)}"

    return database_url


def resolve_database_url(
    *,
    fallback_url: str | None = None,
    env: Mapping[str, str] | None = None,
) -> str:
    load_database_env()

    database_url = _get_setting(_DATABASE_URL_KEYS, env=env)
    if database_url:
        return database_url

    if fallback_url:
        return fallback_url

    settings = {
        alias: _get_setting(keys, env=env)
        for alias, keys in _DB_CONFIG_KEYS.items()
    }

    try:
        return build_database_url_from_settings(settings)
    except ValueError as exc:
        raise ValueError(
            "Database configuration is missing. Set DATABASE_URL or define "
            "DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD in .env."
        ) from exc


def get_connection(
    *,
    fallback_url: str | None = None,
    autocommit: bool | None = None,
    connect_timeout: int | None = None,
    application_name: str | None = None,
) -> PGConnection:
    database_url = resolve_database_url(fallback_url=fallback_url)

    connect_kwargs: dict[str, object] = {}
    if connect_timeout is not None:
        connect_kwargs["connect_timeout"] = connect_timeout
    if application_name:
        connect_kwargs["application_name"] = application_name

    conn = psycopg2.connect(database_url, **connect_kwargs)
    if autocommit is not None:
        conn.autocommit = autocommit
    return conn
