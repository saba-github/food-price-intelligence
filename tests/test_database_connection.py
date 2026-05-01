import pytest

from database.connection import build_database_url_from_settings, resolve_database_url


def test_build_database_url_from_discrete_settings():
    url = build_database_url_from_settings(
        {
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "food_price_intelligence",
            "DB_USER": "postgres",
            "DB_PASSWORD": "secret",
            "DB_SSLMODE": "require",
        }
    )

    assert (
        url
        == "postgresql://postgres:secret@localhost:5432/food_price_intelligence?sslmode=require"
    )


def test_resolve_database_url_prefers_database_url_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://env-user:env-pass@db:5432/envdb")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_NAME", "ignored")
    monkeypatch.setenv("DB_USER", "ignored")
    monkeypatch.setenv("DB_PASSWORD", "ignored")

    assert (
        resolve_database_url()
        == "postgresql://env-user:env-pass@db:5432/envdb"
    )


def test_resolve_database_url_builds_from_db_env(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "cloud-host")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "cloud_db")
    monkeypatch.setenv("DB_USER", "cloud_user")
    monkeypatch.setenv("DB_PASSWORD", "cloud_password")
    monkeypatch.setenv("DB_SSLMODE", "require")

    assert (
        resolve_database_url()
        == "postgresql://cloud_user:cloud_password@cloud-host:5432/cloud_db?sslmode=require"
    )


def test_resolve_database_url_uses_fallback_url_when_env_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    assert (
        resolve_database_url(fallback_url="postgresql://streamlit-secret")
        == "postgresql://streamlit-secret"
    )


def test_build_database_url_requires_complete_settings():
    with pytest.raises(ValueError):
        build_database_url_from_settings(
            {
                "DB_HOST": "localhost",
                "DB_NAME": "db",
                "DB_USER": "postgres",
            }
        )
