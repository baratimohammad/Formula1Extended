from src.storage.postgres_writer import build_postgres_url


def test_build_postgres_url_uses_default_connection_values(monkeypatch):
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)

    assert build_postgres_url() == "postgresql+psycopg://formula1:formula1@localhost:5432/formula1"
