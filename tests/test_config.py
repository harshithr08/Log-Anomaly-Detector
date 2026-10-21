from logad.common.config import Settings


def test_defaults_match_docker_compose():
    settings = Settings(_env_file=None)
    assert settings.redis_url == "redis://127.0.0.1:6379/0"
    assert settings.database_url.endswith("@127.0.0.1:5433/logad")


def test_environment_variables_override_defaults(monkeypatch):
    monkeypatch.setenv("LOGAD_REDIS_URL", "redis://redis:6379/1")
    assert Settings(_env_file=None).redis_url == "redis://redis:6379/1"
