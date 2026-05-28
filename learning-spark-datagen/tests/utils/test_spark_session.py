"""Tests for SparkSession builder configuration."""

from learning_spark_datagen.utils import spark_session


class _FakeBuilder:
    def __init__(self):
        self.configs = {}

    def master(self, _value):
        return self

    def appName(self, _value):
        return self

    def config(self, key, value):
        self.configs[key] = value
        return self

    def getOrCreate(self):
        return self.configs


def _install_fake_spark(monkeypatch):
    """Install a fresh fake SparkSession so each test starts with empty configs."""
    fake = type("Fake", (), {"builder": _FakeBuilder()})
    monkeypatch.setattr(spark_session, "SparkSession", fake)


def test_generate_spark_session_does_not_set_repositories_by_default(monkeypatch):
    """Without MAVEN_PROXY in env, no spark.jars.repositories is configured."""
    monkeypatch.delenv("MAVEN_PROXY", raising=False)
    _install_fake_spark(monkeypatch)

    configs = spark_session.generate_spark_session()

    assert configs["spark.jars.packages"].startswith("io.delta:delta-spark_2.13")
    assert "spark.jars.repositories" not in configs


def test_generate_spark_session_uses_maven_proxy_when_env_set(monkeypatch):
    """When MAVEN_PROXY is set, its value is wired into spark.jars.repositories."""
    monkeypatch.setenv("MAVEN_PROXY", "https://maven-proxy.example.com")
    _install_fake_spark(monkeypatch)

    configs = spark_session.generate_spark_session()

    assert configs["spark.jars.repositories"] == "https://maven-proxy.example.com"
    # Existing config still applied.
    assert configs["spark.jars.packages"].startswith("io.delta:delta-spark_2.13")


def test_generate_spark_session_does_not_hardcode_vendor_url(monkeypatch):
    """The Spark session module must not hardcode any vendor proxy URL.

    Per the workspace's 'Dependency source notes' (AGENTS.md), the proxy URL
    must come from the MAVEN_PROXY env var only.
    """
    import inspect

    source = inspect.getsource(spark_session)
    assert "databricks" not in source.lower(), (
        "spark_session.py must not hardcode a vendor proxy URL; "
        "use the MAVEN_PROXY env var instead"
    )
