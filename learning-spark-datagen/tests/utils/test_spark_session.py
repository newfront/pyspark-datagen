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


class _FakeSparkSession:
    builder = _FakeBuilder()


def test_generate_spark_session_does_not_set_databricks_proxy(monkeypatch):
    monkeypatch.setattr(spark_session, "SparkSession", _FakeSparkSession)

    configs = spark_session.generate_spark_session()

    assert configs["spark.jars.packages"].startswith("io.delta:delta-spark_2.13")
    assert "spark.jars.repositories" not in configs
