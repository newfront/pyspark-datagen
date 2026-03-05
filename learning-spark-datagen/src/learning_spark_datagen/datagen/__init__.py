"""Data generation package: fake users, orders, and NDJSON I/O."""

from learning_spark_datagen.datagen.gen_user import GenUser
from learning_spark_datagen.datagen.gen_order import GenOrder

__all__ = ["GenUser", "GenOrder"]
