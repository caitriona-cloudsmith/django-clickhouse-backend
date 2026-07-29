import re

from django.db import connection
from django.test import TestCase

from clickhouse_backend.models import MergeTree
from clickhouse_backend.utils.timezone import get_timezone

from . import models


def normalize_engine_full(engine_full):
    """Normalize engine_full for comparison across ClickHouse versions.

    Newer ClickHouse versions preserve parentheses around single-expression
    PARTITION BY / PRIMARY KEY clauses that older versions strip.
    """
    engine_full = re.sub(
        r"PARTITION BY \((.*)\) PRIMARY KEY",
        r"PARTITION BY \1 PRIMARY KEY",
        engine_full,
    )
    engine_full = re.sub(
        r"PRIMARY KEY \(([^)]*)\) ORDER BY",
        r"PRIMARY KEY \1 ORDER BY",
        engine_full,
    )
    return engine_full


class TestMergeTree(TestCase):
    def assertEngineEquals(self, model, engine):
        with connection.cursor() as cursor:
            cursor.execute(
                "select engine_full from system.tables "
                "where database = currentDatabase() and table = %s",
                [model._meta.db_table],
            )
            engine_full = cursor.fetchone()[0]
        self.assertEqual(
            normalize_engine_full(engine_full.partition(" SETTINGS ")[0]),
            normalize_engine_full(engine),
        )

    def test_table(self):
        self.assertEngineEquals(
            models.Event,
            f"MergeTree PARTITION BY toYYYYMMDD(timestamp, '{get_timezone()}') PRIMARY KEY timestamp ORDER BY (timestamp, id)",
        )
        self.assertEngineEquals(
            models.ReplacingMergeTree, "ReplacingMergeTree(ver, is_deleted) ORDER BY id"
        )
        self.assertEngineEquals(
            models.ReplicatedReplacingMergeTree,
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver, is_deleted) ORDER BY id",
        )
        # ClickHouse expands {database} in the stored engine_full.
        db_name = connection.settings_dict["NAME"]
        self.assertEngineEquals(
            models.ReplicatedReplacingMergeTreeWithZooReplica,
            f"ReplicatedReplacingMergeTree('/clickhouse/tables/{db_name}/{{shard}}/table_name', '{{replica}}') ORDER BY id",
        )

    def test_mergetree_init_exception(self):
        with self.assertRaisesMessage(
            AssertionError, "At least one of order_by or primary_key must be provided"
        ):
            MergeTree()
        with self.assertRaisesMessage(ValueError, "None is not allowed in order_by"):
            MergeTree(order_by=(None, "a"))
        with self.assertRaisesMessage(
            ValueError, "primary_key must be a prefix of order_by"
        ):
            MergeTree(order_by=("a", "b"), primary_key=["b"])
        with self.assertRaisesMessage(
            ValueError, "primary_key must be a prefix of order_by"
        ):
            MergeTree(order_by=("a", "b"), primary_key=["a", "b", "c"])


class TestEngineSettings(TestCase):
    def test(self):
        opts = models.EngineWithSettings._meta
        with connection.cursor() as cursor:
            cursor.execute(
                "select engine_full from system.tables "
                "where database = currentDatabase() and table = %s",
                [opts.db_table],
            )
            engine_full = cursor.fetchone()[0]
        for k, v in opts.engine.settings.items():
            self.assertTrue(f"{k} = {v}" in engine_full)
