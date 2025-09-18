from logging import exception
from django.test import TestCase
from django.db import connection
from django.db.utils import OperationalError as django_OperationalError
from clickhouse_driver.dbapi.errors import OperationalError as ch_driver_OperationalError
from clickhouse_driver.errors import PartiallyConsumedQueryError
from clickhouse_backend.driver import connect, pool
from .. import models

class Tests(TestCase):
    def test_pool_size(self):
        conn = connect(host="localhost", connections_min=2, connections_max=4)
        assert conn.pool.connections_min == 2
        assert conn.pool.connections_max == 4
        assert len(conn.pool._pool) == 2

class IterationTests(TestCase):
    """
    These tests demonstrate issues with a connection being re-added to a pool
    after an iteration is interrupted. This leaves the connection in a bad state
    which then causes errors on subsequent queries.
    """
    @classmethod
    def setUpTestData(cls):
        cls.a1, cls.a2, cls.a3 = models.Author.objects.bulk_create(
            [models.Author(name="a1"), models.Author(name="a2"), models.Author(name="a3")]
        )

    def find_root_exceptions(self, exception):
        """Recursively finds the root cause in an exception chain"""
        if exception.__cause__ is not None:
            return self.find_root_exceptions(exception.__cause__)
        elif exception.__context__ is not None:
            return self.find_root_exceptions(exception.__context__)
        else:
            return exception

    
    def test_connection_unusable_when_iteration_interrupted(self):
        pool = connection.connection.pool
        pool._set_check_if_connection_valid(False)
        connection_count_before = len(pool._pool)

        assert connection_count_before == 1

        # Asserts most recent exception is Django OperationalError
        with self.assertRaises(django_OperationalError) as ex_context:
            # Get queryset
            authors = models.Author.objects.all()
            # Access iterator, but break after first item
            for author in authors.iterator(1):
                author = author.name
                break

            # Assert connection pool size is unchanged despite broken connection
            connection_count_after_iterator = len(pool._pool)
            assert connection_count_after_iterator == 1

            # Try to access queryset again, which won't work via same connection
            author = authors.get(id=self.a1.id)

        # Caused by ch driver driver Operational error
        self.assertIsInstance(ex_context.exception.__cause__, ch_driver_OperationalError)

        # ...The context of which is a PartiallyConsumedQueryError
        self.assertIsInstance(ex_context.exception.__cause__.__context__, PartiallyConsumedQueryError)
    
    def test_connection_not_reused_when_iteration_interrupted(self):
        pool = connection.connection.pool
        pool._set_check_if_connection_valid(True)

        connection_count_before = len(pool._pool)
        assert connection_count_before == 1

        authors = models.Author.objects.all()
        for author in authors.iterator(1):
            author = author.name
            break
            
        connection_count_after_iterator = len(pool._pool)
        # Connection was closed and not returned to pool
        assert connection_count_after_iterator == 0

        author = authors.get(id=self.a1.id)
    