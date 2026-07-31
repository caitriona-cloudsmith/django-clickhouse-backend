from django.db import NotSupportedError, OperationalError
from django.db.models import Count, Window
from django.db.models.functions import Rank
from django.db.models.sql import Query as DjangoQuery
from django.test import TestCase

from clickhouse_backend import compat

from . import models


class QueriesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.a1, cls.a2 = models.Author.objects.bulk_create(
            [models.Author(name="a1", num=1001), models.Author(name="a2", num=2002)]
        )
        cls.b1, cls.b2, cls.b3, cls.b4 = models.Book.objects.bulk_create(
            [
                models.Book(name="b1", author=cls.a1),
                models.Book(name="b2", author=cls.a1),
                models.Book(name="b3", author=cls.a2),
                models.Book(name="b4", author=cls.a2),
            ]
        )
        models.Article.objects.bulk_create(
            [
                models.Article(title="t1", book=cls.b1.id),
                models.Article(title="t2", book=cls.b2.id),
            ]
        )

    def test_prewhere(self):
        qs = models.Author.objects.prewhere(name="a1")
        self.assertIn("PREWHERE", str(qs.query))
        self.assertEqual(qs[0].name, "a1")

    def test_prewhere_fk(self):
        b1, b2 = (
            models.Book.objects.filter(author__name=self.a1.name)
            .prewhere(author_id=self.a1.id)
            .order_by("id")
        )
        self.assertTrue(b1.id == self.b1.id and b2.id == self.b2.id)

    # clickhouse backend will generate suitable query, but clickhouse will raise exception.
    # clickhouse 23.11
    # DB::Exception: Missing columns: 'clickhouse_queries_article.book' while processing query: 'SELECT name FROM clickhouse_queries_book AS U0 PREWHERE id = clickhouse_queries_article.book', required columns: 'name' 'id' 'clickhouse_queries_article.book', maybe you meant: 'name' or 'id': While processing (SELECT U0.name FROM clickhouse_queries_book AS U0 PREWHERE U0.id = clickhouse_queries_article.book) AS book_name.
    # clickhouse 24.6
    # DB::Exception: Resolve identifier 'clickhouse_queries_article.book' from parent scope only supported for constants and CTE. Actual test_default.clickhouse_queries_article.book node type COLUMN. In scope (SELECT U0.name FROM clickhouse_queries_book AS U0 PREWHERE U0.id = clickhouse_queries_article.book) AS book_name.
    # def test_prewhere_subquery(self):
    #     a = models.Article.objects.annotate(
    #         book_name=Subquery(
    #             models.Book.objects.prewhere(id=OuterRef("book")).values("name")
    #         )
    #     ).get(title="t1")
    #     self.assertEqual(a.book_name, self.b1.name)

    def test_prewhere_agg(self):
        with self.assertRaisesMessage(
            NotSupportedError,
            "Aggregate function is disallowed in the prewhere clause.",
        ):
            list(
                models.Author.objects.annotate(count=Count("books")).prewhere(
                    count__gt=0
                )
            )

    if compat.dj_ge42:

        def test_prewhere_window(self):
            with self.assertRaisesMessage(
                NotSupportedError,
                "Window function is disallowed in the prewhere clause.",
            ):
                list(
                    models.Book.objects.annotate(
                        rank=Window(Rank(), partition_by="author", order_by="name")
                    ).prewhere(rank__gt=1)
                )


class SampleTests(TestCase):
    total = 10000

    @classmethod
    def setUpTestData(cls):
        cls.author = models.Author.objects.create(name="a1", num=1001)
        models.Visit.objects.bulk_create(
            models.Visit(uid=i, author=cls.author) for i in range(cls.total)
        )

    def test_sample(self):
        sql = str(models.Visit.objects.sample(0.1).query)
        self.assertIn("SAMPLE 0.1", sql)
        self.assertNotIn("OFFSET", sql)

    def test_sample_offset(self):
        self.assertIn(
            "SAMPLE 0.1 OFFSET 0.05",
            str(models.Visit.objects.sample(0.1, 0.05).query),
        )

    def test_sample_rows(self):
        self.assertIn("SAMPLE 1000", str(models.Visit.objects.sample(1000).query))

    def test_sample_before_prewhere_and_where(self):
        sql = str(models.Visit.objects.sample(0.1).prewhere(uid=1).filter(id=2).query)
        self.assertLess(sql.index("SAMPLE 0.1"), sql.index("PREWHERE"))
        self.assertLess(sql.index("PREWHERE"), sql.index("WHERE"))

    def test_sample_before_join(self):
        """SAMPLE belongs to the leftmost table, it must precede any JOIN.

        Note that ClickHouse itself does not support SAMPLE together with JOIN
        (it fails with an internal error up to at least 24.3), so only the
        generated SQL is asserted here.
        """
        sql = str(
            models.Visit.objects.sample(0.1).filter(author__name=self.author.name).query
        )
        self.assertRegex(sql, r'FROM "[^"]*visit" SAMPLE 0\.1 INNER JOIN ')

    def test_sample_in_aggregate_subquery(self):
        sql = str(models.Visit.objects.sample(0.1).values("uid").query)
        self.assertIn("SAMPLE 0.1", sql)
        self.assertEqual(models.Visit.objects.sample(1).count(), self.total)

    def test_sample_result(self):
        """SAMPLE k and SAMPLE k OFFSET k read disjoint halves of the table."""
        first = models.Visit.objects.sample(0.5).count()
        second = models.Visit.objects.sample(0.5, 0.5).count()
        self.assertGreater(first, 0)
        self.assertGreater(second, 0)
        self.assertEqual(first + second, self.total)

    def test_sample_manager(self):
        self.assertEqual(models.Visit.objects.sample(1).count(), self.total)

    def test_sample_invalid_value(self):
        """Sample values are not validated, ClickHouse rejects what it dislikes."""
        for value in ["0.1", -1]:
            with self.subTest(value=value):
                with self.assertRaises(OperationalError):
                    models.Visit.objects.sample(value).count()

    def test_sample_sliced(self):
        with self.assertRaisesMessage(
            TypeError, "Cannot sample a query once a slice has been taken."
        ):
            models.Visit.objects.all()[:1].sample(0.1)

    def test_sample_combined_query(self):
        qs = models.Visit.objects.all() | models.Visit.objects.all()
        with self.assertRaisesMessage(
            NotSupportedError,
            "Calling QuerySet.sample() after union() is not supported.",
        ):
            models.Visit.objects.union(qs).sample(0.1)

    def test_no_sample_on_plain_query(self):
        """A stock django Query has no sample attributes, it must still compile."""
        query = DjangoQuery(models.Visit)
        sql, _ = query.get_compiler(using="default").as_sql()
        self.assertNotIn("SAMPLE", sql)
        self.assertRegex(sql, r'FROM "[^"]*visit"')
