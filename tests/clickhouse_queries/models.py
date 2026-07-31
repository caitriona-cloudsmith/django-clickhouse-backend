from django.db.models import CASCADE, ForeignKey

from clickhouse_backend import models


class Author(models.ClickhouseModel):
    name = models.StringField(max_length=10)
    num = models.UInt32Field()


class Book(models.ClickhouseModel):
    author = ForeignKey(Author, on_delete=CASCADE, related_name="books")
    name = models.StringField(max_length=10)


class Article(models.ClickhouseModel):
    title = models.StringField(max_length=10)
    book = models.Int64Field()


class Visit(models.ClickhouseModel):
    """A table that supports the SAMPLE clause."""

    uid = models.UInt64Field()
    author = ForeignKey(Author, on_delete=CASCADE, related_name="visits")

    class Meta:
        engine = models.MergeTree(
            order_by=(models.farmFingerprint64("uid"), "id"),
            sample_by=models.farmFingerprint64("uid"),
        )
