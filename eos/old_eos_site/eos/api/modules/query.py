from django.db import connection


def select_one(query_str):
    with connection.cursor() as cursor:
        cursor.execute(
            query_str
        )
        row = cursor.fetchone()
    return row[0]


def select_many(query_str):
    with connection.cursor() as cursor:
        cursor.execute(
            query_str
        )
        rows = cursor.fetchall()
    return rows


def insert(query_str):
    with connection.cursor() as cursor:
        cursor.execute(
            query_str
        )


def update(query_str):
    with connection.cursor() as cursor:
        cursor.execute(
            query_str
        )


def delete(query_str):
    with connection.cursor() as cursor:
        cursor.execute(
            query_str
        )
