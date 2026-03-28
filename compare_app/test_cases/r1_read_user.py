from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R1ReadUserByEmailTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r1_read_user_by_email")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_row(
            query=(
                "SELECT id_user, username, email, phone, id_role "
                "FROM users WHERE email = %s LIMIT 1"
            ),
            params=("benchmark_user@example.com",),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        connector.read_one(
            collection_name="users",
            filter_query={"email": "benchmark_user@example.com"},
            projection={
                "_id": 0,
                "id_user": 1,
                "username": 1,
                "email": 1,
                "phone": 1,
                "id_role": 1,
            },
        )

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        connector.read_one(
            collection_name="users",
            filter_query={"email": "benchmark_user@example.com"},
            projection={
                "_id": 0,
                "id_user": 1,
                "username": 1,
                "email": 1,
                "phone": 1,
                "id_role": 1,
            },
        )