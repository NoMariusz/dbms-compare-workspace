from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D4DeleteUserWithoutOrdersTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d4_delete_user_without_orders")
        self.user_id_to_delete: int | None = None

    def prepare_for_postgresql(self, connector: PostgresConnector) -> None:
        inserted_user = connector.read_row(
            query=(
                "INSERT INTO users (username, email, password, phone, id_role) "
                "VALUES (%s, ('benchmark_d4_' || clock_timestamp()::text || '@example.com'), %s, %s, %s) "
                "RETURNING id_user"
            ),
            params=(
                "benchmark_d4_delete_user",
                "benchmark_password",
                "+10000000001",
                2,
            ),
        )
        if not inserted_user:
            raise ValueError("Failed to prepare D4 test case: could not insert user")
        self.user_id_to_delete = int(inserted_user["id_user"])

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        if self.user_id_to_delete is None:
            raise ValueError("D4 test case is not prepared: missing user id to delete")
        connector.delete_rows(
            query=(
                "DELETE FROM users "
                "WHERE id_user = %s "
                "AND NOT EXISTS ("
                "SELECT 1 FROM orders WHERE orders.id_user = users.id_user"
                ")"
            ),
            params=(
                self.user_id_to_delete,
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
