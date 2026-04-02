from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase
from datetime import datetime

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
        
    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        user_id = connector.get_next_business_id("users")
        unique_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

        connector.insert_one(
            collection_name="users",
            document={
                "id_user": user_id,
                "username": "benchmark_d4_delete_user",
                "email": f"benchmark_d4_{unique_suffix}@example.com",
                "password": "benchmark_password",
                "phone": "+10000000001",
                "id_role": 2,
            },
        )
        self.user_id_to_delete = user_id


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        user_id = connector.get_next_business_id("users")
        unique_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

        connector.insert_one(
            collection_name="users",
            document={
                "id_user": user_id,
                "username": "benchmark_d4_delete_user",
                "email": f"benchmark_d4_{unique_suffix}@example.com",
                "password": "benchmark_password",
                "phone": "+10000000001",
                "id_role": 2,
            },
        )
        self.user_id_to_delete = user_id


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        if self.user_id_to_delete is None:
            raise ValueError("D4 test case is not prepared: missing user id to delete")

        user_orders = connector.read_one(
            collection_name="orders",
            filter_query={"id_user": self.user_id_to_delete},
            projection={"id_order": 1, "_id": 0},
        )
        if user_orders is None:
            connector.delete_one(
                collection_name="users",
                filter_query={"id_user": self.user_id_to_delete},
            )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        if self.user_id_to_delete is None:
            raise ValueError("D4 test case is not prepared: missing user id to delete")

        user_orders = connector.read_one(
            collection_name="orders",
            filter_query={"id_user": self.user_id_to_delete},
            projection={"id_order": 1, "_id": 0},
        )
        if user_orders is None:
            connector.delete_one(
                collection_name="users",
                filter_query={"id_user": self.user_id_to_delete},
            )