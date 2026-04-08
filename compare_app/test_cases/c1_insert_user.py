from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class C1InsertUserTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="c1_insert_user")

    def _payload(self) -> dict[str, object]:
        return {
            "id_user": 999999,
            "username": "benchmark_user",
            "email": "benchmark_user@example.com",
            "password": "benchmark_password",
            "phone": "+10000000000",
            "id_role": 2,
            "type": "users",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = dict(self._payload())
        payload.pop("type", None)
        payload.pop("id_user", None)
        connector.insert_row(
            query=(
                "INSERT INTO users (username, email, password, phone, id_role) "
                "SELECT %s, %s, %s, %s, %s "
                "WHERE NOT EXISTS (SELECT 1 FROM users WHERE email = %s)"
            ),
            params=(
                payload["username"],
                payload["email"],
                payload["password"],
                payload["phone"],
                payload["id_role"],
                payload["email"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        payload = dict(self._payload())
        payload.pop("type", None)
        connector.insert_one_ignore_duplicates(
            collection_name="users",
            document=payload,
        )

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        connector.insert_one_ignore_duplicates(
            collection_name="users",
            document=self._payload(),
        )
        
    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.delete_many(
            collection_name="users",
            filter_query={"id_user": self._payload()["id_user"]},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        connector.delete_one_by_business_id(
            collection_name="users",
            business_id=int(self._payload()["id_user"]),
        )