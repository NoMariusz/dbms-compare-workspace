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
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (email) DO NOTHING"
            ),
            params=(
                payload["username"],
                payload["email"],
                payload["password"],
                payload["phone"],
                payload["id_role"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        raise NotImplementedError("run_for_mongodb is not implemented yet")

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        raise NotImplementedError("run_for_couchdb is not implemented yet")
