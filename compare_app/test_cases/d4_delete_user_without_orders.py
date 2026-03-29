from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D4DeleteUserWithoutOrdersTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d4_delete_user_without_orders")

    def _payload(self) -> dict[str, object]:
        return {
            "target_user_id": 1,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.delete_rows(
            query=(
                "DELETE FROM users "
                "WHERE id_user = %s "
                "AND NOT EXISTS ("
                "SELECT 1 FROM orders WHERE orders.id_user = users.id_user"
                ")"
            ),
            params=(
                payload["target_user_id"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
