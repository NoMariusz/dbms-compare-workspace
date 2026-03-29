from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D1DeleteOrderWithItemsTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d1_delete_order_with_items")

    def _payload(self) -> dict[str, object]:
        return {
            "target_order_id": 1,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        # items will be deleted because ON CASCADE is set for the foreign key constraint between order_items and orders
        connector.delete_rows(
            query=(
                "DELETE FROM orders "
                "WHERE id_order = %s"
            ),
            params=(
                payload["target_order_id"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
