from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D3DeleteOrderItemTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d3_delete_order_item")

    def _payload(self) -> dict[str, object]:
        return {
            "target_order_item_id": 1,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.delete_rows(
            query=(
                "DELETE FROM order_items "
                "WHERE id_order_item = %s"
            ),
            params=(
                payload["target_order_item_id"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
