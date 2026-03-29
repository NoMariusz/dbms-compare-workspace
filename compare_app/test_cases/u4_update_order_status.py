from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U4UpdateOrderStatusTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u4_update_order_status")

    def _payload(self) -> dict[str, object]:
        return {
            "target_order_id": 1,
            "new_status_id": 3,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "UPDATE orders "
                "SET id_status = %s "
                "WHERE id_order = %s"
            ),
            params=(
                payload["new_status_id"],
                payload["target_order_id"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
