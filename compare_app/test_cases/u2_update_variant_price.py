from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U2UpdateVariantPriceTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u2_update_variant_price")

    def _payload(self) -> dict[str, object]:
        return {
            "model_name": "benchmark_model",
            "color_name": "red",
            "new_price": 949.99,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "WITH target_model AS ("
                "SELECT id_model FROM models WHERE model_name = %s "
                "ORDER BY id_model DESC LIMIT 1"
                ") "
                "UPDATE product "
                "SET price = %s "
                "WHERE id_model = (SELECT id_model FROM target_model) "
                "AND color_name = %s"
            ),
            params=(
                payload["model_name"],
                payload["new_price"],
                payload["color_name"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
