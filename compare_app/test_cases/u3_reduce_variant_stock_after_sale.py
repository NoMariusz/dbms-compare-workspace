from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U3ReduceVariantStockAfterSaleTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u3_reduce_variant_stock_after_sale")

    def _payload(self) -> dict[str, object]:
        return {
            "model_name": "benchmark_model",
            "color_name": "red",
            "size_value": "42",
            "sold_quantity": 3,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "WITH target_variant AS ("
                "SELECT p.id_product "
                "FROM product p "
                "JOIN models m ON m.id_model = p.id_model "
                "WHERE m.model_name = %s "
                "AND p.color_name = %s "
                "AND p.size_value = %s "
                "ORDER BY p.id_product DESC LIMIT 1"
                ") "
                "UPDATE product "
                "SET stock_quantity = GREATEST(stock_quantity - %s, 0) "
                "WHERE id_product = (SELECT id_product FROM target_variant)"
            ),
            params=(
                payload["model_name"],
                payload["color_name"],
                payload["size_value"],
                payload["sold_quantity"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
