from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D6CleanupStaleZeroStockVariantsTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d6_cleanup_stale_zero_stock_variants")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.delete_rows(
            query=(
                "DELETE FROM product p "
                "WHERE p.stock_quantity = 0 "
                "AND NOT EXISTS ("
                "SELECT 1 "
                "FROM order_items oi "
                "JOIN orders o ON o.id_order = oi.id_order "
                "WHERE oi.id_product = p.id_product "
                "AND o.order_date >= NOW() - INTERVAL '1 year'"
                ") "
                "AND NOT EXISTS ("
                "SELECT 1 FROM order_items oi WHERE oi.id_product = p.id_product"
                ")"
            ),
            params=(),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
