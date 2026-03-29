from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R6StaleInventoryReportTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r6_stale_inventory_report")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_row(
            query=(
                "SELECT COALESCE(json_agg(report_rows), '[]'::json) AS stale_inventory "
                "FROM ("
                "SELECT "
                "p.id_product, p.id_model, p.color_name, p.size_value, p.stock_quantity, p.price, "
                "MAX(o.order_date) AS last_purchase_at "
                "FROM product p "
                "LEFT JOIN order_items oi ON oi.id_product = p.id_product "
                "LEFT JOIN orders o ON o.id_order = oi.id_order "
                "WHERE p.stock_quantity > 0 "
                "GROUP BY p.id_product, p.id_model, p.color_name, p.size_value, p.stock_quantity, p.price "
                "ORDER BY last_purchase_at ASC NULLS FIRST, p.stock_quantity DESC, p.id_product "
                "LIMIT %s"
                ") AS report_rows"
            ),
            params=(10,),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
