from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R3SearchProductsByManufacturerTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r3_search_products_by_manufacturer")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_row(
            query=(
                "SELECT COALESCE(json_agg(products), '[]'::json) AS products "
                "FROM ("
                "SELECT "
                "p.id_product, p.color_name, p.size_value, p.stock_quantity, p.price, "
                "m.id_model, m.model_name, mf.id_manufacturer, mf.name AS manufacturer_name "
                "FROM product p "
                "JOIN models m ON m.id_model = p.id_model "
                "JOIN manufacturers mf ON mf.id_manufacturer = m.id_manufacturer "
                "WHERE mf.name = %s "
                "ORDER BY p.id_product"
                ") AS products"
            ),
            params=("benchmark_manufacturer",),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
