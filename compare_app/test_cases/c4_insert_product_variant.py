from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class C4InsertProductVariantTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="c4_insert_product_variant")

    def _variant_payload(self) -> dict[str, object]:
        return {
            "color_name": "red",
            "size_value": "42",
            "stock_quantity": 20,
            "price": 899.99,
            "description": "Benchmark variant of an existing product model.",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        variant = self._variant_payload()

        connector.insert_row(
            query=(
                "WITH base_product AS ("
                "SELECT id_model, id_specification "
                "FROM product "
                "ORDER BY id_product DESC LIMIT 1"
                ") "
                "INSERT INTO product ("
                "id_model, id_specification, color_name, size_value, stock_quantity, price, description"
                ") "
                "SELECT id_model, id_specification, %s, %s, %s, %s, %s "
                "FROM base_product"
            ),
            params=(
                variant["color_name"],
                variant["size_value"],
                variant["stock_quantity"],
                variant["price"],
                variant["description"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
