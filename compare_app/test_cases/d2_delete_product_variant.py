from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D2DeleteProductVariantTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d2_delete_product_variant")
        self.product_id_to_delete: int | None = None

    def prepare_for_postgresql(self, connector: PostgresConnector) -> None:
        inserted_product = connector.read_row(
            query=(
                "WITH selected_model AS ("
                "SELECT id_model FROM models ORDER BY id_model DESC LIMIT 1"
                "), selected_specification AS ("
                "SELECT id_specification FROM gear_specifications ORDER BY id_specification DESC LIMIT 1"
                ") "
                "INSERT INTO product ("
                "id_model, id_specification, color_name, size_value, stock_quantity, price, description"
                ") "
                "SELECT "
                "selected_model.id_model, selected_specification.id_specification, %s, %s, %s, %s, %s "
                "FROM selected_model CROSS JOIN selected_specification "
                "RETURNING id_product"
            ),
            params=(
                "benchmark_d2",
                "42",
                10,
                299.99,
                "benchmark_d2_delete_product_variant",
            ),
        )
        if not inserted_product:
            raise ValueError("Failed to prepare D2 test case: could not insert product")
        self.product_id_to_delete = int(inserted_product["id_product"])

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        if self.product_id_to_delete is None:
            raise ValueError("D2 test case is not prepared: missing product id to delete")
        connector.delete_rows(
            query=(
                "DELETE FROM product "
                "WHERE id_product = %s"
            ),
            params=(
                self.product_id_to_delete,
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
