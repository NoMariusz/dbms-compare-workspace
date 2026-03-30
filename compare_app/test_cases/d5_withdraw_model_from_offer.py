from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D5WithdrawModelFromOfferTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d5_withdraw_model_from_offer")
        self.model_id_to_delete: int | None = None

    def prepare_for_postgresql(self, connector: PostgresConnector) -> None:
        inserted_model = connector.read_row(
            query=(
                "WITH new_manufacturer AS ("
                "INSERT INTO manufacturers (name) "
                "VALUES ('benchmark_d5_mf_' || clock_timestamp()::text) "
                "RETURNING id_manufacturer"
                "), new_model AS ("
                "INSERT INTO models (id_manufacturer, model_name, description, release_date) "
                "SELECT id_manufacturer, ('benchmark_d5_model_' || clock_timestamp()::text), %s, CURRENT_DATE "
                "FROM new_manufacturer "
                "RETURNING id_model"
                "), new_specs AS ("
                "INSERT INTO gear_specifications (wheel_size, number_of_wheels, blade_material, boot_material, bearing_type) "
                "VALUES (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s) "
                "RETURNING id_specification"
                "), inserted_products AS ("
                "INSERT INTO product (id_model, id_specification, color_name, size_value, stock_quantity, price, description) "
                "SELECT new_model.id_model, new_specs.id_specification, %s, %s, %s, %s, %s "
                "FROM new_model CROSS JOIN new_specs "
                "RETURNING id_product"
                ") "
                "SELECT id_model FROM new_model LIMIT 1"
            ),
            params=(
                "benchmark_d5_withdraw_model_from_offer",
                84,
                4,
                "aluminium",
                "composite",
                "ABEC-7",
                90,
                4,
                "carbon",
                "composite",
                "ILQ-9",
                "benchmark_d5_color",
                "42",
                5,
                499.99,
                "benchmark_d5_product",
            ),
        )
        if not inserted_model:
            raise ValueError("Failed to prepare D5 test case: could not insert model")
        self.model_id_to_delete = int(inserted_model["id_model"])

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        if self.model_id_to_delete is None:
            raise ValueError("D5 test case is not prepared: missing model id to delete")
        connector.delete_rows(
            query=(
                "WITH model_products AS ("
                "SELECT id_product, id_specification FROM product WHERE id_model = %s"
                "), "
                "deleted_products AS ("
                "DELETE FROM product "
                "WHERE id_model = %s "
                "RETURNING id_specification"
                "), "
                "deleted_specs AS ("
                "DELETE FROM gear_specifications gs "
                "WHERE gs.id_specification IN (SELECT DISTINCT id_specification FROM deleted_products) "
                "AND NOT EXISTS ("
                "SELECT 1 FROM product p WHERE p.id_specification = gs.id_specification"
                ")"
                ") "
                "DELETE FROM models WHERE id_model = %s"
            ),
            params=(
                self.model_id_to_delete,
                self.model_id_to_delete,
                self.model_id_to_delete,
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
