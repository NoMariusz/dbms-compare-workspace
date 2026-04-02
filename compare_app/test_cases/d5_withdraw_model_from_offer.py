from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase
from datetime import date, datetime


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
            
    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        manufacturer_id = connector.get_next_business_id("manufacturers")
        model_id = connector.get_next_business_id("models")
        first_spec_id = connector.get_next_business_id("gear_specifications")
        second_spec_id = first_spec_id + 1
        first_product_id = connector.get_next_business_id("product")
        second_product_id = first_product_id + 1
        unique_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

        connector.insert_one(
            collection_name="manufacturers",
            document={
                "id_manufacturer": manufacturer_id,
                "name": f"benchmark_d5_mf_{unique_suffix}",
            },
        )

        connector.insert_one(
            collection_name="models",
            document={
                "id_model": model_id,
                "id_manufacturer": manufacturer_id,
                "model_name": f"benchmark_d5_model_{unique_suffix}",
                "description": "benchmark_d5_withdraw_model_from_offer",
                "release_date": date.today().isoformat(),
            },
        )

        connector.insert_many(
            collection_name="gear_specifications",
            documents=[
                {
                    "id_specification": first_spec_id,
                    "wheel_size": 84,
                    "number_of_wheels": 4,
                    "blade_material": "aluminium",
                    "boot_material": "composite",
                    "bearing_type": "ABEC-7",
                },
                {
                    "id_specification": second_spec_id,
                    "wheel_size": 90,
                    "number_of_wheels": 4,
                    "blade_material": "carbon",
                    "boot_material": "composite",
                    "bearing_type": "ILQ-9",
                },
            ],
        )

        connector.insert_many(
            collection_name="product",
            documents=[
                {
                    "id_product": first_product_id,
                    "id_model": model_id,
                    "id_specification": first_spec_id,
                    "color_name": "benchmark_d5_color",
                    "size_value": "42",
                    "stock_quantity": 5,
                    "price": 499.99,
                    "description": "benchmark_d5_product",
                },
                {
                    "id_product": second_product_id,
                    "id_model": model_id,
                    "id_specification": second_spec_id,
                    "color_name": "benchmark_d5_color",
                    "size_value": "42",
                    "stock_quantity": 5,
                    "price": 499.99,
                    "description": "benchmark_d5_product",
                },
            ],
        )

        self.model_id_to_delete = model_id


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        manufacturer_id = connector.get_next_business_id("manufacturers")
        model_id = connector.get_next_business_id("models")
        first_spec_id = connector.get_next_business_id("gear_specifications")
        second_spec_id = first_spec_id + 1
        first_product_id = connector.get_next_business_id("product")
        second_product_id = first_product_id + 1
        unique_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

        connector.insert_one(
            collection_name="manufacturers",
            document={
                "id_manufacturer": manufacturer_id,
                "name": f"benchmark_d5_mf_{unique_suffix}",
            },
        )

        connector.insert_one(
            collection_name="models",
            document={
                "id_model": model_id,
                "id_manufacturer": manufacturer_id,
                "model_name": f"benchmark_d5_model_{unique_suffix}",
                "description": "benchmark_d5_withdraw_model_from_offer",
                "release_date": date.today().isoformat(),
            },
        )

        connector.insert_many(
            collection_name="gear_specifications",
            documents=[
                {
                    "id_specification": first_spec_id,
                    "wheel_size": 84,
                    "number_of_wheels": 4,
                    "blade_material": "aluminium",
                    "boot_material": "composite",
                    "bearing_type": "ABEC-7",
                },
                {
                    "id_specification": second_spec_id,
                    "wheel_size": 90,
                    "number_of_wheels": 4,
                    "blade_material": "carbon",
                    "boot_material": "composite",
                    "bearing_type": "ILQ-9",
                },
            ],
        )

        connector.insert_many(
            collection_name="product",
            documents=[
                {
                    "id_product": first_product_id,
                    "id_model": model_id,
                    "id_specification": first_spec_id,
                    "color_name": "benchmark_d5_color",
                    "size_value": "42",
                    "stock_quantity": 5,
                    "price": 499.99,
                    "description": "benchmark_d5_product",
                },
                {
                    "id_product": second_product_id,
                    "id_model": model_id,
                    "id_specification": second_spec_id,
                    "color_name": "benchmark_d5_color",
                    "size_value": "42",
                    "stock_quantity": 5,
                    "price": 499.99,
                    "description": "benchmark_d5_product",
                },
            ],
        )

        self.model_id_to_delete = model_id


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        if self.model_id_to_delete is None:
            raise ValueError("D5 test case is not prepared: missing model id to delete")

        model_products = connector.read_many(
            collection_name="product",
            filter_query={"id_model": self.model_id_to_delete},
            projection={"id_product": 1, "id_specification": 1, "_id": 0},
        )
        specification_ids = sorted(
            {product["id_specification"] for product in model_products if "id_specification" in product}
        )

        if model_products:
            connector.delete_many(
                collection_name="product",
                filter_query={"id_model": self.model_id_to_delete},
            )

        for specification_id in specification_ids:
            still_used = connector.read_one(
                collection_name="product",
                filter_query={"id_specification": specification_id},
                projection={"id_product": 1, "_id": 0},
            )
            if still_used is None:
                connector.delete_one(
                    collection_name="gear_specifications",
                    filter_query={"id_specification": specification_id},
                )

        connector.delete_one(
            collection_name="models",
            filter_query={"id_model": self.model_id_to_delete},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        if self.model_id_to_delete is None:
            raise ValueError("D5 test case is not prepared: missing model id to delete")

        model_products = connector.read_many(
            collection_name="product",
            filter_query={"id_model": self.model_id_to_delete},
            projection={"id_product": 1, "id_specification": 1, "_id": 0},
        )
        specification_ids = sorted(
            {product["id_specification"] for product in model_products if "id_specification" in product}
        )

        if model_products:
            connector.delete_many(
                collection_name="product",
                filter_query={"id_model": self.model_id_to_delete},
            )

        for specification_id in specification_ids:
            still_used = connector.read_one(
                collection_name="product",
                filter_query={"id_specification": specification_id},
                projection={"id_product": 1, "_id": 0},
            )
            if still_used is None:
                connector.delete_one(
                    collection_name="gear_specifications",
                    filter_query={"id_specification": specification_id},
                )

        connector.delete_one(
            collection_name="models",
            filter_query={"id_model": self.model_id_to_delete},
        )
