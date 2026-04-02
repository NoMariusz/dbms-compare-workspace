from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class C3InsertProductWithSpecificationTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="c3_insert_product_with_specification")

    def _spec_payload(self) -> dict[str, object]:
        return {
            "wheel_size": 90,
            "number_of_wheels": 4,
            "blade_material": "Aluminum",
            "boot_material": "Synthetic",
            "bearing_type": "ABEC-9",
        }

    def _product_payload(self) -> dict[str, object]:
        return {
            "color_name": "black",
            "size_value": "42",
            "stock_quantity": 25,
            "price": 899.99,
            "description": "Benchmark product inserted with new gear specification.",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        spec = self._spec_payload()
        product = self._product_payload()

        connector.insert_row(
            query=(
                "WITH new_specification AS ("
                "INSERT INTO gear_specifications ("
                "wheel_size, number_of_wheels, blade_material, boot_material, bearing_type"
                ") VALUES (%s, %s, %s, %s, %s) "
                "RETURNING id_specification"
                "), selected_model AS ("
                "SELECT id_model FROM models ORDER BY id_model DESC LIMIT 1"
                ") "
                "INSERT INTO product ("
                "id_model, id_specification, color_name, size_value, stock_quantity, price, description"
                ") "
                "SELECT selected_model.id_model, new_specification.id_specification, %s, %s, %s, %s, %s "
                "FROM selected_model CROSS JOIN new_specification"
            ),
            params=(
                spec["wheel_size"],
                spec["number_of_wheels"],
                spec["blade_material"],
                spec["boot_material"],
                spec["bearing_type"],
                product["color_name"],
                product["size_value"],
                product["stock_quantity"],
                product["price"],
                product["description"],
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.delete_many(
            collection_name="product",
            filter_query={"description": self._product_payload()["description"]},
        )
        connector.delete_many(
            collection_name="gear_specifications",
            filter_query={"bearing_type": self._spec_payload()["bearing_type"]},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        connector.delete_many(
            collection_name="product",
            filter_query={"description": self._product_payload()["description"]},
        )
        connector.delete_many(
            collection_name="gear_specifications",
            filter_query={"bearing_type": self._spec_payload()["bearing_type"]},
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        selected_model = connector.read_latest("models")
        if selected_model is None:
            raise RuntimeError("No model found for C3InsertProductWithSpecificationTestCase")

        spec = self._spec_payload()
        product = self._product_payload()

        specification_id = connector.get_next_business_id("gear_specifications")
        product_id = connector.get_next_business_id("product")

        connector.insert_one(
            collection_name="gear_specifications",
            document={
                "id_specification": specification_id,
                "wheel_size": spec["wheel_size"],
                "number_of_wheels": spec["number_of_wheels"],
                "blade_material": spec["blade_material"],
                "boot_material": spec["boot_material"],
                "bearing_type": spec["bearing_type"],
            },
        )

        connector.insert_one(
            collection_name="product",
            document={
                "id_product": product_id,
                "id_model": selected_model["id_model"],
                "id_specification": specification_id,
                "color_name": product["color_name"],
                "size_value": product["size_value"],
                "stock_quantity": product["stock_quantity"],
                "price": product["price"],
                "description": product["description"],
            },
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        selected_model = connector.read_latest("models")
        if selected_model is None:
            raise RuntimeError("No model found for C3InsertProductWithSpecificationTestCase")

        spec = self._spec_payload()
        product = self._product_payload()

        specification_id = connector.get_next_business_id("gear_specifications")
        product_id = connector.get_next_business_id("product")

        connector.insert_one(
            collection_name="gear_specifications",
            document={
                "id_specification": specification_id,
                "wheel_size": spec["wheel_size"],
                "number_of_wheels": spec["number_of_wheels"],
                "blade_material": spec["blade_material"],
                "boot_material": spec["boot_material"],
                "bearing_type": spec["bearing_type"],
            },
        )

        connector.insert_one(
            collection_name="product",
            document={
                "id_product": product_id,
                "id_model": selected_model["id_model"],
                "id_specification": specification_id,
                "color_name": product["color_name"],
                "size_value": product["size_value"],
                "stock_quantity": product["stock_quantity"],
                "price": product["price"],
                "description": product["description"],
            },
        )
