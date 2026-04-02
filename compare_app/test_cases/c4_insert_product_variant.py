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

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.delete_many(
            collection_name="product",
            filter_query={"description": self._variant_payload()["description"]},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        connector.delete_many(
            collection_name="product",
            filter_query={"description": self._variant_payload()["description"]},
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        base_product = connector.read_latest("product")
        if base_product is None:
            raise RuntimeError("No product found for C4InsertProductVariantTestCase")

        variant = self._variant_payload()
        product_id = connector.get_next_business_id("product")

        connector.insert_one(
            collection_name="product",
            document={
                "id_product": product_id,
                "id_model": base_product["id_model"],
                "id_specification": base_product["id_specification"],
                "color_name": variant["color_name"],
                "size_value": variant["size_value"],
                "stock_quantity": variant["stock_quantity"],
                "price": variant["price"],
                "description": variant["description"],
            },
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        base_product = connector.read_latest("product")
        if base_product is None:
            raise RuntimeError("No product found for C4InsertProductVariantTestCase")

        variant = self._variant_payload()
        product_id = connector.get_next_business_id("product")

        connector.insert_one(
            collection_name="product",
            document={
                "id_product": product_id,
                "id_model": base_product["id_model"],
                "id_specification": base_product["id_specification"],
                "color_name": variant["color_name"],
                "size_value": variant["size_value"],
                "stock_quantity": variant["stock_quantity"],
                "price": variant["price"],
                "description": variant["description"],
            },
        )
