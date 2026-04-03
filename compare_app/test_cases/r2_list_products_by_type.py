from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R2ListProductsByTypeTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r2_list_products_by_type")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_rows(
            query=(
                "SELECT "
                "p.id_product, p.id_model, p.id_specification, p.color_name, p.size_value, "
                "p.stock_quantity, p.price, p.description "
                "FROM product p "
                "JOIN models_to_product_types mpt ON mpt.id_model = p.id_model "
                "JOIN product_types pt ON pt.id_type = mpt.id_type "
                "WHERE pt.type_name = %s"
            ),
            params=("In-line Skates",),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        product_type = connector.read_one(
            collection_name="product_types",
            filter_query={"type_name": "In-line Skates"},
            projection={"_id": 0, "id_type": 1, "type_name": 1},
        )
        if product_type is None:
            return

        mappings = connector.read_many(
            collection_name="models_to_product_types",
            filter_query={"id_type": product_type["id_type"]},
            projection={"_id": 0, "id_model": 1},
        )
        model_ids = [mapping["id_model"] for mapping in mappings if "id_model" in mapping]
        if not model_ids:
            return

        products = connector.read_many_in_batches(
            collection_name="product",
            field_name="id_model",
            values=model_ids,
            projection={
                "_id": 0,
                "id_product": 1,
                "id_model": 1,
                "id_specification": 1,
                "color_name": 1,
                "size_value": 1,
                "stock_quantity": 1,
                "price": 1,
                "description": 1,
            },
            chunk_size=5000,
        )

        products.sort(key=lambda product: int(product["id_product"]))
        _ = products


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        product_type = connector.read_one(
            collection_name="product_types",
            filter_query={"type_name": "In-line Skates"},
            projection={"_id": 0, "id_type": 1, "type_name": 1},
        )
        if product_type is None:
            return

        mappings = connector.read_many(
            collection_name="models_to_product_types",
            filter_query={"id_type": product_type["id_type"]},
            projection={"_id": 0, "id_model": 1},
        )
        model_ids = [mapping["id_model"] for mapping in mappings if "id_model" in mapping]
        if not model_ids:
            return

        products = connector.read_many(
            collection_name="product",
            filter_query={"id_model": {"$in": model_ids}},
            projection={
                "_id": 0,
                "id_product": 1,
                "id_model": 1,
                "id_specification": 1,
                "color_name": 1,
                "size_value": 1,
                "stock_quantity": 1,
                "price": 1,
                "description": 1,
            },
        )

        products.sort(key=lambda product: int(product["id_product"]))
        _ = products
