from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R3SearchProductsByManufacturerTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r3_search_products_by_manufacturer")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_rows(
            query=(
                "SELECT "
                "p.id_product, p.color_name, p.size_value, p.stock_quantity, p.price, "
                "m.id_model, m.model_name, mf.id_manufacturer, mf.name AS manufacturer_name "
                "FROM product p "
                "JOIN models m ON m.id_model = p.id_model "
                "JOIN manufacturers mf ON mf.id_manufacturer = m.id_manufacturer "
                "WHERE mf.name = %s"
            ),
            params=("benchmark_manufacturer",),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        manufacturer = connector.read_one(
            collection_name="manufacturers",
            filter_query={"name": "benchmark_manufacturer"},
            projection={"_id": 0, "id_manufacturer": 1, "name": 1},
        )
        if manufacturer is None:
            return

        models = connector.read_many(
            collection_name="models",
            filter_query={"id_manufacturer": manufacturer["id_manufacturer"]},
            projection={"_id": 0, "id_model": 1, "model_name": 1, "id_manufacturer": 1},
        )
        if not models:
            return

        models_by_id = {model["id_model"]: model for model in models}
        model_ids = list(models_by_id.keys())

        products = connector.read_many_in_batches(
            collection_name="product",
            field_name="id_model",
            values=model_ids,
            projection={
                "_id": 0,
                "id_product": 1,
                "id_model": 1,
                "color_name": 1,
                "size_value": 1,
                "stock_quantity": 1,
                "price": 1,
            },
            chunk_size=5000,
        )

        products.sort(key=lambda product: int(product["id_product"]))

        _ = [
            {
                "id_product": product["id_product"],
                "color_name": product["color_name"],
                "size_value": product["size_value"],
                "stock_quantity": product["stock_quantity"],
                "price": product["price"],
                "id_model": product["id_model"],
                "model_name": models_by_id[product["id_model"]]["model_name"],
                "id_manufacturer": manufacturer["id_manufacturer"],
                "manufacturer_name": manufacturer["name"],
            }
            for product in products
            if product["id_model"] in models_by_id
        ]


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        manufacturer = connector.read_one(
            collection_name="manufacturers",
            filter_query={"name": "benchmark_manufacturer"},
            projection={"_id": 0, "id_manufacturer": 1, "name": 1},
        )
        if manufacturer is None:
            return

        models = connector.read_many(
            collection_name="models",
            filter_query={"id_manufacturer": manufacturer["id_manufacturer"]},
            projection={"_id": 0, "id_model": 1, "model_name": 1, "id_manufacturer": 1},
        )
        if not models:
            return

        models_by_id = {model["id_model"]: model for model in models}
        model_ids = list(models_by_id.keys())

        products = connector.read_many(
            collection_name="product",
            filter_query={"id_model": {"$in": model_ids}},
            projection={
                "_id": 0,
                "id_product": 1,
                "id_model": 1,
                "color_name": 1,
                "size_value": 1,
                "stock_quantity": 1,
                "price": 1,
            },
        )

        result = [
            {
                "id_product": product["id_product"],
                "color_name": product["color_name"],
                "size_value": product["size_value"],
                "stock_quantity": product["stock_quantity"],
                "price": product["price"],
                "id_model": product["id_model"],
                "model_name": models_by_id[product["id_model"]]["model_name"],
                "id_manufacturer": manufacturer["id_manufacturer"],
                "manufacturer_name": manufacturer["name"],
            }
            for product in products
            if product["id_model"] in models_by_id
        ]

        result.sort(key=lambda product: int(product["id_product"]))
        _ = result
