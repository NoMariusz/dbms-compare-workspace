from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R4ProductDetailsCardTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r4_product_details_card")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_rows(
            query=(
                "WITH selected_product AS ("
                "SELECT "
                "p.id_product, p.id_model, p.id_specification, p.color_name, p.size_value, "
                "p.stock_quantity, p.price, p.description, "
                "m.model_name, m.release_date, "
                "mf.id_manufacturer, mf.name AS manufacturer_name, "
                "gs.wheel_size, gs.number_of_wheels, gs.blade_material, gs.boot_material, gs.bearing_type "
                "FROM product p "
                "JOIN models m ON m.id_model = p.id_model "
                "JOIN manufacturers mf ON mf.id_manufacturer = m.id_manufacturer "
                "JOIN gear_specifications gs ON gs.id_specification = p.id_specification "
                "ORDER BY p.id_product DESC LIMIT 1"
                ") "
                "SELECT "
                "sp.id_product AS product_id, sp.id_model, sp.id_specification, sp.color_name, sp.size_value, "
                "sp.stock_quantity, sp.price, sp.description, "
                "sp.model_name, sp.release_date, sp.id_manufacturer, sp.manufacturer_name, "
                "sp.wheel_size, sp.number_of_wheels, sp.blade_material, sp.boot_material, sp.bearing_type, "
                "p2.id_product AS variant_id_product, p2.color_name AS variant_color_name, "
                "p2.size_value AS variant_size_value, p2.stock_quantity AS variant_stock_quantity, p2.price AS variant_price "
                "FROM selected_product sp "
                "JOIN product p2 ON p2.id_model = sp.id_model "
                "ORDER BY p2.id_product"
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        selected_product = connector.read_latest("product")
        if selected_product is None:
            return

        model = connector.read_one(
            collection_name="models",
            filter_query={"id_model": selected_product["id_model"]},
            projection={
                "_id": 0,
                "id_model": 1,
                "id_manufacturer": 1,
                "model_name": 1,
                "release_date": 1,
            },
        )
        if model is None:
            return

        manufacturer = connector.read_one(
            collection_name="manufacturers",
            filter_query={"id_manufacturer": model["id_manufacturer"]},
            projection={"_id": 0, "id_manufacturer": 1, "name": 1},
        )
        specification = connector.read_one(
            collection_name="gear_specifications",
            filter_query={"id_specification": selected_product["id_specification"]},
            projection={
                "_id": 0,
                "wheel_size": 1,
                "number_of_wheels": 1,
                "blade_material": 1,
                "boot_material": 1,
                "bearing_type": 1,
            },
        )

        variants = connector.read_many(
            collection_name="product",
            filter_query={"id_model": selected_product["id_model"]},
            projection={
                "_id": 0,
                "id_product": 1,
                "color_name": 1,
                "size_value": 1,
                "stock_quantity": 1,
                "price": 1,
            },
            sort=[("id_product", 1)],
        )

        _ = {
            "product": {
                **selected_product,
                "model_name": model.get("model_name"),
                "release_date": model.get("release_date"),
                "id_manufacturer": model.get("id_manufacturer"),
                "manufacturer_name": manufacturer.get("name") if manufacturer else None,
                "wheel_size": specification.get("wheel_size") if specification else None,
                "number_of_wheels": specification.get("number_of_wheels") if specification else None,
                "blade_material": specification.get("blade_material") if specification else None,
                "boot_material": specification.get("boot_material") if specification else None,
                "bearing_type": specification.get("bearing_type") if specification else None,
            },
            "variants": variants,
        }


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        selected_product = connector.read_latest("product")
        if selected_product is None:
            return

        model = connector.read_one(
            collection_name="models",
            filter_query={"id_model": selected_product["id_model"]},
            projection={
                "_id": 0,
                "id_model": 1,
                "id_manufacturer": 1,
                "model_name": 1,
                "release_date": 1,
            },
        )
        if model is None:
            return

        manufacturer = connector.read_one(
            collection_name="manufacturers",
            filter_query={"id_manufacturer": model["id_manufacturer"]},
            projection={"_id": 0, "id_manufacturer": 1, "name": 1},
        )
        specification = connector.read_one(
            collection_name="gear_specifications",
            filter_query={"id_specification": selected_product["id_specification"]},
            projection={
                "_id": 0,
                "wheel_size": 1,
                "number_of_wheels": 1,
                "blade_material": 1,
                "boot_material": 1,
                "bearing_type": 1,
            },
        )

        variants = connector.read_many(
            collection_name="product",
            filter_query={"id_model": selected_product["id_model"]},
            projection={
                "_id": 0,
                "id_product": 1,
                "color_name": 1,
                "size_value": 1,
                "stock_quantity": 1,
                "price": 1,
            },
            sort=[("id_product", 1)],
        )

        _ = {
            "product": {
                **selected_product,
                "model_name": model.get("model_name"),
                "release_date": model.get("release_date"),
                "id_manufacturer": model.get("id_manufacturer"),
                "manufacturer_name": manufacturer.get("name") if manufacturer else None,
                "wheel_size": specification.get("wheel_size") if specification else None,
                "number_of_wheels": specification.get("number_of_wheels") if specification else None,
                "blade_material": specification.get("blade_material") if specification else None,
                "boot_material": specification.get("boot_material") if specification else None,
                "bearing_type": specification.get("bearing_type") if specification else None,
            },
            "variants": variants,
        }