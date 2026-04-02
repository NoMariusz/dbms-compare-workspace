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
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
