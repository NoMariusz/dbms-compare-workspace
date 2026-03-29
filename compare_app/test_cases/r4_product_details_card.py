from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R4ProductDetailsCardTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r4_product_details_card")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_row(
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
                "), color_variants AS ("
                "SELECT "
                "p2.id_product, p2.color_name, p2.size_value, p2.stock_quantity, p2.price "
                "FROM product p2 "
                "JOIN selected_product sp ON sp.id_model = p2.id_model "
                "ORDER BY p2.id_product"
                ") "
                "SELECT json_build_object("
                "'product', (SELECT row_to_json(sp) FROM selected_product sp), "
                "'variants', (SELECT COALESCE(json_agg(cv), '[]'::json) FROM color_variants cv)"
                ") AS product_card"
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
