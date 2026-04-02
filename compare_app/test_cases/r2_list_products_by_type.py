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
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
