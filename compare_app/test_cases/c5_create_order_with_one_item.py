from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class C5CreateOrderWithOneItemTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="c5_create_order_with_one_item")

    def _payload(self) -> dict[str, object]:
        return {
            "id_status": 1,
            "quantity": 1,
            "shipping_address": "Benchmark Street 10, 00-001 Warsaw",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()

        connector.insert_row(
            query=(
                "WITH selected_user AS ("
                "SELECT id_user FROM users ORDER BY id_user DESC LIMIT 1"
                "), selected_product AS ("
                "SELECT id_product, price FROM product ORDER BY id_product DESC LIMIT 1"
                "), new_order AS ("
                "INSERT INTO orders (id_user, id_status, total_amount, shipping_address) "
                "SELECT selected_user.id_user, %s, (selected_product.price * %s), %s "
                "FROM selected_user CROSS JOIN selected_product "
                "RETURNING id_order"
                ") "
                "INSERT INTO order_items (id_order, id_product, quantity, unit_price) "
                "SELECT new_order.id_order, selected_product.id_product, %s, selected_product.price "
                "FROM new_order CROSS JOIN selected_product"
            ),
            params=(
                payload["id_status"],
                payload["quantity"],
                payload["shipping_address"],
                payload["quantity"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
