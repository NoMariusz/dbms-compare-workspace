from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase
import datetime


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

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        benchmark_orders = connector.read_many(
            collection_name="orders",
            filter_query={"shipping_address": self._payload()["shipping_address"]},
            projection={"id_order": 1, "_id": 0},
        )
        order_ids = [order["id_order"] for order in benchmark_orders if "id_order" in order]
        if order_ids:
            connector.delete_many(
                collection_name="order_items",
                filter_query={"id_order": {"$in": order_ids}},
            )
            connector.delete_many(
                collection_name="orders",
                filter_query={"id_order": {"$in": order_ids}},
            )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        pass


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        payload = self._payload()

        selected_user = connector.read_latest("users")
        selected_product = connector.read_latest("product")

        if selected_user is None:
            raise RuntimeError("No user found for C5CreateOrderWithOneItemTestCase")
        if selected_product is None:
            raise RuntimeError("No product found for C5CreateOrderWithOneItemTestCase")

        order_id = connector.get_next_business_id("orders")
        order_item_id = connector.get_next_business_id("order_items")
        total_amount = float(selected_product["price"]) * int(payload["quantity"])

        connector.insert_one(
            collection_name="orders",
            document={
                "id_order": order_id,
                "id_user": selected_user["id_user"],
                "id_status": payload["id_status"],
                "total_amount": total_amount,
                "shipping_address": payload["shipping_address"],
                "order_date": datetime.datetime.now(datetime.UTC).isoformat(),
            },
        )

        connector.insert_one(
            collection_name="order_items",
            document={
                "id_order_item": order_item_id,
                "id_order": order_id,
                "id_product": selected_product["id_product"],
                "quantity": payload["quantity"],
                "unit_price": selected_product["price"],
            },
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        payload = self._payload()

        selected_user = connector.read_latest("users")
        selected_product = connector.read_latest("product")

        if selected_user is None:
            raise RuntimeError("No user found for C5CreateOrderWithOneItemTestCase")
        if selected_product is None:
            raise RuntimeError("No product found for C5CreateOrderWithOneItemTestCase")

        order_id = connector.get_next_business_id("orders")
        order_item_id = connector.get_next_business_id("order_items")
        total_amount = float(selected_product["price"]) * int(payload["quantity"])

        connector.insert_one(
            collection_name="orders",
            document={
                "id_order": order_id,
                "id_user": selected_user["id_user"],
                "id_status": payload["id_status"],
                "total_amount": total_amount,
                "shipping_address": payload["shipping_address"],
                "order_date": datetime.datetime.now(datetime.UTC).isoformat(),
            },
        )

        connector.insert_one(
            collection_name="order_items",
            document={
                "id_order_item": order_item_id,
                "id_order": order_id,
                "id_product": selected_product["id_product"],
                "quantity": payload["quantity"],
                "unit_price": selected_product["price"],
            },
        )
