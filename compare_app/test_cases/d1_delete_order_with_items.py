from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase
from datetime import datetime


class D1DeleteOrderWithItemsTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d1_delete_order_with_items")
        self.order_id_to_delete: int | None = None

    def prepare_for_postgresql(self, connector: PostgresConnector) -> None:
        inserted_order = connector.read_row(
            query=(
                "WITH selected_user AS ("
                "SELECT id_user FROM users ORDER BY id_user DESC LIMIT 1"
                "), selected_product AS ("
                "SELECT id_product, price FROM product ORDER BY id_product DESC LIMIT 1"
                "), new_order AS ("
                "INSERT INTO orders (id_user, id_status, total_amount, shipping_address) "
                "SELECT selected_user.id_user, 1, selected_product.price, %s "
                "FROM selected_user CROSS JOIN selected_product "
                "RETURNING id_order"
                "), inserted_item AS ("
                "INSERT INTO order_items (id_order, id_product, quantity, unit_price) "
                "SELECT new_order.id_order, selected_product.id_product, 1, selected_product.price "
                "FROM new_order CROSS JOIN selected_product "
                "RETURNING id_order"
                ") "
                "SELECT id_order FROM new_order LIMIT 1"
            ),
            params=("benchmark_d1_delete_order_with_items",),
        )
        if not inserted_order:
            raise ValueError("Failed to prepare D1 test case: could not insert order")
        self.order_id_to_delete = int(inserted_order["id_order"])

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        if self.order_id_to_delete is None:
            raise ValueError("D1 test case is not prepared: missing order id to delete")
        # items will be deleted because ON CASCADE is set for the foreign key constraint between order_items and orders
        connector.delete_rows(
            query=(
                "DELETE FROM orders "
                "WHERE id_order = %s"
            ),
            params=(
                self.order_id_to_delete,
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        selected_user = connector.read_latest("users")
        selected_product = connector.read_latest("product")

        if selected_user is None:
            raise ValueError("Failed to prepare D1 test case: could not find user")
        if selected_product is None:
            raise ValueError("Failed to prepare D1 test case: could not find product")

        order_id = connector.get_next_business_id("orders")
        order_item_id = connector.get_next_business_id("order_items")

        connector.insert_one(
            collection_name="orders",
            document={
                "id_order": order_id,
                "id_user": selected_user["id_user"],
                "id_status": 1,
                "total_amount": selected_product["price"],
                "shipping_address": "benchmark_d1_delete_order_with_items",
                "order_date": datetime.utcnow().isoformat(),
            },
        )
        connector.insert_one(
            collection_name="order_items",
            document={
                "id_order_item": order_item_id,
                "id_order": order_id,
                "id_product": selected_product["id_product"],
                "quantity": 1,
                "unit_price": selected_product["price"],
            },
        )
        self.order_id_to_delete = order_id


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        selected_user = connector.read_latest("users")
        selected_product = connector.read_latest("product")

        if selected_user is None:
            raise ValueError("Failed to prepare D1 test case: could not find user")
        if selected_product is None:
            raise ValueError("Failed to prepare D1 test case: could not find product")

        order_id = connector.get_next_business_id("orders")
        order_item_id = connector.get_next_business_id("order_items")

        connector.insert_one(
            collection_name="orders",
            document={
                "id_order": order_id,
                "id_user": selected_user["id_user"],
                "id_status": 1,
                "total_amount": selected_product["price"],
                "shipping_address": "benchmark_d1_delete_order_with_items",
                "order_date": datetime.utcnow().isoformat(),
            },
        )
        connector.insert_one(
            collection_name="order_items",
            document={
                "id_order_item": order_item_id,
                "id_order": order_id,
                "id_product": selected_product["id_product"],
                "quantity": 1,
                "unit_price": selected_product["price"],
            },
        )
        self.order_id_to_delete = order_id


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        if self.order_id_to_delete is None:
            raise ValueError("D1 test case is not prepared: missing order id to delete")

        connector.delete_many(
            collection_name="order_items",
            filter_query={"id_order": self.order_id_to_delete},
        )
        connector.delete_one(
            collection_name="orders",
            filter_query={"id_order": self.order_id_to_delete},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        if self.order_id_to_delete is None:
            raise ValueError("D1 test case is not prepared: missing order id to delete")

        connector.delete_many(
            collection_name="order_items",
            filter_query={"id_order": self.order_id_to_delete},
        )
        connector.delete_one_by_business_id(
            collection_name="orders",
            business_id=self.order_id_to_delete,
        )