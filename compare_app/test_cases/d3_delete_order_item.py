from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D3DeleteOrderItemTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d3_delete_order_item")
        self.order_item_id_to_delete: int | None = None

    def prepare_for_postgresql(self, connector: PostgresConnector) -> None:
        inserted_order_item = connector.read_row(
            query=(
                "WITH selected_order AS ("
                "SELECT id_order FROM orders ORDER BY id_order DESC LIMIT 1"
                "), selected_product AS ("
                "SELECT id_product, price FROM product ORDER BY id_product DESC LIMIT 1"
                ") "
                "INSERT INTO order_items (id_order, id_product, quantity, unit_price) "
                "SELECT selected_order.id_order, selected_product.id_product, 1, selected_product.price "
                "FROM selected_order CROSS JOIN selected_product "
                "RETURNING id_order_item"
            )
        )
        if not inserted_order_item:
            raise ValueError("Failed to prepare D3 test case: could not insert order item")
        self.order_item_id_to_delete = int(inserted_order_item["id_order_item"])

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        if self.order_item_id_to_delete is None:
            raise ValueError("D3 test case is not prepared: missing order item id to delete")
        connector.delete_rows(
            query=(
                "DELETE FROM order_items "
                "WHERE id_order_item = %s"
            ),
            params=(
                self.order_item_id_to_delete,
            ),
        )
    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        selected_order = connector.read_latest("orders")
        selected_product = connector.read_latest("product")

        if selected_order is None:
            raise ValueError("Failed to prepare D3 test case: could not find order")
        if selected_product is None:
            raise ValueError("Failed to prepare D3 test case: could not find product")

        order_item_id = connector.get_next_business_id("order_items")
        connector.insert_one(
            collection_name="order_items",
            document={
                "id_order_item": order_item_id,
                "id_order": selected_order["id_order"],
                "id_product": selected_product["id_product"],
                "quantity": 1,
                "unit_price": selected_product["price"],
            },
        )
        self.order_item_id_to_delete = order_item_id


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        selected_order = connector.read_latest("orders")
        selected_product = connector.read_latest("product")

        if selected_order is None:
            raise ValueError("Failed to prepare D3 test case: could not find order")
        if selected_product is None:
            raise ValueError("Failed to prepare D3 test case: could not find product")

        order_item_id = connector.get_next_business_id("order_items")
        connector.insert_one(
            collection_name="order_items",
            document={
                "id_order_item": order_item_id,
                "id_order": selected_order["id_order"],
                "id_product": selected_product["id_product"],
                "quantity": 1,
                "unit_price": selected_product["price"],
            },
        )
        self.order_item_id_to_delete = order_item_id


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        if self.order_item_id_to_delete is None:
            raise ValueError("D3 test case is not prepared: missing order item id to delete")
        connector.delete_one(
            collection_name="order_items",
            filter_query={"id_order_item": self.order_item_id_to_delete},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        if self.order_item_id_to_delete is None:
            raise ValueError("D3 test case is not prepared: missing order item id to delete")
        connector.delete_one_by_business_id(
            collection_name="order_items",
            business_id=self.order_item_id_to_delete,
        )