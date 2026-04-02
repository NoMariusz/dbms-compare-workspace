from __future__ import annotations

from datetime import datetime

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


def _normalize_order_date_for_sort(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class R6StaleInventoryReportTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r6_stale_inventory_report")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_rows(
            query=(
                "SELECT "
                "p.id_product, p.id_model, p.color_name, p.size_value, p.stock_quantity, p.price, "
                "MAX(o.order_date) AS last_purchase_at "
                "FROM product p "
                "LEFT JOIN order_items oi ON oi.id_product = p.id_product "
                "LEFT JOIN orders o ON o.id_order = oi.id_order "
                "WHERE p.stock_quantity > 0 "
                "GROUP BY p.id_product, p.id_model, p.color_name, p.size_value, p.stock_quantity, p.price "
                "ORDER BY last_purchase_at ASC NULLS FIRST, p.stock_quantity DESC, p.id_product "
                "LIMIT %s"
            ),
            params=(10,),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        products = connector.read_many(
            collection_name="product",
            filter_query={"stock_quantity": {"$gt": 0}},
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
        if not products:
            return

        product_ids = [product["id_product"] for product in products if "id_product" in product]
        if not product_ids:
            return

        order_items = connector.read_many(
            collection_name="order_items",
            filter_query={"id_product": {"$in": product_ids}},
            projection={"_id": 0, "id_product": 1, "id_order": 1},
        )

        order_ids = sorted(
            {order_item["id_order"] for order_item in order_items if "id_order" in order_item}
        )

        orders_by_id = {}
        if order_ids:
            orders = connector.read_many(
                collection_name="orders",
                filter_query={"id_order": {"$in": order_ids}},
                projection={"_id": 0, "id_order": 1, "order_date": 1},
            )
            orders_by_id = {
                order["id_order"]: _normalize_order_date_for_sort(order.get("order_date"))
                for order in orders
                if "id_order" in order
            }

        last_purchase_by_product = {}
        for order_item in order_items:
            product_id = order_item.get("id_product")
            order_id = order_item.get("id_order")
            if product_id is None or order_id is None:
                continue

            order_date = orders_by_id.get(order_id)
            if order_date is None:
                continue

            current_last = last_purchase_by_product.get(product_id)
            if current_last is None or order_date > current_last:
                last_purchase_by_product[product_id] = order_date

        report_rows = []
        for product in products:
            report_rows.append(
                {
                    "id_product": product["id_product"],
                    "id_model": product["id_model"],
                    "color_name": product["color_name"],
                    "size_value": product["size_value"],
                    "stock_quantity": product["stock_quantity"],
                    "price": product["price"],
                    "last_purchase_at": last_purchase_by_product.get(product["id_product"]),
                }
            )

        report_rows.sort(
            key=lambda row: (
                row["last_purchase_at"] is not None,
                row["last_purchase_at"] or "",
                -int(row["stock_quantity"]),
                int(row["id_product"]),
            )
        )

        _ = report_rows[:10]

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        products = connector.read_many(
            collection_name="product",
            filter_query={"stock_quantity": {"$gt": 0}},
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
        if not products:
            return

        products_by_id = {
            product["id_product"]: product
            for product in products
            if "id_product" in product
        }
        if not products_by_id:
            return

        order_items = connector.read_many(
            collection_name="order_items",
            filter_query={},
            projection={"_id": 0, "id_product": 1, "id_order": 1},
        )

        relevant_order_items = [
            order_item
            for order_item in order_items
            if order_item.get("id_product") in products_by_id
        ]

        order_ids = sorted(
            {order_item["id_order"] for order_item in relevant_order_items if "id_order" in order_item}
        )

        orders_by_id = {}
        if order_ids:
            orders = connector.read_many(
                collection_name="orders",
                filter_query={"id_order": {"$in": order_ids}},
                projection={"_id": 0, "id_order": 1, "order_date": 1},
            )
            orders_by_id = {
                order["id_order"]: _normalize_order_date_for_sort(order.get("order_date"))
                for order in orders
                if "id_order" in order
            }

        last_purchase_by_product = {}
        for order_item in relevant_order_items:
            product_id = order_item.get("id_product")
            order_id = order_item.get("id_order")
            if product_id is None or order_id is None:
                continue

            order_date = orders_by_id.get(order_id)
            if order_date is None:
                continue

            current_last = last_purchase_by_product.get(product_id)
            if current_last is None or order_date > current_last:
                last_purchase_by_product[product_id] = order_date

        report_rows = []
        for product_id, product in products_by_id.items():
            report_rows.append(
                {
                    "id_product": product["id_product"],
                    "id_model": product["id_model"],
                    "color_name": product["color_name"],
                    "size_value": product["size_value"],
                    "stock_quantity": product["stock_quantity"],
                    "price": product["price"],
                    "last_purchase_at": last_purchase_by_product.get(product_id),
                }
            )

        report_rows.sort(
            key=lambda row: (
                row["last_purchase_at"] is not None,
                row["last_purchase_at"] or "",
                -int(row["stock_quantity"]),
                int(row["id_product"]),
            )
        )

        _ = report_rows[:10]