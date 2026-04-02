from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class C6BulkAddOrderItemsTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="c6_bulk_add_order_items")

    def _payload(self) -> dict[str, object]:
        return {
            "variants_count": 3,
            "quantity_per_item": 1,
            "color_prefix": "bulk_variant",
            "size_value": "42",
            "stock_quantity": 20,
            "price_delta": 5.00,
            "description": "Bulk-created product variant for an existing order.",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()

        connector.insert_row(
            query=(
                "WITH selected_order AS ("
                "SELECT id_order FROM orders ORDER BY id_order DESC LIMIT 1"
                "), base_product AS ("
                "SELECT id_model, id_specification, price "
                "FROM product ORDER BY id_product DESC LIMIT 1"
                "), new_products AS ("
                "INSERT INTO product ("
                "id_model, id_specification, color_name, size_value, stock_quantity, price, description"
                ") "
                "SELECT "
                "base_product.id_model, "
                "base_product.id_specification, "
                "(%s || '_' || gs.seq::text), "
                "%s, %s, (base_product.price + %s), %s "
                "FROM base_product "
                "CROSS JOIN generate_series(1, %s) AS gs(seq) "
                "RETURNING id_product, price"
                "), inserted_items AS ("
                "INSERT INTO order_items (id_order, id_product, quantity, unit_price) "
                "SELECT selected_order.id_order, new_products.id_product, %s, new_products.price "
                "FROM selected_order CROSS JOIN new_products "
                "RETURNING id_order, (quantity * unit_price) AS line_total"
                ") "
                "UPDATE orders "
                "SET total_amount = total_amount + COALESCE(("
                "SELECT SUM(line_total) FROM inserted_items"
                "), 0) "
                "WHERE id_order IN (SELECT id_order FROM selected_order)"
            ),
            params=(
                payload["color_prefix"],
                payload["size_value"],
                payload["stock_quantity"],
                payload["price_delta"],
                payload["description"],
                payload["variants_count"],
                payload["quantity_per_item"],
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        payload = self._payload()

        latest_order = connector.read_latest("orders")
        if latest_order is None:
            return

        benchmark_products = connector.read_many(
            collection_name="product",
            filter_query={"description": payload["description"]},
            projection={"id_product": 1, "_id": 0},
        )
        benchmark_product_ids = [
            product["id_product"] for product in benchmark_products if "id_product" in product
        ]

        if benchmark_product_ids:
            connector.delete_many(
                collection_name="order_items",
                filter_query={
                    "id_order": latest_order["id_order"],
                    "id_product": {"$in": benchmark_product_ids},
                },
            )

        remaining_items = connector.read_many(
            collection_name="order_items",
            filter_query={"id_order": latest_order["id_order"]},
            projection={
                "_id": 0,
                "quantity": 1,
                "unit_price": 1,
            },
        )
        recalculated_total = sum(
            float(item["unit_price"]) * int(item["quantity"])
            for item in remaining_items
        )

        connector.update_one(
            collection_name="orders",
            filter_query={"id_order": latest_order["id_order"]},
            update_query={"$set": {"total_amount": recalculated_total}},
        )

        if benchmark_product_ids:
            connector.delete_many(
                collection_name="product",
                filter_query={"id_product": {"$in": benchmark_product_ids}},
            )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        payload = self._payload()

        latest_order = connector.read_latest("orders")
        if latest_order is None:
            return

        benchmark_products = connector.read_many(
            collection_name="product",
            filter_query={"description": payload["description"]},
            projection={"id_product": 1, "_id": 0},
        )
        benchmark_product_ids = [
            product["id_product"] for product in benchmark_products if "id_product" in product
        ]

        if benchmark_product_ids:
            connector.delete_many(
                collection_name="order_items",
                filter_query={
                    "id_order": latest_order["id_order"],
                    "id_product": {"$in": benchmark_product_ids},
                },
            )

        remaining_items = connector.read_many(
            collection_name="order_items",
            filter_query={"id_order": latest_order["id_order"]},
            projection={
                "_id": 0,
                "quantity": 1,
                "unit_price": 1,
            },
        )
        recalculated_total = sum(
            float(item["unit_price"]) * int(item["quantity"])
            for item in remaining_items
        )

        connector.update_one(
            collection_name="orders",
            filter_query={"id_order": latest_order["id_order"]},
            update_query={"$set": {"total_amount": recalculated_total}},
        )

        if benchmark_product_ids:
            connector.delete_many(
                collection_name="product",
                filter_query={"id_product": {"$in": benchmark_product_ids}},
            )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        payload = self._payload()

        selected_order = connector.read_latest("orders")
        base_product = connector.read_latest("product")

        if selected_order is None:
            raise RuntimeError("No order found for C6BulkAddOrderItemsTestCase")
        if base_product is None:
            raise RuntimeError("No product found for C6BulkAddOrderItemsTestCase")

        next_product_id = connector.get_next_business_id("product")
        new_products: list[dict[str, object]] = []

        for index in range(1, int(payload["variants_count"]) + 1):
            new_products.append(
                {
                    "id_product": next_product_id + index - 1,
                    "id_model": base_product["id_model"],
                    "id_specification": base_product["id_specification"],
                    "color_name": f"{payload['color_prefix']}_{index}",
                    "size_value": payload["size_value"],
                    "stock_quantity": payload["stock_quantity"],
                    "price": float(base_product["price"]) + float(payload["price_delta"]),
                    "description": payload["description"],
                }
            )

        connector.insert_many(collection_name="product", documents=new_products)

        next_order_item_id = connector.get_next_business_id("order_items")
        new_order_items: list[dict[str, object]] = []

        for index, product in enumerate(new_products, start=0):
            new_order_items.append(
                {
                    "id_order_item": next_order_item_id + index,
                    "id_order": selected_order["id_order"],
                    "id_product": product["id_product"],
                    "quantity": payload["quantity_per_item"],
                    "unit_price": product["price"],
                }
            )

        connector.insert_many(collection_name="order_items", documents=new_order_items)

        added_total = sum(
            float(item["unit_price"]) * int(item["quantity"])
            for item in new_order_items
        )

        connector.update_one(
            collection_name="orders",
            filter_query={"id_order": selected_order["id_order"]},
            update_query={"$inc": {"total_amount": added_total}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        payload = self._payload()

        selected_order = connector.read_latest("orders")
        base_product = connector.read_latest("product")

        if selected_order is None:
            raise RuntimeError("No order found for C6BulkAddOrderItemsTestCase")
        if base_product is None:
            raise RuntimeError("No product found for C6BulkAddOrderItemsTestCase")

        next_product_id = connector.get_next_business_id("product")
        new_products: list[dict[str, object]] = []

        for index in range(1, int(payload["variants_count"]) + 1):
            new_products.append(
                {
                    "id_product": next_product_id + index - 1,
                    "id_model": base_product["id_model"],
                    "id_specification": base_product["id_specification"],
                    "color_name": f"{payload['color_prefix']}_{index}",
                    "size_value": payload["size_value"],
                    "stock_quantity": payload["stock_quantity"],
                    "price": float(base_product["price"]) + float(payload["price_delta"]),
                    "description": payload["description"],
                }
            )

        connector.insert_many(collection_name="product", documents=new_products)

        next_order_item_id = connector.get_next_business_id("order_items")
        new_order_items: list[dict[str, object]] = []

        for index, product in enumerate(new_products, start=0):
            new_order_items.append(
                {
                    "id_order_item": next_order_item_id + index,
                    "id_order": selected_order["id_order"],
                    "id_product": product["id_product"],
                    "quantity": payload["quantity_per_item"],
                    "unit_price": product["price"],
                }
            )

        connector.insert_many(collection_name="order_items", documents=new_order_items)

        added_total = sum(
            float(item["unit_price"]) * int(item["quantity"])
            for item in new_order_items
        )

        connector.update_one(
            collection_name="orders",
            filter_query={"id_order": selected_order["id_order"]},
            update_query={"$inc": {"total_amount": added_total}},
        )