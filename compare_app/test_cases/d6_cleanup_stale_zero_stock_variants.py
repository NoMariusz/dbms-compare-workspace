from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase
from datetime import datetime, timedelta


class D6CleanupStaleZeroStockVariantsTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d6_cleanup_stale_zero_stock_variants")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.delete_rows(
            query=(
                "DELETE FROM product p "
                "WHERE p.stock_quantity = 0 "
                "AND NOT EXISTS ("
                "SELECT 1 "
                "FROM order_items oi "
                "JOIN orders o ON o.id_order = oi.id_order "
                "WHERE oi.id_product = p.id_product "
                "AND o.order_date >= NOW() - INTERVAL '1 year'"
                ") "
                "AND NOT EXISTS ("
                "SELECT 1 FROM order_items oi WHERE oi.id_product = p.id_product"
                ")"
            ),
            params=(),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        zero_stock_products = connector.read_many(
            collection_name="product",
            filter_query={"stock_quantity": 0},
            projection={"_id": 0, "id_product": 1},
        )
        if not zero_stock_products:
            return

        zero_stock_product_ids = [
            product["id_product"] for product in zero_stock_products if "id_product" in product
        ]
        if not zero_stock_product_ids:
            return

        referenced_order_items = connector.read_many(
            collection_name="order_items",
            filter_query={"id_product": {"$in": zero_stock_product_ids}},
            projection={"_id": 0, "id_product": 1},
        )
        referenced_product_ids = {
            item["id_product"] for item in referenced_order_items if "id_product" in item
        }

        ids_to_delete = [
            product_id
            for product_id in zero_stock_product_ids
            if product_id not in referenced_product_ids
        ]
        if not ids_to_delete:
            return

        connector.delete_many(
            collection_name="product",
            filter_query={"id_product": {"$in": ids_to_delete}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        zero_stock_products = connector.read_many(
            collection_name="product",
            filter_query={"stock_quantity": 0},
            projection={"_id": 0, "id_product": 1},
        )
        if not zero_stock_products:
            return

        zero_stock_product_ids = [
            product["id_product"] for product in zero_stock_products if "id_product" in product
        ]
        if not zero_stock_product_ids:
            return

        referenced_order_items = connector.read_many(
            collection_name="order_items",
            filter_query={"id_product": {"$in": zero_stock_product_ids}},
            projection={"_id": 0, "id_product": 1},
        )
        referenced_product_ids = {
            item["id_product"] for item in referenced_order_items if "id_product" in item
        }

        ids_to_delete = [
            product_id
            for product_id in zero_stock_product_ids
            if product_id not in referenced_product_ids
        ]
        if not ids_to_delete:
            return

        connector.delete_many(
            collection_name="product",
            filter_query={"id_product": {"$in": ids_to_delete}},
        )