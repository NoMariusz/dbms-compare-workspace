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

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
