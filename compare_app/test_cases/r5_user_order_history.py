from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R5UserOrderHistoryTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r5_user_order_history")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_rows(
            query=(
                "SELECT "
                "o.id_order, o.id_user, o.id_status, os.status_name, "
                "o.order_date, o.total_amount, o.shipping_address "
                "FROM orders o "
                "JOIN users u ON u.id_user = o.id_user "
                "JOIN order_status os ON os.id_status = o.id_status "
                "WHERE u.email = %s "
                "ORDER BY o.order_date DESC, o.id_order DESC"
            ),
            params=("benchmark_user@example.com",),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        user = connector.read_one(
            collection_name="users",
            filter_query={"email": "benchmark_user@example.com"},
            projection={"_id": 0, "id_user": 1},
        )
        if user is None:
            return

        orders = connector.read_many(
            collection_name="orders",
            filter_query={"id_user": user["id_user"]},
            projection={
                "_id": 0,
                "id_order": 1,
                "id_user": 1,
                "id_status": 1,
                "order_date": 1,
                "total_amount": 1,
                "shipping_address": 1,
            },
            sort=[("order_date", -1), ("id_order", -1)],
        )

        status_ids = sorted({order["id_status"] for order in orders if "id_status" in order})
        statuses = connector.read_many(
            collection_name="order_status",
            filter_query={"id_status": {"$in": status_ids}},
            projection={"_id": 0, "id_status": 1, "status_name": 1},
        )
        statuses_by_id = {status["id_status"]: status["status_name"] for status in statuses}

        _ = [
            {
                **order,
                "status_name": statuses_by_id.get(order["id_status"]),
            }
            for order in orders
        ]


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        user = connector.read_one(
            collection_name="users",
            filter_query={"email": "benchmark_user@example.com"},
            projection={"_id": 0, "id_user": 1},
        )
        if user is None:
            return

        orders = connector.read_many(
            collection_name="orders",
            filter_query={"id_user": user["id_user"]},
            projection={
                "_id": 0,
                "id_order": 1,
                "id_user": 1,
                "id_status": 1,
                "order_date": 1,
                "total_amount": 1,
                "shipping_address": 1,
            },
        )

        orders.sort(
            key=lambda order: (
                str(order.get("order_date") or ""),
                int(order.get("id_order", 0)),
            ),
            reverse=True,
        )

        status_ids = sorted({order["id_status"] for order in orders if "id_status" in order})
        statuses = connector.read_many(
            collection_name="order_status",
            filter_query={"id_status": {"$in": status_ids}},
            projection={"_id": 0, "id_status": 1, "status_name": 1},
        )
        statuses_by_id = {status["id_status"]: status["status_name"] for status in statuses}

        _ = [
            {
                **order,
                "status_name": statuses_by_id.get(order["id_status"]),
            }
            for order in orders
        ]