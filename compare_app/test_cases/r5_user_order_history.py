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
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
