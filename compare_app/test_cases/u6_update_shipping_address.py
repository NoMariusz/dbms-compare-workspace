from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U6UpdateShippingAddressTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u6_update_shipping_address")

    def _payload(self) -> dict[str, object]:
        return {
            "target_order_id": 1,
            "new_shipping_address": "123 New Street, City, Country 12345",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "UPDATE orders "
                "SET shipping_address = %s "
                "WHERE id_order = %s "
                "AND id_status IN (1, 2)"
            ),
            params=(
                payload["new_shipping_address"],
                payload["target_order_id"],
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.update_one(
            collection_name="orders",
            filter_query={"id_order": int(self._payload()["target_order_id"])},
            update_query={
                "$set": {
                    "shipping_address": "Original benchmark address",
                    "id_status": 1,
                }
            },
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        connector.update_one(
            collection_name="orders",
            filter_query={"id_order": int(self._payload()["target_order_id"])},
            update_query={
                "$set": {
                    "shipping_address": "Original benchmark address",
                    "id_status": 1,
                }
            },
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        payload = self._payload()
        connector.update_one(
            collection_name="orders",
            filter_query={
                "id_order": int(payload["target_order_id"]),
                "id_status": {"$in": [1, 2]},
            },
            update_query={"$set": {"shipping_address": payload["new_shipping_address"]}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        payload = self._payload()
        connector.update_one(
            collection_name="orders",
            filter_query={
                "id_order": int(payload["target_order_id"]),
                "id_status": {"$in": [1, 2]},
            },
            update_query={"$set": {"shipping_address": payload["new_shipping_address"]}},
        )