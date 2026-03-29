from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D5WithdrawModelFromOfferTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d5_withdraw_model_from_offer")

    def _payload(self) -> dict[str, object]:
        return {
            "target_model_id": 1,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.delete_rows(
            query=(
                "WITH model_products AS ("
                "SELECT id_product, id_specification FROM product WHERE id_model = %s"
                "), "
                "deleted_order_items AS ("
                "DELETE FROM order_items "
                "WHERE id_product IN (SELECT id_product FROM model_products)"
                "), "
                "deleted_products AS ("
                "DELETE FROM product "
                "WHERE id_model = %s "
                "RETURNING id_specification"
                "), "
                "deleted_specs AS ("
                "DELETE FROM gear_specifications gs "
                "WHERE gs.id_specification IN (SELECT DISTINCT id_specification FROM deleted_products) "
                "AND NOT EXISTS ("
                "SELECT 1 FROM product p WHERE p.id_specification = gs.id_specification"
                ")"
                ") "
                "DELETE FROM models WHERE id_model = %s"
            ),
            params=(
                payload["target_model_id"],
                payload["target_model_id"],
                payload["target_model_id"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
