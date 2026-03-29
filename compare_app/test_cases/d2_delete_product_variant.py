from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class D2DeleteProductVariantTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="d2_delete_product_variant")

    def _payload(self) -> dict[str, object]:
        return {
            "target_product_id": 1,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.delete_rows(
            query=(
                "DELETE FROM product "
                "WHERE id_product = %s"
            ),
            params=(
                payload["target_product_id"],
            ),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
