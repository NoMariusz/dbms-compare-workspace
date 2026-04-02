from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U2UpdateVariantPriceTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u2_update_variant_price")

    def _payload(self) -> dict[str, object]:
        return {
            "model_name": "benchmark_model",
            "color_name": "red",
            "new_price": 949.99,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "WITH target_model AS ("
                "SELECT id_model FROM models WHERE model_name = %s "
                "ORDER BY id_model DESC LIMIT 1"
                ") "
                "UPDATE product "
                "SET price = %s "
                "WHERE id_model = (SELECT id_model FROM target_model) "
                "AND color_name = %s"
            ),
            params=(
                payload["model_name"],
                payload["new_price"],
                payload["color_name"],
            ),
        )

    def _latest_model_id(self, connector) -> int | None:
        models = connector.read_many(
            collection_name="models",
            filter_query={"model_name": self._payload()["model_name"]},
            projection={"_id": 0, "id_model": 1},
        )
        if not models:
            return None
        return max(int(model["id_model"]) for model in models if "id_model" in model)


    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        model_id = self._latest_model_id(connector)
        if model_id is None:
            return

        connector.update_many(
            collection_name="product",
            filter_query={
                "id_model": model_id,
                "color_name": self._payload()["color_name"],
            },
            update_query={"$set": {"price": 899.99}},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        model_id = self._latest_model_id(connector)
        if model_id is None:
            return

        connector.update_many(
            collection_name="product",
            filter_query={
                "id_model": model_id,
                "color_name": self._payload()["color_name"],
            },
            update_query={"$set": {"price": 899.99}},
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        model_id = self._latest_model_id(connector)
        if model_id is None:
            return

        connector.update_many(
            collection_name="product",
            filter_query={
                "id_model": model_id,
                "color_name": self._payload()["color_name"],
            },
            update_query={"$set": {"price": self._payload()["new_price"]}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        model_id = self._latest_model_id(connector)
        if model_id is None:
            return

        connector.update_many(
            collection_name="product",
            filter_query={
                "id_model": model_id,
                "color_name": self._payload()["color_name"],
            },
            update_query={"$set": {"price": self._payload()["new_price"]}},
        )