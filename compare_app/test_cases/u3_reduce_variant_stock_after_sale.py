from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U3ReduceVariantStockAfterSaleTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u3_reduce_variant_stock_after_sale")

    def _payload(self) -> dict[str, object]:
        return {
            "model_name": "benchmark_model",
            "color_name": "red",
            "size_value": "42",
            "sold_quantity": 3,
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "WITH target_variant AS ("
                "SELECT p.id_product "
                "FROM product p "
                "JOIN models m ON m.id_model = p.id_model "
                "WHERE m.model_name = %s "
                "AND p.color_name = %s "
                "AND p.size_value = %s "
                "ORDER BY p.id_product DESC LIMIT 1"
                ") "
                "UPDATE product "
                "SET stock_quantity = GREATEST(stock_quantity - %s, 0) "
                "WHERE id_product = (SELECT id_product FROM target_variant)"
            ),
            params=(
                payload["model_name"],
                payload["color_name"],
                payload["size_value"],
                payload["sold_quantity"],
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


    def _latest_variant(self, connector) -> dict | None:
        model_id = self._latest_model_id(connector)
        if model_id is None:
            return None

        variants = connector.read_many(
            collection_name="product",
            filter_query={
                "id_model": model_id,
                "color_name": self._payload()["color_name"],
                "size_value": self._payload()["size_value"],
            },
            projection={"_id": 0, "id_product": 1, "stock_quantity": 1},
        )
        if not variants:
            return None

        return max(variants, key=lambda variant: int(variant["id_product"]))


    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        variant = self._latest_variant(connector)
        if variant is None:
            return

        connector.update_one(
            collection_name="product",
            filter_query={"id_product": variant["id_product"]},
            update_query={"$set": {"stock_quantity": 20}},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        variant = self._latest_variant(connector)
        if variant is None:
            return

        connector.update_one(
            collection_name="product",
            filter_query={"id_product": variant["id_product"]},
            update_query={"$set": {"stock_quantity": 20}},
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        variant = self._latest_variant(connector)
        if variant is None:
            return

        new_stock = max(int(variant["stock_quantity"]) - int(self._payload()["sold_quantity"]), 0)

        connector.update_one(
            collection_name="product",
            filter_query={"id_product": variant["id_product"]},
            update_query={"$set": {"stock_quantity": new_stock}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        variant = self._latest_variant(connector)
        if variant is None:
            return

        new_stock = max(int(variant["stock_quantity"]) - int(self._payload()["sold_quantity"]), 0)

        connector.update_one(
            collection_name="product",
            filter_query={"id_product": variant["id_product"]},
            update_query={"$set": {"stock_quantity": new_stock}},
        )