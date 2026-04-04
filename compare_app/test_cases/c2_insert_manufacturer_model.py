from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class C2InsertManufacturerModelTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="c2_insert_manufacturer_model")

    def _manufacturer_name(self) -> str:
        return "benchmark_manufacturer"

    def _model_payload(self) -> dict[str, object]:
        return {
            "model_name": "benchmark_model",
            "description": "Benchmark model created together with a new manufacturer.",
            "release_date": "2026-01-01",
            "type": "models",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        model_payload = self._model_payload()
        connector.insert_row(
            query=(
                "WITH new_manufacturer AS ("
                "INSERT INTO manufacturers (name) VALUES (%s) "
                "RETURNING id_manufacturer"
                ") "
                "INSERT INTO models (id_manufacturer, model_name, description, release_date) "
                "SELECT id_manufacturer, %s, %s, %s "
                "FROM new_manufacturer"
            ),
            params=(
                self._manufacturer_name(),
                model_payload["model_name"],
                model_payload["description"],
                model_payload["release_date"],
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.delete_many(
            collection_name="models",
            filter_query={"model_name": self._model_payload()["model_name"]},
        )
        connector.delete_many(
            collection_name="manufacturers",
            filter_query={"name": self._manufacturer_name()},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        pass


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        manufacturer_id = connector.get_next_business_id("manufacturers")
        model_id = connector.get_next_business_id("models")

        connector.insert_one(
            collection_name="manufacturers",
            document={
                "id_manufacturer": manufacturer_id,
                "name": self._manufacturer_name(),
            },
        )

        model_payload = self._model_payload()
        model_payload.pop("type", None)

        connector.insert_one(
            collection_name="models",
            document={
                "id_model": model_id,
                "id_manufacturer": manufacturer_id,
                "model_name": model_payload["model_name"],
                "description": model_payload["description"],
                "release_date": model_payload["release_date"],
            },
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        manufacturer_id = connector.get_next_business_id("manufacturers")
        model_id = connector.get_next_business_id("models")

        connector.insert_one(
            collection_name="manufacturers",
            document={
                "id_manufacturer": manufacturer_id,
                "name": self._manufacturer_name(),
            },
        )

        model_payload = self._model_payload()

        connector.insert_one(
            collection_name="models",
            document={
                "id_model": model_id,
                "id_manufacturer": manufacturer_id,
                "model_name": model_payload["model_name"],
                "description": model_payload["description"],
                "release_date": model_payload["release_date"],
            },
        )
