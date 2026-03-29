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

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
