from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class R1ReadUserByEmailTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="r1_read_user_by_email")

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        connector.read_row(
            query=(
                "SELECT id_user, username, email, phone, id_role "
                "FROM users WHERE email = %s LIMIT 1"
            ),
            params=("benchmark_user@example.com",),
        )

    def run_for_mongodb(self, connector: MongoConnector) -> None:
        raise NotImplementedError("run_for_mongodb is not implemented yet")

    def run_for_couchdb(self, connector: CouchConnector) -> None:
        raise NotImplementedError("run_for_couchdb is not implemented yet")
