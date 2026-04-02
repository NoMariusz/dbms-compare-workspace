from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U1UpdateUserContactTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u1_update_user_contact")

    def _payload(self) -> dict[str, object]:
        return {
            "target_id": "1", # assuming that the user with id_user=1 exists in the database; adjust as needed
            "new_phone": "+19999999999",
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "UPDATE users "
                "SET phone = %s "
                "WHERE id_user = %s"
            ),
            params=(
                payload["new_phone"],
                payload["target_id"],
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.update_one(
            collection_name="users",
            filter_query={"id_user": int(self._payload()["target_id"])},
            update_query={"$set": {"phone": "+10000000000"}},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        connector.update_one(
            collection_name="users",
            filter_query={"id_user": int(self._payload()["target_id"])},
            update_query={"$set": {"phone": "+10000000000"}},
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        payload = self._payload()
        connector.update_one(
            collection_name="users",
            filter_query={"id_user": int(payload["target_id"])},
            update_query={"$set": {"phone": payload["new_phone"]}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        payload = self._payload()
        connector.update_one(
            collection_name="users",
            filter_query={"id_user": int(payload["target_id"])},
            update_query={"$set": {"phone": payload["new_phone"]}},
        )
