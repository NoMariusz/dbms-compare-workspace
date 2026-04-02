from __future__ import annotations

from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from test_cases.base import BaseTestCase


class U5UpdateProductDescriptionTestCase(BaseTestCase):
    def __init__(self) -> None:
        super().__init__(name="u5_update_product_description")

    def _payload(self) -> dict[str, object]:
        return {
            "target_product_id": 1,
            "new_description": 
            """Updated product description with improved details and specifications.
            String is intentionally long to test the performance of updating larger text fields in the database.
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
            Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
            Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
            Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
            Additional text to further increase the size of the description and test the performance impact of updating larger fields in the database.
            Even more text to ensure that the description is sufficiently large for testing purposes and to observe any potential performance differences when updating larger text fields in the database.
            Final addition to the description to make it even larger and to thoroughly test the performance of updating large text fields in the database, which can be a common scenario in real-world applications where product descriptions may contain extensive details and specifications.
            This test case will help evaluate how well the database handles updates to larger text fields and whether there are any performance implications when dealing with such updates, especially in scenarios where product descriptions may need to be frequently updated with new information or changes in specifications."""
        }

    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        payload = self._payload()
        connector.update_rows(
            query=(
                "UPDATE product "
                "SET description = %s "
                "WHERE id_product = %s"
            ),
            params=(
                payload["new_description"],
                payload["target_product_id"],
            ),
        )

    def prepare_for_mongodb(self, connector: MongoConnector) -> None:
        connector.update_one(
            collection_name="product",
            filter_query={"id_product": int(self._payload()["target_product_id"])},
            update_query={"$set": {"description": "Original benchmark description"}},
        )


    def prepare_for_couchdb(self, connector: CouchConnector) -> None:
        connector.update_one(
            collection_name="product",
            filter_query={"id_product": int(self._payload()["target_product_id"])},
            update_query={"$set": {"description": "Original benchmark description"}},
        )


    def run_for_mongodb(self, connector: MongoConnector) -> None:
        payload = self._payload()
        connector.update_one(
            collection_name="product",
            filter_query={"id_product": int(payload["target_product_id"])},
            update_query={"$set": {"description": payload["new_description"]}},
        )


    def run_for_couchdb(self, connector: CouchConnector) -> None:
        payload = self._payload()
        connector.update_one(
            collection_name="product",
            filter_query={"id_product": int(payload["target_product_id"])},
            update_query={"$set": {"description": payload["new_description"]}},
        )
