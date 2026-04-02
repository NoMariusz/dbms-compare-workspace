from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import config
from connectors.base import BaseConnector
from constants import DBMSType


class CouchConnector(BaseConnector):
    _ID_FIELD_BY_COLLECTION = {
        "user_roles": "id_role",
        "users": "id_user",
        "manufacturers": "id_manufacturer",
        "product_types": "id_type",
        "models": "id_model",
        "models_to_product_types": None,
        "gear_specifications": "id_specification",
        "product": "id_product",
        "order_status": "id_status",
        "orders": "id_order",
        "order_items": "id_order_item",
    }

    _INDEX_BY_COLLECTION_AND_FIELD = {
        ("users", "email"): "idx_users_email",
        ("users", "id_user"): "idx_users_id_user",
        ("manufacturers", "id_manufacturer"): "idx_manufacturers_id_manufacturer",
        ("models", "id_model"): "idx_models_id_model",
        ("gear_specifications", "id_specification"): "idx_gear_specifications_id_specification",
        ("product", "id_product"): "idx_product_id_product",
        ("product", "id_model"): "idx_product_model",
        ("orders", "id_order"): "idx_orders_id_order",
        ("orders", "id_user"): "idx_orders_user_date",
        ("order_items", "id_order_item"): "idx_order_items_id_order_item",
        ("order_items", "id_order"): "idx_order_items_order",
        ("models", "model_name"): "idx_models_model_name",
        ("product", "stock_quantity"): "idx_product_stock_quantity",
        ("order_items", "id_product"): "idx_order_items_product",
    }

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        super().__init__(dbms_type=DBMSType.CouchDB, name=DBMSType.CouchDB.name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database_name = config.COUCHDB_DATABASE

    def connect(self) -> None:
        print(
            f"DEBUG: Connecting to {self.dbms_type.name} "
            f"at {self.host}:{self.port} to database {self.database_name}"
        )
        self._request_json("GET", "/")
        self._request_json("GET", f"/{self.database_name}")
        self.client = True

    def close(self) -> None:
        self.client = None
        
    def _ensure_runtime_indexes(self) -> None:
        runtime_indexes = [
            ("idx_models_model_name", ["type", "model_name"]),
            ("idx_product_stock_quantity", ["type", "stock_quantity"]),
            ("idx_order_items_product", ["type", "id_product"]),
        ]

        for name, fields in runtime_indexes:
            self._create_index(name=name, fields=fields)

    def restore_data(self, size_label: str) -> None:
        print(f"DEBUG: Restoring {self.dbms_type.name} to size {size_label} using presets...")

        backup_path = self._resolve_backup_path(size_label)
        if not backup_path.exists():
            print(
                f"WARNING: Backup file not found for {self.dbms_type.name} "
                f"and size {size_label}: {backup_path}"
            )
            return

        with backup_path.open("r", encoding="utf-8") as backup_file:
            payload = json.load(backup_file)

        documents = payload.get("documents", [])
        indexes = payload.get("indexes", [])

        if self.client:
            self.close()

        self._delete_database_if_exists()
        self._create_database()
        self._restore_indexes(indexes)
        self._ensure_runtime_indexes()
        self._restore_documents(documents)

        self.connect()

    def _resolve_backup_path(self, size_label: str) -> Path:
        return Path(f"./data/db_backups/couchdb_{size_label}.json")

    def _base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def _auth_header(self) -> str:
        raw = f"{self.user}:{self.password}".encode("utf-8")
        return "Basic " + base64.b64encode(raw).decode("ascii")

    def _request_json(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url()}{path}"
        body = None
        headers = {
            "Authorization": self._auth_header(),
            "Accept": "application/json",
        }

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url=url, data=body, headers=headers, method=method)

        try:
            with request.urlopen(req) as response:
                raw = response.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"CouchDB request failed: {method} {path} -> {exc.code} {details}"
            ) from exc

    def _create_database(self) -> None:
        try:
            self._request_json("PUT", f"/{self.database_name}")
        except RuntimeError as exc:
            if "412" not in str(exc) and "file_exists" not in str(exc):
                raise

    def _delete_database_if_exists(self) -> None:
        try:
            self._request_json("DELETE", f"/{self.database_name}")
        except RuntimeError as exc:
            if "404" not in str(exc) and "not_found" not in str(exc):
                raise

    def _create_index(self, name: str, fields: list[str]) -> None:
        self._request_json(
            method="POST",
            path=f"/{self.database_name}/_index",
            payload={
                "index": {"fields": fields},
                "name": name,
                "type": "json",
            },
        )

    def _restore_indexes(self, indexes: list[dict[str, Any]]) -> None:
        for index_definition in indexes:
            name = index_definition.get("name")
            fields = index_definition.get("fields")
            if not name or not isinstance(fields, list):
                continue
            self._create_index(name=name, fields=fields)

    def _chunked(self, items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    def _bulk_docs(self, documents: list[dict[str, Any]], chunk_size: int = 500) -> None:
        if not documents:
            return

        for chunk in self._chunked(documents, chunk_size):
            result = self._request_json(
                method="POST",
                path=f"/{self.database_name}/_bulk_docs",
                payload={"docs": chunk},
            )
            if not isinstance(result, list):
                continue

            for item in result:
                if item.get("error") and item.get("error") != "conflict":
                    raise RuntimeError(f"CouchDB bulk operation failed: {item}")

    def _restore_documents(self, documents: list[dict[str, Any]]) -> None:
        prepared_documents: list[dict[str, Any]] = []
        for document in documents:
            restored = dict(document)
            restored.pop("_rev", None)
            prepared_documents.append(restored)
        self._bulk_docs(prepared_documents, chunk_size=1000)

    def _normalize_document(self, document: dict[str, Any]) -> dict[str, Any]:
        result = dict(document)
        result.pop("_id", None)
        result.pop("_rev", None)
        return result

    def _projection_to_fields(self, projection: dict[str, int] | None) -> list[str] | None:
        if projection is None:
            return None

        included_fields = [
            field_name
            for field_name, flag in projection.items()
            if flag and field_name not in {"_id", "_rev"}
        ]
        return included_fields or None

    def _document_exists_by_id(self, doc_id: str) -> bool:
        try:
            self._request_json("GET", f"/{self.database_name}/{parse.quote(doc_id, safe='')}")
            return True
        except RuntimeError as exc:
            text = str(exc)
            if "404" in text or "not_found" in text:
                return False
            raise

    def _sort_to_mango(self, sort: list[tuple[str, int]] | None) -> list[dict[str, str]] | None:
        if sort is None:
            return None

        result: list[dict[str, str]] = []
        for field_name, direction in sort:
            result.append({field_name: "asc" if direction >= 0 else "desc"})
        return result

    def _is_timeout_error(self, exc: RuntimeError) -> bool:
        text = str(exc)
        return '"error":"timeout"' in text or "The request could not be processed in a reasonable amount of time" in text

    def _choose_index_name(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
    ) -> str | None:
        for field_name in filter_query.keys():
            index_name = self._INDEX_BY_COLLECTION_AND_FIELD.get((collection_name, field_name))
            if index_name is not None:
                return index_name

        if sort:
            first_sort_field = sort[0][0]
            index_name = self._INDEX_BY_COLLECTION_AND_FIELD.get((collection_name, first_sort_field))
            if index_name is not None:
                return index_name

        id_field = self._ID_FIELD_BY_COLLECTION.get(collection_name)
        if id_field is not None:
            return self._INDEX_BY_COLLECTION_AND_FIELD.get((collection_name, id_field))

        return None

    def _find_docs(
        self,
        selector: dict[str, Any],
        fields: list[str] | None = None,
        sort: list[dict[str, str]] | None = None,
        limit: int = 1_000_000,
        use_index: str | None = None,
        retries: int = 6,
        retry_delay: float = 2.0,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "selector": selector,
            "limit": limit,
        }
        if fields is not None:
            payload["fields"] = fields
        if sort is not None:
            payload["sort"] = sort
        if use_index is not None:
            payload["use_index"] = use_index

        last_exc: RuntimeError | None = None

        for attempt in range(retries):
            try:
                result = self._request_json(
                    method="POST",
                    path=f"/{self.database_name}/_find",
                    payload=payload,
                )
                return result.get("docs", [])
            except RuntimeError as exc:
                if self._is_timeout_error(exc) and attempt < retries - 1:
                    last_exc = exc
                    time.sleep(retry_delay)
                    continue
                raise

        if last_exc is not None:
            raise last_exc

        return []

    def _with_collection_type(self, collection_name: str, filter_query: dict[str, Any]) -> dict[str, Any]:
        selector = dict(filter_query)
        selector["type"] = collection_name
        return selector

    def _prepare_document_for_insert(self, collection_name: str, document: dict[str, Any]) -> dict[str, Any]:
        prepared = dict(document)
        prepared.setdefault("type", collection_name)

        id_field = self._ID_FIELD_BY_COLLECTION.get(collection_name)
        if prepared.get("_id") is None and id_field and prepared.get(id_field) is not None:
            prepared["_id"] = f"{collection_name}:{prepared[id_field]}"

        return prepared

    def _save_document(self, document: dict[str, Any]) -> Any:
        prepared = dict(document)
        doc_id = prepared.pop("_id", None)

        if doc_id is not None:
            encoded_doc_id = parse.quote(doc_id, safe="")
            return self._request_json(
                method="PUT",
                path=f"/{self.database_name}/{encoded_doc_id}",
                payload={"_id": doc_id, **prepared},
            )

        return self._request_json(
            method="POST",
            path=f"/{self.database_name}",
            payload=prepared,
        )

    def insert_one_ignore_duplicates(self, collection_name: str, document: dict[str, Any]) -> bool:
        prepared_document = self._prepare_document_for_insert(collection_name, document)

        doc_id = prepared_document.get("_id")
        if isinstance(doc_id, str) and self._document_exists_by_id(doc_id):
            return False

        try:
            self._save_document(prepared_document)
            return True
        except RuntimeError as exc:
            if "409" in str(exc) or "conflict" in str(exc):
                return False
            raise

    def insert_one(self, collection_name: str, document: dict[str, Any]) -> Any:
        prepared_document = self._prepare_document_for_insert(collection_name, document)
        result = self._save_document(prepared_document)
        return result.get("id") if isinstance(result, dict) else result

    def insert_many(self, collection_name: str, documents: list[dict[str, Any]]) -> list[Any]:
        if not documents:
            return []

        prepared_documents = [
            self._prepare_document_for_insert(collection_name, document)
            for document in documents
        ]
        self._bulk_docs(prepared_documents, chunk_size=500)

        inserted_ids: list[Any] = []
        for document in prepared_documents:
            inserted_ids.append(document.get("_id"))
        return inserted_ids

    def read_one(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        projection: dict[str, int] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> dict[str, Any] | None:
        selector = self._with_collection_type(collection_name, filter_query)
        fields = self._projection_to_fields(projection)
        mango_sort = self._sort_to_mango(sort)
        use_index = self._choose_index_name(collection_name, filter_query, sort)

        documents = self._find_docs(
            selector=selector,
            fields=fields,
            sort=mango_sort,
            limit=1,
            use_index=use_index,
        )
        if not documents:
            return None

        return self._normalize_document(documents[0])

    def read_many(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        projection: dict[str, int] | None = None,
        sort: list[tuple[str, int]] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        selector = self._with_collection_type(collection_name, filter_query)
        fields = self._projection_to_fields(projection)
        mango_sort = self._sort_to_mango(sort)
        use_index = self._choose_index_name(collection_name, filter_query, sort)

        documents = self._find_docs(
            selector=selector,
            fields=fields,
            sort=mango_sort,
            limit=limit if limit is not None else 1_000_000,
            use_index=use_index,
        )
        return [self._normalize_document(document) for document in documents]

    def read_latest(
        self,
        collection_name: str,
        id_field: str | None = None,
        projection: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        if id_field is None:
            id_field = self._ID_FIELD_BY_COLLECTION.get(collection_name)
        if id_field is None:
            raise ValueError(f"No business id field configured for collection: {collection_name}")

        filter_query = {id_field: {"$gte": 0}}
        return self.read_one(
            collection_name=collection_name,
            filter_query=filter_query,
            projection=projection,
            sort=[(id_field, -1)],
        )

    def update_one(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        update_query: dict[str, Any],
        upsert: bool = False,
    ) -> int:
        return self.update_many(
            collection_name=collection_name,
            filter_query=filter_query,
            update_query=update_query,
            upsert=upsert,
            limit=1,
        )

    def update_many(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        update_query: dict[str, Any],
        upsert: bool = False,
        limit: int | None = None,
    ) -> int:
        selector = self._with_collection_type(collection_name, filter_query)
        documents = self._find_docs(
            selector=selector,
            limit=limit if limit is not None else 1_000_000,
            use_index=self._choose_index_name(collection_name, filter_query),
        )

        set_payload = update_query.get("$set", {})
        inc_payload = update_query.get("$inc", {})

        if not isinstance(set_payload, dict) or not isinstance(inc_payload, dict):
            raise ValueError("CouchConnector.update_many supports only '$set' and '$inc' operators.")

        if not documents and upsert:
            new_document = {**filter_query}
            for key, value in set_payload.items():
                new_document[key] = value
            for key, value in inc_payload.items():
                new_document[key] = value
            new_document = self._prepare_document_for_insert(collection_name, new_document)
            self._save_document(new_document)
            return 1

        if not documents:
            return 0

        updated_documents: list[dict[str, Any]] = []
        for document in documents:
            updated_document = dict(document)

            for key, value in set_payload.items():
                updated_document[key] = value

            for key, value in inc_payload.items():
                current_value = updated_document.get(key, 0)
                if not isinstance(current_value, (int, float)):
                    raise ValueError(f"Cannot apply $inc to non-numeric field: {key}")
                updated_document[key] = current_value + value

            updated_documents.append(updated_document)

        self._bulk_docs(updated_documents, chunk_size=500)
        return len(updated_documents)

    def delete_one(self, collection_name: str, filter_query: dict[str, Any]) -> int:
        return self.delete_many(collection_name=collection_name, filter_query=filter_query, limit=1)

    def delete_many(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        limit: int | None = None,
    ) -> int:
        selector = self._with_collection_type(collection_name, filter_query)
        documents = self._find_docs(
            selector=selector,
            fields=["_id", "_rev"],
            limit=limit if limit is not None else 1_000_000,
            use_index=self._choose_index_name(collection_name, filter_query),
        )
        if not documents:
            return 0

        deleted_documents = [
            {
                "_id": document["_id"],
                "_rev": document["_rev"],
                "_deleted": True,
            }
            for document in documents
        ]
        self._bulk_docs(deleted_documents, chunk_size=500)
        return len(deleted_documents)

    def get_next_business_id(self, collection_name: str, id_field: str | None = None) -> int:
        if id_field is None:
            id_field = self._ID_FIELD_BY_COLLECTION.get(collection_name)
        if id_field is None:
            raise ValueError(f"No business id field configured for collection: {collection_name}")

        latest_document = self.read_latest(
            collection_name=collection_name,
            id_field=id_field,
            projection={id_field: 1, "_id": 0},
        )
        if latest_document is None:
            return 1

        current_value = latest_document.get(id_field)
        if not isinstance(current_value, int):
            raise ValueError(f"Field {id_field} in collection {collection_name} is not an integer.")

        return current_value + 1