from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any
from urllib import error, request, parse
import time

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
            raise RuntimeError(f"CouchDB request failed: {method} {path} -> {exc.code} {details}") from exc

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
    
    def _choose_index_name(self, collection_name: str, filter_query: dict[str, Any]) -> str | None:
        keys = set(filter_query.keys())

        if collection_name == "users" and keys == {"email"}:
            return "idx_users_email"
        if collection_name == "users" and keys == {"id_user"}:
            return "idx_users_id_user"

        if collection_name == "orders" and keys == {"id_user"}:
            return "idx_orders_user_date"

        if collection_name == "order_items" and keys == {"id_order"}:
            return "idx_order_items_order"

        if collection_name == "product" and keys == {"id_model"}:
            return "idx_product_model"

        return None

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
            return self._request_json(
                method="PUT",
                path=f"/{self.database_name}/{doc_id}",
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
        use_index = self._choose_index_name(collection_name, filter_query)

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

    def update_many(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        update_query: dict[str, Any],
        upsert: bool = False,
    ) -> int:
        selector = self._with_collection_type(collection_name, filter_query)
        documents = self._find_docs(selector=selector, limit=1_000_000)

        set_payload = update_query.get("$set")
        if set_payload is None or not isinstance(set_payload, dict):
            raise ValueError("CouchConnector.update_many currently supports only the '$set' operator.")

        if not documents and upsert:
            upsert_document = self._prepare_document_for_insert(
                collection_name,
                {**filter_query, **set_payload},
            )
            self._save_document(upsert_document)
            return 1

        if not documents:
            return 0

        updated_documents: list[dict[str, Any]] = []
        for document in documents:
            updated_document = dict(document)
            updated_document.update(set_payload)
            updated_documents.append(updated_document)

        self._bulk_docs(updated_documents, chunk_size=500)
        return len(updated_documents)

    def delete_many(self, collection_name: str, filter_query: dict[str, Any]) -> int:
        selector = self._with_collection_type(collection_name, filter_query)
        documents = self._find_docs(
            selector=selector,
            fields=["_id", "_rev"],
            limit=1_000_000,
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
    
    def _is_timeout_error(self, exc: RuntimeError) -> bool:
        text = str(exc)
        return '"error":"timeout"' in text or "The request could not be processed in a reasonable amount of time" in text
    
    