from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any

COUCHDB_ENCRYPTED_DB_NAME = "skates_shop_encrypted"
COUCHDB_ENCRYPTED_PREFIX = "enc::"
COUCHDB_SENSITIVE_FIELDS_BY_COLLECTION: dict[str, set[str]] = {
    "users": {"email", "password", "phone"},
    "orders": {"shipping_address"},
}


def build_couch_encryption_key(
    admin_password: str,
    database_name: str,
    configured_key: str | None = None,
) -> bytes:
    if configured_key:
        return hashlib.sha256(configured_key.encode("utf-8")).digest()

    base_material = f"{admin_password}:{database_name}"
    return hashlib.sha256(base_material.encode("utf-8")).digest()


class CouchFieldCrypto:
    def __init__(
        self,
        encryption_key: bytes,
        encrypted_prefix: str = COUCHDB_ENCRYPTED_PREFIX,
        sensitive_fields_by_collection: dict[str, set[str]] | None = None,
    ) -> None:
        self._encryption_key = encryption_key
        self._encrypted_prefix = encrypted_prefix
        self._sensitive_fields_by_collection = (
            sensitive_fields_by_collection
            if sensitive_fields_by_collection is not None
            else COUCHDB_SENSITIVE_FIELDS_BY_COLLECTION
        )

    def is_sensitive_field(self, collection_name: str, field_name: str) -> bool:
        return field_name in self._sensitive_fields_by_collection.get(collection_name, set())

    def token_field_name(self, field_name: str) -> str:
        return f"{field_name}_token"

    def make_token(self, value: Any) -> str:
        normalized = str(value).encode("utf-8")
        digest = hmac.new(self._encryption_key, normalized, hashlib.sha256).hexdigest()
        return digest

    def encrypt_string(self, value: str) -> str:
        if value.startswith(self._encrypted_prefix):
            return value

        value_bytes = value.encode("utf-8")
        encrypted_bytes = bytes(
            byte ^ self._encryption_key[index % len(self._encryption_key)]
            for index, byte in enumerate(value_bytes)
        )
        encoded = base64.urlsafe_b64encode(encrypted_bytes).decode("ascii")
        return f"{self._encrypted_prefix}{encoded}"

    def decrypt_string(self, value: str) -> str:
        if not value.startswith(self._encrypted_prefix):
            return value

        encoded = value[len(self._encrypted_prefix):]
        encrypted_bytes = base64.urlsafe_b64decode(encoded.encode("ascii"))
        decrypted_bytes = bytes(
            byte ^ self._encryption_key[index % len(self._encryption_key)]
            for index, byte in enumerate(encrypted_bytes)
        )
        return decrypted_bytes.decode("utf-8")

    def transform_document_for_storage(
        self,
        collection_name: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        transformed = dict(document)
        for field_name in self._sensitive_fields_by_collection.get(collection_name, set()):
            value = transformed.get(field_name)
            if value is None:
                continue
            if not isinstance(value, str):
                value = str(value)
            plaintext_value = self.decrypt_string(value)
            transformed[field_name] = self.encrypt_string(plaintext_value)
            transformed[self.token_field_name(field_name)] = self.make_token(plaintext_value)

        return transformed

    def transform_filter_for_storage(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
    ) -> dict[str, Any]:
        transformed: dict[str, Any] = {}
        for field_name, value in filter_query.items():
            if not self.is_sensitive_field(collection_name, field_name):
                transformed[field_name] = value
                continue

            token_field = self.token_field_name(field_name)
            if isinstance(value, dict):
                if "$eq" in value and value["$eq"] is not None:
                    transformed[token_field] = self.make_token(value["$eq"])
                else:
                    transformed[field_name] = value
                continue

            transformed[token_field] = self.make_token(value)

        return transformed

    def transform_document_for_read(
        self,
        collection_name: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        transformed = dict(document)
        for field_name in self._sensitive_fields_by_collection.get(collection_name, set()):
            value = transformed.get(field_name)
            if isinstance(value, str):
                transformed[field_name] = self.decrypt_string(value)
            transformed.pop(self.token_field_name(field_name), None)

        return transformed
