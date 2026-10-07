"""The only module that touches credentials.

Rules:
  * The only environment variable the platform reads is AZURE_KEYVAULT_URL.
  * Everything else (endpoints, deployment names, connection ids) comes from
    Key Vault at runtime, via managed identity / `az login` (DefaultAzureCredential).
  * No secret value is ever logged or included in an error message.
"""
from __future__ import annotations

import os
from typing import Protocol

from .errors import SecretUnavailable

KEYVAULT_URL_ENV = "AZURE_KEYVAULT_URL"


class SecretProvider(Protocol):
    def get(self, name: str) -> str: ...


class KeyVaultProvider:
    """Reads secrets from Azure Key Vault. Azure SDKs are imported lazily."""

    def __init__(self) -> None:
        self._client = None

    def _get_client(self):
        if self._client is None:
            vault_url = os.environ.get(KEYVAULT_URL_ENV)
            if not vault_url:
                raise SecretUnavailable(f"{KEYVAULT_URL_ENV} is not set")
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            self._client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
        return self._client

    def get(self, name: str) -> str:
        client = self._get_client()
        try:
            value = client.get_secret(name).value
        except Exception as exc:  # exception text may contain sensitive detail: keep type only
            raise SecretUnavailable(f"could not read secret {name!r}", secret=name, cause=type(exc).__name__) from exc
        if not value:
            raise SecretUnavailable(f"secret {name!r} is empty", secret=name)
        return value


_provider: SecretProvider | None = None
_cache: dict[str, str] = {}


def set_provider(provider: SecretProvider | None) -> None:
    """Swap the provider (tests use a fake; production uses the default)."""
    global _provider
    _provider = provider
    _cache.clear()


def clear_cache() -> None:
    _cache.clear()


def secret(name: str) -> str:
    if name in _cache:
        return _cache[name]
    global _provider
    if _provider is None:
        _provider = KeyVaultProvider()
    value = _provider.get(name)
    _cache[name] = value
    return value
