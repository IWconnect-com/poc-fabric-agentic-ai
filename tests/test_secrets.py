import pytest

from migration_kit import secrets
from migration_kit.errors import SecretUnavailable

from conftest import FakeSecrets


def test_secret_is_fetched_once_and_cached():
    fake = FakeSecrets({"ai-endpoint": "value-1"})
    secrets.set_provider(fake)
    assert secrets.secret("ai-endpoint") == "value-1"
    assert secrets.secret("ai-endpoint") == "value-1"
    assert fake.calls == ["ai-endpoint"]


def test_missing_secret_is_typed_error():
    secrets.set_provider(FakeSecrets())
    with pytest.raises(SecretUnavailable):
        secrets.secret("nope")


def test_keyvault_provider_requires_only_the_vault_env_var(monkeypatch):
    monkeypatch.delenv(secrets.KEYVAULT_URL_ENV, raising=False)
    with pytest.raises(SecretUnavailable) as info:
        secrets.KeyVaultProvider().get("anything")
    assert secrets.KEYVAULT_URL_ENV in info.value.message


def test_provider_errors_never_leak_exception_text(monkeypatch):
    class Boom:
        def get_secret(self, name):
            raise RuntimeError("super-secret-token-123")

    provider = secrets.KeyVaultProvider()
    provider._client = Boom()
    with pytest.raises(SecretUnavailable) as info:
        provider.get("db")
    assert "super-secret-token-123" not in info.value.message
    assert "super-secret-token-123" not in str(info.value.context)
    assert info.value.context["cause"] == "RuntimeError"


def test_empty_secret_is_rejected():
    class Empty:
        class _S:
            value = ""

        def get_secret(self, name):
            return self._S()

    provider = secrets.KeyVaultProvider()
    provider._client = Empty()
    with pytest.raises(SecretUnavailable):
        provider.get("db")
