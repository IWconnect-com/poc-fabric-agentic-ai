from pathlib import Path

import pytest

from migration_kit import secrets
from migration_kit.audit import AuditLog

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config"
SRC = ROOT / "src" / "migration_kit"


class FakeSecrets:
    def __init__(self, values=None):
        self.values = values or {}
        self.calls = []

    def get(self, name):
        self.calls.append(name)
        if name not in self.values:
            from migration_kit.errors import SecretUnavailable

            raise SecretUnavailable(f"missing {name}")
        return self.values[name]


@pytest.fixture
def events():
    return []


@pytest.fixture
def audit(events):
    return AuditLog(sink=events.append, identity="test-identity")


@pytest.fixture(autouse=True)
def _reset_secrets():
    secrets.set_provider(None)
    yield
    secrets.set_provider(None)
