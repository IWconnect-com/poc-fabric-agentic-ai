"""Typed error hierarchy. Every failure in the platform is one of these.

Code decides what to do from `code` and `retryable`, never by parsing messages.
"""
from __future__ import annotations

from typing import Any


class KitError(Exception):
    code = "kit_error"
    retryable = False

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = context

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "context": self.context,
        }


# --- policy / governance (never retryable: retrying will not change the answer)
class PolicyViolation(KitError):
    code = "policy_violation"


class ToolDenied(PolicyViolation):
    code = "tool_denied"


class ParamViolation(PolicyViolation):
    code = "param_violation"


class ApprovalRequired(PolicyViolation):
    code = "approval_required"


class ModelNotApproved(PolicyViolation):
    code = "model_not_approved"


# --- configuration / secrets
class SecretUnavailable(KitError):
    code = "secret_unavailable"


class SpecInvalid(KitError):
    code = "spec_invalid"


# --- external systems (transient by nature)
class SourceUnavailable(KitError):
    code = "source_unavailable"
    retryable = True


class Throttled(KitError):
    code = "throttled"
    retryable = True


class StepFailed(KitError):
    """An untyped failure inside a pipeline step, wrapped so callers only see typed errors."""

    code = "step_failed"


# --- data correctness
class SchemaDrift(KitError):
    code = "schema_drift"


class ReconciliationMismatch(KitError):
    code = "reconciliation_mismatch"
