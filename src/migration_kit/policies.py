"""Machine-readable governance rules (the five pillars) loaded from config/policies.yaml."""
from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PoliciesSection(_Strict):
    agents_read_row_values: bool
    agents_write_environments: list[str]
    max_tables_per_spec: int = Field(gt=0)


class ApprovalsSection(_Strict):
    auto_approve_min_confidence: float = Field(ge=0, le=1)
    human_review_min_confidence: float = Field(ge=0, le=1)
    always_human: list[str]

    @model_validator(mode="after")
    def _ordered(self) -> "ApprovalsSection":
        if self.human_review_min_confidence > self.auto_approve_min_confidence:
            raise ValueError("human_review_min_confidence must be <= auto_approve_min_confidence")
        return self


class GuardrailsSection(_Strict):
    screen_input: bool
    screen_output: bool
    pii_detection_before_model: bool
    max_agent_iterations: int = Field(gt=0)
    agent_timeout_seconds: int = Field(gt=0)
    max_tokens_per_run: int = Field(gt=0)


class EvaluationSection(_Strict):
    min_pass_rate: float = Field(ge=0, le=1)
    rerun_on: list[str]
    schedule: str


class OversightSection(_Strict):
    alert_on: list[str]
    audit_fields: list[str]


class Policies(_Strict):
    version: int
    policies: PoliciesSection
    approvals: ApprovalsSection
    guardrails: GuardrailsSection
    evaluation: EvaluationSection
    oversight: OversightSection

    @classmethod
    def load(cls, path: str | Path) -> "Policies":
        with open(path, encoding="utf-8") as fh:
            return cls.model_validate(yaml.safe_load(fh))


Route = Literal["auto_approve", "human_review", "reject"]


def route_by_confidence(confidence: float, approvals: ApprovalsSection) -> Route:
    """Deterministic gate on a model-reported confidence. Code decides, not the LLM."""
    if confidence >= approvals.auto_approve_min_confidence:
        return "auto_approve"
    if confidence >= approvals.human_review_min_confidence:
        return "human_review"
    return "reject"
