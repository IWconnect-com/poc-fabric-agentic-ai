"""ToolGuard: deterministic, default-deny gate in front of every tool call.

It runs in code, outside the LLM. An agent can only call a tool that is listed
for it in config/allowed_tools.yaml, with parameters that satisfy the rules
there. Every decision (allowed or denied) is written to the audit log.
"""
from __future__ import annotations

import functools
import re
from pathlib import Path
from typing import Any, Callable

import yaml

from .audit import AuditLog
from .errors import ApprovalRequired, KitError, ParamViolation, ToolDenied


class ToolGuard:
    def __init__(self, policy: dict[str, Any], audit: AuditLog | None = None) -> None:
        self._agents: dict[str, Any] = (policy or {}).get("agents", {}) or {}
        self.audit = audit or AuditLog()

    @classmethod
    def from_yaml(cls, path: str | Path, audit: AuditLog | None = None) -> "ToolGuard":
        with open(path, encoding="utf-8") as fh:
            return cls(yaml.safe_load(fh), audit)

    def check(self, agent: str, tool: str, params: dict[str, Any] | None = None, approved: bool = False) -> None:
        params = params or {}
        try:
            self._evaluate(agent, tool, params, approved)
        except KitError as exc:
            self.audit.record(agent=agent, action=tool, result="denied", reason=exc.message, params=params)
            raise
        self.audit.record(agent=agent, action=tool, result="allowed", params=params)

    def wrap(self, agent: str, tool: str, fn: Callable[..., Any], approved: bool = False) -> Callable[..., Any]:
        """Return `fn` guarded. Tools are called with keyword arguments only."""

        @functools.wraps(fn)
        def guarded(**kwargs: Any) -> Any:
            self.check(agent, tool, kwargs, approved)
            return fn(**kwargs)

        return guarded

    # ------------------------------------------------------------------ rules
    def _evaluate(self, agent: str, tool: str, params: dict[str, Any], approved: bool) -> None:
        if agent not in self._agents:
            raise ToolDenied(f"unknown agent {agent!r}", agent=agent)
        agent_cfg = self._agents[agent] or {}
        if tool not in agent_cfg:
            raise ToolDenied(f"agent {agent!r} may not call {tool!r}", agent=agent, tool=tool)
        tool_cfg = agent_cfg[tool] or {}

        if tool_cfg.get("requires_approval") and not approved:
            raise ApprovalRequired(f"{tool!r} requires human approval", agent=agent, tool=tool)

        rules: dict[str, Any] = tool_cfg.get("params", {}) or {}
        for name in params:
            if name not in rules:
                raise ParamViolation(f"unexpected parameter {name!r} for {tool!r}", tool=tool, param=name)
        for name, rule in rules.items():
            rule = rule or {}
            if name not in params:
                if rule.get("required"):
                    raise ParamViolation(f"missing required parameter {name!r}", tool=tool, param=name)
                continue
            self._check_param(tool, name, params[name], rule)

    @staticmethod
    def _check_param(tool: str, name: str, value: Any, rule: dict[str, Any]) -> None:
        def fail(why: str) -> ParamViolation:
            return ParamViolation(f"{name!r}: {why}", tool=tool, param=name)

        if "allowed_values" in rule and value not in rule["allowed_values"]:
            raise fail(f"value not in allowed set {rule['allowed_values']}")
        if "max" in rule:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value > rule["max"]:
                raise fail(f"must be a number <= {rule['max']}")
        if "min" in rule:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < rule["min"]:
                raise fail(f"must be a number >= {rule['min']}")
        text = str(value)
        if "pattern" in rule and re.fullmatch(rule["pattern"], text) is None:
            raise fail("does not match the required pattern")
        for forbidden in rule.get("forbidden_patterns", []) or []:
            if re.search(forbidden, text, re.IGNORECASE):
                raise fail("contains a forbidden pattern")
