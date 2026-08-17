"""Schema registry and contract validation (AWS Glue Schema Registry analogue).

This is the control point of the whole design. The 2025 incident was not a code
failure -- the Spark job ran green for 11 days -- it was a contract failure that
nothing was positioned to notice. So the contract lives here, versioned, with
compatibility rules and explicit field mappings, and every record is checked
against it before it is allowed anywhere near the gold layer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from jsonschema import Draft202012Validator

from .config import Config


@dataclass
class ValidationResult:
    ok: bool
    schema_version: str
    canonical: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)
    observed_fields: List[str] = field(default_factory=list)

    @property
    def reason(self) -> str:
        return "; ".join(self.errors[:3]) if self.errors else ""


class SchemaRegistry:
    """Versioned contracts with FULL-compatibility checking and field mappings."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._validators: Dict[str, Draft202012Validator] = {}
        self._active: List[str] = []
        self.change_log: List[Dict[str, Any]] = []
        self.load("v1")
        self.activate("v1")

    # ------------------------------------------------------------------
    def load(self, version: str) -> Dict[str, Any]:
        path = self.cfg.contracts_dir / f"shipment_event.{version}.json"
        schema = json.loads(path.read_text())
        self._schemas[version] = schema
        self._validators[version] = Draft202012Validator(schema)
        return schema

    def activate(self, version: str, actor: str = "platform-oncall", note: str = "") -> None:
        if version not in self._schemas:
            self.load(version)
        if version not in self._active:
            self._active.append(version)
            self.change_log.append(
                {
                    "changed_at": datetime.now(timezone.utc),
                    "schema_version": version,
                    "action": "ACTIVATE",
                    "actor": actor,
                    "note": note,
                }
            )

    @property
    def active_versions(self) -> List[str]:
        return list(self._active)

    def mappings(self, version: str) -> List[Dict[str, str]]:
        return self._schemas.get(version, {}).get("x-swiftlogix-field-mappings", [])

    # ------------------------------------------------------------------
    def check_compatibility(self, old: str, new: str) -> Tuple[bool, List[str]]:
        """A minimal FULL-compatibility check.

        Removing a required field or narrowing a type breaks consumers; adding
        an optional field does not. A rename registers as both a removal and an
        addition, which is precisely why it must not be waved through.
        """
        problems: List[str] = []
        o, n = self._schemas[old], self._schemas[new]
        o_req, n_req = set(o.get("required", [])), set(n.get("required", []))
        for removed in o_req - n_req:
            problems.append(f"required field removed: {removed}")
        o_props, n_props = o.get("properties", {}), n.get("properties", {})
        for name in set(o_props) - set(n_props):
            problems.append(f"field dropped: {name}")
        for name in set(o_props) & set(n_props):
            ot, nt = o_props[name].get("type"), n_props[name].get("type")
            if ot and nt and ot != nt:
                problems.append(f"type changed on {name}: {ot} -> {nt}")
        return (not problems, problems)

    # ------------------------------------------------------------------
    @staticmethod
    def _apply_mapping(payload: Dict[str, Any], mapping: Dict[str, str]) -> None:
        """Move a value from a dotted source path to a dotted target path."""
        src, dst = mapping["from"].split("."), mapping["to"].split(".")
        node: Any = payload
        for part in src[:-1]:
            if not isinstance(node, dict) or part not in node:
                return
            node = node[part]
        if not isinstance(node, dict) or src[-1] not in node:
            return
        value = node.pop(src[-1])

        target: Any = payload
        for part in dst[:-1]:
            target = target.setdefault(part, {})
        target[dst[-1]] = value

    def _to_canonical(self, payload: Dict[str, Any], version: str) -> Dict[str, Any]:
        """Rewrite a non-v1 payload into the canonical v1 shape."""
        out = json.loads(json.dumps(payload))
        for mapping in self.mappings(version):
            self._apply_mapping(out, mapping)
        # Drop containers the mapping emptied out.
        for key in [k for k, v in out.items() if isinstance(v, dict) and not v]:
            if key not in ("geo", "delivery_window", "metadata"):
                out.pop(key)
        return out

    # ------------------------------------------------------------------
    def validate(self, payload: Dict[str, Any]) -> ValidationResult:
        """Validate against every active version, newest first.

        A record that matches no active contract is a quarantine candidate --
        never a silent null, and never dropped.
        """
        observed = sorted(payload.keys())
        errors: List[str] = []

        for version in reversed(self._active):
            validator = self._validators[version]
            problems = sorted(validator.iter_errors(payload), key=lambda e: e.path)
            if not problems:
                canonical = (
                    payload if version == "v1" else self._to_canonical(payload, version)
                )
                # A mapped payload must still satisfy the canonical contract.
                if version != "v1":
                    residual = list(self._validators["v1"].iter_errors(canonical))
                    if residual:
                        errors = [f"post-mapping v1 violation: {residual[0].message}"]
                        continue
                return ValidationResult(True, version, canonical, [], observed)
            if version == self._active[-1]:
                errors = [self._describe(e) for e in problems[:5]]

        return ValidationResult(False, "unknown", None, errors, observed)

    @staticmethod
    def _describe(err: Any) -> str:
        location = "$" + "".join(f".{p}" for p in err.absolute_path)
        return f"{location}: {err.message}"
