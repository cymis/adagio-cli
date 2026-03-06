"""Helpers for pulling promoted parameter specs from pipeline JSON."""

from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel


class Parameter(BaseModel):
    id: UUID
    name: str
    required: bool
    default: Optional[Any] = None
    type: str


class Input(BaseModel):
    id: UUID
    name: str
    required: bool
    type: str


def _extract_signature(data: Any) -> dict[str, Any]:
    signature = (
        data.get("spec", {}).get("signature")
        if isinstance(data, dict)
        else None
    ) or (data.get("signature") if isinstance(data, dict) else None)

    if not isinstance(signature, dict):
        raise ValueError(
            "Invalid pipeline: missing 'signature' section in pipeline JSON."
        )

    return signature


def parse_parameters(data: Any) -> List[Parameter]:
    """Parse promoted parameters from supported pipeline JSON layouts.

    We currently accept either:
    - {"spec": {"signature": {"parameters": [...]}}}
    - {"signature": {"parameters": [...]} }
    """
    signature = _extract_signature(data)

    raw_parameters = signature.get("parameters")
    if not isinstance(raw_parameters, list):
        raise ValueError(
            "Invalid pipeline: missing 'signature.parameters' list in pipeline JSON."
        )

    return [Parameter(**param) for param in raw_parameters]


def parse_inputs(data: Any) -> List[Input]:
    """Parse promoted inputs from supported pipeline JSON layouts."""
    signature = _extract_signature(data)

    raw_inputs = signature.get("inputs")
    if not isinstance(raw_inputs, list):
        raise ValueError(
            "Invalid pipeline: missing 'signature.inputs' list in pipeline JSON."
        )

    return [Input(**input_item) for input_item in raw_inputs]
