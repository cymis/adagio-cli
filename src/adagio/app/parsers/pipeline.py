"""This is a placeholder for the pipeline parser. It will be implemented in the future when we have a better understanding of the pipeline specification.

This can and should be updated to something more robust
"""

from pydantic import BaseModel
from typing import Any, List, Optional
from uuid import UUID


class Parameter(BaseModel):
    id: UUID
    name: str
    required: bool
    default: Optional[Any] = None
    type: str


def parse_parameters(data: Any) -> List[Parameter]:
    """Parse a list of parameter dictionaries into a list of Parameter objects."""
    parameters = []
    for param in data["spec"]["signature"]["parameters"]:
        parameters.append(Parameter(**param))
    return parameters
