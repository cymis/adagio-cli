import typing as t
from pydantic import BaseModel, Field

from .task import AllowableValue

InputValue = str | list[str] | dict[str, str]


class AdagioArguments(BaseModel):
    inputs: dict[str, InputValue]
    parameters: dict[str, AllowableValue]
    outputs: str | dict[str, str]

    def __repr__(self):
        """Format arguments for display."""
        return '\n'.join([
            *self._format_repr_sect(self.inputs, 'inputs'),
            *self._format_repr_sect(self.parameters, 'parameters'),
            *self._format_repr_sect(self.outputs, 'outputs'),
        ])

    def _format_repr_sect(self, section, name):
        """Format a single argument section."""
        lines = []
        if not section:
            lines.append(f'{name}: {{}}')
        else:
            lines.append(f'{name}:')
            for name, value in section.items():
                lines.append(f'    {name}: {value!r}')

        return lines


class AdagioArgumentsFile(BaseModel):
    """Represent arguments loaded from a JSON file."""

    version: int = 1
    inputs: dict[str, InputValue] = Field(default_factory=dict)
    parameters: dict[str, AllowableValue] = Field(default_factory=dict)
    outputs: str | dict[str, str] | None = None
