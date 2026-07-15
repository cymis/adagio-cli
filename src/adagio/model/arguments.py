from pydantic import BaseModel, Field

from .task import AllowableValue

InputValue = str | list[str] | dict[str, str]


class AdagioArguments(BaseModel):
    inputs: dict[str, InputValue]
    parameters: dict[str, AllowableValue]
    outputs: str | dict[str, str]
    publish: dict[str, str] = Field(default_factory=dict)

    def __repr__(self):
        """Format arguments for display."""
        return '\n'.join([
            *self._format_repr_sect(self.inputs, 'inputs'),
            *self._format_repr_sect(self.parameters, 'parameters'),
            *self._format_repr_sect(self.outputs, 'outputs'),
            *self._format_repr_sect(self.publish, 'publish'),
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
    publish: dict[str, str] = Field(default_factory=dict)
