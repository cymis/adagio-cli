import typing as t
from pydantic import BaseModel

from .task import AllowableValue


class AdagioArguments(BaseModel):
    inputs: dict[str, str]
    parameters: dict[str, AllowableValue]
    outputs: str | dict[str, str]

    def __repr__(self):
        return '\n'.join([
            *self._format_repr_sect(self.inputs, 'inputs'),
            *self._format_repr_sect(self.parameters, 'parameters'),
            *self._format_repr_sect(self.outputs, 'outputs'),
        ])

    def _format_repr_sect(self, section, name):
        lines = []
        if not section:
            lines.append(f'{name}: {{}}')
        else:
            lines.append(f'{name}:')
            for name, value in section.items():
                lines.append(f'    {name}: {value!r}')

        return lines

