import typing as t

from pydantic import BaseModel, RootModel, model_validator


from .arguments import AdagioArguments
from .task import AllowableValue, AdagioTask
from .ast import TypeAST


class AdagioPipeline(BaseModel):
    type: t.Literal['pipeline']
    # meta: 'AdagioPipelineMetadata'
    signature: 'AdagioSignature'
    graph: list['AdagioTask']

    def validate_graph(self):
        pass

    def iter_tasks(self) -> t.Generator['AdagioTask', None, None]:
        yield from self.graph



class AdagioPipelineMetadata(RootModel):
    root: dict[str, t.Any]

    @model_validator(mode='before')
    def check_version(cls, data):
        if 'version' not in data:
            raise AssertionError('Missing "version" field.')


class AdagioSignature(BaseModel):
    inputs: 'list[_InputDef]'
    parameters: 'list[_ParameterDef]'
    outputs: 'list[_OutputDef]'

    def to_default_arguments(self):
        """Placeholders for every argument, plus any default an older spec still carries.

        A current spec declares variables without values; they arrive from the
        bundled config and the layers above it (see ``adagio.bundled_config``).
        """
        inputs = {}
        for input in self.inputs:
            inputs[input.name] = '<fill me>'
        params = {}
        for param in self.parameters:
            if param.required and param.default is None:
                params[param.name] = '<fill me>'
            else:
                params[param.name] = param.default
        outputs = {}
        for output in self.outputs:
            outputs[output.name] = '<fill me>'

        return AdagioArguments(inputs=inputs, parameters=params, outputs=outputs)

    def names(self) -> tuple[set[str], set[str], set[str]]:
        """Input, parameter and output names, for filtering a config to this signature."""
        return (
            {input.name for input in self.inputs},
            {param.name for param in self.parameters},
            {output.name for output in self.outputs},
        )

    def validate_arguments(self, args: AdagioArguments):
        return


    def get_params(self, args: AdagioArguments):
        lookup = {}
        for param in self.parameters:
            lookup[param.id] = args.parameters.get(param.name, param.default)
        return lookup


class _Def(BaseModel):
    id: str
    name: str
    type: str
    ast: TypeAST
    description: str | None = None
    user_description: str | None = None


class _InputDef(_Def):
    required: bool


class _ParameterDef(_Def):
    # The signature provides no value, so a variable is required of the layers
    # above it unless one of them supplies it. `default` only appears in specs
    # written before configs existed.
    required: bool = True
    default: 'AllowableValue | None' = None


class _OutputDef(_Def):
    pass
