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
        inputs = {}
        for input in self.inputs:
            inputs[input.name] = '<fill me>'
        params = {}
        for param in self.parameters:
            if param.required:
                params[param.name] = '<fill me>'
            else:
                params[param.name] = param.default
        outputs = {}
        for output in self.outputs:
            outputs[output.name] = '<fill me>'

        return AdagioArguments(inputs=inputs, parameters=params, outputs=outputs)

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
    required: bool
    default: 'AllowableValue | None' = None


class _OutputDef(_Def):
    pass
