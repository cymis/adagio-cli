import typing as t
import os
import json

from pydantic import BaseModel, RootModel, model_validator, Field


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

    def load_inputs(self, ctx, arguments, scope):
        from adagio.io import load_input, load_metadata

        for input in self.inputs:
            source = arguments.inputs[input.name]
            if input.ast.name.startswith('Metadata') and input.ast.builtin:
                print("SCHEDULED:", f'load_metadata({source!r})')
                scope[input.id] = load_metadata(ctx=ctx, source=source)
                # IIFE for the dreaded for-loop in the parent closure problem.
                scope[input.id]._future_.add_done_callback((lambda str: (lambda x: print("DONE:", str)))(f'load_metadata({source!r})'))
            else:
                print("SCHEDULED:", f'load_input({source!r})')
                scope[input.id] = load_input(ctx=ctx, source=source)
                # IIFE for the dreaded for-loop in the parent closure problem.
                scope[input.id]._future_.add_done_callback((lambda str: (lambda x: print("DONE:", str)))(f'load_input({source!r})'))

    def save_outputs(self, ctx, arguments: AdagioArguments, scope):
        from adagio.io import save_output

        futures = []
        for output in self.outputs:
            if type(arguments.outputs) is str:
                dest = os.path.join(arguments.outputs, output.name)
            elif type(arguments.outputs) is dict:
                dest = arguments.outputs[output.name]
            else:
                raise NotImplementedError('impossible')
            print("SCHEDULED:", f'{output.name}.save({dest!r})')
            future = save_output(ctx=ctx, output=scope[output.id], destination=dest)
            # IIFE for the dreaded for-loop in the parent closure problem.
            future.add_done_callback((lambda str: (lambda x: print("DONE:", str)))(f'{output.name}.save({dest!r})'))
            futures.append(future)

        for future in futures:
            try:
                future.result()
            except Exception:
                pass



class _Def(BaseModel):
    id: str
    name: str
    type: str
    ast: TypeAST


class _InputDef(_Def):
    required: bool


class _ParameterDef(_Def):
    required: bool
    default: 'AllowableValue | None' = None


class _OutputDef(_Def):
    pass
