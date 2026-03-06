import typing as t
from pydantic import BaseModel, Field


class _BaseTask(BaseModel):
    id: str
    kind: str
    inputs: dict[str, 'InputVal']
    parameters: dict[str, 'LiteralVal | MetadataVal | PromotedVal']
    outputs: dict[str, 'OutputVal']

    def exec(self, ctx, params, scope):
        raise NotImplementedError


class PluginActionTask(_BaseTask):
    id: str
    kind: t.Literal['plugin-action']
    plugin: str
    action: str

    def exec(self, ctx, params, scope):
        from adagio.io import convert_metadata

        action = ctx.get_action(self.plugin, self.action)
        kwargs = {}
        metadata = {}
        for name, src in self.inputs.items():
            if src.kind == 'archive':
                kwargs[name] = scope[src.id]
            elif src.kind == 'metadata':
                # store for second pass in params
                metadata[name] = scope[src.id]
            else:
                raise NotImplemented('impossible')

        for name, param in self.parameters.items():
            if param.kind == 'metadata':
                if param.column.kind == 'literal':
                    col = param.value
                elif param.column.kind == 'promoted':
                    col = params[param.column.id]
                else:
                    raise NotImplementedError('impossible')

                source = metadata.pop(name)
                md = convert_metadata(ctx=ctx, metadata=source)
                kwargs[name] = md.get_column(col)

            elif param.kind == 'literal':
                kwargs[name] = param.value

            elif param.kind == 'promoted':
                kwargs[name] = params[param.id]

            else:
                raise NotImplementedError('impossible')

        # any remaining metadata is used directly
        for name, value in metadata.items():
            kwargs[name] = convert_metadata(ctx=ctx, metadata=value)

        results = action(**kwargs)
        for name, dest in self.outputs.items():
            scope[dest.id] = getattr(results, name)


class RootInputTask(_BaseTask):
    kind: t.Literal['built-in']
    name: t.Literal['root-input']

    def exec(self, ctx, params, scope):
        for name, src in self.inputs.items():
            dst = self.outputs[name]
            scope[dst.id] = scope[src.id]


class InputVal(BaseModel):
    kind: t.Literal['archive', 'metadata']
    id: str


class OutputVal(BaseModel):
    kind: t.Literal['archive']
    id: str


class PromotedVal(BaseModel):
    kind: t.Literal['promoted']
    id: str


class LiteralVal(BaseModel):
    kind: t.Literal['literal']
    value: 'AllowableValue'


class LiteralStrVal(LiteralVal):
    value: str


class MetadataVal(BaseModel):
    kind: t.Literal['metadata']
    column: PromotedVal | LiteralStrVal


Primitive = int | float | str | bool | t.Literal[None]
Collection = list[Primitive] | dict[str, Primitive]
AllowableValue = Primitive | Collection
AdagioTask = t.Annotated[t.Union[PluginActionTask, RootInputTask],
                         Field(discriminator='kind')]
