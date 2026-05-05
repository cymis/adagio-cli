import typing as t
from pydantic import BaseModel, Field


class _BaseTask(BaseModel):
    id: str
    kind: str
    inputs: dict[str, "TaskInputVal"]
    parameters: dict[str, "LiteralVal | MetadataVal | PromotedVal"]
    outputs: dict[str, "OutputVal"]

    def exec(self, ctx, params, scope):
        raise NotImplementedError


class PluginActionTask(_BaseTask):
    id: str
    kind: t.Literal["plugin-action"]
    name: str | None = None
    plugin: str
    action: str

    def exec(self, ctx, params, scope):
        from adagio.io import convert_metadata

        action = ctx.get_action(self.plugin, self.action)
        kwargs = {}
        metadata = {}
        for name, src in self.inputs.items():
            if src.kind == "archive":
                if src.id not in scope:
                    continue
                kwargs[name] = scope[src.id]
            elif src.kind == "archive-collection":
                kwargs[name] = _flatten_collection_values(
                    [scope[item.id] for item in src.items if item.id in scope]
                )
            elif src.kind == "metadata":
                if src.id not in scope:
                    continue
                # store for second pass in params
                metadata[name] = scope[src.id]
            else:
                raise NotImplementedError("impossible")

        for name, param in self.parameters.items():
            if param.kind == "metadata":
                if param.column.kind == "literal":
                    col = param.value
                elif param.column.kind == "promoted":
                    col = params[param.column.id]
                else:
                    raise NotImplementedError("impossible")

                source = metadata.pop(name)
                md = convert_metadata(ctx=ctx, metadata=source)
                kwargs[name] = md.get_column(col)

            elif param.kind == "literal":
                kwargs[name] = param.value

            elif param.kind == "promoted":
                kwargs[name] = params[param.id]

            else:
                raise NotImplementedError("impossible")

        # any remaining metadata is used directly
        for name, value in metadata.items():
            kwargs[name] = convert_metadata(ctx=ctx, metadata=value)

        results = action(**kwargs)
        for name, dest in self.outputs.items():
            scope[dest.id] = getattr(results, name)


class RootInputTask(_BaseTask):
    kind: t.Literal["built-in"]
    name: t.Literal["root-input"]

    def exec(self, ctx, params, scope):
        for name, src in self.inputs.items():
            dst = self.outputs[name]
            if src.id in scope:
                scope[dst.id] = scope[src.id]


class ConvertToMetadataTask(_BaseTask):
    kind: t.Literal["built-in"]
    name: t.Literal["convert-to-metadata"]

    def exec(self, ctx, params, scope):
        src = self.inputs["data"]
        dst = self.outputs["metadata"]
        if src.id in scope:
            scope[dst.id] = scope[src.id]


class InputVal(BaseModel):
    kind: t.Literal["archive", "metadata"]
    id: str


class ArchiveCollectionItemVal(BaseModel):
    key: str
    id: str


class ArchiveCollectionInputVal(BaseModel):
    kind: t.Literal["archive-collection"]
    style: t.Literal["list"]
    items: list[ArchiveCollectionItemVal]


class OutputVal(BaseModel):
    kind: t.Literal["archive"]
    id: str


class PromotedVal(BaseModel):
    kind: t.Literal["promoted"]
    id: str


class LiteralVal(BaseModel):
    kind: t.Literal["literal"]
    value: "AllowableValue"


class LiteralStrVal(LiteralVal):
    value: str


class MetadataVal(BaseModel):
    kind: t.Literal["metadata"]
    column: PromotedVal | LiteralStrVal


Primitive = int | float | str | bool | t.Literal[None]
Collection = list[Primitive] | dict[str, Primitive]
AllowableValue = Primitive | Collection
TaskInputVal = t.Annotated[
    t.Union[InputVal, ArchiveCollectionInputVal], Field(discriminator="kind")
]
BuiltInTask = t.Annotated[
    t.Union[RootInputTask, ConvertToMetadataTask], Field(discriminator="name")
]
AdagioTask = t.Annotated[
    t.Union[PluginActionTask, BuiltInTask], Field(discriminator="kind")
]


def input_source_ids(value: TaskInputVal) -> list[str]:
    if value.kind == "archive-collection":
        return [item.id for item in value.items]
    return [value.id]


def _flatten_collection_values(values: list[t.Any]) -> list[t.Any]:
    result: list[t.Any] = []
    for value in values:
        if isinstance(value, list):
            result.extend(value)
        elif isinstance(value, dict):
            result.extend(value.values())
        else:
            result.append(value)
    return result
