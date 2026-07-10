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
    user_description: str | None = None


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


class DataImportTask(_BaseTask):
    """Import raw (non-QIIME) data into a typed QIIME artifact.

    Emitted by the editor's built-in ``data-import`` node. ``source`` is a raw
    data path; the ``semantic_type`` / ``input_format`` / ``validate_level``
    parameters describe how to import it. ``user_description`` mirrors
    ``PluginActionTask`` so ``pipeline show -v`` surfaces editor-authored notes.
    """

    kind: t.Literal["built-in"]
    name: t.Literal["data-import"]
    user_description: str | None = None

    def _param_value(self, name, params, default=None):
        param = self.parameters.get(name)
        if param is None:
            return default
        if param.kind == "literal":
            return param.value
        if param.kind == "promoted":
            return params[param.id]
        raise NotImplementedError(f"Unsupported parameter kind for {name!r}: {param.kind}")

    def exec(self, ctx, params, scope):
        from qiime2 import Artifact

        src = self.inputs["source"]
        if src.id not in scope:
            return
        source = scope[src.id]

        semantic_type = self._param_value("semantic_type", params)
        input_format = self._param_value("input_format", params)
        validate_level = self._param_value("validate_level", params, "max")

        artifact = Artifact.import_data(
            semantic_type, source, view_type=input_format or None
        )
        if validate_level in ("min", "max"):
            artifact.validate(level=validate_level)

        scope[self.outputs["artifact"].id] = artifact


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
    t.Union[RootInputTask, ConvertToMetadataTask, DataImportTask],
    Field(discriminator="name"),
]
AdagioTask = t.Annotated[
    t.Union[PluginActionTask, BuiltInTask], Field(discriminator="kind")
]


def input_source_ids(value: TaskInputVal) -> list[str]:
    if value.kind == "archive-collection":
        return [item.id for item in value.items]
    return [value.id]
