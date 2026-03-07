import collections
from typing import Any, cast

DEFAULT_SCHEMA_VERSION = "0.1.0"


def generate_qapi_payload(*, schema_version: str = DEFAULT_SCHEMA_VERSION) -> dict[str, Any]:
    import qiime2
    import qiime2.core.transform as transform
    import qiime2.sdk
    from qiime2.core.type.grammar import IntersectionExp, PredicateExp, UnionExp
    from qiime2.core.type.meta import TypeExp, TypeVarExp

    plugin_manager = qiime2.sdk.PluginManager()

    def flatten_type_maps(qiime_type: Any) -> Any:
        if isinstance(qiime_type, TypeVarExp):
            final = []
            for outer in list(qiime_type):
                if isinstance(outer, PredicateExp):
                    final.append(outer)
                    continue
                for inner in list(outer):
                    final.append(flatten_type_maps(inner))
            final_union = UnionExp(final)
            final_union.normalize()
            return final_union

        if isinstance(qiime_type, TypeExp):
            final_fields = [flatten_type_maps(field) for field in qiime_type.fields]

            final_predicate = None
            if isinstance(qiime_type.predicate, UnionExp):
                predicate = qiime_type.predicate.unpack_union()
                final_predicate = UnionExp([flatten_type_maps(elem) for elem in predicate])
                final_predicate.normalize()
            elif isinstance(qiime_type.predicate, IntersectionExp):
                predicate = qiime_type.predicate.unpack_intersection()
                final_predicate = IntersectionExp(
                    [flatten_type_maps(elem) for elem in predicate]
                )
                final_predicate.normalize()
            elif isinstance(qiime_type.predicate, PredicateExp):
                final_predicate = flatten_type_maps(qiime_type.predicate)

            return qiime_type.duplicate(final_fields, final_predicate)

        return qiime_type

    def ast_to_basename(ast: dict[str, Any]) -> str:
        if not ast.get("fields"):
            return cast(str, ast["name"])

        fields = [ast_to_basename(field) for field in cast(list[dict[str, Any]], ast["fields"])]
        return f"{ast['name']}[{', '.join(fields)}]"

    def add_metadata_flag(ast: dict[str, Any]) -> dict[str, Any]:
        try:
            key = ast_to_basename(ast)
            artifact_class = plugin_manager.artifact_classes[key]
            from_type = transform.ModelType.from_view_type(artifact_class.format)
            to_type = transform.ModelType.from_view_type(qiime2.Metadata)
            ast["has_metadata"] = from_type.has_transformation(to_type)
        except Exception:
            return ast
        return ast

    def optional_desc(value: Any) -> str | None:
        no_value = qiime2.core.type.signature.__NoValueMeta  # type: ignore[attr-defined]
        return value if type(value) is not no_value else None

    def build_inspect_dict(action: Any) -> dict[str, Any]:
        return {
            "id": action.id,
            "inputs": [
                {
                    "name": name,
                    "type": repr(spec.qiime_type),
                    "ast": flatten_type_maps(spec.qiime_type).to_ast(),
                    "required": not spec.has_default(),
                    "description": optional_desc(spec.description),
                }
                for name, spec in action.signature.inputs.items()
            ],
            "parameters": [
                {
                    "name": name,
                    "type": repr(spec.qiime_type),
                    "ast": flatten_type_maps(spec.qiime_type).to_ast(),
                    "required": not spec.has_default(),
                    "default": spec.default if spec.has_default() else None,
                    "description": optional_desc(spec.description),
                }
                for name, spec in action.signature.parameters.items()
            ],
            "outputs": [
                {
                    "name": name,
                    "type": repr(spec.qiime_type),
                    "ast": add_metadata_flag(flatten_type_maps(spec.qiime_type).to_ast()),
                    "description": optional_desc(spec.description),
                }
                for name, spec in action.signature.outputs.items()
            ],
            "name": action.name,
            "description": action.description,
            "source": action.source.replace("\n```python\n", "").replace("```\n", ""),
        }

    def build_data_dict(data: Any) -> dict[str, Any]:
        result: dict[str, Any] = collections.defaultdict(dict)
        for key, value in data.items():
            result[key] = build_inspect_dict(value)
        return result

    qapi: dict[str, Any] = {}
    for plugin_name in sorted(plugin_manager.plugins):
        plugin = plugin_manager.plugins[plugin_name]
        methods_dict = build_data_dict(plugin.actions)
        methods_dict.update(build_data_dict(plugin.pipelines))
        qapi[plugin_name] = {"methods": methods_dict}

    return {
        "qiime_version": qiime2.__version__,
        "schema_version": schema_version,
        "data": qapi,
    }
