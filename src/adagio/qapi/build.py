import collections
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast

DEFAULT_SCHEMA_VERSION = "0.1.0"
PRIVATE_QIIME_ACTION_PREFIXES = ("_", "-")
ADAGIO_BUILTIN_PLUGIN = "adagio_builtin"
CONVERT_TO_METADATA_ACTION_ID = "convert_to_metadata"
CONVERT_TO_METADATA_ACTION_NAME = "convert-to-metadata"


def _metadata_ast() -> dict[str, Any]:
    return {
        "name": "Metadata",
        "type": "expression",
        "fields": [],
        "builtin": True,
        "predicate": None,
    }


def _union_ast(members: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "union", "members": members}


def _build_convert_to_metadata_action(
    source_types: Sequence[tuple[str, dict[str, Any]]],
) -> dict[str, Any] | None:
    """Build the synthetic QAPI action for artifact-to-Metadata conversion."""
    if not source_types:
        return None

    sorted_source_types = sorted(source_types, key=lambda item: item[0])
    type_names = [type_name for type_name, _ in sorted_source_types]
    source_ast = _union_ast([ast for _, ast in sorted_source_types])
    metadata_ast = _metadata_ast()

    return {
        "id": CONVERT_TO_METADATA_ACTION_ID,
        "name": CONVERT_TO_METADATA_ACTION_NAME,
        "description": (
            "Convert an artifact with a registered QIIME 2 metadata transformer "
            "into metadata for downstream actions."
        ),
        "inputs": [
            {
                "name": "data",
                "type": " | ".join(type_names),
                "ast": source_ast,
                "required": True,
                "description": ("Artifact that can be viewed as QIIME 2 Metadata."),
            }
        ],
        "parameters": [],
        "outputs": [
            {
                "name": "metadata",
                "type": "Metadata",
                "ast": metadata_ast,
                "description": "Metadata view of the input artifact.",
            }
        ],
        "adagio_builtin": "metadata_transformer",
    }


def _private_qiime_action_id(action_key: object, action: Any) -> str | None:
    action_id = getattr(action, "id", None)
    for value in (action_id, action_key):
        if isinstance(value, str) and value.startswith(PRIVATE_QIIME_ACTION_PREFIXES):
            return value
    return None


def _iter_public_qiime_actions(
    actions: Mapping[object, Any],
    *,
    plugin_name: str | None = None,
    on_skipped_private_action: Callable[[str], None] | None = None,
) -> Iterator[tuple[object, Any]]:
    for key, action in actions.items():
        private_action_id = _private_qiime_action_id(key, action)
        if private_action_id is not None:
            if on_skipped_private_action is not None:
                skipped_action_id = (
                    f"{plugin_name}.{private_action_id}"
                    if plugin_name is not None
                    else private_action_id
                )
                on_skipped_private_action(skipped_action_id)
            continue
        yield key, action


def normalize_plugin_selection(plugin_names: Sequence[str] | None) -> list[str] | None:
    """Normalize repeated or comma-separated plugin names."""
    if plugin_names is None:
        return None

    normalized: list[str] = []
    for plugin_name in plugin_names:
        for token in plugin_name.split(","):
            stripped = token.strip()
            if stripped:
                normalized.append(stripped)

    return normalized


def generate_qapi_payload(
    *,
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    plugins: Sequence[str] | None = None,
    on_skipped_private_action: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Generate a QAPI payload for all plugins or a selected subset."""
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
                final_predicate = UnionExp(
                    [flatten_type_maps(elem) for elem in predicate]
                )
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

        fields = [
            ast_to_basename(field)
            for field in cast(list[dict[str, Any]], ast["fields"])
        ]
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

    def iter_metadata_transformer_source_types() -> Iterator[
        tuple[str, dict[str, Any]]
    ]:
        to_type = transform.ModelType.from_view_type(qiime2.Metadata)
        for _, artifact_class in sorted(plugin_manager.artifact_classes.items()):
            try:
                from_type = transform.ModelType.from_view_type(artifact_class.format)
                if not from_type.has_transformation(to_type):
                    continue
                semantic_type = artifact_class.semantic_type
                yield (
                    repr(semantic_type),
                    flatten_type_maps(semantic_type).to_ast(),
                )
            except Exception:
                continue

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
                    "ast": add_metadata_flag(
                        flatten_type_maps(spec.qiime_type).to_ast()
                    ),
                    "description": optional_desc(spec.description),
                }
                for name, spec in action.signature.outputs.items()
            ],
            "name": action.name,
            "description": action.description,
            "source": action.source.replace("\n```python\n", "").replace("```\n", ""),
        }

    def build_data_dict(
        *, plugin_name: str, data: Mapping[object, Any]
    ) -> dict[str, Any]:
        result: dict[str, Any] = collections.defaultdict(dict)
        for key, value in _iter_public_qiime_actions(
            data,
            plugin_name=plugin_name,
            on_skipped_private_action=on_skipped_private_action,
        ):
            result[str(key)] = build_inspect_dict(value)
        return result

    qapi: dict[str, Any] = {}
    requested_plugins = normalize_plugin_selection(plugins)
    selected_plugins = sorted(plugin_manager.plugins)
    if requested_plugins is not None:
        available_plugins = set(plugin_manager.plugins)
        missing_plugins = sorted(set(requested_plugins) - available_plugins)
        if missing_plugins:
            missing = ", ".join(missing_plugins)
            raise ValueError(f"Unknown plugin name(s): {missing}")
        selected_plugins = sorted(set(requested_plugins))

    for plugin_name in selected_plugins:
        plugin = plugin_manager.plugins[plugin_name]
        methods_dict = build_data_dict(plugin_name=plugin_name, data=plugin.actions)
        methods_dict.update(
            build_data_dict(plugin_name=plugin_name, data=plugin.pipelines)
        )
        qapi[plugin_name] = {"methods": methods_dict}

    if requested_plugins is None:
        convert_to_metadata = _build_convert_to_metadata_action(
            list(iter_metadata_transformer_source_types())
        )
        if convert_to_metadata is not None:
            qapi[ADAGIO_BUILTIN_PLUGIN] = {
                "methods": {
                    CONVERT_TO_METADATA_ACTION_ID: convert_to_metadata,
                }
            }

    return {
        "qiime_version": qiime2.__version__,
        "schema_version": schema_version,
        "data": qapi,
    }
