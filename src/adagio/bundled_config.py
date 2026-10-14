"""The configuration bundled in a pipeline file.

A pipeline's spec is its signature: which inputs, variables and outputs exist.
Their values live in configs - the same shape as an ``--arguments`` file - and
the .adg envelope carries them in a ``configs`` list beside ``spec``. The CLI
applies the first bundled config as the lowest layer: an ``--arguments`` file
overrides it, and command-line flags override both.

Files written before configs existed kept variable defaults inside the spec
(``signature.parameters[].default`` and ``meta.editor_variables[].value``);
those read as a bundled config too, so an older file keeps working unchanged.
"""

from typing import Any

from .model.arguments import AdagioArguments


def _record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _configured(value: Any) -> bool:
    return value is not None and value != ""


def bundled_config(data: Any) -> dict[str, Any] | None:
    """The config a pipeline file carries, or ``None`` when it carries no values.

    ``data`` is the parsed file: the .adg envelope, or a bare spec.
    """
    if not isinstance(data, dict):
        return None
    configs = data.get("configs")
    if isinstance(configs, list):
        for entry in configs:
            if isinstance(entry, dict):
                return {
                    "inputs": _record(entry.get("inputs")),
                    "parameters": _record(entry.get("parameters")),
                    "outputs": _record(entry.get("outputs")),
                }
    return _legacy_config(data.get("spec", data))


def _legacy_config(spec: Any) -> dict[str, Any] | None:
    """Variable values an older spec stored in its signature or editor metadata."""
    if not isinstance(spec, dict):
        return None
    parameters: dict[str, Any] = {}
    signature = _record(spec.get("signature"))
    for parameter in signature.get("parameters") or []:
        if (
            isinstance(parameter, dict)
            and isinstance(parameter.get("name"), str)
            and _configured(parameter.get("default"))
        ):
            parameters[parameter["name"]] = parameter["default"]
    meta = _record(spec.get("meta"))
    for variable in meta.get("editor_variables") or []:
        if (
            isinstance(variable, dict)
            and isinstance(variable.get("name"), str)
            and variable.get("kind") != "Path or URL"
            and _configured(variable.get("value"))
            and variable["name"] not in parameters
        ):
            parameters[variable["name"]] = variable["value"]
    if not parameters:
        return None
    return {"inputs": {}, "parameters": parameters, "outputs": {}}


def apply_bundled_config(
    arguments: AdagioArguments,
    config: dict[str, Any] | None,
    *,
    input_names: set[str],
    parameter_names: set[str],
    output_names: set[str],
) -> None:
    """Lay the bundled config under ``arguments``, for the names the signature has.

    The author's config may hold values for variables nothing uses yet; those
    are not arguments of this pipeline and are skipped rather than refused,
    unlike unknown names in a user's own ``--arguments`` file.
    """
    if config is None:
        return
    for name, value in config["inputs"].items():
        if name in input_names and _configured(value):
            arguments.inputs[name] = value
    for name, value in config["parameters"].items():
        if name in parameter_names and _configured(value):
            arguments.parameters[name] = value
    outputs = {
        name: value
        for name, value in config["outputs"].items()
        if name in output_names and isinstance(value, str) and value
    }
    if outputs:
        arguments.outputs = outputs
