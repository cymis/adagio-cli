import re
from enum import StrEnum


class ParamType(StrEnum):
    INPUT = "input"
    PARAM = "param"


def promote_positional_pipeline(argv: list[str]) -> tuple[list[str], str | None]:
    """Allow `adagio run <pipeline>` by rewriting it to `--pipeline <pipeline>`."""
    if len(argv) < 2 or argv[0] != "run":
        return argv, None

    candidate = argv[1]
    if candidate.startswith("-"):
        return argv, None

    rewritten = ["run", "--pipeline", candidate, *argv[2:]]
    return rewritten, candidate


def extract_flag_value(argv: list[str], *flags: str) -> str | None:
    """Supports: --flag value, -f value, --flag=value."""
    flag_set = set(flags)
    for i, tok in enumerate(argv):
        if tok in flag_set:
            return argv[i + 1] if i + 1 < len(argv) else None
        for flag in flags:
            if tok.startswith(flag + "="):
                return tok.split("=", 1)[1]
    return None


def to_identifier(name: str, prefix: str | None = None) -> str:
    """Turn arbitrary names into valid Python identifiers for kwargs keys."""
    clean = (name or "").strip()
    clean = re.sub(r"[^0-9a-zA-Z_]+", "_", clean)
    if not clean:
        raise ValueError("Empty parameter name in pipeline file.")
    if clean[0].isdigit():
        clean = "_" + clean
    if prefix:
        return f"{prefix}_{clean}"
    return clean


def dynamic_opt(name: str, param_type: ParamType) -> str:
    return f"--{param_type}-{name.replace('_', '-')}"
