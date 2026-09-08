"""WorkflowContextResolver: template -> real params (V1.3 §37-§40).

Supported syntax ONLY (§38):
    {{ variables.date }}
    {{ steps.download_orders.result.file_path }}

No eval/exec/Jinja/Python (§39): resolution is explicit dotted-path walking
over the JSON context. A whole-string template preserves the referenced
value's JSON type (numbers stay numbers); a partial substitution stringifies.

Unknown path => WorkflowParamResolutionFailed - the workflow never guesses (§81).
Referencing a step whose entry is not SUCCESS also fails (future-step refs).
"""

import re

from app.workflow.errors import WorkflowParamResolutionFailed

_TEMPLATE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\}\}")


def _lookup(path: str, context: dict):
    node: object = context
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            raise WorkflowParamResolutionFailed(f"{{{{ {path} }}}}", f"path segment '{part}' not found")
    return node


def _lookup_step_ref(path: str, context: dict, valid_steps: set[str]):
    parts = path.split(".")
    # steps.<name>.<field...>
    if len(parts) < 3 or parts[0] != "steps":
        raise WorkflowParamResolutionFailed(f"{{{{ {path} }}}}", "step refs must be steps.<name>.<field>")
    step_name = parts[1]
    if step_name not in valid_steps:
        raise WorkflowParamResolutionFailed(f"{{{{ {path} }}}}", f"unknown step '{step_name}'")
    entry = context.get("steps", {}).get(step_name)
    if entry is None:
        raise WorkflowParamResolutionFailed(
            f"{{{{ {path} }}}}", f"step '{step_name}' has not completed yet"
        )
    if entry.get("status") != "SUCCESS":
        raise WorkflowParamResolutionFailed(
            f"{{{{ {path} }}}}", f"step '{step_name}' is {entry.get('status')}, not SUCCESS"
        )
    node: object = entry
    for part in parts[2:]:
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            raise WorkflowParamResolutionFailed(
                f"{{{{ {path} }}}}", f"path segment '{part}' not found in step '{step_name}'"
            )
    return node


def resolve_template(value: str, context: dict, valid_steps: set[str]):
    """Resolve one string. Whole-string template keeps the JSON type."""
    match = _TEMPLATE.fullmatch(value.strip())
    if match:
        path = match.group(1)
        if path.startswith("variables."):
            return _lookup(path, context)
        return _lookup_step_ref(path, context, valid_steps)

    def _sub(m: re.Match) -> str:
        path = m.group(1)
        if path.startswith("variables."):
            resolved = _lookup(path, context)
        else:
            resolved = _lookup_step_ref(path, context, valid_steps)
        if isinstance(resolved, str):
            return resolved
        if isinstance(resolved, (dict, list)):
            raise WorkflowParamResolutionFailed(
                f"{{{{ {path} }}}}", "object/array refs are only allowed as the whole value"
            )
        import json

        return json.dumps(resolved) if isinstance(resolved, bool) else str(resolved)

    return _TEMPLATE.sub(_sub, value)


def resolve_params(params: dict, context: dict, valid_steps: set[str]) -> dict:
    """Deep-resolve a step's params dict (lists/dicts walked recursively)."""

    def _walk(node):
        if isinstance(node, dict):
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_walk(item) for item in node]
        if isinstance(node, str):
            return resolve_template(node, context, valid_steps)
        return node

    return _walk(params)
