import ast
import inspect
import textwrap

from django.core import checks

from .registry import register

# Modules that are generally non-deterministic and should not be used in workflows
NON_DETERMINISTIC_MODULES = {
    "random",
    "secrets",
    "uuid",
    "requests",
    "httpx",
    "urllib",
    "urllib3",
}

# Fully-qualified function calls that are obviously non-deterministic
NON_DETERMINISTIC_CALLS = {
    "time.time",
    "datetime.datetime.now",
    "datetime.datetime.utcnow",
}


def _full_name(node: ast.AST) -> str:
    """Return a dotted path for an AST node representing a name or attribute."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_full_name(node.value)}.{node.attr}"
    return ""


@checks.register()
def check_workflow_determinism(app_configs, **kwargs):
    errors: list[checks.CheckMessage] = []

    for name, fn in register.workflows.items():
        try:
            source = textwrap.dedent(inspect.getsource(fn))
        except (OSError, TypeError):
            # Can't retrieve source code; skip
            continue
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    if top in NON_DETERMINISTIC_MODULES:
                        errors.append(
                            checks.Warning(
                                f"Workflow '{name}' imports non-deterministic module '{alias.name}'",
                                id="django_durable.W001",
                            )
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    top = node.module.split(".")[0]
                    if top in NON_DETERMINISTIC_MODULES:
                        errors.append(
                            checks.Warning(
                                f"Workflow '{name}' imports non-deterministic module '{node.module}'",
                                id="django_durable.W001",
                            )
                        )
            elif isinstance(node, ast.Call):
                full = _full_name(node.func)
                top = full.split(".")[0]
                if full in NON_DETERMINISTIC_CALLS or top in NON_DETERMINISTIC_MODULES:
                    errors.append(
                        checks.Warning(
                            f"Workflow '{name}' calls non-deterministic function '{full}'",
                            id="django_durable.W001",
                        )
                    )

    return errors
