import ast
from collections.abc import Iterator
from importlib.util import resolve_name
from pathlib import Path
from sys import stdlib_module_names

SOURCE = Path(__file__).resolve().parents[1] / "src"


def imports(path: Path) -> Iterator[str]:
    package = ".".join(path.parent.relative_to(SOURCE).parts)
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = resolve_name("." * node.level + (node.module or ""), package)
            if node.module is None:
                yield from (f"{module}.{alias.name}" for alias in node.names)
            else:
                yield module


def test_inner_layers_have_no_infrastructure_imports() -> None:
    for layer in ("domain", "application"):
        allowed = {"importlens.domain"}
        if layer == "application":
            allowed.add("importlens.application")
        for path in (SOURCE / "importlens" / layer).rglob("*.py"):
            for module in imports(path):
                permitted = module.split(".")[0] in stdlib_module_names or any(
                    module == prefix or module.startswith(f"{prefix}.") for prefix in allowed
                )
                assert permitted, f"{path.relative_to(SOURCE)} imports {module}"
