"""Engine adapters for xraygraph-bench."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.xraybench.adapters.base import BaseAdapter

# Lazy imports to avoid requiring all adapter dependencies
_ADAPTER_REGISTRY: dict[str, str] = {
    "memgraph": "tools.xraybench.adapters.memgraph.MemgraphAdapter",
    "neo4j": "tools.xraybench.adapters.neo4j.Neo4jAdapter",
    "xraygraphdb": "tools.xraybench.adapters.xraygraphdb.XrayGraphDBAdapter",
    "xraygraphdb-bolt": "tools.xraybench.adapters.xraygraphdb.XrayGraphDBAdapter",
    "xraygraphdb-native": "tools.xraybench.adapters.xraygraphdb_native.XrayGraphDBNativeAdapter",
}


def get_adapter(name: str) -> type[BaseAdapter]:
    """Resolve an adapter class by engine name.

    Args:
        name: Engine name (e.g., 'memgraph', 'neo4j', 'xraygraphdb').

    Returns:
        Adapter class (not instance).

    Raises:
        ValueError: If the adapter name is not registered.
    """
    if name not in _ADAPTER_REGISTRY:
        raise ValueError(
            f"Unknown adapter: {name}. "
            f"Available adapters: {', '.join(sorted(_ADAPTER_REGISTRY))}"
        )

    module_path, class_name = _ADAPTER_REGISTRY[name].rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def list_adapters() -> list[str]:
    """Return list of registered adapter names."""
    return sorted(_ADAPTER_REGISTRY.keys())
