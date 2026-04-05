"""EmergentEdgeCollector — collects emergent edge engine metrics from xrayGraphDB.

Queries xrayGraphDB's emergent edge procedures for cache stats, invalidation
stats, learning stats, and config. Gracefully handles engines that don't have
these procedures by returning empty dicts.
"""

from __future__ import annotations

import time
from typing import Any

from tools.xraybench.adapters.base import BaseAdapter

# ---------------------------------------------------------------------------
# Query constants
# ---------------------------------------------------------------------------

CACHE_STATS_QUERY = "CALL xray.emergent_cache_stats() YIELD * RETURN *"
INVALIDATION_STATS_QUERY = "CALL xray.emergent_invalidation_stats() YIELD * RETURN *"
LEARNING_STATS_QUERY = "CALL xray.emergent_learning_stats() YIELD * RETURN *"
CONFIG_QUERY = "CALL xray.emergent_config_show() YIELD * RETURN *"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _int(val: Any) -> int:
    """Safe int conversion — returns 0 on failure."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------


class EmergentEdgeCollector:
    """Collect emergent edge engine metrics from an xrayGraphDB adapter.

    All collect_* methods return an empty dict when the target procedure
    does not exist or otherwise raises an exception, so the collector is
    safe to use against engines that have not implemented the emergent edge
    subsystem.
    """

    def __init__(self, adapter: BaseAdapter) -> None:
        self._adapter = adapter

    # ------------------------------------------------------------------
    # Individual collectors
    # ------------------------------------------------------------------

    def collect_cache_stats(self) -> dict[str, Any]:
        """Execute CACHE_STATS_QUERY and return the first row as a dict.

        Computes hit_rate = total_hits / (total_hits + total_misses).
        Returns {} on any exception.
        """
        try:
            result = self._adapter.execute(CACHE_STATS_QUERY)
            if not result.rows:
                return {}
            row = dict(result.rows[0])
            total_hits = _int(row.get("total_hits", 0))
            total_misses = _int(row.get("total_misses", 0))
            denominator = total_hits + total_misses
            row["hit_rate"] = total_hits / denominator if denominator > 0 else 0.0
            return row
        except Exception:
            return {}

    def collect_invalidation_stats(self) -> dict[str, Any]:
        """Execute INVALIDATION_STATS_QUERY and return the first row as a dict.

        Returns {} on any exception.
        """
        try:
            result = self._adapter.execute(INVALIDATION_STATS_QUERY)
            if not result.rows:
                return {}
            return dict(result.rows[0])
        except Exception:
            return {}

    def collect_learning_stats(self) -> dict[str, Any]:
        """Execute LEARNING_STATS_QUERY and return the first row as a dict.

        Returns {} on any exception.
        """
        try:
            result = self._adapter.execute(LEARNING_STATS_QUERY)
            if not result.rows:
                return {}
            return dict(result.rows[0])
        except Exception:
            return {}

    def collect_config(self) -> dict[str, Any]:
        """Execute CONFIG_QUERY and parse rows into a {param: value} dict.

        Returns {} on any exception.
        """
        try:
            result = self._adapter.execute(CONFIG_QUERY)
            config: dict[str, Any] = {}
            for row in result.rows:
                param = row.get("param") or row.get("name") or row.get("key")
                value = row.get("value")
                if param is not None:
                    config[str(param)] = value
            return config
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Aggregate collectors
    # ------------------------------------------------------------------

    def collect_all(self) -> dict[str, Any]:
        """Collect all emergent metrics and return as a single snapshot dict.

        Returns a dict with keys: cache, invalidation, learning, config,
        and timestamp (epoch seconds float).
        """
        return {
            "cache": self.collect_cache_stats(),
            "invalidation": self.collect_invalidation_stats(),
            "learning": self.collect_learning_stats(),
            "config": self.collect_config(),
            "timestamp": time.time(),
        }

    def snapshot(self) -> dict[str, Any]:
        """Alias for collect_all()."""
        return self.collect_all()

    # ------------------------------------------------------------------
    # Delta computation
    # ------------------------------------------------------------------

    def compute_delta(
        self, before: dict[str, Any], after: dict[str, Any]
    ) -> dict[str, Any]:
        """Compute numeric deltas between two snapshots.

        For each section (cache, invalidation, learning), numeric fields
        produce a ``<field>_delta`` entry equal to after - before.
        Non-numeric fields include both the before and after values.

        Args:
            before: A snapshot returned by collect_all() / snapshot().
            after: A later snapshot returned by collect_all() / snapshot().

        Returns:
            Dict with per-section delta information.
        """
        delta: dict[str, Any] = {}
        for section in ("cache", "invalidation", "learning"):
            before_sec: dict[str, Any] = before.get(section, {})
            after_sec: dict[str, Any] = after.get(section, {})
            section_delta: dict[str, Any] = {}

            all_keys = set(before_sec) | set(after_sec)
            for key in all_keys:
                b_val = before_sec.get(key)
                a_val = after_sec.get(key)
                try:
                    b_num = float(b_val)  # type: ignore[arg-type]
                    a_num = float(a_val)  # type: ignore[arg-type]
                    section_delta[f"{key}_delta"] = a_num - b_num
                except (TypeError, ValueError):
                    section_delta[f"{key}_before"] = b_val
                    section_delta[f"{key}_after"] = a_val

            delta[section] = section_delta

        return delta
