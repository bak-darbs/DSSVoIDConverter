from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def resolve_top_level_partition_counts(
    rows: Iterable[Dict[str, Any]],
    iri_key: str,
    partition_key: str,
    count_key: str,
    graph_key: str = "graphDesc",
) -> List[Tuple[str, Optional[int]]]:
    """
    Resolve top-level class/property counts from raw SPARQL rows.

    Sum partition counts for each partition, if one class/property appears in mulitple graph level partitions

    Returns the resolved (iri, count) tuples.
    """
    grouped: Dict[str, Dict[str, Any]] = {}

    for index, row in enumerate(rows):
        iri = row.get(iri_key)
        if not iri:
            continue

        if iri not in grouped:
            grouped[iri] = {
                "iri": iri,
                "partitions": {},
            }

        iri_group = grouped[iri]

        graph = row.get(graph_key)
        partition = row.get(partition_key) or f"__row_{index}"
        partition_identity = (graph, partition)

        if partition_identity not in iri_group["partitions"]:
            iri_group["partitions"][partition_identity] = {
                "partition": partition,
                "count_values": [],
                "_count_set": set(),
            }

        partition_group = iri_group["partitions"][partition_identity]

        count = _to_int(row.get(count_key))
        if count is not None and count not in partition_group["_count_set"]:
            partition_group["count_values"].append(count)
            partition_group["_count_set"].add(count)

    resolved: List[Tuple[str, Optional[int]]] = []

    for iri, iri_group in grouped.items():
        partition_totals = []

        for partition_group in iri_group["partitions"].values():
            count_values = list(partition_group["count_values"])
            resolved_partition_count = sum(count_values) if count_values else None
            if resolved_partition_count is not None:
                partition_totals.append(resolved_partition_count)

        resolved_count = sum(partition_totals) if partition_totals else None
        resolved.append((iri, resolved_count))

    return resolved
