from __future__ import annotations

from typing import Any, Dict, List

from SPARQLWrapper import JSON, SPARQLWrapper


def query_sparql_json(
    endpoint_url: str,
    query: str,
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """
    Execute a SPARQL SELECT query and return results as dictionaries.

    Each returned dict maps variable names to their string value.
    """
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setTimeout(timeout)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    raw = sparql.query().convert()
    bindings = raw.get("results", {}).get("bindings", [])

    rows: List[Dict[str, Any]] = []
    for binding in bindings:
        row: Dict[str, Any] = {}
        for var, info in binding.items():
            row[var] = info.get("value")
        rows.append(row)
    return rows
