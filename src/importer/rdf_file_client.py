from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from rdflib import Graph


class RdfFileQueryClient:
    """Run SPARQL SELECT queries against an RDF file loaded into memory."""

    def __init__(self, rdf_file: str, rdf_format: Optional[str] = "turtle") -> None:
        resolved = os.path.abspath(rdf_file)
        if not os.path.isfile(resolved):
            raise FileNotFoundError(f"RDF file not found: {resolved}")

        self.rdf_file = resolved
        self.rdf_format = rdf_format or None
        self.graph = Graph()
        self.graph.parse(resolved, format=self.rdf_format)

    def query_rows(self, query: str, timeout: int = 60) -> List[Dict[str, Any]]:
        """
        Execute a local SPARQL SELECT query
        """
        del timeout

        rows: List[Dict[str, Any]] = []
        for result_row in self.graph.query(query):
            row: Dict[str, Any] = {}
            for var, term in result_row.asdict().items():
                if term is not None:
                    row[str(var)] = str(term)
            rows.append(row)
        return rows
