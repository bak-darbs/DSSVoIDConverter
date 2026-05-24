from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ExportMetadata:
    db_schema: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    endpoint_url: Optional[str] = None
    named_graph: Optional[str] = None


@dataclass(frozen=True)
class ExportClass:
    id: int
    iri: str
    cnt: Optional[int]
    is_literal: bool


@dataclass(frozen=True)
class ExportProperty:
    id: int
    iri: str
    cnt: Optional[int]


@dataclass(frozen=True)
class ExportCpRel:
    id: int
    class_id: int
    class_iri: str
    property_id: int
    property_iri: str
    cnt: Optional[int]


@dataclass(frozen=True)
class ExportCpcRel:
    cp_rel_id: int
    target_class_id: int
    target_class_iri: str
    cnt: Optional[int]


@dataclass(frozen=True)
class ExportPdRel:
    property_id: int
    property_iri: str
    datatype_iri: str
    cnt: Optional[int]


@dataclass(frozen=True)
class ExportCpdRel:
    cp_rel_id: int
    datatype_iri: str
    cnt: Optional[int]


@dataclass(frozen=True)
class DssExportSnapshot:
    metadata: ExportMetadata
    classes: list[ExportClass] = field(default_factory=list)
    properties: list[ExportProperty] = field(default_factory=list)
    cp_rels: list[ExportCpRel] = field(default_factory=list)
    cpc_rels: list[ExportCpcRel] = field(default_factory=list)
    pd_rels: list[ExportPdRel] = field(default_factory=list)
    cpd_rels: list[ExportCpdRel] = field(default_factory=list)


@dataclass(frozen=True)
class DssToVoidOptions:
    output_path: str
    rdf_format: str = "turtle"
