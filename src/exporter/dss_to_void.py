from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, XSD

from src.config import get_db_config, get_export_config, get_schema_config
from src.db.dss_connector import DSSPostgresConnector
from src.exporter.models import DssExportSnapshot, DssToVoidOptions

logger = logging.getLogger(__name__)

VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")

RDF_TYPE_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _is_absolute_iri(value: Optional[str]) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return bool(parsed.scheme)


def _stable_hash(*parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


def _add_optional_count(
    graph: Graph, subject: URIRef, predicate: URIRef, value: Optional[int]
) -> None:
    if value is not None:
        graph.add((subject, predicate, Literal(value, datatype=XSD.integer)))


def export_dss_schema_to_file(cfg: Dict[str, Any]) -> str:
    db_cfg = get_db_config(cfg)
    db_schema = get_schema_config(cfg)["db_schema"]
    export_cfg = get_export_config(cfg)
    options = DssToVoidOptions(**export_cfg)

    connector = DSSPostgresConnector(db_cfg)
    try:
        snapshot = connector.read_export_snapshot(db_schema)
    finally:
        connector.close()

    exporter = DssToVoidExporter(snapshot, options)
    return exporter.serialize(options.output_path, options.rdf_format)


class DssToVoidExporter:
    def __init__(self, snapshot: DssExportSnapshot, options: DssToVoidOptions):
        self.snapshot = snapshot
        self.options = options
        self.graph = Graph()
        self.graph.bind("void", VOID)
        self.graph.bind("void_ext", VOID_EXT)
        self.graph.bind("sd", SD)
        self.graph.bind("dcterms", DCTERMS)

        self._class_partition_by_iri: Dict[str, URIRef] = {}
        self._property_partition_by_iri: Dict[str, URIRef] = {}

        self.service_uri = self._resolve_service_uri()
        self.dataset_uri = self._resolve_dataset_uri()
        self.named_graph_uri = self._resolve_named_graph_uri()
        self.graph_uri = self._resolve_graph_uri()

    def _exported_properties(self) -> list:
        return [
            prop
            for prop in self.snapshot.properties
            if self._include_nested_property(prop.iri)
        ]

    def _resolve_service_uri(self) -> URIRef:
        value = self.snapshot.metadata.endpoint_url
        if not _is_absolute_iri(value):
            value = f"urn:void-export:{self.snapshot.metadata.db_schema}:service"
        return URIRef(value)

    def _resolve_dataset_uri(self) -> URIRef:
        service_str = str(self.service_uri)
        value = None if service_str.startswith("urn:") else f"{service_str}#dataset"
        if not _is_absolute_iri(value):
            value = f"urn:void-export:{self.snapshot.metadata.db_schema}:dataset"
        return URIRef(value)

    def _resolve_graph_uri(self) -> URIRef:
        dataset_str = str(self.dataset_uri)
        value = None if dataset_str.startswith("urn:") else f"{dataset_str}#graph"
        if not _is_absolute_iri(value):
            value = f"urn:void-export:{self.snapshot.metadata.db_schema}:graph"
        return URIRef(value)

    def _resolve_named_graph_uri(self) -> URIRef:
        dataset_str = str(self.dataset_uri)
        value = (
            None if dataset_str.startswith("urn:") else f"{dataset_str}#named-graph"
        )
        if not _is_absolute_iri(value):
            value = f"urn:void-export:{self.snapshot.metadata.db_schema}:named-graph"
        return URIRef(value)

    def _make_node(self, kind: str, *parts: str) -> URIRef:
        return URIRef(f"{self.graph_uri}!{kind}!{_stable_hash(*parts)}")

    def _include_nested_property(self, property_iri: str) -> bool:
        return property_iri != RDF_TYPE_IRI

    def build_graph(self) -> Graph:
        self._add_metadata()
        self._add_graph_summary()
        self._add_top_level_class_partitions()
        self._add_top_level_property_partitions()
        self._add_nested_property_partitions()
        self._add_linksets()
        return self.graph

    def _add_metadata(self) -> None:
        self.graph.add((self.service_uri, RDF.type, SD.Service))
        self.graph.add((self.service_uri, SD.defaultDataset, self.dataset_uri))
        if _is_absolute_iri(self.snapshot.metadata.endpoint_url):
            endpoint = URIRef(self.snapshot.metadata.endpoint_url)
            self.graph.add((self.service_uri, SD.endpoint, endpoint))
            self.graph.add((self.dataset_uri, VOID.sparqlEndpoint, endpoint))

        self.graph.add((self.dataset_uri, RDF.type, SD.Dataset))
        self.graph.add((self.dataset_uri, SD.defaultGraph, self.graph_uri))
        self.graph.add((self.dataset_uri, SD.namedGraph, self.named_graph_uri))

        if self.snapshot.metadata.display_name:
            self.graph.add(
                (
                    self.dataset_uri,
                    DCTERMS.title,
                    Literal(self.snapshot.metadata.display_name),
                )
            )
        if self.snapshot.metadata.description:
            self.graph.add(
                (
                    self.dataset_uri,
                    DCTERMS.description,
                    Literal(self.snapshot.metadata.description),
                )
            )

        self.graph.add((self.named_graph_uri, RDF.type, SD.NamedGraph))
        self.graph.add((self.named_graph_uri, SD.graph, self.graph_uri))
        if _is_absolute_iri(self.snapshot.metadata.named_graph):
            self.graph.add(
                (
                    self.named_graph_uri,
                    SD.name,
                    URIRef(self.snapshot.metadata.named_graph),
                )
            )

        self.graph.add((self.graph_uri, RDF.type, SD.Graph))

    def _add_graph_summary(self) -> None:
        classes = [cls for cls in self.snapshot.classes if not cls.is_literal]
        exported_properties = self._exported_properties()
        self.graph.add(
            (self.graph_uri, VOID.classes, Literal(len(classes), datatype=XSD.integer))
        )
        self.graph.add(
            (
                self.graph_uri,
                VOID.properties,
                Literal(len(exported_properties), datatype=XSD.integer),
            )
        )
        property_counts = [prop.cnt for prop in exported_properties]
        if property_counts and all(cnt is not None for cnt in property_counts):
            self.graph.add(
                (
                    self.graph_uri,
                    VOID.triples,
                    Literal(sum(property_counts), datatype=XSD.integer),
                )
            )

    def _add_top_level_class_partitions(self) -> None:
        for cls in self.snapshot.classes:
            if cls.is_literal:
                continue
            partition = self._make_node("class", "class-partition", cls.iri)
            self._class_partition_by_iri[cls.iri] = partition
            self.graph.add((self.graph_uri, VOID.classPartition, partition))
            self.graph.add((partition, RDF.type, VOID.Dataset))
            self.graph.add((partition, VOID["class"], URIRef(cls.iri)))
            _add_optional_count(self.graph, partition, VOID.entities, cls.cnt)

    def _add_top_level_property_partitions(self) -> None:
        pd_by_property: Dict[str, list] = {}
        for pd_rel in self.snapshot.pd_rels:
            if pd_rel.property_iri not in pd_by_property:
                pd_by_property[pd_rel.property_iri] = []
            pd_by_property[pd_rel.property_iri].append(pd_rel)

        for prop in self._exported_properties():
            partition = self._make_node("property", "property-partition", prop.iri)
            self._property_partition_by_iri[prop.iri] = partition
            self.graph.add((self.graph_uri, VOID.propertyPartition, partition))
            self.graph.add((partition, RDF.type, VOID.Dataset))
            self.graph.add((partition, VOID.property, URIRef(prop.iri)))
            _add_optional_count(self.graph, partition, VOID.triples, prop.cnt)

            for pd_rel in pd_by_property.get(prop.iri, []):
                datatype_partition = self._make_node(
                    "pd", "property-datatype", prop.iri, pd_rel.datatype_iri
                )
                self.graph.add(
                    (partition, VOID_EXT.datatypePartition, datatype_partition)
                )
                self.graph.add((datatype_partition, RDF.type, VOID.Dataset))
                self.graph.add(
                    (
                        datatype_partition,
                        VOID_EXT.datatype,
                        URIRef(pd_rel.datatype_iri),
                    )
                )
                _add_optional_count(
                    self.graph, datatype_partition, VOID.triples, pd_rel.cnt
                )

    def _add_nested_property_partitions(self) -> None:
        cpc_by_cp_rel: Dict[int, list] = {}
        for cpc_rel in self.snapshot.cpc_rels:
            if cpc_rel.cp_rel_id not in cpc_by_cp_rel:
                cpc_by_cp_rel[cpc_rel.cp_rel_id] = []
            cpc_by_cp_rel[cpc_rel.cp_rel_id].append(cpc_rel)

        cpd_by_cp_rel: Dict[int, list] = {}
        for cpd_rel in self.snapshot.cpd_rels:
            if cpd_rel.cp_rel_id not in cpd_by_cp_rel:
                cpd_by_cp_rel[cpd_rel.cp_rel_id] = []
            cpd_by_cp_rel[cpd_rel.cp_rel_id].append(cpd_rel)

        for cp_rel in self.snapshot.cp_rels:
            if not self._include_nested_property(cp_rel.property_iri):
                continue
            class_partition = self._class_partition_by_iri.get(cp_rel.class_iri)
            if class_partition is None:
                continue

            cp_partition = self._make_node(
                "cp", "cp-partition", cp_rel.class_iri, cp_rel.property_iri
            )
            self.graph.add((class_partition, VOID.propertyPartition, cp_partition))
            self.graph.add((cp_partition, RDF.type, VOID.Dataset))
            self.graph.add((cp_partition, VOID.property, URIRef(cp_rel.property_iri)))
            _add_optional_count(self.graph, cp_partition, VOID.triples, cp_rel.cnt)

            for cpc_rel in cpc_by_cp_rel.get(cp_rel.id, []):
                target_partition = self._make_node(
                    "cpc",
                    "cpc-partition",
                    cp_rel.class_iri,
                    cp_rel.property_iri,
                    cpc_rel.target_class_iri,
                )
                self.graph.add((cp_partition, VOID.classPartition, target_partition))
                self.graph.add((target_partition, RDF.type, VOID.Dataset))
                self.graph.add(
                    (target_partition, VOID["class"], URIRef(cpc_rel.target_class_iri))
                )
                _add_optional_count(
                    self.graph, target_partition, VOID.triples, cpc_rel.cnt
                )

            for cpd_rel in cpd_by_cp_rel.get(cp_rel.id, []):
                datatype_partition = self._make_node(
                    "cpd",
                    "cpd-partition",
                    cp_rel.class_iri,
                    cp_rel.property_iri,
                    cpd_rel.datatype_iri,
                )
                self.graph.add(
                    (cp_partition, VOID_EXT.datatypePartition, datatype_partition)
                )
                self.graph.add((datatype_partition, RDF.type, VOID.Dataset))
                self.graph.add(
                    (
                        datatype_partition,
                        VOID_EXT.datatype,
                        URIRef(cpd_rel.datatype_iri),
                    )
                )
                _add_optional_count(
                    self.graph, datatype_partition, VOID.triples, cpd_rel.cnt
                )

    def _add_linksets(self) -> None:
        cpc_by_cp_rel: Dict[int, list] = {}
        for cpc_rel in self.snapshot.cpc_rels:
            if cpc_rel.cp_rel_id not in cpc_by_cp_rel:
                cpc_by_cp_rel[cpc_rel.cp_rel_id] = []
            cpc_by_cp_rel[cpc_rel.cp_rel_id].append(cpc_rel)

        for cp_rel in self.snapshot.cp_rels:
            if not self._include_nested_property(cp_rel.property_iri):
                continue
            source_partition = self._class_partition_by_iri.get(cp_rel.class_iri)
            if source_partition is None:
                continue
            for cpc_rel in cpc_by_cp_rel.get(cp_rel.id, []):
                target_partition = self._class_partition_by_iri.get(
                    cpc_rel.target_class_iri
                )
                if target_partition is None:
                    continue
                linkset = self._make_node(
                    "linkset",
                    "linkset",
                    cp_rel.class_iri,
                    cp_rel.property_iri,
                    cpc_rel.target_class_iri,
                )
                self.graph.add((self.graph_uri, VOID.subset, linkset))
                self.graph.add((linkset, RDF.type, VOID.Linkset))
                self.graph.add((linkset, VOID.subjectsTarget, source_partition))
                self.graph.add(
                    (linkset, VOID.linkPredicate, URIRef(cp_rel.property_iri))
                )
                self.graph.add((linkset, VOID.objectsTarget, target_partition))
                _add_optional_count(self.graph, linkset, VOID.triples, cpc_rel.cnt)

    def serialize(self, output_path: str, rdf_format: str = "turtle") -> str:
        graph = self.build_graph()
        resolved = os.path.abspath(output_path)
        parent = os.path.dirname(resolved)
        if parent:
            os.makedirs(parent, exist_ok=True)
        graph.serialize(destination=resolved, format=rdf_format)
        logger.info(
            "Exported DSS schema '%s' to %s", self.snapshot.metadata.db_schema, resolved
        )
        return resolved
