from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.config import (
    get_classification_property,
    get_db_config,
    get_relationship_source_mode,
    get_schema_config,
    get_source_config,
)
from src.db.dss_connector import DSSPostgresConnector
from src.importer.dss_import_finalizer import DSSImportFinalizer
from src.importer.sparql_client import query_sparql_json
from src.utils.top_level_count_resolver import resolve_top_level_partition_counts
from src.importer.void_generator_queries import (
    CLASS_SELECT_QUERY,
    CPD_RELS_DATATYPE_QUERY,
    LINKSETS_SELECT_QUERY,
    PARTITION_CP_RELS_SELECT_QUERY,
    PD_RELS_DATATYPE_QUERY,
    PROPERTY_SELECT_QUERY,
    SERVICE_METADATA_SELECT_QUERY,
    build_unresolved_ot_fallback_query,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VoidGeneratorImportSelection:
    sparql_endpoint: str
    db_schema: str


class VoidGeneratorImporter:
    """Full import pipeline for direct void-generator input."""

    def __init__(self, cfg: Dict[str, Any]) -> None:
        source = get_source_config(cfg)
        schema = get_schema_config(cfg)
        self.cfg = cfg
        self.selection = VoidGeneratorImportSelection(
            sparql_endpoint=source["sparql_endpoint"],
            db_schema=schema["db_schema"],
        )
        self.auto_create = schema.get("mode", "manual") == "auto"
        self.relationship_source_mode = get_relationship_source_mode(cfg)
        self.finalizer = DSSImportFinalizer(cfg, self.query_service_metadata)

    @property
    def db_config(self) -> Dict[str, Any]:
        return get_db_config(self.cfg)

    def query_service_metadata(self, timeout: int = 60) -> Dict[str, Optional[str]]:
        """Read the direct VoID service IRI and declared endpoint URL."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            SERVICE_METADATA_SELECT_QUERY,
            timeout=timeout,
        )
        if not rows:
            return {
                "service_iri": None,
                "endpoint_url": None,
            }

        row = rows[0]
        return {
            "service_iri": row.get("service"),
            "endpoint_url": row.get("endpoint") or row.get("service"),
        }

    def query_classes(self, timeout: int = 60) -> List[Tuple[str, int | None]]:
        """Read top-level classes from sd:Graph-anchored VoID metadata."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            CLASS_SELECT_QUERY,
            timeout=timeout,
        )
        return resolve_top_level_partition_counts(
            rows,
            iri_key="class",
            partition_key="classPartition",
            count_key="entities",
        )

    def query_properties(self, timeout: int = 60) -> List[Tuple[str, int | None]]:
        """Read top-level properties from sd:Graph-anchored VoID metadata."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            PROPERTY_SELECT_QUERY,
            timeout=timeout,
        )
        return resolve_top_level_partition_counts(
            rows,
            iri_key="prop",
            partition_key="pp",
            count_key="triples",
        )

    def query_partition_cp_rels(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Read nested class-property-class data from VoID partitions."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            PARTITION_CP_RELS_SELECT_QUERY,
            timeout=timeout,
        )

        grouped: Dict[tuple, Dict[str, Any]] = {}
        for row in rows:
            source = row.get("sourceClass")
            prop = row.get("property")
            if not source or not prop:
                continue

            key = (source, prop)
            if key not in grouped:
                cp_triples_raw = row.get("cpTriples")
                cp_triples = (
                    int(cp_triples_raw) if cp_triples_raw is not None else None
                )
                grouped[key] = {
                    "source_class": source,
                    "property": prop,
                    "triples": cp_triples,
                    "targets": [],
                }

            target_class = row.get("targetClass")
            if target_class:
                target_triples_raw = row.get("targetTriples")
                target_triples = (
                    int(target_triples_raw)
                    if target_triples_raw is not None
                    else None
                )
                existing_target = None
                for target in grouped[key]["targets"]:
                    if target["class"] == target_class:
                        existing_target = target
                        break

                if existing_target is None:
                    grouped[key]["targets"].append(
                        {
                            "class": target_class,
                            "triples": target_triples,
                        }
                    )
                elif target_triples is not None:
                    if existing_target["triples"] is None:
                        existing_target["triples"] = target_triples
                    else:
                        existing_target["triples"] += target_triples

        return list(grouped.values())

    def _resolve_unresolved_ot(
        self,
        ot_uri: str,
        timeout: int,
    ) -> Optional[str]:
        """
        Resolve a void:objectsTarget URI that lacks direct void:class by
        querying the corresponding vocabulary partition.
        """
        if ">" in ot_uri or "<" in ot_uri or " " in ot_uri:
            return None
        vocab_partition_uri = ot_uri.replace("#!", "#vocabulary!", 1)
        query = build_unresolved_ot_fallback_query(vocab_partition_uri)
        rows = query_sparql_json(self.selection.sparql_endpoint, query, timeout=timeout)
        return rows[0].get("fallbackClass") if rows else None

    def query_linkset_cp_rels(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Read void:Linkset relationships from direct VoID metadata."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            LINKSETS_SELECT_QUERY,
            timeout=timeout,
        )

        fallback_class: Dict[str, Optional[str]] = {}
        unresolved_ots = {
            row["ot"]
            for row in rows
            if row.get("ot") and "#!" in row["ot"] and not row.get("targetClass")
        }
        for ot_uri in unresolved_ots:
            fallback_class[ot_uri] = self._resolve_unresolved_ot(ot_uri, timeout)

        result: List[Dict[str, Any]] = []
        for row in rows:
            source = row.get("sourceClass")
            prop = row.get("property")
            target = row.get("targetClass")
            ot = row.get("ot")

            if target is None and ot in fallback_class:
                target = fallback_class[ot]

            if not prop:
                continue

            triples_raw = row.get("triples")
            triples = int(triples_raw) if triples_raw is not None else None
            result.append(
                {
                    "source_class": source,
                    "property": prop,
                    "target_class": target,
                    "triples": triples,
                }
            )
        return result

    def query_pd_datatypes(self, timeout: int = 60) -> List[tuple]:
        """Read property-level datatype partitions."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            PD_RELS_DATATYPE_QUERY,
            timeout=timeout,
        )

        grouped: Dict[tuple, Optional[int]] = {}
        for row in rows:
            prop = row.get("prop")
            datatype = row.get("datatype")
            if not prop or not datatype:
                continue
            key = (prop, datatype)
            triples_raw = row.get("ppTriples")
            triples = int(triples_raw) if triples_raw is not None else None
            if key not in grouped or (grouped[key] is None and triples is not None):
                grouped[key] = triples

        return [(prop, datatype, triples) for (prop, datatype), triples in grouped.items()]

    def query_cpd_datatypes(self, timeout: int = 60) -> List[tuple]:
        """Read class-property-level datatype partitions."""
        rows = query_sparql_json(
            self.selection.sparql_endpoint,
            CPD_RELS_DATATYPE_QUERY,
            timeout=timeout,
        )

        grouped: Dict[tuple, Dict[str, Any]] = {}
        for row in rows:
            cls = row.get("class")
            prop = row.get("prop")
            datatype = row.get("datatype")
            if not cls or not prop or not datatype:
                continue
            key = (cls, prop, datatype)
            triples_raw = row.get("ppTriples")
            triples = int(triples_raw) if triples_raw is not None else None
            has_object_target = row.get("hasObjectTarget") == "true"
            object_sum_raw = row.get("objectTargetTriplesSum")
            object_target_count_sum = (
                int(object_sum_raw) if object_sum_raw is not None else None
            )
            has_missing_object_target_count = (
                row.get("hasMissingObjectTargetCount") == "true"
            )
            existing = grouped.get(key)
            if existing is None:
                grouped[key] = {
                    "triples": triples,
                    "has_object_target": has_object_target,
                    "object_target_count_sum": object_target_count_sum,
                    "has_missing_object_target_count": has_missing_object_target_count,
                }
                continue
            if existing["triples"] is None and triples is not None:
                existing["triples"] = triples
            existing["has_object_target"] = (
                existing["has_object_target"] or has_object_target
            )
            if (
                existing["object_target_count_sum"] is None
                and object_target_count_sum is not None
            ):
                existing["object_target_count_sum"] = object_target_count_sum
            existing["has_missing_object_target_count"] = (
                existing["has_missing_object_target_count"]
                or has_missing_object_target_count
            )

        return [
            (
                cls,
                prop,
                datatype,
                info["triples"],
                info["has_object_target"],
                info["object_target_count_sum"],
                info["has_missing_object_target_count"],
            )
            for (cls, prop, datatype), info in grouped.items()
        ]

    def query_datatypes(self, timeout: int = 60) -> tuple[List[tuple], List[tuple]]:
        """Read property-level and class-property-level datatype evidence."""
        return self.query_pd_datatypes(timeout), self.query_cpd_datatypes(timeout)

    def import_classes(self) -> int:
        connector = DSSPostgresConnector(self.db_config)
        try:
            logger.info(
                "Querying classes from SPARQL endpoint: %s",
                self.selection.sparql_endpoint,
            )
            class_tuples = self.query_classes()
            inserted = connector.import_classes_from_void(
                class_tuples,
                self.selection.db_schema,
                auto_create=self.auto_create,
            )
            logger.info(
                "Done - %d class(es) saved to schema '%s'.",
                inserted,
                self.selection.db_schema,
            )
            return inserted
        finally:
            connector.close()

    def import_properties(self) -> int:
        connector = DSSPostgresConnector(self.db_config)
        try:
            logger.info(
                "Querying properties from SPARQL endpoint: %s",
                self.selection.sparql_endpoint,
            )
            property_tuples = self.query_properties()
            inserted = connector.import_properties_from_void(
                property_tuples,
                self.selection.db_schema,
            )
            logger.info(
                "Done - %d property(ies) saved to schema '%s'.",
                inserted,
                self.selection.db_schema,
            )
            return inserted
        finally:
            connector.close()

    def import_partition_cp_rels(self) -> Dict[str, int]:
        connector = DSSPostgresConnector(self.db_config)
        try:
            logger.info(
                "Querying cp_rels from SPARQL endpoint: %s",
                self.selection.sparql_endpoint,
            )
            cp_data = self.query_partition_cp_rels()

            counts = connector.import_rels_from_void(
                cp_data,
                self.selection.db_schema,
            )
            logger.info(
                "Done - %d outgoing + %d incoming cp_rel(s), %d cpc_rel(s) saved to schema '%s'.",
                counts["outgoing_cp"],
                counts["incoming_cp"],
                counts["cpc"],
                self.selection.db_schema,
            )
            return counts
        finally:
            connector.close()

    def import_linkset_cp_rels(self) -> Dict[str, int]:
        connector = DSSPostgresConnector(self.db_config)
        try:
            logger.info(
                "Querying linkset cp_rels from SPARQL endpoint: %s",
                self.selection.sparql_endpoint,
            )
            linkset_data = self.query_linkset_cp_rels()

            counts = connector.import_linkset_rels_from_void(
                linkset_data,
                self.selection.db_schema,
            )
            logger.info(
                "Done - %d outgoing + %d incoming cp_rel(s), %d cpc_rel(s) saved to schema '%s' (linksets).",
                counts["outgoing_cp"],
                counts["incoming_cp"],
                counts["cpc"],
                self.selection.db_schema,
            )
            return counts
        finally:
            connector.close()

    def import_datatypes(self) -> Dict[str, int]:
        connector = DSSPostgresConnector(self.db_config)
        try:
            logger.info(
                "Querying datatype partitions from SPARQL endpoint: %s",
                self.selection.sparql_endpoint,
            )
            pd_data, cpd_data = self.query_datatypes()
            counts = connector.import_datatypes_from_void(
                pd_data,
                cpd_data,
                self.selection.db_schema,
            )
            logger.info(
                "Done - %d datatype(s), %d pd_rel(s), %d cpd_rel(s) saved to schema '%s'.",
                counts["datatypes"],
                counts["pd_rels"],
                counts["cpd_rels"],
                self.selection.db_schema,
            )
            return counts
        finally:
            connector.close()

    def import_classification_property_cp_rels(self) -> Dict[str, int]:
        classification_property = get_classification_property(self.cfg)
        connector = DSSPostgresConnector(self.db_config)
        try:
            counts = connector.import_classification_property_cp_rels(
                self.selection.db_schema,
                classification_property,
            )
            logger.info(
                "Done - %d classification-property outgoing cp_rel(s), %d incoming cp_rel(s), %d cpc_rel(s) saved to schema '%s'.",
                counts["outgoing_cp"],
                counts["incoming_cp"],
                counts["cpc"],
                self.selection.db_schema,
            )
            return counts
        finally:
            connector.close()

    def run(self) -> None:
        """Run the void-generator import pipeline."""
        self.import_classes()
        self.import_properties()

        if self.relationship_source_mode in ("partitions", "both"):
            self.import_partition_cp_rels()

        if self.relationship_source_mode in ("linksets", "both"):
            self.import_linkset_cp_rels()

        self.finalizer.save_classification_property()
        self.import_classification_property_cp_rels()

        if self.relationship_source_mode == "linksets":
            logger.info(
                "Skipping datatype import because relationship_source_mode='linksets' "
                "datatype evidence is only available in partitions."
            )
        else:
            self.import_datatypes()

        self.finalizer.finalize_rendering_fields()
        self.finalizer.run_post_processing()
        self.finalizer.persist_parameters()
        self.finalizer.register_schema()
