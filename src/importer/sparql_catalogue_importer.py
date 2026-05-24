from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from src.config import get_source_config, get_schema_config
from src.db.dss_connector import DSSPostgresConnector
from src.importer.dss_import_finalizer import DSSImportFinalizer
from src.importer.sparql_catalogue_queries import (
    CATALOGUE_CLASS_ANNOTATIONS_SELECT_QUERY,
    CATALOGUE_CLASS_SELECT_QUERY,
    CATALOGUE_LINKSET_SELECT_QUERY,
    CATALOGUE_PROPERTY_ANNOTATIONS_SELECT_QUERY,
    CATALOGUE_PROPERTY_SELECT_QUERY,
    CATALOGUE_SERVICE_METADATA_SELECT_QUERY,
    for_service,
)
from src.importer.sparql_client import query_sparql_json
from src.utils.top_level_count_resolver import resolve_top_level_partition_counts


logger = logging.getLogger(__name__)


def _group_annotation_rows(
    rows: List[Dict[str, Any]],
    *,
    entity_key: str,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    seen = set()
    for row in rows:
        entity_iri = row.get(entity_key)
        annot_property = row.get("annotProp")
        annot_value = row.get("annotValue")
        if not entity_iri or not annot_property or annot_value is None:
            continue

        language = row.get("language") or ""
        dedup_key = (entity_iri, annot_property, language, annot_value)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        if entity_iri not in grouped:
            grouped[entity_iri] = {"fullName": entity_iri, "Labels": []}

        entity = grouped[entity_iri]
        entity["Labels"].append(
            {
                "property": annot_property,
                "value": annot_value,
                "language": language,
            }
        )

    return list(grouped.values())


@dataclass(frozen=True)
class CatalogueImportSelection:

    sparql_endpoint: str
    service_name: str
    db_schema: str


class SPARQLCatalogueImporter:

    def __init__(self, cfg: Dict[str, Any]) -> None:
        source = get_source_config(cfg)
        schema = get_schema_config(cfg)
        self.cfg = cfg
        self.selection = CatalogueImportSelection(
            sparql_endpoint=source["sparql_endpoint"],
            service_name=source["service_name"],
            db_schema=schema["db_schema"],
        )
        self.auto_create = schema.get("mode", "manual") == "auto"
        self.finalizer = DSSImportFinalizer(cfg, self.query_service_metadata)

    def query_classes(self, timeout: int = 60) -> List[Tuple[str, int | None]]:
        """Read service top-level classes from catalogue metadata."""
        query = for_service(
            CATALOGUE_CLASS_SELECT_QUERY, self.selection.service_name
            )
        rows = query_sparql_json(
            self.selection.sparql_endpoint, query, timeout=timeout
        )
        return resolve_top_level_partition_counts(
            rows,
            iri_key="class",
            partition_key="classPartition",
            count_key="entities",
            graph_key="service",
        )

    def query_properties(self, timeout: int = 60) -> List[Tuple[str, int | None]]:
        """Read service top-level properties from catalogue metadata."""
        query = for_service(
            CATALOGUE_PROPERTY_SELECT_QUERY, self.selection.service_name
        )
        rows = query_sparql_json(
            self.selection.sparql_endpoint, query, timeout=timeout
        )
        return resolve_top_level_partition_counts(
            rows,
            iri_key="property",
            partition_key="propertyPartition",
            count_key="triples",
            graph_key="service",
        )

    def query_linkset_cp_rels(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Read service top-level linkset relationships from catalogue metadata."""
        query = for_service(
            CATALOGUE_LINKSET_SELECT_QUERY, self.selection.service_name
            )
        rows = query_sparql_json(
            self.selection.sparql_endpoint, query, timeout=timeout
        )

        result: List[Dict[str, Any]] = []
        for row in rows:
            source = row.get("sourceClass")
            prop = row.get("property")
            target = row.get("targetClass")
            if not source or not prop or not target:
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

    def query_service_metadata(self, timeout: int = 60) -> Dict[str, Any]:
        """Read selected catalogue service metadata for parameters."""
        query = for_service(
            CATALOGUE_SERVICE_METADATA_SELECT_QUERY, self.selection.service_name
        )
        rows = query_sparql_json(
            self.selection.sparql_endpoint, query, timeout=timeout
        )
        if not rows:
            return {
                "service_iri": self.selection.service_name,
                "endpoint_url": None,
            }

        row = rows[0]
        return {
            "service_iri": row.get("service") or self.selection.service_name,
            "endpoint_url": row.get("endpoint"),
        }

    def query_class_annotations(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Read service-scoped class label/comment annotations from catalogue metadata."""
        query = for_service(
            CATALOGUE_CLASS_ANNOTATIONS_SELECT_QUERY, self.selection.service_name
        )
        rows = query_sparql_json(
            self.selection.sparql_endpoint, query, timeout=timeout
        )
        return _group_annotation_rows(rows, entity_key="class")

    def query_property_annotations(self, timeout: int = 60) -> List[Dict[str, Any]]:
        """Read service-scoped property label/comment annotations from catalogue metadata."""
        query = for_service(
            CATALOGUE_PROPERTY_ANNOTATIONS_SELECT_QUERY, self.selection.service_name
        )
        rows = query_sparql_json(
            self.selection.sparql_endpoint, query, timeout=timeout
        )
        return _group_annotation_rows(rows, entity_key="property")

    def import_classes(self) -> int:
        """save catalogue classes into DSS."""
        db_cfg = self.cfg["database"]
        connector = DSSPostgresConnector(db_cfg)
        try:
            class_tuples = self.query_classes()
            inserted = connector.import_classes_from_void(
                class_tuples,
                self.selection.db_schema,
                auto_create=self.auto_create,
            )
            logger.info(
                "Catalogue class import done - %d class(es) saved to schema '%s'.",
                inserted,
                self.selection.db_schema,
            )
            return inserted
        finally:
            connector.close()

    def import_properties(self) -> int:
        """Save catalogue properties into DSS."""
        db_cfg = self.cfg["database"]
        connector = DSSPostgresConnector(db_cfg)
        try:
            property_tuples = self.query_properties()
            inserted = connector.import_properties_from_void(
                property_tuples,
                self.selection.db_schema,
            )
            logger.info(
                "Catalogue property import done - %d property(ies) saved to schema '%s'.",
                inserted,
                self.selection.db_schema,
            )
            return inserted
        finally:
            connector.close()

    def import_relationships(self) -> Dict[str, int]:
        """Save catalogue cp_rels/cpc_rels from service linksets."""
        db_cfg = self.cfg["database"]
        connector = DSSPostgresConnector(db_cfg)
        try:
            linkset_rows = self.query_linkset_cp_rels()
            counts = connector.import_linkset_rels_from_void(
                linkset_rows,
                self.selection.db_schema,
            )
            logger.info(
                "Catalogue relationship import done - %d outgoing + %d incoming cp_rels, %d cpc_rels saved to schema '%s'.",
                counts["outgoing_cp"],
                counts["incoming_cp"],
                counts["cpc"],
                self.selection.db_schema,
            )
            return counts
        finally:
            connector.close()

    def import_class_annotations(self) -> int:
        """Save catalogue class annotations into DSS."""
        db_cfg = self.cfg["database"]
        connector = DSSPostgresConnector(db_cfg)
        try:
            rows = self.query_class_annotations()
            inserted = connector.import_class_annotations(
                rows, self.selection.db_schema
            )
            logger.info(
                "Catalogue class annotation import done - %d class annotation target(s) saved to schema '%s'.",
                inserted,
                self.selection.db_schema,
            )
            return inserted
        finally:
            connector.close()

    def import_property_annotations(self) -> int:
        """Save catalogue property annotations into DSS."""
        db_cfg = self.cfg["database"]
        connector = DSSPostgresConnector(db_cfg)
        try:
            rows = self.query_property_annotations()
            inserted = connector.import_property_annotations(
                rows, self.selection.db_schema
            )
            logger.info(
                "Catalogue property annotation import done - %d property annotation target(s) saved to schema '%s'.",
                inserted,
                self.selection.db_schema,
            )
            return inserted
        finally:
            connector.close()

    def run(self) -> None:
        """Run the catalogue import pipeline."""
        self.import_classes()
        self.import_properties()
        self.import_class_annotations()
        self.import_property_annotations()
        self.import_relationships()
        self.finalizer.save_classification_property()
        self.finalizer.finalize_rendering_fields()
        self.finalizer.force_catalogue_cp_rels_data_cnt()
        self.finalizer.run_post_processing()
        self.finalizer.persist_parameters()
        self.finalizer.register_schema()
