from __future__ import annotations

import logging
from typing import Any, Callable, Dict

from src.config import (
    build_env,
    get_classification_property,
    get_db_config,
    get_schema_config,
    merge_source_metadata_into_env,
)
from src.db.dss_connector import DSSPostgresConnector


logger = logging.getLogger(__name__)
SourceMetadataResolver = Callable[[], Dict[str, Any]]


class DSSImportFinalizer:
    """Shared DSS finishing stages for source-specific importers."""

    def __init__(
        self,
        cfg: Dict[str, Any],
        source_metadata_resolver: SourceMetadataResolver,
    ) -> None:
        self.cfg = cfg
        self._source_metadata_resolver = source_metadata_resolver

    @property
    def db_schema(self) -> str:
        return get_schema_config(self.cfg)["db_schema"]

    @property
    def db_config(self) -> Dict[str, Any]:
        return get_db_config(self.cfg)

    def save_classification_property(self) -> None:
        """Persist configured class classification metadata into DSS classes."""
        classification_property = get_classification_property(self.cfg)

        if not classification_property:
            logger.info(
                "Skipping classification property persistence for schema '%s' because no classification_property is configured.",
                self.db_schema,
            )
            return

        connector = DSSPostgresConnector(self.db_config)
        try:
            updated = connector.apply_classification_property(
                self.db_schema, classification_property
            )
            logger.info(
                "Done - classification property '%s' saved to %d class(es) in schema '%s'.",
                classification_property,
                updated,
                self.db_schema,
            )
        finally:
            connector.close()

    def finalize_rendering_fields(self) -> None:
        """Fill derived DSS fields needed by ViziQuer rendering."""
        classification_property = get_classification_property(self.cfg)
        connector = DSSPostgresConnector(self.db_config)
        try:
            counts = connector.finalize_void_rendering_fields(
                self.db_schema, classification_property
            )
            logger.info(
                "Rendering finalization done - %d cp object counts, %d property object counts, %d cp cover sets, %d cpc cover sets updated in '%s'.",
                counts["cp_object_cnt_updated"],
                counts["property_object_cnt_updated"],
                counts["cp_cover_set_updated"],
                counts["cpc_cover_set_updated"],
                self.db_schema,
            )
        finally:
            connector.close()

    def force_catalogue_cp_rels_data_cnt(self) -> None:
        """Catalogue-only compatibility override for Add Attribute property listing."""
        connector = DSSPostgresConnector(self.db_config)
        try:
            updated = connector.force_cp_rels_data_cnt_to_cnt(self.db_schema)
            logger.info(
                "Catalogue cp_rels data_cnt override done - %d row(s) updated in '%s'.",
                updated,
                self.db_schema,
            )
        finally:
            connector.close()

    def run_post_processing(self) -> None:
        """Run post-processing SQL updates on derived DSS columns."""
        connector = DSSPostgresConnector(self.db_config)
        try:
            counts = connector.run_post_processing(self.db_schema)
            logger.info(
                "Post-processing done - %d property rows, %d class rows updated in '%s'.",
                counts["values_have_cp_updated"],
                counts["self_cp_rels_updated"],
                self.db_schema,
            )
        finally:
            connector.close()

    def persist_parameters(self) -> None:
        """Populate the parameters table with endpoint/schema metadata."""
        env = build_env(self.cfg)
        source_metadata = self._source_metadata_resolver()
        env = merge_source_metadata_into_env(env, source_metadata)

        connector = DSSPostgresConnector(self.db_config)
        try:
            source_type = env.get("source_type", "void-generator")
            import_source = (
                "sparql-catalogue" if source_type == "sparql-catalogue" else "void"
            )
            relationship_source_mode = env.get("relationship_source_mode", "both")
            if source_type == "sparql-catalogue":
                relationship_source_mode = "linksets"

            void_params = {
                "importSource": import_source,
                "sparqlEndpoint": env.get("sparql_url", ""),
                "relationshipSourceMode": relationship_source_mode,
            }
            if source_type == "sparql-catalogue":
                void_params["catalogueServiceName"] = env.get("service_name", "")
            if source_metadata.get("service_iri"):
                void_params["serviceIri"] = source_metadata["service_iri"]

            connector.add_parameters(self.db_schema, void_params, env)
            logger.info("Parameters written to schema '%s'.", self.db_schema)
        finally:
            connector.close()

    def register_schema(self) -> None:
        """Register the schema in public.endpoints / public.schemata."""
        env = build_env(self.cfg)
        source_metadata = self._source_metadata_resolver()
        env = merge_source_metadata_into_env(env, source_metadata)

        connector = DSSPostgresConnector(self.db_config)
        try:
            connector.register_schema(env)
        finally:
            connector.close()
