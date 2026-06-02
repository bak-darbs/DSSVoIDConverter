import os
from typing import Dict, Any, Optional

import yaml


_RELATIONSHIP_SOURCE_MODES = {"partitions", "linksets", "both"}
_SOURCE_TYPES = {"void-generator", "sparql-catalogue"}
_SOURCE_INPUTS = {"endpoint", "file"}
_OPERATIONS = {"import", "export"}


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load and return the YAML configuration as a dict."""
    resolved = os.path.abspath(config_path)
    if not os.path.isfile(resolved):
        raise FileNotFoundError(f"Configuration file not found: {resolved}")

    with open(resolved, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if cfg is None:
        raise ValueError("Configuration file is empty.")

    return cfg


def get_db_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and return the database connection sub-config."""
    db = cfg.get("database")
    if db is None:
        raise KeyError("Missing 'database' section in config.")
    return db


def get_operation(cfg: Dict[str, Any]) -> str:
    """Return and validate the configured top-level operation."""
    operation = cfg.get("operation", "import")
    if operation not in _OPERATIONS:
        allowed = ", ".join(sorted(_OPERATIONS))
        raise ValueError(
            f"Invalid operation: {operation!r}. Expected one of: {allowed}."
        )
    return operation


def get_source_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return the import source configuration."""
    source_input = get_source_input(cfg)
    sparql_endpoint = cfg.get("sparql_endpoint", "")
    rdf_file = cfg.get("rdf_file", "")

    if source_input == "endpoint" and not sparql_endpoint:
        raise KeyError("Missing 'sparql_endpoint' in config.")
    if source_input == "file" and not rdf_file:
        raise KeyError("Missing 'rdf_file' in config when source_input is 'file'.")

    source_type = get_source_type(cfg)
    return {
        "source_input": source_input,
        "sparql_endpoint": sparql_endpoint,
        "rdf_file": rdf_file,
        "rdf_format": cfg.get("rdf_format", "turtle"),
        "source_type": source_type,
        "service_name": get_catalogue_service_name(cfg),
    }


def get_schema_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return the schema-level configuration (db_schema, display_name, mode)."""
    db_schema = cfg.get("db_schema")
    if not db_schema:
        raise KeyError("Missing 'db_schema' in config (target PostgreSQL schema name).")
    classification_property = get_classification_property(cfg)
    return {
        "db_schema": db_schema,
        "display_name": cfg.get("display_name", db_schema),
        "description": cfg.get("description", ""),
        "mode": cfg.get("mode", "manual"),
        "classification_property": classification_property,
    }


def get_classification_property(cfg: Dict[str, Any]) -> str:
    """Return the configured classification property IRI, if any."""
    value = cfg.get("classification_property", "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(
            "Invalid classification_property: expected a string IRI or empty value."
        )
    return value.strip()


def get_relationship_source_mode(cfg: Dict[str, Any]) -> str:
    """Return and validate the cp_rel/cpc_rel evidence-source mode."""
    mode = cfg.get("relationship_source_mode", "both")
    if mode not in _RELATIONSHIP_SOURCE_MODES:
        allowed = ", ".join(sorted(_RELATIONSHIP_SOURCE_MODES))
        raise ValueError(
            f"Invalid relationship_source_mode: {mode!r}. Expected one of: {allowed}."
        )
    return mode


def get_source_type(cfg: Dict[str, Any]) -> str:
    """Return and validate the configured input source type."""
    source_type = cfg.get("source_type", "void-generator")
    if source_type not in _SOURCE_TYPES:
        allowed = ", ".join(sorted(_SOURCE_TYPES))
        raise ValueError(
            f"Invalid source_type: {source_type!r}. Expected one of: {allowed}."
        )
    return source_type


def get_source_input(cfg: Dict[str, Any]) -> str:
    """Return and validate the configured input transport."""
    source_input = cfg.get("source_input", "sparql")
    if source_input not in _SOURCE_INPUTS:
        allowed = ", ".join(sorted(_SOURCE_INPUTS))
        raise ValueError(
            f"Invalid source_input: {source_input!r}. Expected one of: {allowed}."
        )
    return source_input


def get_catalogue_service_name(cfg: Dict[str, Any]) -> str:
    """Return the selected catalogue service name when catalogue mode is active."""
    if get_source_type(cfg) != "sparql-catalogue":
        return ""

    service_name = cfg.get("service_name")
    if not isinstance(service_name, str) or not service_name.strip():
        raise KeyError(
            "Missing 'service_name' in config when source_type is 'sparql-catalogue'."
        )
    return service_name.strip()


def get_endpoint_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return endpoint metadata for DSS registry / parameters."""
    ep = cfg.get("endpoint", {})
    return {
        "sparql_url": ep.get("sparql_url", ""),
        "named_graph": ep.get("named_graph", ""),
        "public_url": ep.get("public_url", ""),
        "endpoint_type": ep.get("endpoint_type", "generic"),
        "schema_kind": ep.get("schema_kind", "default"),
    }


def get_export_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return validated export configuration for DSS-to-VoID export."""
    export = cfg.get("export") or {}
    output_path = export.get("output_path")
    if not output_path:
        raise KeyError(
            "Missing 'export.output_path' in config for operation 'export'."
        )

    return {
        "output_path": output_path,
        "rdf_format": export.get("rdf_format", "turtle"),
    }


def build_env(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the env dict consumed by DSSPostgresConnector.

    Merges schema, endpoint and database-level settings into a single dict.
    """
    sc = get_schema_config(cfg)
    ep = get_endpoint_config(cfg)
    db = get_db_config(cfg)
    source = get_source_config(cfg)
    return {
        "db_schema": sc["db_schema"],
        "display_name": sc["display_name"],
        "description": sc["description"],
        "classification_property": sc["classification_property"],
        "relationship_source_mode": get_relationship_source_mode(cfg),
        "source_type": source["source_type"],
        "source_input": source["source_input"],
        "service_name": source["service_name"],
        "sparql_url": ep["sparql_url"] or source.get("sparql_endpoint", ""),
        "named_graph": ep["named_graph"],
        "public_url": ep["public_url"],
        "endpoint_type": ep["endpoint_type"],
        "schema_kind": ep["schema_kind"],
        "registry_schema": db.get("registry_schema", "public"),
    }


def merge_source_metadata_into_env(
    env: Dict[str, Any], metadata: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Overlay source-derived endpoint URL onto the runtime env."""
    merged = dict(env)
    if not metadata:
        return merged

    endpoint_url = metadata.get("endpoint_url")
    if endpoint_url:
        merged["sparql_url"] = endpoint_url

    return merged
