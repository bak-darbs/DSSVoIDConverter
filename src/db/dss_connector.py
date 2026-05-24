from __future__ import annotations

import json
import logging
import datetime
import os
import subprocess
import tempfile
from typing import Dict, Any, List, Optional, Tuple, Iterable

import psycopg2
import psycopg2.extras

from src.db.type_constants import CP_REL_TYPE
from src.utils.count_derivation import (
    resolve_cpd_rel_count_from_partition_remainder,
    resolve_incoming_cp_rel_count,
    resolve_pd_rel_count_from_sources,
)
from src.utils.namespace_utils import lookup_prefix_cc_abbr, split_iri
from src.utils.void_rendering_derivation import (
    backfill_cp_rel_object_cnt,
    derive_cp_rel_object_cnt,
    derive_property_object_cnt,
    rank_cover_set,
)
from src.exporter.models import (
    DssExportSnapshot,
    ExportClass,
    ExportCpdRel,
    ExportCpRel,
    ExportCpcRel,
    ExportMetadata,
    ExportPdRel,
    ExportProperty,
)

logger = logging.getLogger(__name__)

IMPORTER_VERSION = "2026-03-04-python"


# =====================================================================
# Main connector class
# =====================================================================


class DSSPostgresConnector:

    def __init__(self, db_config: Dict[str, Any]):
        self._host = db_config["host"]
        self._port = int(db_config.get("port", 5432))
        self._dbname = db_config["dbname"]
        self._user = db_config["user"]
        self._password = db_config["password"]
        self._registry_schema = db_config.get("registry_schema", "public")

        self._conn = None  

        # In-memory lookup caches (populated during import)
        self._ns_value_to_id = {}  # type: Dict[str, int]
        self._ns_id_to_name = {}  # type: Dict[int, str]
        self._ns_name_to_value = {}  # type: Dict[str, str]
        self._ns_name_to_id = {}  # type: Dict[str, int]

        self._class_iri_to_id = {}  # type: Dict[str, int]
        self._class_iri_to_cnt = {}  # type: Dict[str, Optional[int]]
        self._prop_iri_to_id = {}  # type: Dict[str, int]

        self._datatype_iri_to_id = {}  # type: Dict[str, int]
        self._datatype_short_to_id = {}  # type: Dict[str, int]

        self._annot_type_iri_to_id = {}  # type: Dict[str, int]

        self._auto_ns_counter = 1

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        if self._conn is not None and not self._conn.closed:
            return
        self._conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._dbname,
            user=self._user,
            password=self._password,
        )
        self._conn.autocommit = False
        logger.info(
            "Connected to PostgreSQL %s:%s/%s", self._host, self._port, self._dbname
        )

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed.")

    def _cursor(self):
        assert self._conn is not None and not self._conn.closed
        return self._conn.cursor()

    # ------------------------------------------------------------------
    # Schema creation (auto mode)
    # ------------------------------------------------------------------

    def schema_exists(self, schema_name: str) -> bool:
        with self._cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (schema_name,),
            )
            return cur.fetchone() is not None

    def create_schema_from_empty(self, schema_name: str) -> None:
        """
        Clone the empty template schema into new schema.

        Requires pg_dump and psql to be on PATH
        """
        logger.info("Creating schema '%s' by cloning 'empty' template ...", schema_name)

        # Check if schema name is allowed
        reserved = {"empty", "public", self._registry_schema}
        if schema_name in reserved:
            raise ValueError(
                f"Cannot import into reserved schema name '{schema_name}'."
            )

        # Verify empty template exists
        with self._cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'empty'"
            )
            if cur.fetchone() is None:
                raise RuntimeError(
                    "The 'empty' template schema does not exist in the database. "
                )

        pg_env = os.environ.copy()
        pg_env["PGPASSWORD"] = self._password

        dump_path = None
        try:
            # Create a temp file to hold the dump
            fd, dump_path = tempfile.mkstemp(suffix=".sql")
            os.close(fd)

            # dump the empty schema
            logger.info("Dumping 'empty' schema ...")
            result = subprocess.run(
                [
                    "pg_dump",
                    "-E",
                    "UTF8",
                    "-h",
                    self._host,
                    "-p",
                    str(self._port),
                    "-U",
                    self._user,
                    "-n",
                    "empty",
                    "-f",
                    dump_path,
                    "-d",
                    self._dbname,
                ],
                env=pg_env,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(f"pg_dump failed:\n{result.stderr}")

            # rename empty → schema_name
            logger.info("Renaming 'empty' -> '%s' ...", schema_name)
            ddl_conn = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._dbname,
                user=self._user,
                password=self._password,
            )
            ddl_conn.autocommit = True
            try:
                with ddl_conn.cursor() as cur:
                    cur.execute(f"ALTER SCHEMA empty RENAME TO {schema_name}")
            finally:
                ddl_conn.close()

            # restore empty from the dump
            logger.info("Restoring 'empty' schema from dump ...")
            result = subprocess.run(
                [
                    "psql",
                    "-h",
                    self._host,
                    "-p",
                    str(self._port),
                    "-U",
                    self._user,
                    "-d",
                    self._dbname,
                    "-f",
                    dump_path,
                ],
                env=pg_env,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(f"psql restore failed:\n{result.stderr}")

            logger.info("Schema '%s' created successfully.", schema_name)

        except FileNotFoundError as exc:
            raise RuntimeError(
                "pg_dump or psql not found in PATH. "
                "Install PostgreSQL client tools and ensure they are on PATH. "
                f"Original error: {exc}"
            ) from exc
        finally:
            if dump_path and os.path.exists(dump_path):
                os.unlink(dump_path)

    # ------------------------------------------------------------------
    # Init — load existing ns and annot_types from the target schema
    # ------------------------------------------------------------------

    def _init_caches(self, schema: str) -> None:
        """Populate in-memory caches from the existing ns/annot_types rows."""
        with self._cursor() as cur:
            cur.execute(f"SELECT id, name, value FROM {schema}.ns")
            for row in cur.fetchall():
                ns_id, name, value = row
                self._remember_prefix(ns_id, name, value)

            cur.execute(f"SELECT id, iri FROM {schema}.annot_types")
            for row in cur.fetchall():
                self._annot_type_iri_to_id[row[1]] = row[0]

        logger.info(
            "Loaded %d namespace(s) and %d annotation type(s) from %s",
            len(self._ns_value_to_id),
            len(self._annot_type_iri_to_id),
            schema,
        )

    def _remember_prefix(self, ns_id: int, name: str, value: str) -> None:
        self._ns_value_to_id[value] = ns_id
        self._ns_id_to_name[ns_id] = name
        self._ns_name_to_value[name] = value
        self._ns_name_to_id[name] = ns_id
        if name == "":
            self._ns_name_to_value[":"] = value
            self._ns_name_to_id[":"] = ns_id

    # ------------------------------------------------------------------
    # Namespace resolution
    # ------------------------------------------------------------------

    def _resolve_ns_prefix(
        self, schema: str, prefix_value: str, prefix_abbr: Optional[str] = None
    ) -> int:
        if prefix_value in self._ns_value_to_id:
            return self._ns_value_to_id[prefix_value]

        base_abbr = (
            prefix_abbr
            or self._lookup_ns_prefixes_table(prefix_value)
            or lookup_prefix_cc_abbr(prefix_value)
            or self._generate_abbr(prefix_value)
        )

        abbr = base_abbr
        suffix = 1
        while abbr in self._ns_name_to_id:
            abbr = f"{base_abbr}{suffix}"
            suffix += 1

        with self._cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.ns (value, name) VALUES (%s, %s) RETURNING id",
                (prefix_value, abbr),
            )
            ns_id = cur.fetchone()[0]

        self._remember_prefix(ns_id, abbr, prefix_value)
        return ns_id

    def _lookup_ns_prefixes_table(self, prefix_value: str) -> Optional[str]:
        """Check public.ns_prefixes for a known abbreviation."""
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT abbr FROM public.ns_prefixes WHERE prefix = %s",
                    (prefix_value,),
                )
                row = cur.fetchone()
                return row[0] if row else None
        except Exception:
            return None

    def _generate_abbr(self, prefix: str) -> str:
        abbr = f"n_{self._auto_ns_counter}"
        self._auto_ns_counter += 1
        return abbr

    # ------------------------------------------------------------------
    # Datatype helpers
    # ------------------------------------------------------------------

    def _add_datatype_by_iri(self, schema: str, iri: str) -> Optional[int]:
        if not iri:
            return None
        if iri in self._datatype_iri_to_id:
            return self._datatype_iri_to_id[iri]

        prefix, local_name = split_iri(iri)
        ns_id = self._ns_value_to_id.get(prefix)
        if ns_id is None:
            logger.warning("Namespace not found for datatype prefix '%s'", prefix)
            return None

        with self._cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.datatypes (iri, ns_id, local_name) "
                "VALUES (%s, %s, %s) RETURNING id",
                (iri, ns_id, local_name),
            )
            dt_id = cur.fetchone()[0]

        self._datatype_iri_to_id[iri] = dt_id
        abbr = self._ns_id_to_name.get(ns_id, "")
        short = f"{abbr}:{local_name}"
        self._datatype_short_to_id[short] = dt_id
        return dt_id

    # ------------------------------------------------------------------
    # Annots
    # ------------------------------------------------------------------

    def _get_or_register_annot_type(self, schema: str, iri: str) -> int:
        if iri in self._annot_type_iri_to_id:
            return self._annot_type_iri_to_id[iri]
        with self._cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.annot_types (iri) VALUES (%s) RETURNING id",
                (iri,),
            )
            tid = cur.fetchone()[0]
        self._annot_type_iri_to_id[iri] = tid
        return tid



    def _add_class_labels(self, schema: str, c: Dict[str, Any]) -> None:
        labels = c.get("Labels", [])
        if not labels:
            return
        class_id = self._class_iri_to_id.get(c["fullName"])
        if class_id is None:
            return
        for lbl in labels:
            type_id = self._get_or_register_annot_type(schema, lbl["property"])
            with self._cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {schema}.class_annots
                        (class_id, type_id, annotation, language_code)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT class_annots_c_t_l_uq
                        DO UPDATE SET annotation = EXCLUDED.annotation""",
                    (class_id, type_id, lbl.get("value"), lbl.get("language", "en")),
                )

   





    # ------------------------------------------------------------------
    # Property labels
    # ------------------------------------------------------------------

    def _add_property_labels(self, schema: str, p: Dict[str, Any]) -> None:
        labels = p.get("Labels", [])
        if not labels:
            return
        prop_id = self._prop_iri_to_id.get(p["fullName"])
        if prop_id is None:
            return
        for lbl in labels:
            type_id = self._get_or_register_annot_type(schema, lbl["property"])
            with self._cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {schema}.property_annots
                        (property_id, type_id, annotation, language_code)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT property_annots_p_t_l_uq
                        DO UPDATE SET annotation = EXCLUDED.annotation""",
                    (prop_id, type_id, lbl.get("value"), lbl.get("language", "en")),
                )

    # ------------------------------------------------------------------
    # Prefix handling
    # ------------------------------------------------------------------

    def _add_prefix_abbrs(self, schema: str, prefixes: List[Dict[str, Any]]) -> None:
        for pref in prefixes:
            ns_val = pref.get("namespace")
            ns_name = pref.get("prefix")
            if ns_val and ns_name:
                self._resolve_ns_prefix(schema, ns_val, ns_name)

    # ------------------------------------------------------------------
    # Parameters
    # ------------------------------------------------------------------

    def _add_parameters(
        self, schema: str, params: Dict[str, Any], env: Dict[str, Any]
    ) -> None:
        parameters = {
            "display_name_default": env.get("display_name", env.get("db_schema")),
            "db_schema_name": env.get("db_schema"),
            "schema_description": env.get("description"),
            "endpoint_url": env.get("sparql_url") or params.get("endpointUrl"),
            "named_graph": env.get("named_graph") or params.get("graphName"),
            "endpoint_public_url": env.get("public_url"),
            "schema_kind": env.get("schema_kind", "default"),
            "endpoint_type": env.get("endpoint_type", "generic"),
            "tree_profile_name": "default",
            "schema_extraction_details": params,
            "schema_import_datetime": datetime.datetime.now().isoformat(),
            "schema_importer_version": IMPORTER_VERSION,
        }

        for name, value in parameters.items():
            if value is None:
                continue
            with self._cursor() as cur:
                if isinstance(value, (dict, list)):
                    cur.execute(
                        f"""INSERT INTO {schema}.parameters (name, jsonvalue)
                            VALUES (%s, %s)
                            ON CONFLICT ON CONSTRAINT parameters_name_key
                            DO UPDATE SET jsonvalue = EXCLUDED.jsonvalue""",
                        (name, json.dumps(value)),
                    )
                else:
                    cur.execute(
                        f"""INSERT INTO {schema}.parameters (name, textvalue)
                            VALUES (%s, %s)
                            ON CONFLICT ON CONSTRAINT parameters_name_key
                            DO UPDATE SET textvalue = EXCLUDED.textvalue""",
                        (name, str(value)),
                    )

    # ------------------------------------------------------------------
    # Schema registry (public.schemata + public.endpoints)
    # ------------------------------------------------------------------

    def _register_schema(self, env: Dict[str, Any]) -> None:
        """Register the imported schema in the public registry tables."""
        reg = self._registry_schema
        display_name = env.get("display_name", env.get("db_schema"))
        db_schema_name = env.get("db_schema", "")[:63]

        with self._cursor() as cur:
            # save endpoint
            cur.execute(
                f"""INSERT INTO {reg}.endpoints
                    (sparql_url, public_url, named_graph, endpoint_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (coalesce(sparql_url, '@@'), coalesce(named_graph, '@@'))
                    DO UPDATE SET public_url = EXCLUDED.public_url,
                                  endpoint_type = EXCLUDED.endpoint_type
                    RETURNING id""",
                (
                    env.get("sparql_url"),
                    env.get("public_url"),
                    env.get("named_graph"),
                    env.get("endpoint_type", "generic"),
                ),
            )
            endpoint_id = cur.fetchone()[0]

            # Insert schema entry
            cur.execute(
                f"""INSERT INTO {reg}.schemata
                    (display_name, db_schema_name, description,
                     endpoint_id, is_active, is_default_for_endpoint)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT schemata_display_name_unique
                    DO UPDATE SET db_schema_name = EXCLUDED.db_schema_name,
                                  description = EXCLUDED.description,
                                  endpoint_id = EXCLUDED.endpoint_id,
                                  is_active = EXCLUDED.is_active,
                                  is_default_for_endpoint = EXCLUDED.is_default_for_endpoint""",
                (
                    display_name,
                    db_schema_name,
                    env.get("description"),
                    endpoint_id,
                    True,
                    True,
                ),
            )

        logger.info(
            "Registered schema '%s' (%s) in registry.", display_name, db_schema_name
        )

    # ------------------------------------------------------------------
    # Public methods for import steps
    # ------------------------------------------------------------------

    def add_parameters(
        self, schema: str, params: Dict[str, Any], env: Dict[str, Any]
    ) -> None:
        """Write endpoint/schema metadata to the parameters table and commit."""
        self.connect()
        self._add_parameters(schema, params, env)
        self._conn.commit()

    def register_schema(self, env: Dict[str, Any]) -> None:
        """Register the schema in the public registry tables and commit.

        Non-fatal — rolls back and logs on failure without raising, so a
        registry outage does not abort a completed import.
        """
        self.connect()
        try:
            self._register_schema(env)
            self._conn.commit()
        except Exception:
            logger.exception("Failed to register schema in registry (non-fatal)")
            if self._conn and not self._conn.closed:
                self._conn.rollback()

    def import_class_annotations(
        self, annotation_rows: List[Dict[str, Any]], db_schema: str
    ) -> int:
        """Persist class annotations for already imported classes."""
        self.connect()
        self._init_caches(db_schema)
        self._load_class_and_property_caches(db_schema)

        inserted = 0
        try:
            for row in annotation_rows:
                if row.get("fullName") not in self._class_iri_to_id:
                    continue
                self._add_class_labels(db_schema, row)
                inserted += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return inserted

    def import_property_annotations(
        self, annotation_rows: List[Dict[str, Any]], db_schema: str
    ) -> int:
        """Persist property annotations for already imported properties."""
        self.connect()
        self._init_caches(db_schema)
        self._load_class_and_property_caches(db_schema)

        inserted = 0
        try:
            for row in annotation_rows:
                if row.get("fullName") not in self._prop_iri_to_id:
                    continue
                self._add_property_labels(db_schema, row)
                inserted += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return inserted

    def run_post_processing(self, db_schema: str) -> Dict[str, int]:
        """
        Run post-processing SQL updates on derived columns.

        Implements DSS json-importer.js steps 4 and 7:

        - C1: properties.values_have_cp — marks properties whose values
          have INCOMING cp_rels.
        - C2: classes.self_cp_rels — resets to false/true based
          on whether the class has any cp_rels at all.

        Must be called after all relationship insertion steps (FR3.3,
        Steps A/B, Priority B) so that cp_rels is fully populated.

        Parameters
        ----------
        db_schema : str
            Target PostgreSQL schema name.

        Returns
        -------
        dict
            Counts: {"values_have_cp_updated": ...,
            "self_cp_rels_updated": ...}.
        """
        self.connect()
        counts = {"values_have_cp_updated": 0, "self_cp_rels_updated": 0}

        try:
            with self._cursor() as cur:
                # C1 — properties.values_have_cp
                cur.execute(
                    f"""
                    UPDATE {db_schema}.properties p
                    SET values_have_cp = EXISTS (
                        SELECT 1 FROM {db_schema}.cp_rels cp
                        WHERE cp.property_id = p.id
                          AND cp.type_id = %s
                    )
                """,
                    (CP_REL_TYPE.INCOMING,),
                )
                counts["values_have_cp_updated"] = cur.rowcount

                # C2 — classes.self_cp_rels
                cur.execute(f"""
                    UPDATE {db_schema}.classes c
                    SET self_cp_rels = EXISTS (
                        SELECT 1 FROM {db_schema}.cp_rels cp
                        WHERE cp.class_id = c.id
                    )
                """)
                counts["self_cp_rels_updated"] = cur.rowcount

            self._conn.commit()
            logger.info(
                "run_post_processing: updated %d properties.values_have_cp, "
                "%d classes.self_cp_rels in %s",
                counts["values_have_cp_updated"],
                counts["self_cp_rels_updated"],
                db_schema,
            )
        except Exception:
            self._conn.rollback()
            raise

        return counts

    def force_cp_rels_data_cnt_to_cnt(self, db_schema: str) -> int:
        """Set cp_rels.data_cnt equal to cp_rels.cnt for all rows."""
        self.connect()

        try:
            with self._cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {db_schema}.cp_rels
                    SET data_cnt = cnt
                    """
                )
                updated = cur.rowcount

            self._conn.commit()
            logger.info(
                "force_cp_rels_data_cnt_to_cnt: updated %d cp_rels rows in %s",
                updated,
                db_schema,
            )
            return updated
        except Exception:
            self._conn.rollback()
            raise

    def finalize_void_rendering_fields(
        self, db_schema: str, classification_property_iri: str = ""
    ) -> Dict[str, int]:
        """
        Fill derived DSS fields required by ViziQuer rendering.

        This finalization runs after relationship insertion so it can derive
        values from persisted DSS evidence rather than guessing from partial
        source rows. It is shared by the direct VoID pipeline and the approved
        SPARQL catalogue pipeline.
        """
        self.connect()
        counts = {
            "cp_object_cnt_updated": 0,
            "cp_data_cnt_updated": 0,
            "property_object_cnt_updated": 0,
            "cp_cover_set_updated": 0,
            "cpc_cover_set_updated": 0,
        }

        try:
            cp_count_updates = self._finalize_cp_rel_object_and_data_counts(
                db_schema, classification_property_iri
            )
            counts.update(cp_count_updates)
            counts["property_object_cnt_updated"] = (
                self._finalize_property_object_counts(db_schema)
            )
            counts["cp_cover_set_updated"] = (
                self._finalize_cp_rel_cover_set_indexes(db_schema)
            )
            counts["cpc_cover_set_updated"] = (
                self._finalize_cpc_rel_cover_set_indexes(db_schema)
            )

            self._conn.commit()
            logger.info(
                "finalize_void_rendering_fields: updated %s in %s",
                counts,
                db_schema,
            )
        except Exception:
            self._conn.rollback()
            raise

        return counts

    def _finalize_cp_rel_object_and_data_counts(
        self, db_schema: str, classification_property_iri: str = ""
    ) -> Dict[str, int]:
        classification_property_iri = (classification_property_iri or "").strip()
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT cp.id,
                       cp.property_id,
                       cp.type_id,
                       cp.cnt,
                       p.iri AS property_iri,
                       EXISTS (
                           SELECT 1 FROM {db_schema}.cpc_rels cpc
                           WHERE cpc.cp_rel_id = cp.id
                       ) AS has_cpc_rels,
                       EXISTS (
                           SELECT 1 FROM {db_schema}.cpd_rels cpd
                           WHERE cpd.cp_rel_id = cp.id
                       ) AS has_cpd_rels
                FROM {db_schema}.cp_rels cp
                JOIN {db_schema}.properties p ON p.id = cp.property_id
                ORDER BY cp.id
                """
            )
            cp_rows = cur.fetchall()

        property_object_presence: Dict[int, bool] = {}
        for (
            _cp_id,
            property_id,
            _type_id,
            cnt,
            property_iri,
            has_cpc_rels,
            has_cpd_rels,
        ) in cp_rows:
            object_cnt = derive_cp_rel_object_cnt(
                _type_id,
                cnt,
                property_iri,
                has_cpc_rels,
                has_cpd_rels,
                classification_property_iri,
            )
            if object_cnt is not None and object_cnt > 0:
                property_object_presence[property_id] = True

        cp_count_updates: List[Tuple[Optional[int], Optional[int], int]] = []
        for (
            cp_id,
            property_id,
            type_id,
            cnt,
            property_iri,
            has_cpc_rels,
            has_cpd_rels,
        ) in cp_rows:
            object_cnt = derive_cp_rel_object_cnt(
                type_id,
                cnt,
                property_iri,
                has_cpc_rels,
                has_cpd_rels,
                classification_property_iri,
            )
            object_cnt = backfill_cp_rel_object_cnt(
                object_cnt,
                type_id,
                cnt,
                property_object_presence.get(property_id, False),
                has_cpd_rels,
            )
            data_cnt = None
            if cnt is not None and object_cnt is not None:
                data_cnt = cnt - object_cnt
            cp_count_updates.append((object_cnt, data_cnt, cp_id))

        with self._cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                f"UPDATE {db_schema}.cp_rels SET object_cnt = %s, data_cnt = %s WHERE id = %s",
                cp_count_updates,
            )

        return {
            "cp_object_cnt_updated": len(cp_count_updates),
            "cp_data_cnt_updated": len(cp_count_updates),
        }

    def _finalize_property_object_counts(self, db_schema: str) -> int:
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT p.id,
                       p.cnt,
                       ARRAY_AGG(cp.object_cnt ORDER BY cp.id) AS cp_object_cnts
                FROM {db_schema}.properties p
                LEFT JOIN {db_schema}.cp_rels cp ON cp.property_id = p.id
                GROUP BY p.id, p.cnt
                ORDER BY p.id
                """
            )
            property_rows = cur.fetchall()

        property_object_cnt_updates: List[Tuple[Optional[int], int]] = []
        for property_id, property_cnt, cp_object_cnts in property_rows:
            object_cnt = derive_property_object_cnt(
                property_cnt,
                cp_object_cnts or [],
            )
            property_object_cnt_updates.append((object_cnt, property_id))

        with self._cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                f"UPDATE {db_schema}.properties SET object_cnt = %s WHERE id = %s",
                property_object_cnt_updates,
            )

        return len(property_object_cnt_updates)

    def _finalize_cp_rel_cover_set_indexes(self, db_schema: str) -> int:
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT cp.id,
                       cp.property_id,
                       cp.type_id,
                       cp.cnt,
                       c.cnt AS class_cnt,
                       c.iri AS class_iri,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT p2.property_id), NULL) AS signature,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT cpc.other_class_id), NULL) AS pair_signature
                FROM {db_schema}.cp_rels cp
                JOIN {db_schema}.classes c ON c.id = cp.class_id
                LEFT JOIN {db_schema}.cp_rels p2
                  ON p2.class_id = cp.class_id
                 AND p2.object_cnt > 0
                LEFT JOIN {db_schema}.cpc_rels cpc
                  ON cpc.cp_rel_id = cp.id
                GROUP BY cp.id, cp.property_id, cp.type_id, cp.cnt, c.cnt, c.iri
                ORDER BY cp.property_id, cp.type_id, cp.id
                """
            )
            cp_cover_rows = cur.fetchall()

        cp_groups: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        for (
            cp_id,
            property_id,
            type_id,
            cnt,
            class_cnt,
            class_iri,
            signature,
            pair_signature,
        ) in cp_cover_rows:
            group_key = (property_id, type_id)
            if group_key not in cp_groups:
                cp_groups[group_key] = []
            cp_groups[group_key].append(
                {
                    "id": cp_id,
                    "cnt": cnt,
                    "class_cnt": class_cnt,
                    "iri": class_iri,
                    "signature": tuple(signature or ()),
                    "pair_signature": tuple(pair_signature or ()),
                }
            )

        cp_cover_updates: List[Tuple[int, int]] = []
        for candidates in cp_groups.values():
            ranked = rank_cover_set(candidates)
            cp_cover_updates.extend(
                (cover_set_index, cp_id)
                for cp_id, cover_set_index in ranked.items()
            )

        with self._cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                f"UPDATE {db_schema}.cp_rels SET cover_set_index = %s WHERE id = %s",
                cp_cover_updates,
            )

        return len(cp_cover_updates)

    def _finalize_cpc_rel_cover_set_indexes(self, db_schema: str) -> int:
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT cpc.id,
                       cpc.cp_rel_id,
                       cpc.cnt,
                       c.cnt AS class_cnt,
                       c.iri AS class_iri,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT p2.property_id), NULL) AS signature
                FROM {db_schema}.cpc_rels cpc
                JOIN {db_schema}.classes c ON c.id = cpc.other_class_id
                LEFT JOIN {db_schema}.cp_rels p2
                  ON p2.class_id = cpc.other_class_id
                 AND p2.object_cnt > 0
                GROUP BY cpc.id, cpc.cp_rel_id, cpc.cnt, c.cnt, c.iri
                ORDER BY cpc.cp_rel_id, cpc.id
                """
            )
            cpc_cover_rows = cur.fetchall()

        cpc_groups: Dict[int, List[Dict[str, Any]]] = {}
        for (
            cpc_id,
            cp_rel_id,
            cnt,
            class_cnt,
            class_iri,
            signature,
        ) in cpc_cover_rows:
            if cp_rel_id not in cpc_groups:
                cpc_groups[cp_rel_id] = []
            cpc_groups[cp_rel_id].append(
                {
                    "id": cpc_id,
                    "cnt": cnt,
                    "class_cnt": class_cnt,
                    "iri": class_iri,
                    "signature": tuple(signature or ()),
                    "pair_signature": (),
                }
            )

        cpc_cover_updates: List[Tuple[int, int]] = []
        for candidates in cpc_groups.values():
            ranked = rank_cover_set(candidates)
            cpc_cover_updates.extend(
                (cover_set_index, cpc_id)
                for cpc_id, cover_set_index in ranked.items()
            )

        with self._cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                f"UPDATE {db_schema}.cpc_rels SET cover_set_index = %s WHERE id = %s",
                cpc_cover_updates,
            )

        return len(cpc_cover_updates)

   

    # ------------------------------------------------------------------
    # DSS export snapshot reads
    # ------------------------------------------------------------------

    def read_export_metadata(self, db_schema: str) -> ExportMetadata:
        self.connect()
        values: Dict[str, Any] = {}
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT name, textvalue, jsonvalue
                    FROM {db_schema}.parameters
                    WHERE name IN (
                        'db_schema_name',
                        'display_name_default',
                        'schema_description',
                        'endpoint_url',
                        'named_graph'
                    )"""
            )
            for name, textvalue, jsonvalue in cur.fetchall():
                values[name] = textvalue if textvalue is not None else jsonvalue

        return ExportMetadata(
            db_schema=str(values.get("db_schema_name") or db_schema),
            display_name=values.get("display_name_default"),
            description=values.get("schema_description"),
            endpoint_url=values.get("endpoint_url"),
            named_graph=values.get("named_graph"),
        )

    def read_export_classes(self, db_schema: str) -> List[ExportClass]:
        self.connect()
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT id, iri, cnt, is_literal
                    FROM {db_schema}.classes
                    WHERE iri IS NOT NULL
                    ORDER BY iri"""
            )
            return [
                ExportClass(id=row[0], iri=row[1], cnt=row[2], is_literal=row[3])
                for row in cur.fetchall()
            ]

    def read_export_properties(self, db_schema: str) -> List[ExportProperty]:
        self.connect()
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT id, iri, cnt
                    FROM {db_schema}.properties
                    WHERE iri IS NOT NULL
                    ORDER BY iri"""
            )
            return [
                ExportProperty(id=row[0], iri=row[1], cnt=row[2])
                for row in cur.fetchall()
            ]

    def read_export_outgoing_cp_rels(self, db_schema: str) -> List[ExportCpRel]:
        self.connect()
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT cp.id,
                           c.id AS class_id,
                           c.iri AS class_iri,
                           p.id AS property_id,
                           p.iri AS property_iri,
                           cp.cnt
                    FROM {db_schema}.cp_rels cp
                    JOIN {db_schema}.classes c ON c.id = cp.class_id
                    JOIN {db_schema}.properties p ON p.id = cp.property_id
                    WHERE cp.type_id = 2
                    ORDER BY c.iri, p.iri"""
            )
            return [
                ExportCpRel(
                    id=row[0],
                    class_id=row[1],
                    class_iri=row[2],
                    property_id=row[3],
                    property_iri=row[4],
                    cnt=row[5],
                )
                for row in cur.fetchall()
            ]

    def read_export_cpc_rels(self, db_schema: str) -> List[ExportCpcRel]:
        self.connect()
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT cpc.cp_rel_id,
                           c.id AS target_class_id,
                           c.iri AS target_class_iri,
                           cpc.cnt
                    FROM {db_schema}.cpc_rels cpc
                    JOIN {db_schema}.classes c ON c.id = cpc.other_class_id
                    ORDER BY cpc.cp_rel_id, c.iri"""
            )
            return [
                ExportCpcRel(
                    cp_rel_id=row[0],
                    target_class_id=row[1],
                    target_class_iri=row[2],
                    cnt=row[3],
                )
                for row in cur.fetchall()
            ]

    def read_export_pd_rels(self, db_schema: str) -> List[ExportPdRel]:
        self.connect()
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT p.id AS property_id,
                           p.iri AS property_iri,
                           d.iri AS datatype_iri,
                           pd.cnt
                    FROM {db_schema}.pd_rels pd
                    JOIN {db_schema}.properties p ON p.id = pd.property_id
                    JOIN {db_schema}.datatypes d ON d.id = pd.datatype_id
                    ORDER BY p.iri, d.iri"""
            )
            return [
                ExportPdRel(
                    property_id=row[0],
                    property_iri=row[1],
                    datatype_iri=row[2],
                    cnt=row[3],
                )
                for row in cur.fetchall()
            ]

    def read_export_cpd_rels(self, db_schema: str) -> List[ExportCpdRel]:
        self.connect()
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT cpd.cp_rel_id,
                           d.iri AS datatype_iri,
                           cpd.cnt
                    FROM {db_schema}.cpd_rels cpd
                    JOIN {db_schema}.datatypes d ON d.id = cpd.datatype_id
                    ORDER BY cpd.cp_rel_id, d.iri"""
            )
            return [
                ExportCpdRel(cp_rel_id=row[0], datatype_iri=row[1], cnt=row[2])
                for row in cur.fetchall()
            ]

    def read_export_snapshot(self, db_schema: str) -> DssExportSnapshot:
        self.connect()
        return DssExportSnapshot(
            metadata=self.read_export_metadata(db_schema),
            classes=self.read_export_classes(db_schema),
            properties=self.read_export_properties(db_schema),
            cp_rels=self.read_export_outgoing_cp_rels(db_schema),
            cpc_rels=self.read_export_cpc_rels(db_schema),
            pd_rels=self.read_export_pd_rels(db_schema),
            cpd_rels=self.read_export_cpd_rels(db_schema),
        )

    # ------------------------------------------------------------------
    # VoID class import 
    # ------------------------------------------------------------------

    def import_classes_from_void(
        self,
        class_tuples: Iterable[tuple],
        db_schema: str,
        auto_create: bool = False,
    ) -> int:
        """
        Persist classes extracted from VoID into db_schema.

        Parameters
        ----------
        class_tuples :
            Iterable of (class_iri: str, entities: int | None) pairs,
            as produced by extract_classes or the FR2.1 SPARQL query.
        db_schema : str
            Target PostgreSQL schema.
        auto_create : bool
            When True and the schema does not yet exist, clone it from
            the empty template automatically (requires pg_dump / psql).
            When False and the schema is missing, raise RuntimeError.

        Returns
        -------
        int
            Number of classes inserted.
        """
        self.connect()

        if not self.schema_exists(db_schema):
            if auto_create:
                self.create_schema_from_empty(db_schema)
            else:
                raise RuntimeError(
                    f"Schema '{db_schema}' does not exist. "
                    "Set mode: auto in config.yaml to create it automatically, "
                    "or create it manually first."
                )

        self._init_caches(db_schema)

        inserted = 0
        try:
            for class_iri, entities in class_tuples:
                if not class_iri:
                    continue

                if class_iri in self._class_iri_to_id:
                    logger.debug("Skipping duplicate class: %s", class_iri)
                    continue

                namespace, local_name = split_iri(class_iri)

                ns_id = (
                    self._resolve_ns_prefix(db_schema, namespace) if namespace else None
                )

                # class row insertion
                with self._cursor() as cur:
                    cur.execute(
                        f"""INSERT INTO {db_schema}.classes
                                (iri, local_name, display_name, ns_id, cnt,
                                 props_in_schema, is_literal)
                            VALUES (%s, %s, %s, %s, %s, true, false)
                            RETURNING id""",
                        (class_iri, local_name, local_name, ns_id, entities),
                    )
                    class_id = cur.fetchone()[0]

                self._class_iri_to_id[class_iri] = class_id
                inserted += 1

            self._conn.commit()
            logger.info(
                "import_classes_from_void: inserted %d class(es) into %s",
                inserted,
                db_schema,
            )
        except Exception:
            self._conn.rollback()
            raise

        return inserted

    # ------------------------------------------------------------------
    # VoID property import
    # ------------------------------------------------------------------

    def import_properties_from_void(
        self,
        property_tuples: List[tuple],
        db_schema: str,
    ) -> int:
        """
        Persist VoID property-partition data into {db_schema}.properties.

        Parameters
        ----------
        property_tuples : list of (str, int | None)
            (property_iri, triples) pairs, de-duplicated by IRI.
        db_schema : str
            Target PostgreSQL schema name (must already exist).

        Returns
        -------
        int
            Number of rows inserted.
        """
        inserted = 0
        self.connect()
        self._init_caches(db_schema)
        try:
            for prop_iri, triples in property_tuples:
                if prop_iri in self._prop_iri_to_id:
                    continue 

                namespace, local_name = split_iri(prop_iri)

                ns_id = (
                    self._resolve_ns_prefix(db_schema, namespace) if namespace else None
                )

                #property row insertion
                with self._cursor() as cur:
                    cur.execute(
                        f"""INSERT INTO {db_schema}.properties
                                (iri, local_name, display_name, ns_id, cnt,
                                 source_cover_complete, target_cover_complete)
                            VALUES (%s, %s, %s, %s, %s, false, false)
                            RETURNING id""",
                        (prop_iri, local_name, local_name, ns_id, triples),
                    )
                    prop_id = cur.fetchone()[0]

                self._prop_iri_to_id[prop_iri] = prop_id
                inserted += 1

            self._conn.commit()
            logger.info(
                "import_properties_from_void: inserted %d property(ies) into %s",
                inserted,
                db_schema,
            )
        except Exception:
            self._conn.rollback()
            raise

        return inserted

 

    def _load_class_and_property_caches(self, schema: str) -> None:
        """
        Populate _class_iri_to_id and _prop_iri_to_id from
        the existing rows in the target schema.
        """
        self._class_iri_to_id = {}
        self._class_iri_to_cnt = {}
        self._prop_iri_to_id = {}

        with self._cursor() as cur:
            cur.execute(f"SELECT id, iri, cnt FROM {schema}.classes")
            for row in cur.fetchall():
                self._class_iri_to_id[row[1]] = row[0]
                self._class_iri_to_cnt[row[1]] = row[2]

            cur.execute(f"SELECT id, iri FROM {schema}.properties")
            for row in cur.fetchall():
                self._prop_iri_to_id[row[1]] = row[0]

        logger.info(
            "_load_class_and_property_caches: %d class(es), %d property(ies) from %s",
            len(self._class_iri_to_id),
            len(self._prop_iri_to_id),
            schema,
        )

    def apply_classification_property(
        self,
        db_schema: str,
        property_iri: str,
    ) -> int:
        """
        Save a configured classification property for all classes.

        The configured property must already exist in {schema}.properties so
        classification_property and classification_property_id.
        """
        configured_iri = (property_iri or "").strip()
        if not configured_iri:
            return 0

        self.connect()
        self._load_class_and_property_caches(db_schema)

        property_id = self._prop_iri_to_id.get(configured_iri)
        if property_id is None:
            raise RuntimeError(
                f"Configured classification_property {configured_iri!r} was not imported into "
                f"{db_schema}.properties. Import the property first."
            )

        try:
            with self._cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {db_schema}.classes
                    SET classification_property = %s,
                        classification_property_id = %s
                    WHERE is_literal = false
                    """,
                    (configured_iri, property_id),
                )
                updated = cur.rowcount

            self._conn.commit()
            logger.info(
                "apply_classification_property: updated %d class(es) in %s using %s",
                updated,
                db_schema,
                configured_iri,
            )
            return updated
        except Exception:
            self._conn.rollback()
            raise

    #create synthetic cp_rels for classification property Because they do not appear in nested partitions or linksets
    def import_classification_property_cp_rels(
        self,
        db_schema: str,
        property_iri: str,
    ) -> Dict[str, int]:
        """
        Create generic OUTGOING cp_rels for the configured classification property. 

        """
        configured_iri = (property_iri or "").strip()
        if not configured_iri:
            return {"outgoing_cp": 0, "incoming_cp": 0, "cpc": 0}

        self.connect()
        self._load_class_and_property_caches(db_schema)

        property_id = self._prop_iri_to_id.get(configured_iri)
        if property_id is None:
            raise RuntimeError(
                f"Configured classification_property {configured_iri!r} was not imported into "
                f"{db_schema}.properties. Import the property first."
            )

        try:
            with self._cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {db_schema}.cp_rels
                        (class_id, property_id, type_id, cnt, details_level)
                    SELECT c.id, %s, 2, c.cnt, 0
                    FROM {db_schema}.classes c
                    WHERE c.is_literal = false
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {db_schema}.cp_rels cp
                          WHERE cp.class_id = c.id
                            AND cp.property_id = %s
                            AND cp.type_id = 2
                      )
                    """,
                    (property_id, property_id),
                )
                inserted = cur.rowcount

            self._conn.commit()
            counts = {
                "outgoing_cp": inserted,
                "incoming_cp": 0,
                "cpc": 0,
            }
            logger.info(
                "import_classification_property_cp_rels: inserted %d OUTGOING cp_rel row(s) into %s using %s",
                inserted,
                db_schema,
                configured_iri,
            )
            return counts
        except Exception:
            self._conn.rollback()
            raise

    def _resolve_cp_entry_ids(
        self,
        entry: Dict[str, Any],
    ) -> Optional[Tuple[str, str, int, int, Optional[int], List[Dict[str, Any]]]]:
        source_iri = entry["source_class"]
        prop_iri = entry["property"]
        cp_triples = entry.get("triples")
        targets = entry.get("targets", [])

        class_id = self._class_iri_to_id.get(source_iri)
        property_id = self._prop_iri_to_id.get(prop_iri)
        if class_id is None or property_id is None:
            logger.debug(
                "Skipping cp_rel: class or property not found — %s / %s",
                source_iri,
                prop_iri,
            )
            return None

        return source_iri, prop_iri, class_id, property_id, cp_triples, targets

    def _save_outgoing_cp_rel(
        self,
        db_schema: str,
        class_id: int,
        property_id: int,
        cp_triples: Optional[int],
        has_targets: bool,
    ) -> Tuple[int, int]:
        outgoing_cp_id = None
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT id, details_level FROM {db_schema}.cp_rels
                    WHERE class_id = %s AND property_id = %s AND type_id = 2""",
                (class_id, property_id),
            )
            row = cur.fetchone()
            if row is not None:
                outgoing_cp_id = row[0]
                existing_details_level = row[1]
                if has_targets and (existing_details_level or 0) < 2:
                    cur.execute(
                        f"""UPDATE {db_schema}.cp_rels
                            SET details_level = 2
                          WHERE id = %s""",
                        (outgoing_cp_id,),
                    )

        if outgoing_cp_id is not None:
            return outgoing_cp_id, 0

        details_level = 2 if has_targets else 0
        with self._cursor() as cur:
            cur.execute(
                f"""INSERT INTO {db_schema}.cp_rels
                        (class_id, property_id, type_id, cnt, details_level)
                    VALUES (%s, %s, 2, %s, %s)
                    RETURNING id""",
                (class_id, property_id, cp_triples, details_level),
            )
            outgoing_cp_id = cur.fetchone()[0]
        return outgoing_cp_id, 1

    def _add_incoming_source(
        self,
        incoming_agg: Dict[tuple, Dict[str, Any]],
        target_iri: str,
        prop_iri: str,
        source_iri: str,
        target_triples: Optional[int],
    ) -> None:
        inc_key = (target_iri, prop_iri)
        if inc_key not in incoming_agg:
            incoming_agg[inc_key] = {
                "triples": 0,
                "has_triples": False,
                "sources": [],
            }
        if target_triples is not None:
            incoming_agg[inc_key]["triples"] += target_triples
            incoming_agg[inc_key]["has_triples"] = True

        incoming_agg[inc_key]["sources"].append((source_iri, target_triples))

    def _insert_outgoing_cpc_rels(
        self,
        db_schema: str,
        outgoing_cp_id: int,
        prop_iri: str,
        source_iri: str,
        targets: List[Dict[str, Any]],
        incoming_agg: Dict[tuple, Dict[str, Any]],
    ) -> int:
        cpc_count = 0
        for tgt in targets:
            target_iri = tgt["class"]
            target_triples = tgt.get("triples")
            target_class_id = self._class_iri_to_id.get(target_iri)
            if target_class_id is None:
                logger.debug("Skipping cpc_rel: target class not found — %s", target_iri)
                continue

            with self._cursor() as cur:
                cur.execute(
                    f"""SELECT 1 FROM {db_schema}.cpc_rels
                        WHERE cp_rel_id = %s AND other_class_id = %s""",
                    (outgoing_cp_id, target_class_id),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        f"""INSERT INTO {db_schema}.cpc_rels
                                (cp_rel_id, other_class_id, cnt)
                            VALUES (%s, %s, %s)""",
                        (outgoing_cp_id, target_class_id, target_triples),
                    )
                    cpc_count += 1

            self._add_incoming_source(
                incoming_agg,
                target_iri,
                prop_iri,
                source_iri,
                target_triples,
            )

        return cpc_count

    def _save_incoming_cp_rel(
        self,
        db_schema: str,
        target_class_id: int,
        property_id: int,
        agg: Dict[str, Any],
    ) -> Tuple[int, int]:
        incoming_cp_id = None
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT id, details_level, principal_class_id, cnt FROM {db_schema}.cp_rels
                    WHERE class_id = %s AND property_id = %s AND type_id = 1""",
                (target_class_id, property_id),
            )
            row = cur.fetchone()
            if row is not None:
                incoming_cp_id = row[0]
                existing_details_level = row[1]
                existing_principal_class_id = row[2]
                existing_cnt = row[3]
                if (existing_details_level or 0) < 2:
                    cur.execute(
                        f"""UPDATE {db_schema}.cp_rels
                            SET details_level = 2
                          WHERE id = %s""",
                        (incoming_cp_id,),
                    )
            else:
                existing_principal_class_id = None
                existing_cnt = None

        agg_cnt = agg["triples"] if agg.get("has_triples") else None
        principal_class_id, inc_cnt, _mode = resolve_incoming_cp_rel_count(
            agg["sources"],
            self._class_iri_to_id,
            self._class_iri_to_cnt,
            agg_cnt,
        )

        if incoming_cp_id is None:
            with self._cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {db_schema}.cp_rels
                            (class_id, property_id, type_id, cnt, details_level, principal_class_id)
                        VALUES (%s, %s, 1, %s, 2, %s)
                        RETURNING id""",
                    (target_class_id, property_id, inc_cnt, principal_class_id),
                )
                incoming_cp_id = cur.fetchone()[0]
            return incoming_cp_id, 1

        if principal_class_id is not None and existing_principal_class_id is None:
            with self._cursor() as cur:
                cur.execute(
                    f"""UPDATE {db_schema}.cp_rels
                            SET principal_class_id = %s,
                                cnt = COALESCE(cnt, %s)
                          WHERE id = %s""",
                    (principal_class_id, inc_cnt, incoming_cp_id),
                )
        elif existing_cnt is None and inc_cnt is not None:
            with self._cursor() as cur:
                cur.execute(
                    f"""UPDATE {db_schema}.cp_rels
                            SET cnt = %s
                          WHERE id = %s""",
                    (inc_cnt, incoming_cp_id),
                )

        return incoming_cp_id, 0

    def _insert_incoming_cpc_rels(
        self,
        db_schema: str,
        incoming_cp_id: int,
        sources: List[Tuple[str, Optional[int]]],
    ) -> int:
        cpc_count = 0
        for source_iri, pair_triples in sources:
            source_class_id = self._class_iri_to_id.get(source_iri)
            if source_class_id is None:
                continue
            with self._cursor() as cur:
                cur.execute(
                    f"""SELECT 1 FROM {db_schema}.cpc_rels
                        WHERE cp_rel_id = %s AND other_class_id = %s""",
                    (incoming_cp_id, source_class_id),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        f"""INSERT INTO {db_schema}.cpc_rels
                                (cp_rel_id, other_class_id, cnt)
                            VALUES (%s, %s, %s)""",
                        (incoming_cp_id, source_class_id, pair_triples),
                    )
                    cpc_count += 1
        return cpc_count

    def _insert_incoming_cp_rels_from_agg(
        self,
        db_schema: str,
        incoming_agg: Dict[tuple, Dict[str, Any]],
    ) -> Tuple[int, int]:
        incoming_cp_count = 0
        cpc_count = 0
        for (target_iri, prop_iri), agg in incoming_agg.items():
            target_class_id = self._class_iri_to_id.get(target_iri)
            property_id = self._prop_iri_to_id.get(prop_iri)
            if target_class_id is None or property_id is None:
                continue

            incoming_cp_id, inserted = self._save_incoming_cp_rel(
                db_schema,
                target_class_id,
                property_id,
                agg,
            )
            incoming_cp_count += inserted
            cpc_count += self._insert_incoming_cpc_rels(
                db_schema,
                incoming_cp_id,
                agg["sources"],
            )

        return incoming_cp_count, cpc_count

    def import_rels_from_void(
        self,
        cp_data: List[Dict[str, Any]],
        db_schema: str,
    ) -> Dict[str, int]:
        """
        Save class-property and class-property-class relationships from VoID into
        cp_rels and cpc_rels.

        Parameters
        ----------
        cp_data : list of dict
            Each dict has source_class, property, triples,
            and targets (list of {"class": str, "triples": int | None}).
        db_schema : str
            Target PostgreSQL schema name.

        Returns
        -------
        dict
            Counts: {"outgoing_cp": ..., "incoming_cp": ..., "cpc": ...}.
        """
        self.connect()
        self._load_class_and_property_caches(db_schema)

        outgoing_cp_count = 0
        incoming_cp_count = 0
        cpc_count = 0
        incoming_agg = {}  # type: Dict[tuple, Dict[str, Any]]

        try:
            for entry in cp_data:
                resolved = self._resolve_cp_entry_ids(entry)
                if resolved is None:
                    continue
                (
                    source_iri,
                    prop_iri,
                    class_id,
                    property_id,
                    cp_triples,
                    targets,
                ) = resolved

                outgoing_cp_id, inserted = self._save_outgoing_cp_rel(
                    db_schema,
                    class_id,
                    property_id,
                    cp_triples,
                    bool(targets),
                )
                outgoing_cp_count += inserted
                cpc_count += self._insert_outgoing_cpc_rels(
                    db_schema,
                    outgoing_cp_id,
                    prop_iri,
                    source_iri,
                    targets,
                    incoming_agg,
                )

            incoming_cp_count, incoming_cpc_count = (
                self._insert_incoming_cp_rels_from_agg(
                    db_schema,
                    incoming_agg,
                )
            )
            cpc_count += incoming_cpc_count

            self._conn.commit()
            counts = {
                "outgoing_cp": outgoing_cp_count,
                "incoming_cp": incoming_cp_count,
                "cpc": cpc_count,
            }
            logger.info(
                "import_cp_rels_from_void: %s into %s",
                counts,
                db_schema,
            )
            return counts

        except Exception:
            self._conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Linkset-based rels
    # ------------------------------------------------------------------

    def import_linkset_rels_from_void(
        self,
        linkset_data: List[Dict[str, Any]],
        db_schema: str,
    ) -> Dict[str, int]:
        """
        Import cp_rels derived from void:Linkset nodes.

        Groups (source_class, property, target_class) linkset rows
        by (source_class, property) into the same structure expected by
        import_cp_rels_from_void.

        Parameters
        ----------
        linkset_data : list of dict
            Each dict has source_class, property, target_class,
            and triples, as returned by
            query_void_linksets_from_sparql or
            extract_linkset_cp_rels.
        db_schema : str
            Target PostgreSQL schema name.
        Returns
        -------
        dict
            Counts as returned by import_cp_rels_from_void.
        """
        # Group rows by (source_class, property). The outgoing cp_rel
        # count is the sum of matching linkset counts.
        grouped: Dict[tuple, Dict[str, Any]] = {}
        for row in linkset_data:
            source = row.get("source_class")
            prop = row.get("property")
            target = row.get("target_class")
            triples = row.get("triples")
            triples = int(triples) if triples is not None else None

            if not source or not prop:
                continue

            key = (source, prop)
            if key not in grouped:
                grouped[key] = {
                    "source_class": source,
                    "property": prop,
                    "triples": None,
                    "_triples_sum": 0,
                    "_has_triples": False,
                    "targets": [],
                }

            if triples is not None:
                grouped[key]["_triples_sum"] += triples
                grouped[key]["_has_triples"] = True

            if target is not None:
                existing_target = None
                for target_row in grouped[key]["targets"]:
                    if target_row["class"] == target:
                        existing_target = target_row
                        break

                if existing_target is None:
                    grouped[key]["targets"].append(
                        {"class": target, "triples": triples}
                    )
                elif triples is not None:
                    if existing_target["triples"] is None:
                        existing_target["triples"] = triples
                    else:
                        existing_target["triples"] += triples

        for entry in grouped.values():
            entry["triples"] = (
                entry["_triples_sum"] if entry.pop("_has_triples") else None
            )
            entry.pop("_triples_sum", None)

        return self.import_rels_from_void(list(grouped.values()), db_schema)

    # ------------------------------------------------------------------
    # Datatype extraction 
    # ------------------------------------------------------------------

    def _collect_cpd_partition_summary(
        self, cpd_data: List[tuple]
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        summary = {}  # type: Dict[Tuple[str, str], Dict[str, Any]]
        for (
            class_iri,
            prop_iri,
            dt_iri,
            _triples,
            has_object_target,
            object_target_count_sum,
            has_missing_object_target_count,
        ) in cpd_data:
            partition_key = (class_iri, prop_iri)
            if partition_key not in summary:
                summary[partition_key] = {
                    "datatype_iris": set(),
                    "has_object_branch": False,
                    "object_target_count_sum": None,
                    "has_missing_object_target_count": False,
                }
            partition_summary = summary[partition_key]
            partition_summary["datatype_iris"].add(dt_iri)
            partition_summary["has_object_branch"] = (
                partition_summary["has_object_branch"] or has_object_target
            )
            if (
                partition_summary["object_target_count_sum"] is None
                and object_target_count_sum is not None
            ):
                partition_summary["object_target_count_sum"] = object_target_count_sum
            partition_summary["has_missing_object_target_count"] = (
                partition_summary["has_missing_object_target_count"]
                or has_missing_object_target_count
            )
        return summary

    def _collect_datatype_iris(
        self, pd_data: List[tuple], cpd_data: List[tuple]
    ) -> set:
        datatype_iris = set()
        for _, dt_iri, _ in pd_data:
            datatype_iris.add(dt_iri)
        for _, _, dt_iri, _, _, _, _ in cpd_data:
            datatype_iris.add(dt_iri)
        return datatype_iris

    def _load_property_counts(self, db_schema: str) -> Dict[str, Optional[int]]:
        prop_iri_to_top_level_cnt = {}  # type: Dict[str, Optional[int]]
        with self._cursor() as cur:
            cur.execute(f"SELECT iri, cnt FROM {db_schema}.properties")
            for prop_iri, cnt in cur.fetchall():
                prop_iri_to_top_level_cnt[prop_iri] = cnt
        return prop_iri_to_top_level_cnt

    def _insert_datatypes_from_iris(self, db_schema: str, datatype_iris: set) -> int:
        inserted = 0
        for dt_iri in sorted(datatype_iris):
            existing = self._datatype_iri_to_id.get(dt_iri)
            if existing is not None:
                continue
            dt_id = self._add_datatype_by_iri(db_schema, dt_iri)
            if dt_id is not None:
                inserted += 1
            else:
                logger.warning(
                    "import_datatypes_from_void: could not insert datatype %s "
                    "(namespace not found)",
                    dt_iri,
                )
        return inserted

    def _insert_graph_level_pd_rels(
        self, db_schema: str, pd_data: List[tuple]
    ) -> Tuple[int, set]:
        inserted = 0
        pd_seen_pairs = set()
        for prop_iri, dt_iri, triples in pd_data:
            property_id = self._prop_iri_to_id.get(prop_iri)
            datatype_id = self._datatype_iri_to_id.get(dt_iri)
            if property_id is None:
                logger.debug("Skipping pd_rel: property not found — %s", prop_iri)
                continue
            if datatype_id is None:
                logger.debug("Skipping pd_rel: datatype not resolved — %s", dt_iri)
                continue

            with self._cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {db_schema}.pd_rels
                            (property_id, datatype_id, cnt)
                        VALUES (%s, %s, %s)
                        ON CONFLICT ON CONSTRAINT pd_rels_property_id_datatype_id_key
                        DO NOTHING""",
                    (property_id, datatype_id, triples),
                )
                pd_seen_pairs.add((prop_iri, dt_iri))
                if cur.rowcount > 0:
                    inserted += 1
        return inserted, pd_seen_pairs

    def _insert_cpd_rels_from_void(
        self,
        db_schema: str,
        cpd_data: List[tuple],
        cpd_partition_summary: Dict[Tuple[str, str], Dict[str, Any]],
    ) -> Tuple[int, Dict[str, Dict[str, Any]], Dict[str, int]]:
        inserted = 0
        pd_nested_summary = {}  # type: Dict[str, Dict[str, Any]]
        cpd_count_source_modes = {}  # type: Dict[str, int]

        for (
            class_iri,
            prop_iri,
            dt_iri,
            triples,
            has_object_target,
            _object_target_count_sum,
            _has_missing_object_target_count,
        ) in cpd_data:
            class_id = self._class_iri_to_id.get(class_iri)
            property_id = self._prop_iri_to_id.get(prop_iri)
            datatype_id = self._datatype_iri_to_id.get(dt_iri)

            if prop_iri not in pd_nested_summary:
                pd_nested_summary[prop_iri] = {
                    "datatype_iris": set(),
                    "has_object_branch": False,
                }
            nested_summary = pd_nested_summary[prop_iri]
            nested_summary["datatype_iris"].add(dt_iri)
            nested_summary["has_object_branch"] = (
                nested_summary["has_object_branch"] or has_object_target
            )

            if class_id is None:
                logger.debug("Skipping cpd_rel: class not found — %s", class_iri)
                continue
            if property_id is None:
                logger.debug("Skipping cpd_rel: property not found — %s", prop_iri)
                continue
            if datatype_id is None:
                logger.debug("Skipping cpd_rel: datatype not resolved — %s", dt_iri)
                continue

            with self._cursor() as cur:
                cur.execute(
                    f"""SELECT id FROM {db_schema}.cp_rels
                        WHERE class_id = %s AND property_id = %s AND type_id = 2""",
                    (class_id, property_id),
                )
                row = cur.fetchone()

            if row is None:
                logger.debug(
                    "Skipping cpd_rel: no OUTGOING cp_rel for class=%s prop=%s",
                    class_iri,
                    prop_iri,
                )
                continue

            partition_summary = cpd_partition_summary[(class_iri, prop_iri)]
            cpd_cnt, cpd_count_source_mode = (
                resolve_cpd_rel_count_from_partition_remainder(
                    triples,
                    partition_summary["datatype_iris"],
                    partition_summary["has_object_branch"],
                    partition_summary["object_target_count_sum"],
                    partition_summary["has_missing_object_target_count"],
                )
            )
            cpd_count_source_modes[cpd_count_source_mode] = (
                cpd_count_source_modes.get(cpd_count_source_mode, 0) + 1
            )
            if cpd_count_source_mode in {
                "AMBIGUOUS_MULTIPLE_DATATYPES",
                "MISSING_OBJECT_TARGET_COUNT",
                "NEGATIVE_REMAINDER",
            }:
                logger.warning(
                    "Leaving cpd_rel cnt NULL for class=%s prop=%s datatype=%s (%s).",
                    class_iri,
                    prop_iri,
                    dt_iri,
                    cpd_count_source_mode,
                )

            cp_rel_id = row[0]
            with self._cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {db_schema}.cpd_rels
                            (cp_rel_id, datatype_id, cnt)
                        VALUES (%s, %s, %s)
                        ON CONFLICT ON CONSTRAINT cpd_rels_cp_rel_id_datatype_id_key
                        DO NOTHING""",
                    (cp_rel_id, datatype_id, cpd_cnt),
                )
                if cur.rowcount > 0:
                    inserted += 1

        return inserted, pd_nested_summary, cpd_count_source_modes

    def _load_cpd_count_by_property_datatype(
        self, db_schema: str
    ) -> Dict[Tuple[str, str], Optional[int]]:
        cpd_count_by_property_datatype = {}  # type: Dict[Tuple[str, str], Optional[int]]
        with self._cursor() as cur:
            cur.execute(
                f"""SELECT p.iri,
                           dt.iri,
                           SUM(cpd.cnt)
                    FROM {db_schema}.cpd_rels cpd
                    JOIN {db_schema}.cp_rels cp
                      ON cp.id = cpd.cp_rel_id
                    JOIN {db_schema}.properties p
                      ON p.id = cp.property_id
                    JOIN {db_schema}.datatypes dt
                      ON dt.id = cpd.datatype_id
                    WHERE cp.type_id = %s
                    GROUP BY p.iri, dt.iri""",
                (CP_REL_TYPE.OUTGOING,),
            )
            for prop_iri, dt_iri, cpd_count_sum in cur.fetchall():
                cpd_count_by_property_datatype[(prop_iri, dt_iri)] = cpd_count_sum
        return cpd_count_by_property_datatype

    def _backfill_pd_rels_from_cpd(
        self,
        db_schema: str,
        pd_nested_summary: Dict[str, Dict[str, Any]],
        pd_seen_pairs: set,
        prop_iri_to_top_level_cnt: Dict[str, Optional[int]],
    ) -> Tuple[int, Dict[str, int]]:
        inserted = 0
        pd_count_source_modes = {}  # type: Dict[str, int]
        cpd_count_by_property_datatype = self._load_cpd_count_by_property_datatype(
            db_schema
        )

        for prop_iri, summary in pd_nested_summary.items():
            for dt_iri in sorted(summary["datatype_iris"]):
                cpd_count_sum = cpd_count_by_property_datatype.get((prop_iri, dt_iri))
                cnt, mode = resolve_pd_rel_count_from_sources(
                    cpd_count_sum,
                    prop_iri_to_top_level_cnt.get(prop_iri),
                    summary["datatype_iris"],
                    summary["has_object_branch"],
                )
                pd_count_source_modes[mode] = pd_count_source_modes.get(mode, 0) + 1

                property_id = self._prop_iri_to_id.get(prop_iri)
                datatype_id = self._datatype_iri_to_id.get(dt_iri)
                if property_id is None or datatype_id is None:
                    continue

                if (prop_iri, dt_iri) in pd_seen_pairs:
                    if cpd_count_sum is not None:
                        with self._cursor() as cur:
                            cur.execute(
                                f"""UPDATE {db_schema}.pd_rels
                                    SET cnt = %s
                                    WHERE property_id = %s
                                      AND datatype_id = %s""",
                                (cpd_count_sum, property_id, datatype_id),
                            )
                            if cur.rowcount > 0:
                                pd_count_source_modes["SUM_CPD_RELS_EXISTING"] = (
                                    pd_count_source_modes.get(
                                        "SUM_CPD_RELS_EXISTING", 0
                                    )
                                    + cur.rowcount
                                )
                    else:
                        pd_count_source_modes["GRAPH_PROPERTY_PARTITION"] = (
                            pd_count_source_modes.get("GRAPH_PROPERTY_PARTITION", 0)
                            + 1
                        )
                    continue

                with self._cursor() as cur:
                    cur.execute(
                        f"""INSERT INTO {db_schema}.pd_rels
                                (property_id, datatype_id, cnt)
                            VALUES (%s, %s, %s)
                            ON CONFLICT ON CONSTRAINT pd_rels_property_id_datatype_id_key
                            DO NOTHING""",
                        (property_id, datatype_id, cnt),
                    )
                    if cur.rowcount > 0:
                        inserted += 1

        return inserted, pd_count_source_modes

    def import_datatypes_from_void(
        self,
        pd_data: List[tuple],
        cpd_data: List[tuple],
        db_schema: str,
    ) -> Dict[str, int]:
        """
        Extract and save datatype information from VoID
        void_ext:datatypePartition nodes.

        Coordinates datatype table inserts, graph-level pd_rels, cpd_rels,
        and nested-evidence pd_rel backfill. Datatype partitions 
        do not have their counts, so counts are calculated from
        parent property partitions and, for property-level datatype counts,
        summed cpd_rels evidence when available.
        Parameters
        ----------
        pd_data : list of (str, str, int | None)
            (property_iri, datatype_iri, triples) from graph-level
            property partitions, where triples is the parent property
            partition's void:triples.
        cpd_data : list of tuple
            (class_iri, property_iri, datatype_iri, triples, has_object_target,
            object_target_count_sum, has_missing_object_target_count) from
            nested class-property partitions.
        db_schema : str
            Target PostgreSQL schema name.

        Returns
        -------
        dict
            Counts: {"datatypes": ..., "pd_rels": ..., "cpd_rels": ...}.
        """
        self.connect()
        self._init_caches(db_schema)
        self._load_class_and_property_caches(db_schema)

        counts = {"datatypes": 0, "pd_rels": 0, "cpd_rels": 0}

        try:
            cpd_partition_summary = self._collect_cpd_partition_summary(cpd_data)
            datatype_iris = self._collect_datatype_iris(pd_data, cpd_data)
            prop_iri_to_top_level_cnt = self._load_property_counts(db_schema)

            counts["datatypes"] = self._insert_datatypes_from_iris(
                db_schema, datatype_iris
            )
            pd_count, pd_seen_pairs = self._insert_graph_level_pd_rels(
                db_schema, pd_data
            )
            counts["pd_rels"] += pd_count

            cpd_count, pd_nested_summary, cpd_count_source_modes = (
                self._insert_cpd_rels_from_void(
                    db_schema, cpd_data, cpd_partition_summary
                )
            )
            counts["cpd_rels"] += cpd_count

            backfilled_pd_count, pd_count_source_modes = (
                self._backfill_pd_rels_from_cpd(
                    db_schema,
                    pd_nested_summary,
                    pd_seen_pairs,
                    prop_iri_to_top_level_cnt,
                )
            )
            counts["pd_rels"] += backfilled_pd_count

            self._conn.commit()
            logger.info(
                "import_datatypes_from_void: inserted %d datatype(s), "
                "%d pd_rel(s), %d cpd_rel(s) into %s (pd count sources: %s, cpd count sources: %s)",
                counts["datatypes"],
                counts["pd_rels"],
                counts["cpd_rels"],
                db_schema,
                pd_count_source_modes,
                cpd_count_source_modes,
            )
        except Exception:
            self._conn.rollback()
            raise

        return counts

    
