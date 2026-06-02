# DSSVoIDConverter

DSSVoIDConverter is a standalone Python tool for importing RDF dataset schema metadata into the
ViziQuer Data Shape Server (DSS) database and exporting DSS schemas back to VoID.

It reads VoID
metadata, maps the supported schema information to DSS tables, and registers the resulting schema
so it can be used by ViziQuer for visual query construction.

## Features

- Import `void-generator` generated VoID metadata into DSS.
- Import selected services from a Sparql Catalogue.
- Register imported schemas in the shared DSS registry tables.
- Export an existing DSS schema back to VoID.

## Supported Sources

### `void-generator`

The `void-generator` import expects VoID metadata shaped around `sd:Graph` nodes:

- top-level `void:classPartition` rows become DSS classes;
- top-level `void:propertyPartition` rows become DSS properties;
- nested class-property partitions become `cp_rels` and `cpc_rels`;
- `void:Linkset` rows can be used as additional relationship information;
- `void_ext:datatypePartition` rows populate datatype-related DSS tables.

### SPARQL Catalogue

The catalogue import path expects service-centered metadata shaped around `sd:Service`.

- service-scoped top-level `void:classPartition` become DSS;
- service-scoped top-level properties;
- service-scoped linksets become `cp_rels` and `cpc_rels`;

## Requirements

- Python 3.10 or newer
- PostgreSQL database used by Data Shape Server
- A DSS `empty` template schema in the database
- `pg_dump` and `psql` available on `PATH` when using `mode: "auto"`

Install Python dependencies:

```powershell
pip install -r requirements.txt
```

## Configuration

The tool reads `config.yaml` by default. You can also pass a config file path on the command line.

Minimal import example:

```yaml
operation: "import"
source_type: "void-generator"
source_input: "endpoint"
sparql_endpoint: "http://localhost:3030/dataset/sparql"

db_schema: "example_void_schema"
display_name: "Example VoID Schema"
description: "Schema imported from VoID metadata"
mode: "auto"

relationship_source_mode: "both"
classification_property: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

database:
  host: "localhost"
  port: 5433
  dbname: "rdfmeta"
  user: "rdfmeta"
  password: "password"
  registry_schema: "public"
```

Catalogue import example:

```yaml
operation: "import"
source_type: "sparql-catalogue"
source_input: "sparql"
sparql_endpoint: "https://sparql-catalogue.ai.wu.ac.at/api/qlever"
service_name: "https://catalogue.ai.wu.ac.at/example_service"

db_schema: "example_catalogue_schema"
display_name: "Example Catalogue Schema"
mode: "auto"

database:
  host: "localhost"
  port: 5433
  dbname: "rdfmeta"
  user: "rdfmeta"
  password: "password"
  registry_schema: "public"
```

RDF file import for `void-generator` metadata:

```yaml
operation: "import"
source_type: "void-generator"
source_input: "file"
rdf_file: "path/void_file.ttl"
rdf_format: "turtle"

db_schema: "example_void_schema"
display_name: "Example VoID Schema"
mode: "auto"

relationship_source_mode: "both"
classification_property: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

database:
  host: "localhost"
  port: 5433
  dbname: "rdfmeta"
  user: "rdfmeta"
  password: "password"
  registry_schema: "public"
```
RDF file import uses `rdflib.Graph`, so the metadata file is loaded into memory
before extraction. This mode is intended for VoID/catalogue metadata only files, not full datasets.

Export example:

```yaml
operation: "export"
db_schema: "example_void_schema"

database:
  host: "localhost"
  port: 5433
  dbname: "rdfmeta"
  user: "rdfmeta"
  password: "password"
  registry_schema: "public"

export:
  output_path: "output/exported_schema.ttl"
  rdf_format: "turtle"
```

## Usage

Run with the default `config.yaml`:

```powershell
python -m src.main
```

Run with a custom config file:

```powershell
python -m src.main path\to\config.yaml
```

## Related Projects

- [void-generator](https://github.com/sib-swiss/void-generator) - tool that generates VoID  from SPARQL endpoints.
- [Sparql Catalogue](https://sparql-catalogue.ai.wu.ac.at/) - used as source by the catalogue import mode.
- [Data Shape Server](https://github.com/LUMII-Syslab/data-shape-server) - ViziQuer schema backend used by this importer/exporter.
