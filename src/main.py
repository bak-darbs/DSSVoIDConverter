import logging
import sys

from src.config import (
    get_export_config,
    get_operation,
    get_source_input,
    get_source_type,
    load_config,
)
from src.importer.sparql_catalogue_importer import (
    RdfFileSPARQLCatalogueImporter,
    SPARQLCatalogueImporter,
)
from src.importer.void_generator_importer import (
    RdfFileVoidGeneratorImporter,
    VoidGeneratorImporter,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    cfg = load_config(config_path)
    operation = get_operation(cfg)

    if operation == "export":
        get_export_config(cfg)
        from src.exporter.dss_to_void import export_dss_schema_to_file

        export_dss_schema_to_file(cfg)
        return

    source_type = get_source_type(cfg)
    source_input = get_source_input(cfg)
    if source_type == "sparql-catalogue":
        if source_input == "file":
            RdfFileSPARQLCatalogueImporter(cfg).run()
            return
        SPARQLCatalogueImporter(cfg).run()
        return

    if source_input == "file":
        RdfFileVoidGeneratorImporter(cfg).run()
        return

    VoidGeneratorImporter(cfg).run()


if __name__ == "__main__":
    main()
