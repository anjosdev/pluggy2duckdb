import argparse
import logging
from pathlib import Path
from pluggy2duckdb.config import generate_endpoints_yaml
from pluggy2duckdb.raw.pipeline import extract_all_items

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="pluggy-raw", description="Fetch Pluggy API data to data/raw/"
    )
    parser.add_argument(
        "--generate-config",
        action="store_true",
        help="Generate config/endpoints.yaml from config/oas3.json",
    )
    parser.add_argument(
        "--item-id",
        dest="item_ids",
        action="append",
        help="Specific item UUID to extract",
    )
    parser.add_argument("--items-file", type=Path, default=Path("items.yaml"))
    parser.add_argument(
        "--endpoints-file", type=Path, default=Path("config/endpoints.yaml")
    )
    parser.add_argument(
        "--settings-file", type=Path, default=Path("config/get_settings.yaml")
    )
    parser.add_argument("--oas-file", type=Path, default=Path("config/oas3.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--allow-stale",
        action="store_true",
        help="Archive data even when upstream sync is incomplete",
    )
    args = parser.parse_args(argv)

    if args.generate_config:
        out = generate_endpoints_yaml(
            oas_path=args.oas_file, endpoints_path=args.endpoints_file
        )
        logger.info("Generated endpoints specification at %s", out)
        return

    extract_all_items(
        items_path=args.items_file,
        output_dir=args.output_dir,
        endpoints_path=args.endpoints_file,
        settings_path=args.settings_file,
        oas_path=args.oas_file,
        item_ids=args.item_ids,
        require_fresh=not args.allow_stale,
    )


if __name__ == "__main__":
    main()
