import argparse
from databricks.sdk import WorkspaceClient


def main():
    parser = argparse.ArgumentParser(description="Copy files using dbutils")
    parser.add_argument("--source", required=True, help="Source path")
    parser.add_argument("--destination", required=True, help="Destination path")
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively copy directories",
    )
    args = parser.parse_args()

    w = WorkspaceClient()
    w.dbutils.fs.cp(args.source, args.destination, recurse=args.recursive)


if __name__ == "__main__":
    main()
