import argparse
from databricks.sdk import WorkspaceClient


def copy_files(w: WorkspaceClient, source: str, destination: str, recursive: bool = False):
    w.dbutils.fs.cp(source, destination, recurse=recursive)


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
    copy_files(w, args.source, args.destination, args.recursive)


if __name__ == "__main__":
    main()
