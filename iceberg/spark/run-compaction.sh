#!/usr/bin/env bash
set -euo pipefail

usage() {
cat <<EOF
Usage: $0 --namespace <namespace> --table <table> [optional args]

Required arguments:
  --namespace <value>     Iceberg namespace (e.g. conformance)
  --table <value>         Iceberg table name (e.g. hydrology)

Optional arguments:
  --catalog-name <value>          Override catalog name
  --catalog-root" <value>         Override catalog root path
  --pg-conn-string <value>        Override JDBC connection string
  --pg-user <value>               Override PG user
  --pg-password <value>           Override PG password
  --max-parallelism <int>         Override parallelism level
  --target-file-size-bytes <int>  Override target file size

Example:
  $0 --namespace conformance --table hydrology
  $0 --namespace conformance --table hydrology --max-parallelism 2
EOF
exit 1
}


NAMESPACE="conformance"
TABLE="hydrology"

# Scan arguments for required keys
for arg in "$@"; do
    case "$arg" in
        --namespace)
            NAMESPACE="set"
            ;;
        --table)
            TABLE="set"
            ;;
    esac
done

if [ -z "$NAMESPACE" ] || [ -z "$TABLE" ]; then
    echo "ERROR: Missing required arguments."
    usage
fi


exec python compact_iceberg.py "$@"
