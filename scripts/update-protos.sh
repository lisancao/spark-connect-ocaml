#!/usr/bin/env bash
# Re-vendor the Spark Connect protobuf definitions from a Spark git tag.
#
#   scripts/update-protos.sh v4.2.0 [/path/to/spark/checkout]
#
# ocaml-protoc's parser is stricter than protoc, so two cosmetic rewrites are
# applied: single-quoted strings become double-quoted, and `reserved "name";`
# lines (field-name reservations, no wire effect) are dropped.
set -euo pipefail
tag="${1:?usage: update-protos.sh <spark-tag> [spark-checkout]}"
src="${2:-$HOME/spark-src}"
dst="$(cd "$(dirname "$0")/.." && pwd)/proto/spark/connect"
mkdir -p "$dst"
for f in base catalog commands common expressions ml ml_common pipelines relations types; do
  git -C "$src" show "$tag:sql/connect/common/src/main/protobuf/spark/connect/$f.proto" > "$dst/$f.proto"
done
sed -i "s/^syntax = 'proto3';/syntax = \"proto3\";/; s/^import '\([^']*\)';/import \"\1\";/" "$dst"/*.proto
sed -i '/^\s*reserved "/d' "$dst"/*.proto
echo "$tag" > "$dst/VERSION"
echo "vendored Spark Connect protos from $tag into $dst"
