#
# Default extension config for DuckDB releases.
#
# This is the extension config that is used to build / test DuckDB releases, e.g.:
#  - Which extensions are statically linked into DuckDB
#  - Which extensions are tested
#  - Which extensions can be autoloaded
#
# Distributions that run this config:
# - Windows (64bit and 32bit) # TODO: 32bit needs autoloading disabled
# - Linux (arm64 and 32bit)
# - OSX (universal binary)

#
## Extensions that are linked
#
duckdb_extension_load(icu)
duckdb_extension_load(json)
duckdb_extension_load(parquet)
duckdb_extension_load(autocomplete)
duckdb_extension_load(httpfs
  SOURCE_DIR extension/httpfs/src
  INCLUDE_DIR extension/httpfs/src/include
)
#GIT_URL https://github.com/duckdb/duckdb-httpfs
#GIT_TAG 85ac4667bcb0d868199e156f8dd918b0278db7b9

#
## Extensions that are not linked, but we do want to test them as part of the release build
#
duckdb_extension_load(tpcds DONT_LINK)
duckdb_extension_load(tpch DONT_LINK)
