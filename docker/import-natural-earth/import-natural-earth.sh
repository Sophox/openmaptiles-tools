#!/bin/sh
set -o errexit
set -o nounset

# For backward compatibility, allow both PG* and POSTGRES_* forms,
# with the non-standard POSTGRES_* form taking precedence.
# An error will be raised if neither form is given, except for the PGPORT
PGHOST="${POSTGRES_HOST:-${PGHOST?}}"
PGDATABASE="${POSTGRES_DB:-${PGDATABASE?}}"
PGUSER="${POSTGRES_USER:-${PGUSER?}}"
PGPASSWORD="${POSTGRES_PASSWORD:-${PGPASSWORD?}}"
PGPORT="${POSTGRES_PORT:-${PGPORT:-5432}}"

PGCONN="${PGCONN:-dbname=$PGDATABASE user=$PGUSER host=$PGHOST password=$PGPASSWORD port=$PGPORT}"

echo "Importing Natural Earth into PostgreSQL"
PGCLIENTENCODING=UTF8 ogr2ogr \
  -progress \
  -f Postgresql \
  -s_srs EPSG:4326 \
  -t_srs EPSG:3857 \
  -clipsrc -180.1 -85.0511 180.1 85.0511 \
  PG:"${PGCONN?}" \
  -lco GEOMETRY_NAME=geometry \
  -lco OVERWRITE=YES \
  -lco DIM=2 \
  -nlt GEOMETRY \
  -overwrite \
  "${NATURAL_EARTH_DB?}"
