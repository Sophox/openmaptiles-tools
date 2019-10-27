from .consts import PIXEL_SCALE
from .tileset import Tileset
from .language import languages_as_fields


def generate_sqltomvt_func(opts):
    """
    Creates a SQL function that returns a single bytea value or null
    """
    return f"""\
CREATE OR REPLACE FUNCTION {opts['fname']}(zoom integer, x integer, y integer)
RETURNS bytea AS $$
{generate_query(opts, "TileBBox(zoom, x, y)", "zoom")};
$$ LANGUAGE SQL STABLE RETURNS NULL ON NULL INPUT;"""


def generate_sqltomvt_preparer(opts):
    """
    Creates a SQL prepared statement that returns 0 or 1 row with a single mvt column.
    """
    return f"""\
-- Delete prepared statement if it already exists
DO $$ BEGIN
IF EXISTS (SELECT * FROM pg_prepared_statements where name = '{opts['fname']}') THEN
  DEALLOCATE {opts['fname']};
END IF;
END $$;

-- Run this statement with   EXECUTE {opts['fname']}(zoom, x, y)
PREPARE {opts['fname']}(integer, integer, integer) AS
{generate_sqltomvt_query(opts)};"""


def generate_sqltomvt_query(opts):
    return generate_query(opts, "TileBBox($1, $2, $3)", "$1")


def generate_sqltomvt_psql(opts):
    return generate_query(opts, "TileBBox(:zoom, :x, :y)", ":zoom")


def generate_sqltomvt_raw(opts):
    return generate_query(opts, None, None)


def generate_query(opts, bbox, zoom):
    if isinstance(opts['tileset'], str):
        tileset = Tileset.parse(opts['tileset'])
    else:
        tileset = opts['tileset']
    languages = tileset.definition.get('languages', [])
    extent = 4096
    pixel_width = PIXEL_SCALE
    pixel_height = PIXEL_SCALE

    queries = []
    for layer in tileset.layers:
        # If mask-layer is set (e.g. to 'water'), add an extra column 'IsEmpty'
        # to each layer's result row. For non-water, or for water in zoom <= mask-zoom,
        # always set it to FALSE. For zoom > mask-zoom, test if the polygon spanning
        # the entire tile is fully within layer's geometry
        if not opts['mask-layer']:
            empty_zoom = False
        elif layer["layer"]['id'] == opts['mask-layer']:
            empty_zoom = opts['mask-zoom']
        else:
            empty_zoom = True
        queries.append(generate_layer(layer, languages, extent, empty_zoom))

    from_clause = "FROM (\n  " + \
                  "\n    UNION ALL\n  ".join(queries) + "\n) AS all_layers\n"

    # If mask-layer is set, wrap query to detect when the IsEmpty column
    # is TRUE (for water), and there are no other rows, and if so, return nothing.
    if opts['mask-layer']:
        from_clause = (
            "FROM (\n" +
            "SELECT IsEmpty, count(*) OVER () AS LayerCount, mvtl " +
            from_clause +
            ") AS counter_layers\n" +
            "HAVING BOOL_AND(NOT IsEmpty OR LayerCount <> 1)")

    query = "SELECT STRING_AGG(mvtl, '') AS mvt " + from_clause

    if bbox is None:
        return query

    query = (query
             .replace("!bbox!", bbox)
             .replace("z(!scale_denominator!)", zoom)
             .replace("!pixel_width!", str(pixel_width))
             .replace("!pixel_height!", str(pixel_height)))

    if '!scale_denominator!' in query:
        raise ValueError(
            'We made an invalid assumption that "!scale_denominator!" is always '
            'used as a parameter to z() function. Either change the layer queries, '
            'or fix this code')

    return query


def generate_layer(layer_def, languages, extent, empty_zoom):
    """
    If empty_zoom is True, adds an extra sql column with a constant value,
    otherwise if it is an integer, tests if the geometry of this layer covers the whole
    tile, and outputs true/false, otherwise no extra column is added
    """
    layer = layer_def["layer"]
    query = layer['datasource']['query']
    has_languages = '{name_languages}' in query
    tags_field = 'tags'
    if has_languages:
        query = query.format(name_languages=tags_field)
    fields, geo_fld = layer_def.get_fields()
    buffer = layer['buffer_size']

    if isinstance(empty_zoom, bool):
        is_empty_geom = ""
        is_empty_fld = "FALSE AS IsEmpty, " if empty_zoom else ""
    else:
        # Test that geometry covers the whole tile.
        # Create a polygon covering the whole tile without invisible margins.
        zero = 0
        wkt_polygon = f"""POLYGON(\
({zero} {extent},{zero} {zero},{extent} {zero},{extent} {extent},{zero} {extent}))"""

        # for zooms higher than empty_zoom test all MVT geometries
        is_empty_geom = f"""\
CASE z(!scale_denominator!) <= {empty_zoom} \
WHEN TRUE THEN FALSE \
ELSE ST_WITHIN(ST_GeomFromText('{wkt_polygon}', 3857), mvtgeometry) \
END AS IsEmpty, """

        # Aggregation of all IsEmpty fields
        is_empty_fld = "(sum(IsEmpty::int) != 0) AS IsEmpty, "

    inner_fields = fields
    if has_languages:
        inner_fields += [tags_field]

    # Convert geometry to a clipped MVT geometry for the given extent and buffer
    query = f"""(\
SELECT ST_AsMVTGeom({geo_fld}, !bbox!, {extent}, {buffer}, true) AS mvtgeometry, \
{', '.join(inner_fields)} FROM {query}\
) AS mvtl1"""

    # Right before converting to mvt, expand name tags into multiple fields (if used)
    mvt_fields = ['mvtgeometry'] + fields
    if has_languages:
        mvt_fields += languages_as_fields(languages)

    # Remove any NULL MVT geometries (TBD if this is required, and if needs ST_IsValid)
    query = f"""(\
SELECT {is_empty_geom}{', '.join(mvt_fields)} FROM {query} \
WHERE ST_IsValid(mvtgeometry)) AS mvtl2"""

    # Combine all layer's features into a single MVT blob representing one layer
    # only if the MVT geometry is not NULL
    # Skip the whole layer if there is nothing in it
    return f"""\
SELECT {is_empty_fld}ST_AsMVT(mvtl2, '{layer['id']}', {extent}, 'mvtgeometry') as mvtl \
FROM {query} \
HAVING COUNT(*) > 0"""
