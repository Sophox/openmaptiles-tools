SELECT STRING_AGG(mvtl, '') AS mvt FROM (
SELECT IsEmpty, count(*) OVER () AS LayerCount, mvtl FROM (
  SELECT (sum(IsEmpty::int) != 0) AS IsEmpty, ST_AsMVT(mvtl2, 'housenumber', 4096, 'mvtgeometry') as mvtl FROM (SELECT CASE $1 <= 8 WHEN TRUE THEN FALSE ELSE ST_WITHIN(ST_GeomFromText('POLYGON((0 4096,0 0,4096 0,4096 4096,0 4096))', 3857), mvtgeometry) END AS IsEmpty, mvtgeometry, housenumber, tags, NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin" FROM (SELECT ST_AsMVTGeom(geometry, TileBBox($1, $2, $3), 4096, 8, true) AS mvtgeometry, housenumber, tags FROM (SELECT geometry, housenumber, tags FROM layer_housenumber(TileBBox($1, $2, $3), $1)) AS t) AS mvtl1 WHERE ST_IsValid(mvtgeometry)) AS mvtl2 HAVING COUNT(*) > 0
    UNION ALL
  SELECT FALSE AS IsEmpty, ST_AsMVT(mvtl2, 'enumfield', 4096, 'mvtgeometry') as mvtl FROM (SELECT mvtgeometry, class FROM (SELECT ST_AsMVTGeom(geometry, TileBBox($1, $2, $3), 4096, 0, true) AS mvtgeometry, class FROM ) AS mvtl1 WHERE ST_IsValid(mvtgeometry)) AS mvtl2 HAVING COUNT(*) > 0
) AS all_layers
) AS counter_layers
HAVING BOOL_AND(NOT IsEmpty OR LayerCount <> 1)
