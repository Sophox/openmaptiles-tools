CREATE OR REPLACE FUNCTION gettile(zoom integer, x integer, y integer)
RETURNS bytea AS $$
SELECT STRING_AGG(mvtl, '') AS mvt FROM (
  SELECT ST_AsMVT(mvtl2, 'housenumber', 4096, 'mvtgeometry') as mvtl FROM (SELECT mvtgeometry, housenumber, tags, NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin" FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 8, true) AS mvtgeometry, housenumber, tags FROM (SELECT geometry, housenumber, tags FROM layer_housenumber(TileBBox(zoom, x, y), zoom)) AS t) AS mvtl1 WHERE ST_IsValid(mvtgeometry)) AS mvtl2 HAVING COUNT(*) > 0
    UNION ALL
  SELECT ST_AsMVT(mvtl2, 'enumfield', 4096, 'mvtgeometry') as mvtl FROM (SELECT mvtgeometry, class FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 0, true) AS mvtgeometry, class FROM ) AS mvtl1 WHERE ST_IsValid(mvtgeometry)) AS mvtl2 HAVING COUNT(*) > 0
) AS all_layers
;
$$ LANGUAGE SQL STABLE RETURNS NULL ON NULL INPUT;
