SELECT 
"STATEFP" as statefp,
state.geom, 
"STUSPS" as stusps, 
"NAME" as name, 
"STATENS" as statens, 
"AFFGEOID" as affgeoid, 
"GEOID" as geoid,
"LSAD" as lsad,
"ALAND" as aland,
"AWATER" as awater,
round(ST_X(centroids.geom)::numeric, 3) as centroid_longitude,
round(ST_Y(centroids.geom)::numeric, 3) as centroid_latitude
FROM public.state_centroids centroids
JOIN cb_2018_us_state_20m state on state.statefp = centroids."STATEFP"
ORDER BY "STATEFP";