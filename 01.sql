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


SELECT 
  county.id,
  county.geom,
  round(ST_X(ST_CENTROID(county.geom))::numeric, 3) as centroid_longitude,
  round(ST_Y(ST_CENTROID(county.geom))::numeric, 3) as centroid_latitude,
  county.name,
  county.statefp,
  county.countyfp,
  county.countyns,
  county.affgeoid,
  county.geoid,
  county.lsad,
  county.aland,
  county.awater,
  state.name as state_name,
  state.stusps as state_stusps
FROM cb_2018_us_county_20m county
JOIN cb_2018_us_state_20m state on state.statefp = county.statefp
WHERE state.stusps = 'IN';