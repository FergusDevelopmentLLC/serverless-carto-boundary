const states = require('./states.js').states

const isSuspicious = (columns) => {
  return columns.reduce((acc, column) => {
    if (!column.match(/^[0-9A-Za-z_]+$/)) {
      acc = true
    }
    return acc
  }, false)
}

const containsLongitudeLatitude = (columns) => {

  const caseInsensitiveFinder = (arrayToLookIn, strToFind) => (arrayToLookIn.findIndex(item => strToFind === item.toLowerCase()) > -1) ? true : false
  
  if(caseInsensitiveFinder(columns, 'longitude') && caseInsensitiveFinder(columns, 'latitude'))
    return true

  return false
}

const validateState = (name) => {
  return states.reduce((acc, st) => {

    if(states.find(st => st.name === name))
      acc = true
    
    return acc
  }, false)
}

const validateCsvData = (csvData) => {
    
  const empty = csvData.find((row) => {
    return row.length === 0
  })

  let error = null
  
  if(empty) 
    error = 'Invalid data detected'
  else if(csvData.length === 0) 
    error = 'Invalid data detected'
  else if(csvData && csvData[0] && csvData[0][0] && csvData[0][0].includes('404: Not Found'))
    error = csvData[0][0]
  else if(!containsLongitudeLatitude(csvData[0]))
    error = "Longitude, latitude colummns are required."
  else if(isSuspicious(csvData[0]))
    error = "Column names must be alphanumeric."
  
  let invalidRow = csvData.reduce((acc, row, i) => {
    if(row.length !== csvData[0].length)
      acc = i + 1
    return acc
  }, undefined)
    
  if(invalidRow) {
    error = `Invalid data detected in row: ${invalidRow}`
  }

  return error

}

const getGeoSQL = (type) => {

  let counties = 
  `
  SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(features.feature)
  )
  FROM (
    SELECT jsonb_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(geom,3)::jsonb,
    'properties', to_jsonb(inputs) - 'geom'
  ) AS feature
  FROM 
    (
    SELECT 
      county.geom,
      #columnsStringWithPrefix
      ,max(pop.type) as type,
      COUNT(geo_points.geom) as geo_points_count,
      MAX(pop.pop_2019) as pop_2019,
      CASE 
        WHEN count(geo_points.geom) = 0 THEN max(pop.pop_2019) 
        ELSE max(pop.pop_2019) / count(geo_points.geom) 
      END
      AS persons_per_location
    FROM county county
    LEFT JOIN
    (
      SELECT 
        ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, 
        #columnsStringWithoutPrefix
      FROM #targetTableName
    )
    AS geo_points on ST_WITHIN(geo_points.geom, county.geom)
    LEFT JOIN population_county pop on pop.name = county.name
    WHERE county.statefp = $1
    AND pop.statefp =  $1
    GROUP BY county.geom, county.name
    ORDER BY persons_per_location desc
    ) inputs
  ) features;
  `
  let points = 
  `
  SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(features.feature)
  )
  FROM (
    SELECT jsonb_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(geom,3)::jsonb,
    'properties', to_jsonb(inputs) - 'geom'
  ) AS feature
  FROM 
    (
      SELECT
        geo_points.geom,
        #columnsStringWithPrefix
      FROM state state
      LEFT JOIN
        (
          SELECT 
            ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, 
            #columnsStringWithoutPrefix
          FROM #targetTableName
        ) AS geo_points on ST_WITHIN(geo_points.geom, state.geom)
      WHERE state.name = $1
    ) inputs
  ) features;
  `
  if(type === 'county')
    return counties
  else if(type === 'point')
    return points
  else
    return null
}

module.exports = { isSuspicious, containsLongitudeLatitude, validateState, validateCsvData, getGeoSQL }