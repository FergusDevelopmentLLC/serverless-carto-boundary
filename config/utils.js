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

const validateStusps = (stusps) => {

  if(states.find(st => st.stusps === stusps))
    return true
  else
    return false
}

const validateType = (type) => {
  if(type === 'point' || type === 'county')
    return true
  else
    return false
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

const getGeoJsonSqlFor = (sql) => {
  
  //remove trailing ; if present
  if(sql.charAt(sql.length - 1) === ';') sql = sql.substr(0, sql.length - 1)
  
  return `SELECT jsonb_build_object(
            'type', 'FeatureCollection',
            'features', jsonb_agg(features.feature)
          )
          FROM 
          (
            SELECT jsonb_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom,3)::jsonb,
            'properties', to_jsonb(inputs) - 'geom'
          ) AS feature
          FROM 
            (
              ${sql}
            ) inputs
          ) features;`
}

const getSqlFor = (type, header) => {

  header = header.filter((column) => column !== 'data_description')//do not return description column passed in

  const columnsStringWithoutPrefix = header.map(column => `${column}`).join(",")//build a comma delimited string of columns

  if(type === 'county') {
    
    let countiesSql = 
    `SELECT 
      county.geom,
      county.name,
      max(county.countyfp) as countyfp,
      max(pop.type) as type,
      COUNT(geo_points.geom) as geo_points_count,
      MAX(pop.pop_2019) as pop_2019,
      CASE 
        WHEN count(geo_points.geom) = 0 THEN max(pop.pop_2019) 
        ELSE max(pop.pop_2019) / count(geo_points.geom) 
      END
      AS persons_per_point
    FROM cb_2018_us_county_20m county
    LEFT JOIN
    (
      SELECT 
        ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, 
        ${columnsStringWithoutPrefix}
      FROM #targetTableName
    )
    AS geo_points on ST_WITHIN(geo_points.geom, county.geom)
    LEFT JOIN population_county pop on pop.name = county.name
    JOIN cb_2018_us_state_20m state on state.statefp = county.statefp
    WHERE 1 = 1 
    AND state.stusps = $1
    AND pop.statefp = state.statefp
    GROUP BY county.geom, county.name
    ORDER BY persons_per_point desc`

    return getGeoJsonSqlFor(countiesSql)
  }
  else {
    columnsStringWithPrefix = header.map(column => `geo_points.${column}`).join(",")
    let pointsSql = 
      `SELECT
        geo_points.geom,
        ${columnsStringWithPrefix}
      FROM cb_2018_us_state_20m state
      LEFT JOIN
        (
          SELECT 
            ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, 
            ${columnsStringWithoutPrefix}
          FROM #targetTableName
        ) AS geo_points on ST_WITHIN(geo_points.geom, state.geom)
      WHERE state.stusps = $1`

    return getGeoJsonSqlFor(pointsSql)
  }
}

module.exports = { isSuspicious, containsLongitudeLatitude, validateType, validateStusps, validateCsvData, getGeoJsonSqlFor, getSqlFor }