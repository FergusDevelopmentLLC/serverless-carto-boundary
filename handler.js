'use strict';
const dbConfig = require('./config/db')
const { Client } = require('pg')

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

module.exports.getDistrictsForState = (event, context, callback) => {

  let sql = 
  `SELECT districts.*, states.stusps as state_abbrev, states.name as state_name
   FROM cb_2018_us_state_20m states
   JOIN cb_2018_us_cd116_20m districts on districts.statefp = states.statefp
   WHERE stusps = $1`.trim()

  sql = getGeoJsonSqlFor(sql)

  const client = new Client(dbConfig)
  client.connect()

  client
    .query(sql, [event.pathParameters.state_abbrev])
    .then((res) => {
      
      const response = {
        statusCode: 200,
        headers: {
          "Access-Control-Allow-Origin": '*',
          "Access-Control-Allow-Methods": 'GET'
        },
        body: JSON.stringify(res.rows[0]['jsonb_build_object']),
      }

      callback(null, response)

      client.end()
    })
    .catch((error) => {

      const errorResponse = {
        statusCode: error.statusCode || 500,
        body: `${error}`,
      }

      callback(null, errorResponse)

      client.end()
    })
}
