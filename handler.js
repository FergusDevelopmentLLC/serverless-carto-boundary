'use strict';
const dbConfig = require('./config/db')
const { Client } = require('pg')
const fastcsv = require('fast-csv')
const format = require('pg-format')
const request = require('request')
//const states = require('./states.js').states
const utils = require('./utils.js')

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

module.exports.getGeoJsonFor = (csvUrl, stateName, type) => {

  return new Promise((resolve, reject) => {
    
    const targetTableName = `csv_import_${ Date.now().toString() }`
    const insertData = []
    const csvData = []

    const client = new Client()
    
    client.connect()
      .then(() => {
        
        fastcsv.parseStream(request(csvUrl))
          .on('error', error => reject(`Error occurred while parsing csv: ${error}`))
          .on('data', (data) => csvData.push(data))
          .on('end', () => {

            if(!utils.validateState(stateName)){
              reject(`Error: State name is invalid`)
              return
            }

            let csvError = utils.validateCsvData(csvData)
            if(csvError) {
              reject(`Error: ${csvError}`)
              return
            }
              
            const header = csvData.shift()//the first line in the csv are the columns
            
            csvData.forEach(columnValues => insertData.push([...columnValues]))

            const columnsString = header.map(column => `${column} character varying`).join(",")

            client.query(`CREATE TABLE ${targetTableName} ( ${columnsString} ) WITH ( OIDS=FALSE );`)
              .then(() => {
                
                client.query(`ALTER TABLE ${targetTableName} OWNER TO geodevdb;`)
                  .then(() => { 

                    const insertStatements = format(`INSERT INTO ${targetTableName} (longitude, latitude, name) VALUES %L`, insertData)
                    
                    client.query(insertStatements)
                      .then(() => {

                        const statefp = states.find(st => st.name === stateName).statefp
                        const columnsStringWithoutPrefix = header.map(column => `${column}`).join(",")

                        let columnsStringWithPrefix
                        let stateArray
                        
                        if(type == 'county') {
                          columnsStringWithPrefix = header.map(column => `max(geo_points.${column})`).join(",")
                          stateArray = [statefp]
                        }
                        else {
                          columnsStringWithPrefix = header.map(column => `geo_points.${column}`).join(",")
                          stateArray = [stateName]
                        }
                        
                        let geoSQL = utils.getGeoSQL(type) 

                        geoSQL = geoSQL
                                  .replace('#columnsStringWithPrefix', columnsStringWithPrefix)
                                  .replace('#columnsStringWithoutPrefix', columnsStringWithoutPrefix)
                                  .replace('#targetTableName', targetTableName)

                        client.query(geoSQL, stateArray)
                          .then((geoResult) => {
                            resolve(geoResult.rows[0]['jsonb_build_object'])
                          })
                          .catch((error) => {
                            reject(error)
                            return
                          })
                      })
                      .catch((error) => {
                        reject(error)
                        return
                      })

                  })
                  .catch((error) => {
                    reject(error)
                    return
                  }) 
              })
              .catch((error) => {
                reject(error)
                return
              })
          })
      
      })
      .catch((error) => {
        reject("Database connection error")
        return
      })
  
  })
}
