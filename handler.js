'use strict';
const { Client } = require('pg')
const fastcsv = require('fast-csv')
const format = require('pg-format')
const request = require('request')
const dbConfig = require('./config/db')
const utils = require('./config/utils.js')

const handleError = (error, callback) => {

  const errorResponse = {
    statusCode: error.statusCode || 500,
    body: {Error: error},
  }

  callback(null, JSON.stringify(errorResponse))

}

module.exports.getStateForStusps = (event, context, callback) => {

  let sqlRaw = 
  ` SELECT 
      ST_Simplify(geom,${ utils.simplificationParam }) as geom,
      stusps, 
      name,
      statefp,
      centroid_longitude,
      centroid_latitude
    FROM cb_2018_us_state_20m states
    WHERE stusps = $1 
    ORDER by stusps`.trim()

  let sql = utils.getGeoJsonSqlFor(sqlRaw)

  const client = new Client(dbConfig)
  
  client.connect()
    .then(() => {
      client.query(sql, [event.pathParameters.stusps])
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
          handleError(`getStateForStusps query error: ${error}`, callback)
          client.end()
        })
    })
    .catch((error) => {
      handleError(`getStateForStusps database connection error: ${error}`, callback)
      client.end()
    })

 }

module.exports.getStates = (event, context, callback) => {

  let sql = 
  `SELECT stusps, name, statefp, centroid_longitude, centroid_latitude
   FROM cb_2018_us_state_20m states
   ORDER by stusps;`.trim()

  const client = new Client(dbConfig)
  
  client.connect()
    .then(() => {
      client.query(sql, null)
        .then((res) => {
          
          const response = {
            statusCode: 200,
            headers: {
              "Access-Control-Allow-Origin": '*',
              "Access-Control-Allow-Methods": 'GET'
            },
            body: JSON.stringify(res.rows),
          }

          callback(null, response)
          client.end()
        })
        .catch((error) => {
          handleError(`getStates query error: ${error}`, callback)
          client.end()
        })
    })
    .catch((error) => {
      handleError(`getStates database connection error: ${error}`, callback)
      client.end()
    })

 }

module.exports.getGeoJsonForCsv = (event, context, callback) => {

  const body = JSON.parse(event.body)

  const data_description = body.data_description
  const stusps = body.stusps
  const csvUrl = body.csvUrl
  
  const targetTableName = `csv_import_${ Date.now().toString() }`
  const insertData = []
  const csvData = []

  const client = new Client(dbConfig)
  
  client.connect()
    .then(() => {
      
      fastcsv.parseStream(request(csvUrl))
        .on('error', error => {
          handleError(`fastcsv.parseStream error: ${error}`, callback)
          client.end()
        })
        .on('data', (data) => csvData.push(data))
        .on('end', () => {

          if(stusps && stusps !== 'all') {
            if(!utils.validateStusps(stusps)){
              handleError(`state abbreviation is invalid: ${stusps}`, callback)
              client.end()
            }
          }

          let csvError = utils.validateCsvData(csvData)
          if(csvError) {
            handleError(`validateCsvData error: ${csvError}`, callback)
            client.end()
          }

          const columns = csvData.shift()//the first line in the csv contains the columns
          columns.unshift('data_description')//add data_description column, will be inserted for each row
          
          csvData.forEach(columnValues => insertData.push([data_description, ...columnValues]))

          const createTableColumnStr = columns.map(column => `${column} character varying`).join(",")
          
          client.query(`CREATE TABLE ${targetTableName} ( ${createTableColumnStr } ) WITH ( OIDS=FALSE );`)
            .then(() => {
              
              client.query(`ALTER TABLE ${targetTableName} OWNER TO awspostgres;`)
                .then(() => { 

                  const columnsStringWithoutPrefix = columns.map(column => `${column}`).join(",")
                  const insertStatements = format(`INSERT INTO ${targetTableName} ( ${columnsStringWithoutPrefix} ) VALUES %L`, insertData)
                  
                  client.query(insertStatements)
                    .then(() => { 

                      let countyGeoSQL = utils.getSqlFor("county", columns) 

                      countyGeoSQL = countyGeoSQL.replace('#targetTableName', targetTableName)
                      
                      if(stusps === 'all') countyGeoSQL = countyGeoSQL.replace('AND state.stusps = $1', '')
                      
                      let stateToPass
                      if(stusps !== 'all') stateToPass = [stusps]

                      client.query(countyGeoSQL, stateToPass)
                        .then((counties) => {

                          let geojsonToReturn = counties.rows[0]['jsonb_build_object']//add counties polygons
                          
                          let pointsGeoSQL = utils.getSqlFor("point", columns) 
                          
                          pointsGeoSQL = pointsGeoSQL.replace('#targetTableName', targetTableName)

                          if(stusps === 'all') pointsGeoSQL = pointsGeoSQL.replace('WHERE state.stusps = $1', '')

                          client.query(pointsGeoSQL, stateToPass)
                            .then((points) => {

                              const pointsGeoJSON = points.rows[0]['jsonb_build_object']

                              //combine the counties and points features
                              geojsonToReturn.features = [...geojsonToReturn.features, ...pointsGeoJSON.features]

                              const response = {
                                statusCode: 200,
                                headers: {
                                  "Access-Control-Allow-Origin": '*',
                                  "Access-Control-Allow-Methods": 'GET'
                                },
                                body: JSON.stringify(geojsonToReturn),
                              }

                              callback(null, response)
                              
                              client.end()
                            })
                            .catch((error) => {
                              handleError(`pointsGeoSQL error: ${error}`, callback)
                              client.end()
                            })
                        })
                        .catch((error) => {
                          handleError(`pointsGeoSQL error: ${error}`, callback)
                          client.end()
                        })
                    })
                    .catch((error) => {
                      handleError(`insertStatements error: ${error}`, callback)
                      client.end()
                    })
                })
                .catch((error) => {
                  handleError(`ALTER TABLE error: ${error}`, callback)
                  client.end()
                }) 
            })
            .catch((error) => {
              handleError(`CREATE TABLE error: ${error}`, callback)
              client.end()
            })
        })
    })
    .catch((error) => {
      handleError(`database connection error: ${error}`, callback)
      client.end()
    })
}
