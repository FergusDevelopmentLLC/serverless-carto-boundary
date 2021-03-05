'use strict';
const { Client } = require('pg')
const fastcsv = require('fast-csv')
const format = require('pg-format')
const request = require('request')
const dbConfig = require('./config/db')
const utils = require('./config/utils.js')

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

module.exports.getGeoJsonForCsv = (event, context, callback) => {

  const handleError = (error, callback) => {

    const errorResponse = {
      statusCode: error.statusCode || 500,
      body: {Error: error},
    }

    callback(null, JSON.stringify(errorResponse))

  }

  let body = JSON.parse(event.body)
  
  const stusps = body.state_abbrev
  const csvUrl = body.csvUrl
  const type = body.type

  const targetTableName = `csv_import_${ Date.now().toString() }`
  const insertData = []
  const csvData = []

  const client = new Client(dbConfig)
    
  client.connect()
    .then(() => {
      
      fastcsv.parseStream(request(csvUrl))
        .on('error', error => reject(`Error occurred while parsing csv: ${error}`))
        .on('data', (data) => csvData.push(data))
        .on('end', () => {

          if(!utils.validateType(type)){
            handleError("Invalid type: type must be 'point' or 'county'", callback)
            client.end()
          }

          if(!utils.validateStusps(stusps)){
            handleError("State abbreviation is invalid", callback)
            client.end()
          }
          
          let csvError = utils.validateCsvData(csvData)
          if(csvError) {
            handleError(csvError, callback)
            client.end()
          }
          
          const header = csvData.shift()//the first line in the csv are the columns
          
          csvData.forEach(columnValues => insertData.push([...columnValues]))

          const columnsString = header.map(column => `${column} character varying`).join(",")
          const columnsStringWithoutPrefix = header.map(column => `${column}`).join(",")

          client.query(`CREATE TABLE ${targetTableName} ( ${columnsString} ) WITH ( OIDS=FALSE );`)
            .then(() => {
              
              client.query(`ALTER TABLE ${targetTableName} OWNER TO awspostgres;`)
                .then(() => { 

                  const insertStatements = format(`INSERT INTO ${targetTableName} ( ${columnsStringWithoutPrefix} ) VALUES %L`, insertData)
                  
                  client.query(insertStatements)
                    .then(() => {

                      let columnsStringWithPrefix
                      
                      if(type == 'county')
                        columnsStringWithPrefix = header.map(column => `max(geo_points.${column})`).join(",")
                      else
                        columnsStringWithPrefix = header.map(column => `geo_points.${column}`).join(",")
                      
                      let geoSQL = utils.getSqlFor(type) 

                      geoSQL = geoSQL
                                .replace('#columnsStringWithPrefix', columnsStringWithPrefix)
                                .replace('#columnsStringWithoutPrefix', columnsStringWithoutPrefix)
                                .replace('#targetTableName', targetTableName)

                      client.query(geoSQL, [stusps])
                        .then((geoResult) => {

                          const response = {
                            statusCode: 200,
                            headers: {
                              "Access-Control-Allow-Origin": '*',
                              "Access-Control-Allow-Methods": 'GET'
                            },
                            body: JSON.stringify(geoResult.rows[0]['jsonb_build_object']),
                          }

                          callback(null, response)

                          client.end()

                        })
                        .catch((error) => {
                          handleError(error, callback)
                          client.end()
                        })
                    })
                    .catch((error) => {
                      handleError(error, callback)
                      client.end()
                    })

                })
                .catch((error) => {
                  handleError(error, callback)
                  client.end()
                }) 
            })
            .catch((error) => {
              handleError(error, callback)
              client.end()
            })
        })
    
    })
    .catch((error) => {
      handleError(error, callback)
      client.end()
    })
}
