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

module.exports.getGeoJsonForCsv = (event, context, callback) => {

  const body = JSON.parse(event.body)

  const description = body.description
  const stusps = body.state_abbrev
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

          if(!utils.validateStusps(stusps)){
            handleError(`state abbreviation is invalid: ${stusps}`, callback)
            client.end()
          }

          let csvError = utils.validateCsvData(csvData)
          if(csvError) {
            handleError(`validateCsvData error: ${csvError}`, callback)
            client.end()
          }

          const columns = csvData.shift()//the first line in the csv contains the columns
          columns.unshift('description')//add description column, will be inserted for each row
          
          csvData.forEach(columnValues => insertData.push([description, ...columnValues]))

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

                      client.query(countyGeoSQL, [stusps])
                        .then((counties) => {

                          let geojsonToReturn = counties.rows[0]['jsonb_build_object']//add counties polygons
                          
                          let pointsGeoSQL = utils.getSqlFor("point", columns) 
                          
                          pointsGeoSQL = pointsGeoSQL.replace('#targetTableName', targetTableName)

                          client.query(pointsGeoSQL, [stusps])
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
