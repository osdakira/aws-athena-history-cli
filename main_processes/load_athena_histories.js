const _ = require('lodash');
const AWS = require('aws-sdk');
const sqlite3 = require('sqlite3').verbose();

const athena = new AWS.Athena({ region: 'ap-northeast-1' });

const db = new sqlite3.Database('athena_histories.sqlite3');

const dataSchema = {
  QueryExecutionId: 'TEXT PRIMARY KEY',
  Query: 'TEXT',
  StatementType: 'TEXT',
  OutputLocation: 'TEXT',
  State: 'TEXT',
  SubmissionDateTime: 'REAL',
  CompletionDateTime: 'REAL',
  EngineExecutionTimeInMillis: 'INTEGER',
  DataScannedInBytes: 'INTEGER',
  WorkGroup: 'TEXT',
};

const columns = _.map(dataSchema, (v, k) => `${k} ${v}`);
const createDDL = `CREATE TABLE IF NOT EXISTS histories (${columns});`;

const valueStatements = _.map(_.keys(dataSchema), (k) => `$${k}`);
const insertDML = `INSERT INTO histories VALUES (${valueStatements});`;


function createTable() {
  db.serialize(() => db.run(createDDL));
}
createTable();

function insert(history) {
  const flattened = _.flatMap(history, (v, k) => (_.isObject(v) ? _.toPairs(v) : [[k, v]]));
  const flattenedObject = _.fromPairs(flattened);
  const param = _.mapKeys(flattenedObject, (_v, k) => `$${k}`);

  db.run(insertDML, param);
}

function batchGetQueryExecution(params) {
  athena.batchGetQueryExecution(params, (err, data) => {
    if (err) {
      console.log(err, err.stack);
    } else {
      console.log(data);
      data.QueryExecutions.forEach((history) => insert(history));
    }
  });
}

async function listQueryExecutions(nextToken = null) {
  const params = { MaxResults: '5', NextToken: nextToken };
  athena.listQueryExecutions(params, (err, data) => {
    if (err) console.log(err, err.stack);
    else data;
  });
}

async function main() {
  const data = await listQueryExecutions();
  // const nextToken = data.NextToken;
  const params = { QueryExecutionIds: data.QueryExecutionIds };
  batchGetQueryExecution(params);
}
main();

function sample() { // eslint-disable-line no-unused-vars
  return {
    QueryExecutions: [
      {
        QueryExecutionId: '9ca9ccb5-e020-4f78-b634-36902fdccb59',
        Query: "SELECT distinct(u.deviceid) from\n      (\n        SELECT UPPER(g.deviceid) deviceid\n        FROM ase.geohashes g\n        JOIN file_upload_segment.geohashes_production_20190830_20190401_20190828_3_22af793fbc714ad19f1367ff27744e7f t ON t.geohash = SUBSTR(g.geohash, 1, 8)\n        WHERE g.dt between '2019-04-01' and '2019-08-28'\n        GROUP BY g.deviceid\n        HAVING COUNT(g.deviceid) >= 3\n      ) u",
        StatementType: 'DML',
        ResultConfiguration: {
          OutputLocation: 's3://fout-fox-production/upload_segment/device_id/20190830/geohashes_production_20190830_20190401_20190828_3_22af793fbc714ad19f1367ff27744e7f/9ca9ccb5-e020-4f78-b634-36902fdccb59.csv',
        },
        QueryExecutionContext: {},
        Status: {
          State: 'SUCCEEDED',
          SubmissionDateTime: 1567159291.603,
          CompletionDateTime: 1567159369.977,
        },
        Statistics: {
          EngineExecutionTimeInMillis: 78021,
          DataScannedInBytes: 397917982675,
        },
        WorkGroup: 'primary',
      },
    ],
    UnprocessedQueryExecutionIds: [],
  };
}

// sample().QueryExecutions.forEach((history) => insert(history));
