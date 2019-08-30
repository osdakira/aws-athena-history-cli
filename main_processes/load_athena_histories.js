const AWS = require('aws-sdk');
const sqlite3 = require('sqlite3');

const athena = new AWS.Athena({ region: 'ap-northeast-1' });

const db = new sqlite3.Database('athena_histories.sqlite3');

function createTable() {
  db.run(`
    CREATE TABLE IF NOT EXISTS histories (
      QueryExecutionId TEXT PRIMARY KEY,
      Query TEXT,
      StatementType TEXT,
      ResultConfiguration JSON,
      Status JSON,
      Statistics JSON,
      WorkGroup TEXT
    );
  `);
}
createTable();

function insert(history) {
  db.run(`
    INSERT INTO histories VALUES (
      $QueryExecutionId,
      $Query,
      $StatementType,
      $ResultConfiguration,
      $Status,
      $Statistics,
      $WorkGroup
    );
  `, history);
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

function listQueryExecutions(nextToken = null) {
  const params = {
    MaxResults: '5',
    NextToken: nextToken,
  };

  athena.listQueryExecutions(params, (err, data) => {
    if (err) {
      console.log(err, err.stack);
    } else {
      console.log(data);
      // data.NextToken;
      const params1 = { QueryExecutionIds: data.QueryExecutionIds };
      batchGetQueryExecution(params1);
    }
  });
}
listQueryExecutions();

// function sample() {
//   return {
//     "QueryExecutions": [
//       {
//         "QueryExecutionId": "9ca9ccb5-e020-4f78-b634-36902fdccb59",
//         "Query": "SELECT distinct(u.deviceid) from\n      (\n        SELECT UPPER(g.deviceid) deviceid\n        FROM ase.geohashes g\n        JOIN file_upload_segment.geohashes_production_20190830_20190401_20190828_3_22af793fbc714ad19f1367ff27744e7f t ON t.geohash = SUBSTR(g.geohash, 1, 8)\n        WHERE g.dt between '2019-04-01' and '2019-08-28'\n        GROUP BY g.deviceid\n        HAVING COUNT(g.deviceid) >= 3\n      ) u",
//         "StatementType": "DML",
//         "ResultConfiguration": {
//           "OutputLocation": "s3://fout-fox-production/upload_segment/device_id/20190830/geohashes_production_20190830_20190401_20190828_3_22af793fbc714ad19f1367ff27744e7f/9ca9ccb5-e020-4f78-b634-36902fdccb59.csv"
//         },
//         "QueryExecutionContext": {},
//         "Status": {
//           "State": "SUCCEEDED",
//           "SubmissionDateTime": 1567159291.603,
//           "CompletionDateTime": 1567159369.977
//         },
//         "Statistics": {
//           "EngineExecutionTimeInMillis": 78021,
//           "DataScannedInBytes": 397917982675
//         },
//         "WorkGroup": "primary"
//       }
//     ],
//     "UnprocessedQueryExecutionIds": []
//   };
// }
