#!/usr/bin/env node

const {
  createConnection,
  getInformationSchema,
  printSchema
} = require('../dist');

const getopt = require('../lib/getopt');

const options = getopt([
  ['  ', '--dialect'],
  ['-u', '--user'],
  ['-p', '--password'],
  ['  ', '--json', true],
  ['  ', '--export', true]
]);

if (options.json || options.export) {
  const database = options.argv[0];

  const connection = createConnection(options.dialect || 'mysql', {
    user: options.user,
    password: options.password,
    database
  });

  getInformationSchema(connection, database).then(schema => {
    if (options.export) {
      console.log(printSchema(schema));
    } else {
      console.log(JSON.stringify(schema, null, 4));
    }
    connection.disconnect();
  });
}
