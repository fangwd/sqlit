const { createConnection, getInformationSchema } = require('../dist');
const getopt = require('../lib/getopt');

const options = getopt(
  [
    ['  ', '--dialect'],
    ['-u', '--user'],
    ['-p', '--password'],
    ['  ', '--json', true]
  ],
  process.argv,
  2
);

if (options.json) {
  const database = options.argv[0];

  const connection = createConnection(options.dialect || 'mysql', {
    user: options.user,
    password: options.password,
    database
  });

  getInformationSchema(connection, database).then(schema => {
    console.log(JSON.stringify(schema, null, 4));
    connection.disconnect();
  });
}
