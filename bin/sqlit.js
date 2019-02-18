#!/usr/bin/env node

const {
  Database,
  printSchema,
  printSchemaJava,
  printSchemaTypeScript,
  pluraliser,
  XstreamSerialiser,
  selectTree
} = require('../dist');

const getopt = require('../lib/getopt');

const options = getopt([
  ['  ', '--dialect'],
  ['-u', '--user'],
  ['-p', '--password'],
  ['  ', '--json', true],
  ['  ', '--export', true],
  ['  ', '--java', true],
  ['  ', '--typescript', true],
  ['  ', '--path'],
  ['  ', '--package'],
  ['  ', '--select'],
  ['  ', '--xstream', true],
  ['  ', '--xml', true]
]);

(async function() {
  const db = new Database({
    dialect: options.dialect || 'mysql',
    connection: {
      user: options.user,
      password: options.password,
      database: options.argv[0],
      timezone: 'Z'
    }
  });

  pluraliser.style = 'java';

  await db.buildSchema();

  const schema = db.schema;

  if (options.export) {
    if (options.java) {
      printSchemaJava(schema, options.path, options.package);
    } else if (options.typescript) {
      console.log(printSchemaTypeScript(schema));
    } else {
      console.log(printSchema(schema));
    }
  } else if (options.select) {
    const [name, value] = options.select.split(':');
    const table = db.table(name);
    const pk = table.model.keyField().name;
    const filter = { [pk]: value };
    if (options.xstream || options.xml) {
      const result = await selectTree(table, filter);
      const serialiser = new XstreamSerialiser(result);
      console.log(serialiser.serialise(table.model));
    } else {
      const result = await table.selectTree(filter);
      console.log(JSON.stringify(result));
    }
  } else {
    console.log(JSON.stringify(schema.database, null, 4));
  }

  db.end();
})();
