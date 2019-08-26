import { Schema, setModelName } from 'sqlex';
import { Schema as SchemaConfig } from 'sqlex/dist/config';
import { getInformationSchema } from '../src/engine';
import helper = require('./helper');

const NAME = 'schema';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('getInformationSchema', async done => {
  const connection = helper.createTestConnection(NAME);
  console.log(connection.dialect);
  if (connection.dialect !== 'sqlite3') {
    const schemaInfo = await getInformationSchema(
      connection,
      helper.getDatabaseName(NAME)
    );
    const schema = new Schema(schemaInfo);
    const model = schema.model('order_shipping');
    expect(model.primaryKey.fields[0].name).toBe('order');
    expect(model.getForeignKeyCount(schema.model('order'))).toBe(1);
  }
  done();
});

test('setModelName', () => {
  const schema = new Schema(helper.getExampleData());
  const model = schema.model('post');
  const config: SchemaConfig = { models: [{ table: 'post', name: 'Post' }] };
  setModelName(config, model, 'WebPost');
  {
    const model = config.models.find(entry => entry.table === 'post');
    expect(model.name).toBe('WebPost');
  }
  {
    const model = config.models.find(entry => entry.table === 'user');
    const field = model.fields.find(entry => entry.column === 'first_post_id');
    expect(field.name).toBe('firstWebPost');
  }
  {
    const model = config.models.find(entry => entry.table === 'comment');
    const field = model.fields.find(entry => entry.column === 'post_id');
    expect(field.name).toBe('webPost');
  }
});
