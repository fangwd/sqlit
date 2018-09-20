import { Schema } from '../src/model';
import { getInformationSchema } from '../src/engine';
import helper = require('./helper');

const NAME = 'schema';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('getInformationSchema', async done => {
  const connection = helper.createTestConnection(NAME);
  const schemaInfo = await getInformationSchema(
    connection,
    helper.getDatabaseName(NAME)
  );
  const schema = new Schema(schemaInfo);
  const model = schema.model('order_shipping');
  expect(model.primaryKey.fields[0].name).toBe('order');
  expect(model.getForeignKeyCount(schema.model('order'))).toBe(1);
  done();
});
