import { Schema, ForeignKeyField } from '../src/model';
import { Value } from '../src/engine';
import helper = require('./helper');

const NAME = 'copy';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('copy', async done => {
  const db = helper.connectToDatabase(NAME);
  const order = db.getModels().Order({ id: 1 });
  await order.copy({ code: 'order-1-copy' });
  db.table('order')
    .select('*', { where: { code: 'order-1-copy' } })
    .then(rows => {
      expect(rows.length).toBe(1);
      done();
    });
});
