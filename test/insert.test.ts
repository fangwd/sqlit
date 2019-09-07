import helper = require('./helper');

const NAME = 'insert';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('insert one (auto id)', async done => {
  const db = helper.connectToDatabase(NAME);
  const conn = await db.pool.getConnection();
  const insertId = await conn.insert(db.table('product'), ['sku'], ['sku #1']);
  const product = await db.table('product').get(insertId);
  expect(product.sku).toBe('sku #1');
  done();
});

test('insert one (manual id)', async done => {
  const db = helper.connectToDatabase(NAME);
  const conn = await db.pool.getConnection();
  const insertId = await conn.insert(
    db.table('product'),
    ['id', 'sku'],
    [100, 'sku #2']
  );
  const product = await db.table('product').get(insertId);
  expect(product.sku).toBe('sku #2');
  done();
});

test('insert many (auto id)', async done => {
  const db = helper.connectToDatabase(NAME);
  const conn = await db.pool.getConnection();
  const insertId = await conn.insert(
    db.table('product'),
    ['sku'],
    [['sku #3'], ['sku#4']]
  );
  const product = await db.table('product').get(insertId);
  expect(product.sku).toBe('sku #1');
  done();
});
