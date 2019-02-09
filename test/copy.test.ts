import helper = require('./helper');
import { Schema } from '../src';

const NAME = 'copy';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('copy', async done => {
  const db = helper.connectToDatabase(NAME);
  const order = db.getModels().Order({ id: 1 });
  await order.copy({ code: 'order-1-copy' });
  db.table('order')
    .select({ orderItems: '*' }, { where: { code: 'order-1-copy' } })
    .then(rows => {
      expect(rows.length).toBe(1);
      expect(rows[0].orderItems.length).toBe(2);
      done();
    });
});

test('replace', async done => {
  const db = helper.connectToDatabase(NAME);

  {
    const data = { email: 'alice@example.com', lastName: 'Blue' };
    const user = await db.table('user').replace(data);
    expect(user.lastName).toBe('Blue');
    const rows = await db
      .table('user')
      .select('*', { where: { email: 'alice@example.com' } });
    expect(rows[0].lastName).toBe('Blue');
  }

  {
    const data = {
      id: 3,
      orders: [
        { id: 1, status: 10 },
        { code: 'order-2', status: 20 },
        { code: 'order-3', status: 30 }
      ]
    };
    await db.table('user').replace(data);
    const rows = await db
      .table('order')
      .select('*', { where: { user: { id: 3 } }, orderBy: 'code' });
    expect(rows.length).toBe(3);
    expect(rows[0].status).toBe(10);
    expect(rows[2].status).toBe(30);
  }

  {
    const data = {
      id: 3,
      orders: [{ id: 1, status: 100 }, { code: 'order-3', status: 300 }]
    };
    await db.table('user').replace(data);
    const rows = await db
      .table('order')
      .select('*', { where: { user: { id: 3 } }, orderBy: 'code' });
    expect(rows.length).toBe(2);
    expect(rows[0].status).toBe(100);
    expect(rows[1].status).toBe(300);
  }

  {
    const data = {
      id: 3,
      orders: [
        {
          id: 1,
          status: 100,
          orderItems: [
            {
              product: { id: 1 },
              quantity: 100
            },
            {
              product: { id: 3 },
              quantity: 200
            }
          ]
        },
        {
          code: 'order-4',
          orderItems: [
            {
              product: { id: 2 },
              quantity: 500
            }
          ]
        },
        {
          code: 'order-5',
          status: 1
        },
        {
          code: 'order-6',
          status: 2
        }
      ]
    };
    await db.table('user').replace(data);
    const rows = await db
      .table('order_item')
      .select('*', { orderBy: ['order.code', 'product.id'] });
    expect(rows.length).toBe(3);
    expect(rows[0].quantity).toBe(100);
    expect(rows[2].quantity).toBe(500);
  }

  done();
});

test('selectTree', async () => {
  const db = helper.connectToDatabase(NAME);

  await db.table('order').selectTree({ id: 1 });
  expect(db.table('order_item').recordList.length).toBeGreaterThan(0);
  expect(db.table('order_shipping').recordList.length).toBeGreaterThan(0);
  db.clear();

  await db.table('product').selectTree({ id: 3 });
  expect(db.table('category_tree').recordList.length).toBeGreaterThan(0);
  db.clear();

  const options = {
    models: [
      {
        table: 'product_category',
        fields: [{ column: 'category_id', throughField: null }]
      }
    ]
  };
  const schema = new Schema(helper.getExampleData(), options);
  const db2 = helper.connectToDatabase(NAME, schema);
  await db2.table('product').selectTree({ id: 3 });
});
