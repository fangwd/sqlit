import helper = require('./helper');
import { selectTree } from '../src/select';

const NAME = 'select';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('selectTree', async () => {
  const db = helper.connectToDatabase(NAME);

  {
    const result = await selectTree(db.table('order'), { id: 1 });
    expect(result['OrderItem'].size).toBeGreaterThan(0);
    expect(result['OrderShipping'].size).toBeGreaterThan(0);
    db.clear();
  }

  {
    const result = await selectTree(db.table('Product'), { id: 3 });
    expect(result['CategoryTree'].size).toBeGreaterThan(0);
    db.clear();
  }

  {
    const result = await db.table('product').selectTree({ id: 3 });
    expect(result[0].categories[0].name.length).toBeGreaterThan(0);
    expect(result[0].categories[0].products.length).toBeGreaterThan(0);
  }
});

test('select foreign key fields', async () => {
  const db = helper.connectToDatabase(NAME);
  const connection = await db.pool.getConnection();
  const rows = await db.table('user_group').select(
    {
      group: '*'
    },
    {},
    undefined,
    connection
  );
  expect((rows[0] as any).group.name.length).toBeGreaterThan(0);
  expect(connection.queryCounter.total).toBe(1);
});

test('select related fields of foreign key fields', async () => {
  const db = helper.connectToDatabase(NAME);
  const connection = await db.pool.getConnection();
  const rows = await db.table('user_group').select(
    {
      group: {
        userGroups: {
          fields: {
            user: '*'
          }
        }
      }
    },
    {},
    undefined,
    connection
  );
  expect(connection.queryCounter.total).toBe(3);
  const row = rows.find((row: any) => row.group.name === 'ADMIN') as any;
  expect(row.group.userGroups.length).toBe(2);
  const alice = row.group.userGroups.find(r => r.user.firstName === 'Alice');
  expect(alice).not.toBe(undefined);
  const bob = row.group.userGroups.find(r => r.user.firstName === 'Bob');
  expect(bob).not.toBe(undefined);
});
