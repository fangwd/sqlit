import { Database } from '../src/database';
import { Schema, SimpleField } from '../src/model';

import helper = require('./helper');

const NAME = 'example';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('query', async done => {
  const { User, OrderItem, Category } = helper
    .connectToDatabase(NAME)
    .getModels();

  {
    const users = await User.table.select('*');
    const fields = User.fields.filter(field => field instanceof SimpleField);
    expect(Object.keys(users[0]).length).toBe(fields.length);
  }

  {
    const users = await User.table.select(['firstName', 'lastName']);
    expect(Object.keys(users[0]).length).toBe(2);
    expect('firstName' in users[0]).toBe(true);
  }

  {
    const users = await User.table.select({
      orders: { code: false },
      userGroups: { fields: { group: {} } }
    });

    // Alice is an admin
    const alice = users.find(user => user.email.startsWith('alice'));
    expect(alice.userGroups[0].group.name).toBe('ADMIN');

    // Grace has orders
    const grace = users.find(user => user.email.startsWith('grace'));
    expect(grace.orders.length).toBeGreaterThan(0);
  }

  {
    const items = await OrderItem.table.select({
      order: { user: '*' }
    });

    // Grace has orders
    const rows = items.filter(item =>
      item.order.user.email.startsWith('grace')
    );

    expect(rows.length).toBeGreaterThan(0);
  }

  {
    const users = await User.table.select('*', {
      where: {
        orders_some: {
          orderItems_some: '*'
        }
      }
    });
    const grace = users.find(user => user.email.startsWith('grace'));
    expect(!!grace).toBe(true);
  }

  {
    const fields = {
      products: {
        fields: {
          name: 'title'
        },
        where: { name_like: '%' },
        orderBy: ['name asc']
      }
    };

    const rows = await Category.table.select(fields, {
      where: { name: 'Apple' }
    });

    expect(rows[0].products[0].title).toBe('American Apple');
  }

  {
    const fields = {
      products: {
        fields: {
          name: 'title'
        },
        where: { name_like: '%' },
        orderBy: ['name desc'],
        limit: 1
      }
    };

    const rows = await Category.table.select(fields);

    const apple = rows.find(row => row.name === 'Apple');
    expect(apple.products.length).toBe(1);
    expect(apple.products[0].title).toBe('Australian Apple');

    const banana = rows.find(row => row.name === 'Banana');
    expect(banana.products.length).toBe(1);
    expect(banana.products[0].title).toBe('Australian Banana');
  }

  done();
});

test('create', async done => {
  const db = await getDatabase();

  const { User, Post } = db.getModels();

  const user = new User({ email: 'user@example.com' });

  const post = new Post({ title: 'My first post', user });

  user.firstPost = post;

  await user.save();

  const posts = await db.table('post').select('*', { where: { user } });

  expect(posts.length).toBe(1);
  expect(post.user.id).toBe(user.id);
  expect(user.firstPost.id).toBe(post.id);

  done();
});

// test('models', async done => {
//   const db = getDatabase();

//   const { User, Order } = db.getModels();

//   const user = new User({ email: 'user02@example.com' });
//   const order = new Order({ code: 'order-2' });

//   await user.orders.add(order);

//   expect(
//     (await db.table('order').select('*', { where: { user } })).length
//   ).toBe(1);

//   user.orders.remove(order);

//   await user.save();

//   expect(
//     (await db.table('order').select('*', { where: { user } })).length
//   ).toBe(0);

//   done();
// });

function getDatabase() {
  const db = new Database({
    dialect: 'mysql',
    connection: {
      host: 'localhost',
      user: 'root',
      password: 'secret',
      database: 'blog',
      timezone: 'Z',
      connectionLimit: 10
    }
  });

  return db.buildSchema().then(() => db);
}

// https://github.com/nodejs/node/issues/8071
require('util').inspect.defaultOptions.customInspect = false;
