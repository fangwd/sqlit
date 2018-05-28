import { Database } from '../src/database';
import { Schema } from '../src/model';

import helper = require('./helper');

const NAME = 'example';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

// test('example', async done => {
//   const db = getDatabase();

//   const { User, Post } = db.getModels();

//   const user = new User({ email: 'user@example.com' });

//   const post = new Post({ title: 'My first post', user });

//   user.firstPost = post;

//   await user.save();

//   const posts = await db.table('post').select('*', { where: { user } });

//   expect(posts.length).toBe(1);
//   expect(post.user.id).toBe(user.id);
//   expect(user.firstPost.id).toBe(post.id);

//   done();
// });

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

// test('select', async done => {
//   const schema = new Schema(helper.getExampleData());
//   const db = helper.connectToDatabase(NAME, schema);

//   let fields;

//   fields = {
//     product: { name: 'title' },
//     order: { code: 'key', user: { password: false, firstName: 'name' } }
//   };

//   const rows = await db
//     .table('order_item')
//     .select(fields, { orderBy: ['order.user.firstName', 'quantity'] });

//   console.log(JSON.stringify(rows, null, 4));

//   done();
// });

test('select - one to many', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);

  let fields;

  fields = {
    code: 'key',
    user: { password: false, firstName: 'name' },
    orderItems: {
      fields: { product: { name: 'title' } },
      where: { product: { name_like: '%Banana%' } },
      limit: 1
    }
  };

  const rows = await db
    .table('order')
    .select(fields, { orderBy: ['user.firstName'] });

  console.log(JSON.stringify(rows, null, 4));

  done();
});

// test('select related', async done => {
//   const schema = new Schema(helper.getExampleData());
//   const db = helper.connectToDatabase(NAME, schema);

//   let fields;

//   fields = {
//     products: {
//       fields: '*',
//       /*
//       fields: {
//         product: { name: 'title' }
//       },
//       */
//       where: { name_like: '%' }
//     }
//   };

//   const rows = await db.table('category').select(fields, { orderBy: ['name'] });

//   console.log(JSON.stringify(rows, null, 4));

//   done();
// });

function getDatabase() {
  return new Database(
    {
      dialect: 'mysql',
      connection: {
        host: 'localhost',
        user: 'root',
        password: 'secret',
        database: 'blog',
        timezone: 'Z',
        connectionLimit: 10
      }
    },
    new Schema(
      JSON.parse(
        require('fs')
          .readFileSync('schema.json')
          .toString()
      )
    )
  );
}

// https://github.com/nodejs/node/issues/8071
require('util').inspect.defaultOptions.customInspect = false;
