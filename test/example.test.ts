import { Database } from '../src/database';
import { SimpleField } from '../src/model';

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
        orderBy: ['name']
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
        orderBy: ['-name'],
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
  const db = helper.connectToDatabase(NAME);

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

test('append #1', async done => {
  const db = helper.connectToDatabase(NAME);
  const title = 'Example Post #1';

  db.table('post').append({ title });

  await db.flush();

  const posts = await db.table('post').select('*', { where: { title } });

  expect(posts.length).toBe(1);

  done();
});

test('append #2', async done => {
  const db = helper.connectToDatabase(NAME);
  const title = 'Example Post #2';
  const content = 'Example Comment #';
  const post = db.table('post').append({ title });
  const comment = db.table('comment').append({ post, content });

  const comment1 = db
    .table('comment')
    .append({ post, content: content + '1', parent: comment });

  const comment2 = db.table('comment').append({
    post,
    content: content + '2',
    parent: comment1
  });

  db.table('comment').append({
    post,
    content: content + '3',
    parent: comment1
  });

  await db.flush();

  const posts = await db.table('post').select('*', { where: { title } });

  expect(posts.length).toBe(1);

  const comments = await db
    .table('comment')
    .select('*', { where: { content_like: content + '%' } });

  expect(comments.length).toBe(4);

  const commentId = comments.find(entry => entry.content === content + '1').id;
  const replies = comments.filter(
    entry => entry.parent && entry.parent.id === commentId
  );

  expect(replies.length).toBe(2);

  done();
});

test('select', async done => {
  const db = helper.connectToDatabase(NAME);

  const post = db.table('post').append({ title: '#3' });
  db.table('comment').append({ post, content: '#3.1' });
  db.table('comment').append({ post: null, content: '#.2' });

  await db.flush();

  const rows = await db.table('comment').select(
    {
      post: {
        comments: {
          content: 'body'
        }
      }
    },
    { where: { content_like: '#%' } }
  );

  expect(rows.length).toBe(2);

  const row = rows.find(row => row.post === null);

  expect(row.content).toBe('#.2');

  done();
});

// https://github.com/nodejs/node/issues/8071
require('util').inspect.defaultOptions.customInspect = false;
