import { Schema } from 'sqlex';
import { Database } from '../src/database';
import { Record } from '../src/record';

import helper = require('./helper');

const NAME = 'flush';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('append', () => {
  const schema = new Schema(helper.getExampleData());
  const db = new Database(null, schema);
  const user = db.append('user', { email: 'user@example.com' });
  const user2 = db.append('user', { email: 'user@example.com' });
  expect(user).toBe(user2);
  expect(user instanceof Record).toBe(true);
  expect(db.table('user').recordList.length).toBe(1);
  user.status = 200;
  expect(user.status).toBe(200);
  expect(() => (user.whatever = 200)).toThrow();
});

test('append #2', () => {
  const schema = new Schema(helper.getExampleData());
  const db = new Database(null, schema);
  const user = db.getModels().User({ email: 'user@example.com' });
  expect(user instanceof Record).toBe(true);
  expect(user.email).toBe('user@example.com');
  expect(user.get('email')).toBe('user@example.com');
  expect(user.__table).toBe(db.table('user'));
  expect(db.table('user').recordList.length).toBe(0);
});

test('delete', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const table = db.table('user');
  const id = await table.insert({ email: 'deleted@example.com' });
  const row = await table.get({ id });
  expect(row.email).toBe('deleted@example.com');
  const record = db.getModels().User({ email: 'deleted@example.com' });
  const deleted = record.delete();
  record.delete().then(async () => {
    expect(await table.get({ id })).toBe(undefined);
    done();
  });
});

test('update', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const table = db.table('user');
  const id = await table.insert({ email: 'updated@example.com', status: 100 });
  const row = await table.get({ id });
  expect(row.status).toBe(100);
  const user = db.getModels().User({ email: 'updated@example.com' });
  await user.update({ status: 200 });
  expect((await table.get({ id })).status).toBe(200);
  done();
});

test('save #1', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const user = db.getModels().User({ email: 'saved01@example.com' });
  user.save().then(async row => {
    expect(row.email).toBe('saved01@example.com');
    const user = await db.table('user').get({ email: 'saved01@example.com' });
    expect(user.id).toBe(row.id);
    done();
  });
});

test('save #2', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const models = db.getModels();
  const user = models.User({ email: 'saved02@example.com' });
  const order = models.Order({ code: 'saved02' });
  order.user = user;
  const saved = await user.save();
  await order.save();
  const saved2 = await db.table('order').get({ code: 'saved02' });
  expect(saved2.user.id).toBe(saved.id);
  done();
});

test('save #3', done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const models = db.getModels();
  const user = models.User({ email: 'saved03@example.com' });
  const order_1 = models.Order({ code: 'saved03-1', user });
  const order_2 = models.Order({ code: 'saved03-2', user });
  Promise.all([order_1.save(), order_2.save()]).then(async () => {
    const saved_0 = await db
      .table('user')
      .get({ email: 'saved03@example.com' });
    const saved_1 = await db.table('order').get({ code: 'saved03-1' });
    const saved_2 = await db.table('order').get({ code: 'saved03-1' });
    expect(saved_1.user.id).toBe(saved_0.id);
    expect(saved_2.user.id).toBe(saved_0.id);
    done();
  });
});

test('save #4', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const models = db.getModels();
  const user = models.User({ email: 'saved04@example.com' });
  const order_1 = models.Order({ code: 'saved04-1', user });
  const order_2 = models.Order({ code: 'saved04-2', user });
  user.status = order_2;
  await order_1.save();
  await user.save();
  const saved_0 = await db.table('user').get({ email: 'saved04@example.com' });
  const saved_1 = await db.table('order').get({ code: 'saved04-1' });
  const saved_2 = await db.table('order').get({ code: 'saved04-2' });
  expect(saved_1.user.id).toBe(saved_0.id);
  expect(saved_2.user.id).toBe(saved_0.id);
  expect(saved_0.status).toBe(saved_2.id);
  done();
});

test('save #5', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const User = db.getModels().User;
  const email = 'saved05@example.com';
  const promises = [];
  promises.push(User({ email }).save());
  promises.push(User({ email, status: 200 }).save());
  promises.push(User({ email }).save());
  Promise.all(promises).then(async () => {
    const user = await db.table('user').get({ email });
    expect(user.email).toBe(email);
    expect(user.status).toBe(200);
    done();
  });
});

test('flush #1', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const table = db.table('category');
  let parent = table.append({ id: 1 });

  await table.insert({ name: 'Child 0', parent: 1 });
  for (let i = 0; i < 5; i++) {
    table.append({
      name: `Child ${i % 3}`,
      parent
    });
  }

  expect(table.recordList.length).toBe(4);

  table.clear();

  parent = table.append({ id: 1 });

  for (let i = 0; i < 5; i++) {
    const rec = table.append();
    rec.name = `Child ${i % 3}`;
    rec.parent = parent;
  }

  expect(table.recordList.length).toBe(6);
  expect(table.recordList[1].__dirty()).toBe(true);

  db.flush().then(async () => {
    const rows = table.recordList;
    expect(rows[3].__state.merged).toBe(null);
    expect(rows[4].__state.merged).toBe(rows[1]);
    expect(rows[5].__state.merged).toBe(rows[2]);
    let rec = await table.get({ id: rows[2].id });
    expect(rec.name).toBe('Child 1');
    done();
  });
});

test('flush #2', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const user = db.table('user').append();
  user.email = 'random';
  const order = db.table('order').append({ code: 'random' });
  order.user = user;
  user.status = order;

  db.flush().then(async () => {
    const user = await db.table('user').get({ email: 'random' });
    const order = await db.table('order').get({ code: 'random' });
    expect(user.status).toBe(order.id);
    done();
  });
});

test('flush #3', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const email = helper.getId();
  const user = db.table('user').append({ email });

  const user2 = db.table('user').append();
  user2.email = email;

  const email2 = helper.getId();
  const user3 = db.table('user').append({ email: email2 });

  const code = helper.getId();

  const order = db.table('order').append({ code });
  order.user = user2;
  user2.status = order;

  const code2 = helper.getId();

  const order2 = db.table('order').append({ code: code2 });
  order2.user = user2;
  user3.status = order2;

  db.flush().then(async connection => {
    expect(connection.queryCounter.total).toBe(8);
    const user = await db.table('user').get({ email });
    const order = await db.table('order').get({ code });
    expect(order.user.id).toBe(user.id);
    expect(user.status).toBe(order.id);
    const user3 = await db.table('user').get({ email: email2 });
    const order2 = await db.table('order').get({ code: code2 });
    expect(user3.status).toBe(order2.id);
    expect(order2.user.id).toBe(user.id);
    done();
  });
});

test('flush #4', async done => {
  if (process.env.DB_TYPE === 'sqlite3') {
    return done();
  }

  const schema = new Schema(helper.getExampleData());

  // 3 connections
  const dbs = [...Array(3).keys()].map(x =>
    helper.connectToDatabase(NAME, schema)
  );

  // 5 users
  const emails = [...Array(5).keys()].map(x => helper.getId());

  // 10 orders
  const codes = [...Array(10).keys()].map(x => helper.getId());

  // Each user has a number of orders
  const map: { [key: string]: string[] } = {};
  emails.forEach((email, i) => {
    map[email] = [codes[2 * i], codes[2 * i + 1]];
  });

  dbs.forEach((db, index) => {
    for (let i = 0; i < 3; i++) {
      for (const email of emails) {
        const user = db.table('user').append({ email });
        let order;
        for (const code of map[email]) {
          order = db.table('order').append({
            code,
            user
          });
        }
        user.status = order;
      }
    }
  });

  const userCount = (await dbs[0].table('user').select('*')).length;
  const orderCount = (await dbs[0].table('order').select('*')).length;

  const promises = dbs.map(db => db.flush());

  Promise.all(promises).then(async () => {
    const db = helper.connectToDatabase(NAME, schema);
    const users = await db.table('user').select('*');
    const orders = await db.table('order').select('*');
    expect(users.length).toBe(userCount + 5);
    expect(orders.length).toBe(orderCount + 10);
    let ok = true;
    for (const email in map) {
      const user = users.find(x => x.email === email);
      let order;
      for (const code of map[email]) {
        order = orders.find(x => x.code === code);
        ok = ok && order.user.id === user.id;
      }
      ok = ok && user.status === order.id;
    }
    expect(ok).toBe(true);
    done();
  });
});

test('flush #5', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);
  const order = db.table('order').append({ code: helper.getId() });

  try {
    order.dateCreated = '23-06-2017';
  } catch (error) {
    expect(/Invalid time value/.test(error.message)).toBe(true);
    db.flush().then(() => done());
  }
});

test('flush #6', async done => {
  const schema = new Schema(helper.getExampleData());
  const db = helper.connectToDatabase(NAME, schema);

  const email = helper.getId();
  db.table('user').append({ email, firstName: 'name' });
  await db.flush();
  db.clear();

  db.table('user').append({ email, firstName: 'name-1' });

  const email2 = helper.getId();
  db.table('user').append({ email: email2, firstName: 'name-2' });

  const email3 = helper.getId();
  db.table('user').append({ email: email3, lastName: 'name-3' });

  const email4 = helper.getId();
  db.table('user').append({ email: email4, lastName: 'name-4' });

  await db.flush();

  let user;

  user = await db.table('user').get({ email });
  expect(user.firstName).toBe('name-1');

  user = await db.table('user').get({ email: email2 });
  expect(user.firstName).toBe('name-2');

  user = await db.table('user').get({ email: email3 });
  expect(user.lastName).toBe('name-3');

  user = await db.table('user').get({ email: email4 });
  expect(user.lastName).toBe('name-4');

  done();
});

test('afterBegin', async done => {
  const db = helper.connectToDatabase(NAME);

  const title = 'Example Post';
  const content = 'Example Comment';

  const post = db.table('post').append({ title });

  const comment = db
    .table('comment')
    .append({ post, content: content + '1', parent: null }) as any;

  const deleted = db
    .table('comment')
    .append({ post, content: content + '2', parent: null }) as any;

  await db.flush();

  db.clear();

  db.table('comment').append({
    id: comment.id,
    content: content + '3',
    parent: null
  });

  db.table('comment').append({ post, content: content + '4', parent: null });

  await db.flush({
    afterBegin: conn =>
      db.table('comment')._delete(conn, { post: (post as any).id })
  });

  const comments = await db
    .table('comment')
    .select('*', { where: { content_like: content + '%' } });

  expect(comments.length).toBe(2);
  expect(!!comments.find(c => c.id === comment.id)).toBe(true);
  if (process.env.DB_TYPE !== 'sqlite3') {
    expect(!!comments.find(c => c.id === deleted.id)).toBe(false);
  }

  done();
});

test('beforeCommit', async done => {
  const db = helper.connectToDatabase(NAME);

  const title = 'Example Post';
  const content = 'Example Comment';

  const post = db.table('post').append({ title });

  const comment = db
    .table('comment')
    .append({ post, content: content + '1', parent: null }) as any;

  const deleted = db
    .table('comment')
    .append({ post, content: content + '2', parent: null }) as any;

  await db.flush();

  db.clear();

  db.table('comment').append({
    id: comment.id,
    content: content + '3',
    parent: null
  });

  db.table('comment').append({ post, content: content + '4', parent: null });

  await db.flush({
    beforeCommit: conn => {
      const ids = db.table('comment').recordList.map(r => (r as any).id);
      return db.table('comment')._delete(conn, { not: { id: ids } });
    }
  });

  const comments = await db
    .table('comment')
    .select('*', { where: { content_like: content + '%' } });

  expect(comments.length).toBe(2);
  expect(!!comments.find(c => c.id === comment.id)).toBe(true);
  expect(!!comments.find(c => c.id === deleted.id)).toBe(false);

  done();
});

test('replaceRecordsIn (1)', async done => {
  const db = helper.connectToDatabase(NAME);

  const alice = db.append('user', { email: 'alice' });
  const bob = db.append('user', { email: 'bob' });

  const createOrders = user => {
    for (let i = 1; i <= 3; i++) {
      const order = db
        .table('order')
        .append({ code: `${user.email}-a${i}`, user });
      for (let j = 1; j <= 3; j++) {
        db.table('order_item').append({ order, product: j, quantity: j });
      }
    }
  };

  createOrders(alice);
  createOrders(bob);

  await db.flush();

  db.clear();

  const order = db.table('order').append({ code: `alice-a1`, user: alice });

  for (let j = 2; j <= 4; j++) {
    db.table('order_item').append({ order, product: j, quantity: j + 1 });
  }

  db.table('order').append({ code: `bob-a1`, user: bob });

  await db.flush({ replaceRecordsIn: ['order'] });

  const aliceItems = await db
    .table('order_item')
    .select('*', { where: { order: { code: 'alice-a1' } } });

  expect(aliceItems.length).toBe(4);

  expect(aliceItems.find(item => (item.product as any).id === 3).quantity).toBe(
    4
  );

  const bobItems = await db
    .table('order_item')
    .select('*', { where: { order: { code: 'bob-a1' } } });

  expect(bobItems.length).toBe(3);

  done();
});

test('replaceRecordsIn (2)', async done => {
  const db = helper.connectToDatabase(NAME);

  let alice = db.append('user', { email: 'alice' });
  let bob = db.append('user', { email: 'bob' });

  const createOrders = user => {
    for (let i = 1; i <= 3; i++) {
      const order = db
        .table('order')
        .append({ code: `${user.email}-b${i}`, user });
      for (let j = 1; j <= 3; j++) {
        db.table('order_item').append({ order, product: j, quantity: j });
      }
    }
  };

  createOrders(alice);
  createOrders(bob);

  await db.flush();

  db.clear();

  alice = db.append('user', { email: 'alice' });
  bob = db.append('user', { email: 'bob' });

  const order = db.table('order').append({ code: `alice-b1`, user: alice });
  for (let j = 2; j <= 4; j++) {
    db.table('order_item').append({ order, product: j, quantity: j + 1 });
  }

  db.table('order').append({ code: `bob-b1`, user: bob });
  db.table('order').append({ code: `bob-b2`, user: bob });

  await db.flush({ replaceRecordsIn: ['user', 'order', 'order_item'] });

  const aliceOrders = await db
    .table('order')
    .select('*', { where: { user: alice } });

  expect(aliceOrders.length).toBe(1);

  const aliceItems = await db
    .table('order_item')
    .select('*', { where: { order: { code: 'alice-b1' } } });

  expect(aliceItems.length).toBe(3);

  expect(aliceItems.find(item => (item.product as any).id === 3).quantity).toBe(
    4
  );

  const bobOrders = await db
    .table('order')
    .select('*', { where: { user: bob } });

  expect(bobOrders.length).toBe(2);

  const bobItems = await db
    .table('order_item')
    .select('*', { where: { order: { code_like: 'bob-b%' } } });

  expect(bobItems.length).toBe(0);

  done();
});

function createTestPosts(db: Database, user) {
  const allPosts = [];
  const allComments = [];

  for (let i = 0; i < 4; i++) {
    const post = db.table('post').append({ title: '', user });
    const comments = [];
    comments.push(
      db.table('comment').append({ post, parent: null, content: '' })
    );
    comments.push(
      db.table('comment').append({ post, parent: null, content: '' })
    );
    for (let j = 0; j < 2; j++) {
      const comment = db
        .table('comment')
        .append({ post, parent: comments[0], content: '' });
      comments.push(comment);
      for (let k = 0; k < 2; k++) {
        comments.push(
          db.table('comment').append({ post, parent: comment, content: '' })
        );
      }
    }
    allPosts.push(post);
    allComments.push(comments);
  }

  return [allPosts, allComments];
}

test('replaceRecordsIn (3)', async done => {
  const db = helper.connectToDatabase(NAME);

  let user = db.append('user', { email: 'alice' });

  const [allPosts, allComments] = createTestPosts(db, user);

  await db.flush();

  db.clear();

  user = db.append('user', { email: 'alice' });

  // post with title '0', no comments
  db.table('post').append({ id: allPosts[0].id, title: '0', user });

  /*
                                               +-------+
                                               |   P   |
                                               ++---+--+
                                                ^   ^
                                                |   |
                                                |   |
                             +---------+        |   |          +---------+
                             | [0] 0   +--------+   +----------+ [1] 1   |
                             +-+-----+-+                       +---------+
                               ^     ^
                               |     |
           +---------+         |     |      +---------+
           | [2] 0.0 +---------+     +------+ [5] 0.1 |
           ++-----+--+                      ++------+-+
            ^     ^                          ^      ^
            |     |                          |      |
            |     |                          |      |
+-----------++   ++-----------+   +----------+-+   ++-----------+
| [3] 0.0.0  |   | [4] 0.0.1  |   | [6] 0.1.0  |   | [7] 0.1.1  |
+------------+   +------------+   +------------+   +------------+
*/

  {
    // post with title '1', just comment 0, comment 0.0, comment 0.0.1 and 1
    let post = db
      .table('post')
      .append({ id: allPosts[1].id, title: '1', user });

    const comments = allComments[1];

    const c0 = db.table('comment').append({
      id: comments[0].id,
      post,
      parent: null,
      content: '0'
    });

    const c1 = db.table('comment').append({
      id: comments[2].id,
      post,
      parent: c0,
      content: '0.0'
    });

    db.table('comment').append({
      id: comments[4].id,
      post,
      parent: c1,
      content: '0.0.1'
    });

    db.table('comment').append({
      id: comments[1],
      post,
      parent: null,
      content: '1'
    });
  }
  {
    // post with title '2', just comment 0, comment 0.1, comment 0.1.0
    let post = db
      .table('post')
      .append({ id: allPosts[2].id, title: '2', user });

    const comments = allComments[2];

    const c0 = db.table('comment').append({
      id: comments[0].id,
      post,
      parent: null,
      content: '0'
    });

    const c1 = db.table('comment').append({
      id: comments[5].id,
      post,
      parent: c0,
      content: '0.1'
    });

    const c2 = db.table('comment').append({
      id: comments[6].id,
      post,
      parent: c1,
      content: '0.1.0'
    });

    db.table('comment').append({
      post,
      parent: c2,
      content: '0.1.1'
    });
  }

  await db.flush({ replaceRecordsIn: ['user', 'post', 'comment'] });

  const posts = await db
    .table('post')
    .select('*', { where: { user }, orderBy: 'title' });

  expect(posts.length).toBe(3);
  expect(posts[0].title).toBe('0');
  expect(posts[1].title).toBe('1');
  expect(posts[2].title).toBe('2');

  {
    const comments = await db
      .table('comment')
      .select('*', { where: { post: posts[0].id }, orderBy: 'content' });
    expect(comments.length).toBe(0);
  }

  {
    const comments = await db
      .table('comment')
      .select('*', { where: { post: posts[1].id }, orderBy: 'content' });
    expect(comments[1].parent.id).toBe(comments[0].id);
    expect(comments[2].parent.id).toBe(comments[1].id);
    expect(comments.length).toBe(4);
  }

  {
    const comments = await db
      .table('comment')
      .select('*', { where: { post: posts[2].id }, orderBy: 'content' });
    expect(comments.length).toBe(4);
    expect(comments[3].parent.id).toBe(comments[2].id);
  }

  done();
});

test('replaceRecordsIn (4)', async done => {
  const db = helper.connectToDatabase(NAME);

  let user = db.append('user', { email: 'alice2' });

  const [allPosts, allComments] = createTestPosts(db, user);

  await db.flush();

  db.clear();

  const bob2 = db.append('user', { email: 'bob2' });

  db.table('post').append({ id: allPosts[0].id, title: '0', user: bob2 });
  db.table('post').append({ id: allPosts[1].id, title: '1', user: bob2 });

  await db.flush({ replaceRecordsIn: ['user', 'post'] });

  const posts = await db
    .table('post')
    .select('*', { where: { user: bob2.id }, orderBy: 'title' });

  expect(posts.length).toBe(2);

  const comments = await db
    .table('comment')
    .select('*', { where: { post: posts[1].id } });

  expect(comments.length).toBe(8);

  done();
});
