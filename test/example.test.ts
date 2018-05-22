const { Database } = require('../src/database');

import helper = require('./helper');

const NAME = 'example';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('example', async done => {
  const db = getDatabase();

  await db.buildSchema();

  const { User, Post } = db.models();

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

//   const { User, Group, Post } = db.models();

//   const user = new User({ email: 'user02@example.com' });
//   const group = new Group({ name: 'Group 02' });

//   user.groups.add(group);

//   await user.save();

//   expect(
//     (await db.table('user_group').select('*', { where: { user } })).length
//   ).toBe(1);

//   user.groups.delete(group);

//   await user.save();

//   expect(
//     (await db.table('user_group').select('*', { where: { user } })).length
//   ).toBe(0);

//   done();
// });

function getDatabase() {
  return new Database({
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
}
