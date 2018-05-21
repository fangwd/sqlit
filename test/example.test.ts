const { Database } = require('../src/database');

test('example', async done => {
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

  const schema = await db.buildSchema();
  const user = db.User({ email: 'user@example.com' });
  const post = db.Post({ title: 'My first post', user });

  user.firstPost = post;

  await user.save();
  const posts = await db.table('post').select('*', { where: { user } });

  expect(posts.length).toBe(1);
  expect(post.user.id).toBe(user.id);
  expect(user.firstPost.id).toBe(post.id);

  done();
});
