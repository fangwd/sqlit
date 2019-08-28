import helper = require('./helper');

const NAME = 'postgres';

test('createDatabase', async done => {
  if (helper.DB_TYPE !== 'postgres') {
    return done();
  }
  const db = await helper.createPostgresDatabase(NAME);
  const result = await db.query('select * from product');
  expect(result.rows.length).toBeGreaterThan(0);
  // database "test" is being accessed by other users
  await db.end();
  await helper.dropPostgresDatabase(NAME);
  done();
});
