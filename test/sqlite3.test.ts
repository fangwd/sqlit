import helper = require('./helper');
import { _ConnectionPool as Pool } from '../src/engine/sqlite3';

const NAME = 'sqlite3';

if (helper.DB_TYPE === 'sqlite3') {
  beforeAll(() => helper.createDatabase(NAME));
  //afterAll(() => helper.dropDatabase(NAME));

  test('pool', async done => {
    const options = {
      database: helper.getDatabaseName(NAME),
      connectionLimit: 3
    };

    const pool = new Pool(options);

    const conn = await pool.getConnection();
    expect(pool.connectionCount).toBe(1);
    expect(pool.pool.length).toBe(0);

    conn.release();
    expect(pool.connectionCount).toBe(1);
    expect(pool.pool.length).toBe(1);

    const conn2 = await pool.getConnection();
    expect(conn2).toBe(conn);
    expect(pool.connectionCount).toBe(1);
    expect(pool.pool.length).toBe(0);

    const conn3 = await pool.getConnection();
    expect(conn3).not.toBe(conn2);
    expect(pool.connectionCount).toBe(2);
    expect(pool.pool.length).toBe(0);

    const rows = await conn3.query('select * from user');
    expect(rows.length).toBeGreaterThan(0);

    conn2.release();
    conn3.release();
    expect(pool.connectionCount).toBe(2);
    expect(pool.pool.length).toBe(2);

    done();
  });

  test('escape', async done => {
    const db = helper.connectToDatabase(NAME);
    const sku = '\\\\';
    const name = "Fancy's \\ House";
    db.table('product').append({ sku, name });
    await db.flush();
    const rows = await db.table('product').select('*', { where: { sku } });
    expect(rows.length).toBe(1);
    expect(rows[0].name).toBe(name);
    done();
  });
}
