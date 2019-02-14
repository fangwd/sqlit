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
