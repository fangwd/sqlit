import helper = require('./helper');
import { selectTree } from '../src/select';

const NAME = 'select';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('selectTree', async () => {
  const db = helper.connectToDatabase(NAME);

  {
    const result = await selectTree(db.table('order'), { id: 1 });
    expect(result['order_item'].size).toBeGreaterThan(0);
    expect(result['order_shipping'].size).toBeGreaterThan(0);
    db.clear();
  }

  {
    const result = await selectTree(db.table('product'), { id: 3 });
    expect(result['category_tree'].size).toBeGreaterThan(0);
    db.clear();
  }
});
