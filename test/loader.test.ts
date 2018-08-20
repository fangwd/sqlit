import helper = require('./helper');

const NAME = 'loader';

beforeAll(() => helper.createDatabase(NAME));
afterAll(() => helper.dropDatabase(NAME));

test('load rows', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('category');

  table.append({
    name: 'All',
    parent: null
  });

  await db.flush();

  const parentId = table.recordList[0].id;

  db.clear();

  const config = {
    category: {
      categoryName: 'name',
      parent_name: 'parent.name',
      parent_parent: 'parent.parent'
    }
  };

  const data = [
    {
      categoryName: 'Example A1',
      parent_name: 'Example A1 Parent',
      parent_parent: ''
    },
    {
      categoryName: 'Example A2',
      parent_name: 'Example A2 Parent',
      parent_parent: parentId
    }
  ];

  await table.xappend(data, config.category);

  expect(table.recordList.length).toBe(5);

  let row = (await table.select('*', {
    where: { name: 'Example A1 Parent' }
  }))[0];

  expect(row.parent).toBe(null);

  row = (await table.select('*', {
    where: { name: 'Example A2 Parent' }
  }))[0];

  expect(row.parent.id).toBe(parentId);

  done();
});

test('load attributes', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('category');

  const config = {
    category: {
      categoryName: 'name',
      parent_name: 'parent.name',
      parent_parent: 'parent.parent',
      '*': 'categoryAttributes[name, value]'
    }
  };

  const data = [
    {
      categoryName: 'Example B1',
      parent_name: 'Example B1 Parent',
      parent_parent: '',
      colour: 'Red',
      weight: '100kg'
    }
  ];

  await table.xappend(data, config.category);

  const rows = await db.table('category_attribute').select('*', {
    where: { category: { name: 'Example B1' } },
    orderBy: ['name desc']
  });

  expect(rows.length).toBe(2);

  const category = (await db.table('category').select('*', {
    where: { name: 'Example B1' }
  }))[0];

  expect(rows[0].name).toBe('weight');
  expect(rows[0].value).toBe('100kg');
  expect(rows[0].category.id).toBe(category.id);
  expect(rows[1].name).toBe('colour');
  expect(rows[1].value).toBe('Red');
  expect(rows[1].category.id).toBe(category.id);

  done();
});

test('load with defaults', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('category');

  const config = {
    category: {
      categoryName: 'name',
      '*': 'categoryAttributes[name, value]'
    }
  };

  {
    const data = [
      {
        categoryName: 'Example C1',
        colour: 'Red',
        weight: '100kg'
      }
    ];

    await table.xappend(data, config.category, { parent: null });

    const rows = await db.table('category_attribute').select('*', {
      where: { category: { name: 'Example C1' } },
      orderBy: ['name desc']
    });

    expect(rows.length).toBe(2);

    const category = (await db.table('category').select('*', {
      where: { name: 'Example C1' }
    }))[0];

    expect(rows[0].name).toBe('weight');
    expect(rows[0].value).toBe('100kg');
    expect(rows[0].category.id).toBe(category.id);
    expect(rows[1].name).toBe('colour');
    expect(rows[1].value).toBe('Red');
    expect(rows[1].category.id).toBe(category.id);

    expect(category.parent).toBe(null);
  }

  {
    const data = [
      {
        categoryName: 'Example C2'
      }
    ];

    await table.xappend(data, config.category, { parent: 1 });

    const category = (await db.table('category').select('*', {
      where: { name: 'Example C2' }
    }))[0];

    expect(category.parent.id).toBe(1);
  }

  {
    const data = [
      {
        categoryName: 'Example C3'
      }
    ];

    await table.xappend(data, config.category, { parent: { id: 2 } });

    const category = (await db.table('category').select('*', {
      where: { name: 'Example C3' }
    }))[0];

    expect(category.parent.id).toBe(2);
  }

  {
    const config = {
      category: {
        categoryName: 'name',
        parent_name: 'parent.name'
      }
    };

    const data = [
      {
        categoryName: 'Example C4',
        parent_name: 'Random 4'
      }
    ];

    await table.xappend(data, config.category, {
      parent: { parent: { id: 3 } }
    });

    const category = (await db.table('category').select('*', {
      where: { name: 'Random 4' }
    }))[0];

    expect(category.parent.id).toBe(3);

    const category2 = (await db.table('category').select('*', {
      where: { name: 'Example C4' }
    }))[0];

    expect(category2.parent.id).toBe(category.id);
  }

  done();
});

test('load many to many #1', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('product');

  const config = {
    sku: 'sku',
    name: 'name',
    price: 'price',
    category: 'categories.name'
  };

  const data = [
    {
      sku: 'prod-1',
      name: 'Product 1',
      price: 10,
      category: 'Fancy'
    },
    {
      sku: 'prod-2',
      name: 'Product 2',
      price: 20,
      category: 'Fancy'
    }
  ];

  await table.xappend(data, config, { categories: { parent: 1 } });

  const products = await db.table('product').select('*', {
    where: { sku_like: 'prod-%' },
    orderBy: ['sku']
  });

  expect(products.length).toBe(2);

  const category = (await db.table('category').select('*', {
    where: { name: 'Fancy' }
  }))[0];

  expect(!!category).toBe(true);

  const rows = await db.table('product_category').select('*', {
    where: { product: { id_in: [products[0].id, products[1].id] }, category }
  });

  expect(rows.length).toBe(2);

  done();
});

test('select related', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('order');

  const rows = await table.select({
    user: { orders: { fields: { code: 'code' } } },

    orderItems: {
      fields: {
        id: 'id',
        product: {
          name: 'name',
          categories: {
            fields: '*'
          }
        }
      }
    }
  });

  // In the test data we only have 2 orders for the same user:
  expect(rows.length).toBe(2);
  expect(rows[0].user.orders.length).toBe(2);
  expect(rows[0].orderItems[0].product.categories.length).toBeGreaterThan(0);

  done();
});

test('select rows', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('category');

  const count = await db.table('product_category').count();

  const config = {
    name: 'name',
    parent_name: 'parent.name',
    product_name: 'products.name',
    product_price: 'products.price'
  };

  const docs = await table.xselect(config);

  expect(docs.length).toBe(count);
  expect(!!docs[count - 1].parent_name).toBe(true);
  expect(docs[count - 1].product_price).toBeGreaterThan(0);

  done();
});

test('select rows with attributes', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('category');

  const count = await db.table('product_category').count();

  const rows = await table.select('*');

  for (const r of rows) {
    db.table('category_attribute').append({
      category: r.id,
      name: 'my-id',
      value: r.id
    });
    db.table('category_attribute').append({
      category: r.id,
      name: 'my-name',
      value: r.name
    });
  }

  await db.flush();

  db.clear();

  const config = {
    name: 'name',
    parent_name: 'parent.name',
    product_name: 'products.name',
    product_price: 'products.price',
    '*': 'categoryAttributes[name, value]'
  };

  const docs = await table.xselect(config);

  expect(docs.length).toBe(count);
  expect(docs[count - 1]['my-name']).toBe(docs[count - 1].name);

  done();
});

test('load many to many #2', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('user');

  const config = {
    email: 'email',
    '*': 'orders[code]'
  };

  const data = [
    {
      email: 'test-user-1',
      x1: 'test-order-1',
      x2: 'test-order-2'
    },
    {
      email: 'test-user-2',
      x1: 'test-order-3'
    }
  ];

  await table.xappend(data, config, { orders: { status: 300 } });

  const users = await table.select('*', {
    where: { email_like: 'test-user-%' },
    orderBy: ['email']
  });

  expect(users.length).toBe(2);
  expect(users[0].email).toBe('test-user-1');
  expect(users[1].email).toBe('test-user-2');

  const rows = await db.table('order').select('*', {
    where: { code_like: 'test-order-%' },
    orderBy: ['code']
  });

  expect(rows.length).toBe(3);

  expect(rows[0].user.id).toBe(users[0].id);
  expect(rows[1].user.id).toBe(users[0].id);
  expect(rows[2].user.id).toBe(users[1].id);

  for (const r of rows) {
    expect(r.status).toBe(300);
  }

  done();
});

test('load many to many #3', async done => {
  const db = helper.connectToDatabase(NAME);
  const table = db.table('product');

  const config = {
    sku: 'sku',
    '*': 'categories[name]'
  };

  const data = [
    {
      sku: 'fancy-1',
      x1: 'New category #1',
      x2: 'Fruit'
    },
    {
      sku: 'fancy-2',
      x1: 'New category #2',
      x2: 'Apple'
    }
  ];

  await table.xappend(data, config, { categories: { parent: 1 } });

  const products = await table.select('*', {
    where: { sku_like: 'fancy-%' },
    orderBy: ['sku']
  });

  expect(products.length).toBe(2);
  expect(products[0].sku).toBe('fancy-1');
  expect(products[1].sku).toBe('fancy-2');

  const rows = await db.table('product_category').select('*', {
    where: { product: { id_in: [products[0].id, products[1].id] } }
  });

  expect(rows.length).toBe(4);

  const ids = rows.map(row => row.category.id);

  {
    const rows = await db.table('category').select('*', {
      where: { id: ids },
      orderBy: ['name']
    });

    const names = ['Apple', 'Fruit', 'New category #1', 'New category #2'];

    for (let i = 0; i < rows.length; i++) {
      expect(rows[i].name).toBe(names[i]);
      expect(rows[i].parent.id).toBe(1);
    }
  }

  {
    const rows = await db.table('category').select('*', {
      where: { name: 'Apple' },
      orderBy: ['name', 'parent.id']
    });
    expect(rows[0].parent.id).toBe(1);
    expect(rows[1].parent.id).toBeGreaterThan(1);
  }

  done();
});
