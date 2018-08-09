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

  await table.load(data, config.category);

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

  await table.load(data, config.category);

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

    await table.load(data, config.category, { parent: null });

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

    await table.load(data, config.category, { parent: 1 });

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

    await table.load(data, config.category, { parent: { id: 2 } });

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

    await table.load(data, config.category, { parent: { parent: { id: 3 } } });

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

test('skip incomplete', async done => {
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
  }

  done();
});
