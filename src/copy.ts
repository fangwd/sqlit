import { Database, Table, Filter, Document, toDocument } from './database';
import { ForeignKeyField, SimpleField } from './model';
import { Record } from './record';
import { Value } from './engine';

interface CopyOptions {
  ignore?: string[];
}

export function copyRecord(
  table: Table,
  filter: Filter,
  data: Document,
  options?: CopyOptions
): Promise<Record> {
  const filterMap = buildTableFilters(table, filter);
  const db = new Database(table.db.pool, table.db.schema);
  return selectRows(filterMap, db).then(() => {
    const model = table.model;
    const record = db.table(model).recordList[0];
    for (const name in data) {
      record[name] = data[name];
    }
    return flushAll(db).then(() => record);
  });
}

function buildTableFilters(table: Table, filter: Filter): Map<Table, Filter> {
  const db = table.db;
  const map = new Map();

  map.set(table, filter);

  while (true) {
    let added = 0;

    for (const table of db.tableList) {
      if (map.has(table)) continue;

      for (const key of table.model.uniqueKeys) {
        for (const field of key.fields) {
          if (field instanceof ForeignKeyField) {
            const referencedTable = db.table(field.referencedField.model);
            if (map.has(referencedTable)) {
              const filter = map.get(referencedTable);
              map.set(table, { [field.name]: filter });
              break;
            }
          }
        }
        if (map.has(table)) {
          added++;
          break;
        }
      }
    }

    if (added === 0) break;
  }

  return map;
}

function selectRows(map: Map<Table, Filter>, db: Database) {
  const promises = [];

  map.forEach((filter, key) => {
    const promise = key.select('*', { where: filter }).then(rows => {
      const table = db.table(key.model);
      rows.forEach(row => append(table, row));
      return table;
    });
    promises.push(promise);
  });

  return Promise.all(promises);
}

function flushAll(db: Database) {
  for (const table of db.tableList) {
    const key = table.model.keyField();
    if (key && key.column.autoIncrement) {
      for (const record of table.recordList) {
        record.__remove_dirty(key.name);
        delete record.__data[key.name];
      }
    }
  }
  return db.flush();
}

// test: a->b->c (table.map honours __getValue())
function append(table: Table, row: Document) {
  const db = table.db;
  const model = table.model;
  const key = model.keyField();
  const value = model.keyValue(row);
  const record = table.append({ [key.name]: value });

  record.__remove_dirty(key.name);

  for (const field of model.fields) {
    if (field instanceof ForeignKeyField) {
      const referencedTable = db.table(field.referencedField.model);
      const key = referencedTable.model.keyField();
      const value = referencedTable.model.keyValue(row);
      const referencedRecord = referencedTable.append({ [key.name]: value });
      referencedRecord.__remove_dirty(key.name);
      record[field.name] = referencedRecord;
    } else if (field instanceof SimpleField && field !== key) {
      record[field.name] = row[field.name];
    }
  }
}
