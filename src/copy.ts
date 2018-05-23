import { Database, Table, Filter, Document, toDocument } from './database';
import { ForeignKeyField, SimpleField } from './model';
import { Record } from './record';
import { Value } from './engine';

interface CopyOptions {
  except?: string[];
}

export function copyRecord(
  record: Record,
  data: Document,
  options?: CopyOptions
): Promise<Record> {
  const table = record.__table;
  const filterMap = buildTableFilters(record, options);
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

function buildTableFilters(record: Record, options): Map<Table, Filter> {
  const db = record.__table.db;
  const map = new Map();

  let except: Set<string>;

  if (options && options.except) {
    except = new Set(options.except);
  } else {
    except = new Set();
  }

  map.set(record.__table, [record.__data]);

  while (true) {
    let added = 0;

    for (const table of db.tableList) {
      if (map.has(table)) continue;

      if (except.has(table.model.name) || except.has(table.model.table.name)) {
        continue;
      }

      for (const key of table.model.uniqueKeys) {
        for (const field of key.fields) {
          if (field instanceof ForeignKeyField) {
            const referencedTable = db.table(field.referencedField.model);
            if (map.has(referencedTable) && referencedTable !== table) {
              const filter = map.get(referencedTable);
              if (map.has(table)) {
                const current = map.get(table);
                if (Array.isArray(current)) {
                  current.push({ [field.name]: filter });
                } else {
                  map.set(table, [current, { [field.name]: filter }]);
                }
              } else {
                map.set(table, { [field.name]: filter });
              }
            }
          }
        }
      }
      if (map.has(table)) {
        added++;
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
        if (Object.keys(record.__data).length > 1) {
          record.__remove_dirty(key.name);
          delete record.__data[key.name];
        }
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
    if (field instanceof ForeignKeyField && row[field.name]) {
      const referencedTable = db.table(field.referencedField.model);
      const key = referencedTable.model.keyField();
      const value = referencedTable.model.keyValue(row[field.name]);
      const referencedRecord = referencedTable.append({ [key.name]: value });
      referencedRecord.__remove_dirty(key.name);
      record[field.name] = referencedRecord;
    } else if (field instanceof SimpleField && field !== key) {
      record[field.name] = row[field.name];
    }
  }
}
