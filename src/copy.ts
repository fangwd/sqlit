import { Database, Table, Filter, Document, toDocument } from './database';
import { Model, ForeignKeyField, SimpleField } from './model';
import { Record } from './record';
import { Value } from './engine';

export interface CopyOptions {
  filter?: { [key: string]: string | null };
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

function buildTableFilters(
  record: Record,
  options: CopyOptions
): Map<Table, Filter> {
  const db = record.__table.db;
  const map = new Map();

  const except: Set<Table> = new Set();

  if (options && options.filter) {
    for (const name in options.filter) {
      const table = db.table(name);
      const value = options.filter[name];
      if (value) {
        const filter = getFilter(table, value, record);
        map.set(table, filter);
      } else {
        except.add(table);
      }
    }
  }

  map.set(record.__table, [record.__data]);

  while (true) {
    let added = 0;

    for (const table of db.tableList) {
      if (map.has(table)) continue;

      if (except.has(table)) {
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
      const value = referencedTable.model.keyValue(row[field.name] as Document);
      const referencedRecord = referencedTable.append({ [key.name]: value });
      if (record[field.name] !== undefined) {
        // No reassignment
        delete record.__data[field.name];
      }
      referencedRecord.__remove_dirty(key.name);
      record[field.name] = referencedRecord;
    } else if (field instanceof SimpleField && field !== key) {
      record[field.name] = row[field.name];
    }
  }
}

function getFilter(table: Table, path: string, record: Record) {
  const fields = path.split('.');
  const filter = {};

  let result = filter;
  let model = table.model;
  let name: string, field: ForeignKeyField;

  for (let i = 0; i < fields.length - 1; i++) {
    name = fields[i];
    field = model.field(name) as ForeignKeyField;
    if (!(field instanceof ForeignKeyField)) {
      throw Error(`Bad filter: ${path} (${name})`);
    }
    result[name] = {};
    result = result[name];
    model = field.referencedField.model;
  }

  name = fields[fields.length - 1];
  field = model.field(name) as ForeignKeyField;

  if (!(field instanceof ForeignKeyField)) {
    throw Error(`Bad filter: ${path} (${name})`);
  }

  model = field.referencedField.model;

  if (model === record.__table.model) {
    result[name] = record.__data;
    return filter;
  }

  const shortest = getShortestPath(model, record.__table.model);

  if (shortest.length === 0) {
    throw Error(`Bad filter: ${path} (not reachable)`);
  }

  result[name] = {};
  result = result[name];

  for (let i = 0; i < shortest.length; i++) {
    name = shortest[i];
    if (i === shortest.length - 1) {
      result[name] = record.__data;
    } else {
      result[name] = {};
      result = result[name];
    }
  }

  return filter;
}

function getPaths(
  from: Model,
  to: Model,
  visited: Set<ForeignKeyField>
): string[][] {
  const result = [];
  for (const field of from.fields) {
    if (field instanceof ForeignKeyField && !visited.has(field)) {
      const model = field.referencedField.model;
      visited.add(field);
      if (model === to) {
        result.push([field.name]);
      } else {
        for (const path of getPaths(model, to, visited)) {
          result.push([field.name, ...path]);
        }
      }
    }
  }
  return result;
}

export function getShortestPath(from: Model, to: Model): string[] {
  if (from === to) return [];

  let result: string[] = null;
  for (const path of getPaths(from, to, new Set())) {
    if (!result || result.length > path.length) {
      result = path;
    }
  }
  return result;
}
