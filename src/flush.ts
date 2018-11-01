import { Database, Table, Document, Filter, toDocument } from './database';
import { Record } from './record';

import { Connection, Row, Value } from './engine';
import { encodeFilter } from './filter';

import { SimpleField, RelatedField } from './model';

export enum FlushMethod {
  INSERT,
  UPDATE,
  DELETE
}

export class FlushState {
  method: FlushMethod = FlushMethod.INSERT;
  dirty: Set<string> = new Set();
  deleted: boolean = false;
  merged?: Record = null;
  selected?: boolean = false;
  clone(): FlushState {
    const state = new FlushState();
    state.method = this.method;
    state.dirty = new Set(this.dirty);
    state.deleted = this.deleted;
    state.merged = null;
    state.selected = false;
    return state;
  }
  json() {
    return {
      method: FlushMethod[this.method],
      dirty: [...this.dirty],
      deleted: this.deleted,
      merged: this.merged ? this.merged.__repr() : null,
      selected: this.selected
    };
  }
}

class FlushContext {
  connection: Connection;
  visited: Set<Record> = new Set();
  promises = [];

  constructor(connection: Connection) {
    this.connection = connection;
  }
}

function collectParentFields(
  record: Record,
  context: FlushContext,
  perfect: number
) {
  if (!record.__dirty() || context.visited.has(record)) return;

  context.visited.add(record);

  record.__state.dirty.forEach(key => {
    const value = record.__data[key];
    if (value instanceof Record) {
      if (value.__flushable(perfect)) {
        // assert value.__state.method === FlushMethod.INSERT
        const promise = _persist(context.connection, value);
        context.promises.push(promise);
      } else {
        collectParentFields(value, context, perfect);
      }
    }
  });
}

export function flushRecord(
  connection: Connection,
  record: Record
): Promise<any> {
  return new Promise((resolve, reject) => {
    function __resolve() {
      const context = new FlushContext(connection);
      collectParentFields(record, context, 1);
      if (context.promises.length > 0) {
        Promise.all(context.promises).then(() => __resolve());
      } else {
        if (record.__flushable(0)) {
          _persist(connection, record).then(() => {
            if (!record.__dirty()) {
              resolve(record);
            } else {
              __resolve();
            }
          });
        } else {
          const context = new FlushContext(connection);
          collectParentFields(record, context, 0);
          if (context.promises.length > 0) {
            Promise.all(context.promises).then(() => __resolve());
          } else {
            reject(Error('Loops in record fields'));
          }
        }
      }
    }

    __resolve();
  });
}

/**
 * Flushes a *flushable* record to disk, updating its dirty fields or setting
 * __state.deleted to true after.
 *
 * @param record Record to be flushed to disk
 */
function _persist(connection: Connection, record: Record): Promise<Record> {
  const method = record.__state.method;
  const model = record.__table.model;
  const filter = model.getUniqueFields(record.__data);
  if (method === FlushMethod.DELETE) {
    return record.__table.delete(filter).then(() => {
      record.__state.deleted = true;
      return record;
    });
  }

  const fields = record.__fields();

  if (method === FlushMethod.UPDATE) {
    return record.__table._update(connection, fields, filter).then(affected => {
      if (affected > 0) {
        record.__remove_dirty(Object.keys(fields));
        return record;
      }
      throw Error(`Row does not exist`);
    });
  }

  return new Promise((resolve, reject) => {
    function _insert() {
      record.__table
        ._insert(connection, fields)
        .then(id => {
          if (record.__primaryKey() === undefined) {
            record.__setPrimaryKey(id);
          }
          record.__remove_dirty(Object.keys(fields));
          record.__state.method = FlushMethod.UPDATE;
          resolve(record);
        })
        .catch(error => {
          if (!isIntegrityError(error)) return reject(error);

          if (Object.keys(fields).length === 1) {
            const name = Object.keys(fields)[0];
            if (record.__table.model.field(name).uniqueKey.primary) {
              record.__remove_dirty(name);
              return resolve(record);
            }
          }

          record.__table._get(connection, filter).then(row => {
            if (row) {
              if (record.__primaryKey() === undefined) {
                const value = row[model.primaryKey.fields[0].name];
                record.__setPrimaryKey(value as Value);
              }
              for (const key in row) {
                if (fields[key] === record.__table.model.valueOf(row, key)) {
                  record.__remove_dirty(key);
                  delete fields[key];
                }
              }
              if (Object.keys(fields).length === 0 || !record.__dirty()) {
                resolve(record);
              } else {
                record.__table._update(connection, fields, filter).then(() => {
                  record.__remove_dirty(Object.keys(fields));
                  resolve(record);
                });
              }
            }
          });
        });
    }
    _insert();
  });
}

function flushTable(
  connection: Connection,
  table: Table,
  perfect?: number
): Promise<number> {
  if (table.recordList.length === 0) {
    return Promise.resolve(0);
  }

  const states = [];

  for (let i = 0; i < table.recordList.length; i++) {
    const record = table.recordList[i];
    states.push({
      data: { ...record.__data },
      state: record.__state.clone()
    });
  }

  return _flushTable(connection, table, perfect).catch(error => {
    for (let i = 0; i < table.recordList.length; i++) {
      const record = table.recordList[i];
      if (record.__dirty()) {
        const state = states[i];
        record.__data = { ...state.data };
        record.__state = state.state.clone();
      }
    }
    throw error;
  });
}

function _flushTable(
  connection: Connection,
  table: Table,
  perfect: number
): Promise<number> {
  mergeRecords(table);

  const filter = [];
  const nameSet = new Set();
  const recordSet = new Set();

  for (const record of table.recordList) {
    if (
      record.__dirty() &&
      record.__flushable(perfect) &&
      !record.__state.selected
    ) {
      const entry = record.__filter();
      for (const name in entry) {
        nameSet.add(name);
      }
      record.__state.dirty.forEach(name => nameSet.add(name));
      recordSet.add(record);
      filter.push(entry);
    }
  }

  const dialect = table.db.pool;
  const model = table.model;

  if (model.keyField()) {
    nameSet.add(model.keyField().name);
  }

  function _select(): Promise<any> {
    if (filter.length === 0) return Promise.resolve();
    const fields = model.fields.filter(field => nameSet.has(field.name));
    const columns = fields.map(field => (field as SimpleField).column.name);
    const from = dialect.escapeId(model.table.name);
    const where = encodeFilter(filter, table.model, dialect);
    const query = `select ${columns.join(',')} from ${from} where ${where}`;
    return connection.query(query).then(rows => {
      const map = makeMapTable(table);
      rows.forEach(row => map.append(toDocument(row, table.model)));
      for (const record of table.recordList) {
        if (!record.__dirty()) continue;
        const existing = map._mapGet(record);
        if (existing) {
          record.__updateState(existing);
        }
      }
    });
  }

  let insertCount;
  let updateCount;

  function _insert() {
    interface MapEntry {
      names: string[];
      records: Record[];
    }

    const nameMap: Map<string, MapEntry> = new Map();

    insertCount = 0;

    const shouldInsert = (record: Record) => {
      return (
        recordSet.has(record) &&
        record.__dirty() &&
        record.__flushable(perfect) &&
        record.__state.method === FlushMethod.INSERT
      );
    };

    const getNames = (record: Record) => {
      const names = [];

      for (const name of record.__state.dirty) {
        if (record.__getValue(name) !== undefined) {
          names.push(name);
        }
      }

      return names;
    };

    for (const record of table.recordList) {
      if (!shouldInsert(record)) continue;

      insertCount++;

      const names = getNames(record);
      const key = names.join('-');
      const me = nameMap.get(key);

      if (me) {
        me.records.push(record);
      } else {
        nameMap.set(key, { names, records: [record] });
      }
    }

    const promises = [];

    for (const entry of nameMap.values()) {
      promises.push(
        _insertRecords(connection, table, entry.names, entry.records)
      );
    }

    return Promise.all(promises).then(results => {
      let i = 0;
      for (const entry of nameMap.values()) {
        let id = results[i++];
        for (const record of entry.records) {
          if (model.primaryKey.autoIncrement()) {
            record.__setPrimaryKey(id++);
          }
          record.__state.selected = true;
          record.__state.method = FlushMethod.UPDATE;
        }
      }
    });
  }

  function _update() {
    const promises = [];
    for (const record of table.recordList) {
      if (!record.__dirty() || !record.__flushable(perfect)) continue;
      if (record.__state.method !== FlushMethod.UPDATE) continue;
      const fields = record.__fields();
      record.__remove_dirty(Object.keys(fields));
      promises.push(table._update(connection, fields, record.__filter()));
    }
    if ((updateCount = promises.length) > 0) {
      return Promise.all(promises);
    }
  }

  return _select()
    .then(() => _insert())
    .then(() => _update())
    .then(() => {
      return filter.length + insertCount + updateCount;
    });
}

function mergeRecords(table: Table) {
  const model = table.model;

  const map = model.uniqueKeys.reduce((map, uc) => {
    map[uc.name()] = {};
    return map;
  }, {});

  for (const record of table.recordList) {
    if (record.__state.merged) continue;
    for (const uc of model.uniqueKeys) {
      const value = record.__valueOf(uc);
      if (value === undefined) continue;
      const existing = map[uc.name()][value];
      if (existing) {
        if (!record.__state.merged) {
          record.__state.merged = existing;
        } else if (record.__state.merged !== existing) {
          throw Error(`Inconsistent`);
        }
      } else {
        map[uc.name()][value] = record;
      }
    }
    if (record.__state.merged) {
      record.__merge();
    }
  }
}

function flushDatabaseA(connection: Connection, db: Database): Promise<any> {
  return new Promise((resolve, reject) => {
    function _flush() {
      const promises = db.tableList.map(table =>
        flushTable(connection, table, 1)
      );
      Promise.all(promises)
        .then(results => {
          if (results.reduce((a, b) => a + b, 0) === 0) {
            resolve();
          } else {
            _flush();
          }
        })
        .catch(error => reject(error));
    }
    _flush();
  });
}

export function flushDatabaseB(connection: Connection, db: Database) {
  return new Promise((resolve, reject) => {
    let waiting = 0;
    function _flush() {
      const promises = db.tableList.map(table => flushTable(connection, table));
      Promise.all(promises)
        .then(results => {
          const count = results.reduce((a, b) => a + b, 0);
          if (count === 0 && db.getDirtyCount() > 0) {
            if (waiting++ > db.tableList.length) {
              dumpDirtyRecords(db);
              throw Error('Circular references');
            }
          } else {
            waiting = 0;
          }
          if (db.getDirtyCount() > 0) {
            _flush();
          } else {
            resolve();
          }
        })
        .catch(error => reject(error));
    }
    _flush();
  });
}

export function flushDatabase(connection: Connection, db: Database) {
  return new Promise((resolve, reject) => {
    let perfect = true;
    const _flush = () => {
      connection.transaction(() => {
        (perfect ? flushDatabaseA(connection, db) : Promise.resolve())
          .then(() =>
            flushDatabaseB(connection, db).then(() => {
              connection.commit().then(() => {
                resolve();
              });
            })
          )
          .catch(error => {
            connection.rollback().then(() => {
              if (perfect && isIntegrityError(error)) {
                perfect = false;
                setTimeout(_flush, Math.random() * 1000);
              } else if (isRetryable(error)) {
                setTimeout(_flush, Math.random() * 1000);
              } else {
                reject(Error(error));
              }
            });
          });
      });
    };
    _flush();
  });
}

function isIntegrityError(error) {
  return /\bDuplicate\b/i.test(error.message);
}

function isRetryable(error) {
  return /\bDeadlock\b/i.test(error.message);
}

export function dumpDirtyRecords(db: Database, all: boolean = false) {
  const tables = {};
  for (const table of db.tableList) {
    const records = [];
    for (const record of table.recordList) {
      if ((record.__dirty() && !record.__state.merged) || all) {
        records.push(record.__dump());
      }
    }
    if (records.length > 0) {
      tables[table.model.name] = records;
    }
  }
  console.log(JSON.stringify(tables, null, 4));
}

function makeMapTable(table: Table) {
  return new Database(table.db.pool, table.db.schema).table(table.model);
}

function __equal(s1: Set<string>, s2: Set<string>): boolean {
  if (s1.size !== s2.size) return false;
  for (const a of s1) if (!s2.has(a)) return false;
  return true;
}

/**
 * Inserts a list of records sharing the same set of dirty fields
 */
export function _insertRecords(
  connection: Connection,
  table: Table,
  names: string[],
  records: Record[]
): Promise<Record[]> {
  const escape = table.db.pool.escapeId;
  const model = table.model;

  const fields = names.map(name => model.field(name) as SimpleField);
  const columns = fields.map(field => escape(field.column.name)).join(',');

  const values = [];

  for (const record of records) {
    const value = [];
    for (const field of fields) {
      value.push(table.escapeValue(field, record.__getValue(field.name)));
      record.__remove_dirty(field.name);
    }
    values.push(`(${value.join(',')})`);
  }

  const into = escape(table.name);
  const query = `insert into ${into} (${columns}) values ${values.join(',')}`;

  return connection.query(query);
}

export async function replaceRecord(
  connection: Connection,
  table: Table,
  doc: Document
): Promise<Record> {
  const record = table.append();

  for (const name in doc) {
    const field = table.model.field(name);
    if (field instanceof SimpleField) {
      record[name] = doc[name];
    }
  }

  await flushTable(connection, table, -1);

  table.clear();

  for (const name in doc) {
    const field = table.model.field(name);

    if (field instanceof RelatedField) {
      const childRecords = [];

      const referencingTable = table.db.table(field.referencingField.model);
      const value = record.__primaryKey();

      const mapTable = await _buildMapTable(connection, referencingTable, {
        [field.referencingField.name]: value
      });

      const matchedSet = new Set();
      const batched = [];

      for (const item of doc[name] as Document[]) {
        const data = {
          ...item,
          [field.referencingField.name]: value
        };
        let batch = true;
        for (const key in item) {
          if (Array.isArray(item[key])) {
            batch = false;
            break;
          }
        }
        if (batch) {
          batched.push(data);
        } else {
          const childRecord = await replaceRecord(
            connection,
            referencingTable,
            data
          );
          matchedSet.add(mapTable._mapGet(childRecord));
          childRecords.push(childRecord);
        }
      }

      const tempTable = table.db.clone().table(referencingTable.name);

      batched.forEach(item => tempTable.append(item));

      await flushTable(connection, tempTable, -1);

      for (const childRecord of tempTable.recordList) {
        matchedSet.add(mapTable._mapGet(childRecord));
        childRecords.push(childRecord);
      }

      record[field.name] = childRecords;

      const values: Value[] = [];

      for (const record of mapTable.recordList) {
        if (!matchedSet.has(record)) {
          values.push(record.__primaryKey());
        }
      }

      if (values.length > 0) {
        await referencingTable._delete(connection, {
          [referencingTable.model.primaryKey.name()]: values
        });
      }
    }
  }

  return record;
}

async function _buildMapTable(
  connection: Connection,
  table: Table,
  filter: Filter
): Promise<Table> {
  const model = table.model;
  const dialect = table.db.pool;
  const fields = model.fields.filter(field => field.uniqueKey);
  const columns = fields.map(field => (field as SimpleField).column.name);
  const from = dialect.escapeId(model.table.name);
  const where = encodeFilter(filter, table.model, dialect);
  const query = `select ${columns.join(',')} from ${from} where ${where}`;
  const rows = await connection.query(query);
  const mapTable = makeMapTable(table);
  rows.forEach(row => mapTable.append(toDocument(row, table.model)));
  return mapTable;
}
