import { Value, ConnectionInfo, createConnectionPool } from './engine';
import { flushDatabase, replaceRecord, FlushOptions } from './flush';
import { RecordProxy, Record, getModel } from './record';
import {
  loadTable,
  RecordConfig,
  recordConfigToDocument,
  mapDocument,
  parseRelatedOption
} from './loader';
import {
  Schema,
  Model,
  Field,
  SimpleField,
  ForeignKeyField,
  RelatedField,
  ColumnInfo,
  SchemaConfig
} from './model';
import {
  Connection,
  ConnectionPool,
  Row,
  getInformationSchema
} from './engine';

export type Document = {
  [key: string]: Value | Value[] | Document | Document[];
};

export type Filter = Document | Document[];

import { encodeFilter, QueryBuilder, AND } from './filter';
import { toArray } from './misc';

import { createNode, moveSubtree, deleteSubtree, treeQuery } from './tree';
import { selectTree, selectTree2, FieldOptions } from './select';
import { JsonSerialiser } from './serialiser';

export class ClosureTable {
  constructor(
    public table: Table,
    public ancestor: ForeignKeyField,
    public descendant: ForeignKeyField,
    public depth?: SimpleField
  ) {}
}

export class Database {
  name: string;
  schema: Schema;
  pool: ConnectionPool;
  tableMap: { [key: string]: Table } = {};
  tableList: Table[] = [];

  constructor(connection: ConnectionPool | ConnectionInfo, schema?: Schema) {
    if (connection instanceof ConnectionPool) {
      this.pool = connection;
      this.name = this.pool.name;
    } else if (connection) {
      this.pool = createConnectionPool(
        connection.dialect,
        connection.connection
      );
      this.name = connection.connection.database || connection.connection.name;
    }
    if (schema) this.setSchema(schema);
  }

  getModels(bulk: boolean = false): { [key: string]: any } {
    return this.tableList.reduce((map, table) => {
      map[table.model.name] = getModel(table, bulk);
      return map;
    }, {});
  }

  buildSchema(config?: SchemaConfig): Promise<Schema> {
    if (this.schema) return Promise.resolve(this.schema);
    return new Promise(resolve =>
      this.pool.getConnection().then(connection =>
        getInformationSchema(connection, this.name).then(schemaInfo => {
          const schema = new Schema(schemaInfo, config);
          this.setSchema(schema);
          connection.release();
          resolve(schema);
        })
      )
    );
  }

  clone(): Database {
    return new Database(this.pool, this.schema);
  }

  private setSchema(schema: Schema) {
    this.schema = schema;

    for (const model of schema.models) {
      const table = new Table(this, model);
      this.tableMap[model.name] = table;
      this.tableMap[model.table.shortName] = table;
      this.tableList.push(table);
    }

    for (const model of schema.models) {
      if (model.config.closureTable) {
        const config = model.config.closureTable;
        const fields: any = config.fields || {};

        const table = this.table(config.name);
        if (!table) {
          throw Error(`Table ${config.name} not found.`);
        }

        let fieldName = fields.ancestor || 'ancestor';
        const ancestor = table.model.field(fieldName);
        if (!ancestor || !(ancestor instanceof ForeignKeyField)) {
          throw Error(`Field ${fieldName} is not a foreign key`);
        }

        fieldName = fields.descendant || 'descendant';
        const descendant = table.model.field(fieldName);
        if (!descendant || !(descendant instanceof ForeignKeyField)) {
          throw Error(`Field ${fieldName} is not a foreign key`);
        }

        let depth: SimpleField;
        if (fields.depth) {
          depth = table.model.field(fields.depth) as SimpleField;
          if (!depth) {
            throw Error(`Field ${fields.depth} not found`);
          }
        }

        this.table(model).closureTable = new ClosureTable(
          table,
          ancestor,
          descendant,
          depth
        );
      }
    }
  }

  table(name: string | Field | Model): Table {
    if (name instanceof Field) {
      name = name.model.name;
    } else if (name instanceof Model) {
      name = name.name;
    }
    return this.tableMap[name];
  }

  model(name: string): Model {
    return this.table(name).model;
  }

  append(name: string, data: { [key: string]: any }): any {
    return this.table(name).append(data);
  }

  getDirtyCount(): number {
    return this.tableList.reduce((count, table) => {
      count += table.getDirtyCount();
      return count;
    }, 0);
  }

  flush(flushOptions?: FlushOptions) {
    return this.pool.getConnection().then(connection =>
      flushDatabase(connection, this, flushOptions).then(() => {
        connection.release();
        return connection;
      })
    );
  }

  end(): Promise<void> {
    return this.pool ? this.pool.end() : Promise.resolve();
  }

  clear() {
    for (const name in this.tableMap) {
      this.tableMap[name].clear();
    }
  }

  json() {
    return this.tableList.reduce((result, table) => {
      result[table.model.name] = table.json();
      return result;
    }, {});
  }
}

export type OrderBy = string | string[];

export interface SelectOptions {
  where?: Filter;
  offset?: number;
  limit?: number;
  orderBy?: OrderBy;
}

export class Table {
  db: Database;
  name: string;
  model: Model;
  closureTable?: ClosureTable;

  recordList: Record[] = [];
  recordMap: { [key: string]: { [key: string]: Record } };

  constructor(db: Database, model: Model) {
    this.db = db;
    this.name = model.table.name;
    this.model = model;
    this._initMap();
  }

  column(name: string): ColumnInfo {
    const field = this.model.field(name) as SimpleField;
    return field.column;
  }

  getParentField(model?: Model): ForeignKeyField {
    return this.model.getForeignKeyOf(model || this.model);
  }

  getAncestors(row: Value | Document, filter?: Filter): Promise<Document[]> {
    const field = this.closureTable.ancestor;
    return this.db.pool.getConnection().then(connection =>
      treeQuery(connection, this, row, field, filter).then(result => {
        connection.release();
        return result;
      })
    );
  }

  getDescendants(row: Value | Document, filter?: Filter): Promise<Document[]> {
    const field = this.closureTable.descendant;
    return this.db.pool.getConnection().then(connection =>
      treeQuery(connection, this, row, field, filter).then(result => {
        connection.release();
        return result;
      })
    );
  }

  select(
    fields: string | Document,
    options: SelectOptions = {},
    filterThunk?: (builder: QueryBuilder) => string,
    connection?: Connection
  ): Promise<Document[]> {
    if (connection) {
      return this._select(connection, fields, options, filterThunk).then(
        result =>
          this._resolveRelatedFields(connection, result, fields).then(
            result => result as Row[]
          )
      );
    }
    return this.db.pool.getConnection().then(connection =>
      this._select(connection, fields, options, filterThunk).then(result =>
        this._resolveRelatedFields(connection, result, fields).then(result => {
          connection.release();
          return result as Row[];
        })
      )
    );
  }

  async _resolveRelatedFields(
    connection: Connection,
    result: Document[],
    fields: string | Document
  ): Promise<Document[]> {
    if (typeof fields === 'string' || fields instanceof SimpleField) {
      return Promise.resolve(result);
    }

    const pk = this.model.keyField().name;
    const values = result.map(row => this.model.valueOf(row, pk));

    for (const name in fields) {
      const field = this.model.field(name);
      const value = fields[name];

      if (field instanceof RelatedField && value) {
        let options, fields;
        if (typeof value !== 'object') {
          options = {};
        } else {
          options = Object.assign({}, value);
          // TODO: Document options.fields for related fields!
          if (options.fields) {
            fields = options.fields;
            delete options.fields;
          } else {
            fields = '*';
          }
        }
        const rows = await this._selectRelated(
          connection,
          field,
          values,
          fields,
          options
        );
        result.forEach((entry, index) => {
          entry[name] = rows[index];
        });
      }
    }

    for (const name in fields) {
      const field = this.model.field(name);
      const value = fields[name] as Document;
      if (value) {
        if (field instanceof ForeignKeyField) {
          const table = this.db.table(field.referencedField.model);
          if (shouldSelectSeparately(table.model, value)) {
            const values: Value[] = result.map(row =>
              table.model.keyValue(row[field.name] as Document)
            );
            const docs = await table.select(
              value,
              {
                where: { [table.model.keyField().name]: values }
              },
              undefined,
              connection
            );
            for (const row of result) {
              const value = table.model.keyValue(row[field.name] as Document);
              const doc = docs.find(doc => table.model.keyValue(doc) === value);
              if (doc) {
                row[field.name] = JSON.parse(JSON.stringify(doc));
              }
            }
          } else if (field instanceof RelatedField) {
            const rows = result
              .map(r => r[name] as Document[])
              .reduce((result, rows) => {
                result = result.concat(rows);
                return result;
              }, []);
            const table = this.db.table(field.referencingField.model);
            await table._resolveRelatedFields(connection, rows, value);
          }
        }
      }
    }

    return result;
  }

  get(key: Value | Filter): Promise<Document> {
    return this.db.pool.getConnection().then(connection =>
      this._get(connection, key).then(result => {
        connection.release();
        return result;
      })
    );
  }

  insert(data: Row): Promise<any> {
    return this.db.pool.getConnection().then(connection =>
      this._insert(connection, data).then(result => {
        connection.release();
        return result;
      })
    );
  }

  create(data: Document): Promise<Document> {
    return this.db.pool.getConnection().then(connection =>
      connection.transaction(() =>
        this._create(connection, data).then(result => {
          connection.release();
          return result;
        })
      )
    );
  }

  update(data: Document, filter: Filter): Promise<any> {
    return this.db.pool.getConnection().then(connection =>
      this._update(connection, data, filter).then(result => {
        connection.release();
        return result;
      })
    );
  }

  upsert(data: Document, update?: Document): Promise<Document> {
    return this.db.pool.getConnection().then(connection =>
      connection.transaction(() =>
        this._upsert(connection, data, update).then(result => {
          connection.release();
          return result;
        })
      )
    );
  }

  modify(data: Document, filter: Filter): Promise<Document> {
    return this.db.pool.getConnection().then(connection =>
      connection.transaction(() =>
        this._modify(connection, data, filter).then(result => {
          connection.release();
          return result;
        })
      )
    );
  }

  delete(filter: Filter): Promise<any> {
    return this.db.pool.getConnection().then(connection => {
      if (this.closureTable) {
        return connection.transaction(() =>
          this._delete(connection, filter).then(result => {
            connection.release();
            return result;
          })
        );
      } else {
        return this._delete(connection, filter).then(result => {
          connection.release();
          return result;
        });
      }
    });
  }

  replace(data: Document): Promise<Record> {
    return this.db.pool.getConnection().then(connection =>
      connection.transaction(() =>
        replaceRecord(connection, this, data).then(record => {
          connection.release();
          return record;
        })
      )
    );
  }

  count(filter?: Filter, expr?: string): Promise<number> {
    let sql;

    if (expr) {
      sql = `select count(${expr}) as result from ${this._name()}`;
    } else {
      sql = `select count(1) as result from ${this._name()}`;
    }

    if (filter) {
      sql += ` where ${this._where(filter)}`;
    }

    return this.db.pool.getConnection().then(connection =>
      connection.query(sql).then(rows => {
        connection.release();
        return parseInt(rows[0].result);
      })
    );
  }

  private _name(): string {
    return this.db.pool.escapeId(this.model.table.name);
  }

  private _where(filter: Filter) {
    return encodeFilter(filter, this.model, this.db.pool);
  }

  private _pair(name: string | SimpleField, value: Value): string {
    if (typeof name === 'string') {
      name = this.model.field(name) as SimpleField;
    }
    return this.escapeName(name) + '=' + this.escapeValue(name, value);
  }

  private _select(
    connection: Connection,
    fields: string | Document,
    options: SelectOptions = {},
    filterThunk?: (builder: QueryBuilder) => string
  ): Promise<Row[]> {
    const builder = new QueryBuilder(this.model, this.db.pool);

    let sql = builder.select(
      fields,
      options.where,
      options.orderBy,
      filterThunk
    );

    if (options.limit !== undefined) {
      sql += ` limit ${parseInt(options.limit + '')}`;
    }
    if (options.offset !== undefined) {
      sql += ` offset ${parseInt(options.offset + '')}`;
    }
    return connection.query(sql).then(rows => {
      return rows.map(row => {
        const doc = toDocument(row, this.model, builder.fieldMap);
        if (filterThunk) {
          for (const key in row) {
            if (key.indexOf('__') !== -1) {
              doc[key] = row[key];
            }
          }
        }
        return doc;
      });
    });
  }

  _update(
    connection: Connection,
    fields: Document,
    filter: Filter
  ): Promise<any> {
    const data = { ...fields };

    for (const name in filter) {
      if (name in data) {
        const lhs = this.model.valueOf(data, name);
        const rhs = this.model.valueOf(filter as Document, name);
        if (lhs === null) {
          if (rhs === null) {
            delete data[name];
          }
          continue;
        }
        if (rhs === null) continue;
        if (lhs.toString() === rhs.toString()) {
          delete data[name];
        }
      }
    }

    if (Object.keys(data).length === 0) {
      return Promise.resolve(0);
    }

    let sql = `update ${this._name()} set`;
    let cnt = 0;

    const keys = Object.keys(data);
    for (let i = 0; i < keys.length; i++) {
      const field = this.model.field(keys[i]);
      if (field instanceof SimpleField) {
        if (i > 0) {
          sql += ',';
        }
        sql += this._pair(field, data[keys[i]] as Value);
        cnt++;
      }
    }

    if (cnt === 0) {
      return Promise.resolve();
    }

    if (filter) {
      if (typeof filter === 'string') {
        sql += ` where ${filter}`;
      } else {
        sql += ` where ${this._where(filter)}`;
      }
    }

    return connection.query(sql);
  }

  _insert(connection: Connection, data: Row): Promise<any> {
    const keys = Object.keys(data);
    if (keys.length === 0) throw Error(`${this.model.name}: No data`);
    const name = keys.map(key => this.escapeName(key)).join(', ');
    const value = keys.map(key => this.escapeValue(key, data[key])).join(', ');
    const sql = `insert into ${this._name()} (${name}) values (${value})`;
    return connection
      .query(sql)
      .then(insertId => (typeof insertId === 'number' ? insertId : 0));
  }

  _delete(connection: Connection, filter: Filter): Promise<any> {
    const scope = filter ? `${this._where(filter)}` : '';

    const __delete = () => {
      let sql = `delete from ${this._name()}`;
      if (scope) {
        sql += ` where ${scope}`;
      }
      return connection.query(sql);
    };

    if (this.closureTable) {
      return deleteSubtree(connection, this, scope).then(() => __delete());
    } else {
      return __delete();
    }
  }

  escapeName(name: SimpleField | string | number): string {
    if (name instanceof SimpleField) {
      name = name.column.name;
    } else {
      if (typeof name === 'number') {
        return name + '';
      }
      if (name === '*') return name;
      name = (this.model.field(name) as SimpleField).column.name;
    }
    return this.db.pool.escapeId(name);
  }

  escapeValue(field: SimpleField | string, value: Value): string {
    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }
    if (value === null || value === undefined) {
      return 'null';
    }
    if (typeof field === 'string') {
      field = this.model.field(field) as SimpleField;
    }
    return this.db.pool.escape(toRow(value, field) + '');
  }

  _get(connection: Connection, key: Value | Filter): Promise<Document> {
    if (key === undefined) throw Error(`Bad filter`);
    if (key === null || typeof key !== 'object') {
      key = {
        [this.model.keyField().name]: key
      };
    } else if (!this.model.checkUniqueKey(key)) {
      const msg = `Bad selector: ${JSON.stringify(key)}`;
      return Promise.reject(Error(msg));
    }
    return this._select(connection, '*', { where: key } as SelectOptions).then(
      rows => rows[0]
    );
  }

  // GraphQL mutations
  _resolveParentFields(
    connection: Connection,
    input: Document,
    filter?: Filter
  ): Promise<Row> {
    const result: Row = {};
    const promises = [];
    const self = this;

    function _createPromise(field: ForeignKeyField, data: Document): void {
      const table = self.db.table(field.referencedField.model);
      const method = Object.keys(data)[0];
      let promise;
      switch (method) {
        case 'connect':
          promise = table._get(connection, data[method] as Document);
          break;
        case 'create':
          promise = table._create(connection, data[method] as Document);
          break;
        case 'update':
          {
            const where = { [field.relatedField.name]: filter };
            promise = table._modify(
              connection,
              data[method] as Document,
              where
            );
          }
          break;
        default:
          throw Error(`Unsuported method '${method}'`);
      }

      if (method !== 'update') {
        promise.then(row => {
          result[field.name] = row
            ? row[field.referencedField.model.keyField().name]
            : null;
          return row;
        });
      }

      promises.push(promise);
    }

    for (const key in input) {
      let field = this.model.field(key);
      if (
        field instanceof ForeignKeyField &&
        input[key] &&
        typeof input[key] === 'object'
      ) {
        _createPromise(field, input[key] as Document);
      } else if (field instanceof SimpleField) {
        result[key] = input[key] as Value;
      }
    }

    return Promise.all(promises).then(() => result);
  }

  _create(connection: Connection, data: Document): Promise<Document> {
    if (Object.keys(data).length === 0) throw Error('Empty data');

    return this._resolveParentFields(connection, data).then(row =>
      this._insert(connection, row).then(id => {
        return this._updateChildFields(connection, data, id).then(() =>
          this._get(connection, id).then(row =>
            this.closureTable
              ? createNode(connection, this, row).then(() => row)
              : row
          )
        );
      })
    );
  }

  private _upsert(
    connection,
    data: Document,
    update?: Document
  ): Promise<Document> {
    if (!this.model.checkUniqueKey(data)) {
      return Promise.reject(`Incomplete: ${JSON.stringify(data)}`);
    }

    const self = this;

    return this._resolveParentFields(connection, data).then(row => {
      const uniqueFields = self.model.getUniqueFields(row);
      return self._get(connection, uniqueFields).then(row => {
        if (!row) {
          return self._create(connection, data);
        } else {
          if (update && Object.keys(update).length > 0) {
            return self._modify(connection, update, uniqueFields);
          } else {
            return row;
          }
        }
      });
    });
  }

  private _modify(
    connection,
    data: Document,
    filter: Filter
  ): Promise<Document> {
    if (!this.model.checkUniqueKey(filter)) {
      return Promise.reject(`Bad filter: ${JSON.stringify(filter)}`);
    }

    const self = this;

    return this._resolveParentFields(connection, data, filter).then(row => {
      return self._update(connection, row, filter).then(() => {
        const where = Object.assign({}, filter);
        for (const key in where) {
          if (key in row) {
            where[key] = row[key];
          }
        }
        return self._get(connection, where as Document).then(row => {
          if (row) {
            const id = row[this.model.keyField().name] as Value;
            return this._updateChildFields(connection, data, id).then(() =>
              !this.closureTable || !data[this.getParentField().name]
                ? row
                : moveSubtree(connection, this, row).then(() => row)
            );
          } else {
            return Promise.resolve(row);
          }
        });
      });
    });
  }

  private _updateChildFields(
    connection: Connection,
    data: Document,
    id: Value
  ): Promise<any> {
    const promises = [];
    for (const key in data) {
      let field = this.model.field(key);
      if (field instanceof RelatedField) {
        promises.push(
          this._updateChildField(connection, field, id, data[key] as Document)
        );
      }
    }
    return Promise.all(promises);
  }

  private _updateChildField(
    connection: Connection,
    related: RelatedField,
    id: Value,
    data: Document
  ): Promise<any> {
    const promises = [];
    const field = related.referencingField;
    if (!field) throw Error(`Bad field ${related.displayName()}`);
    const table = this.db.table(field.model);
    if (!data || field.model.keyValue(data) === null) {
      if (field.column.nullable) {
        return table._update(
          connection,
          { [field.name]: null },
          { [field.name]: id }
        );
      } else {
        return table._delete(connection, { [field.name]: id });
      }
    }
    for (const method in data) {
      const args = data[method] as Document[];
      if (method === 'connect') {
        if (related.throughField) {
          promises.push(this._connectThrough(connection, related, id, args));
          continue;
        }
        // connect: [{parent: {id: 2}, name: 'Apple'}, ...]
        for (const arg of toArray(args)) {
          if (!table.model.checkUniqueKey(arg)) {
            return Promise.reject(`Bad filter (${table.model.name})`);
          }
          let promise;
          if (field.isUnique()) {
            promise = this._disconnectUnique(connection, field, id).then(() =>
              table._update(connection, { [field.name]: id }, args)
            );
          } else {
            promise = table._update(connection, { [field.name]: id }, args);
          }
          promises.push(promise);
        }
      } else if (method === 'create') {
        if (related.throughField) {
          promises.push(this._createThrough(connection, related, id, args));
          continue;
        }
        // create: [{parent: {id: 2}, name: 'Apple'}, ...]
        const docs = toArray(args).map(arg => ({ [field.name]: id, ...arg }));
        if (field.isUnique()) {
          promises.push(
            this._disconnectUnique(connection, field, id).then(() =>
              table._create(connection, docs[0])
            )
          );
        } else {
          for (const doc of docs) {
            promises.push(table._create(connection, doc));
          }
        }
      } else if (method === 'upsert') {
        if (related.throughField) {
          promises.push(this._upsertThrough(connection, related, id, args));
          continue;
        }
        for (const arg of toArray(args)) {
          let { create, update } = arg;
          if (!create && !field.isUnique()) {
            throw Error('Bad data');
          }
          create = Object.assign({ [field.name]: id }, create);
          if (create[field.name] === undefined) {
            update = Object.assign({ [field.name]: id }, update);
          }
          promises.push(
            table._upsert(connection, create as Document, update as Document)
          );
        }
      } else if (method === 'update') {
        if (related.throughField) {
          promises.push(this._updateThrough(connection, related, id, args));
          continue;
        }
        for (const arg of toArray(args)) {
          let data, where;
          if (arg.data === undefined) {
            data = arg;
            where = {};
          } else {
            data = arg.data;
            where = arg.where;
          }
          const filter = { [field.name]: id, ...where };
          promises.push(table._modify(connection, data, filter));
        }
      } else if (method === 'delete') {
        if (related.throughField) {
          promises.push(this._deleteThrough(connection, related, id, args));
          continue;
        }
        const filter = args.map(arg => ({
          ...(arg /*.where*/ as Document),
          [field.name]: id
        }));
        promises.push(table._delete(connection, filter as Filter));
      } else if (method === 'disconnect') {
        if (related.throughField) {
          promises.push(this._disconnectThrough(connection, related, id, args));
          continue;
        }
        const where = args.map(arg => ({ [field.name]: id, ...arg }));
        promises.push(table._update(connection, { [field.name]: null }, where));
      } else if (method === 'set') {
        let promise = related.throughField
          ? this._deleteThrough(connection, related, id, [])
          : table._delete(connection, { [field.name]: id });
        promise = promise.then(() => {
          if (related.throughField) {
            return this._createThrough(connection, related, id, args);
          }
          // create: [{parent: {id: 2}, name: 'Apple'}, ...]
          const docs = toArray(args).map(arg => ({
            [field.name]: id,
            ...arg
          }));
          if (field.isUnique()) {
            return this._disconnectUnique(connection, field, id).then(() =>
              table._create(connection, docs[0])
            );
          } else {
            return Promise.all(docs.map(doc => table._create(connection, doc)));
          }
        });
        promises.push(promise);
      } else {
        throw Error(`Unknown method: ${method}`);
      }
    }

    return Promise.all(promises);
  }

  _disconnectUnique(
    connection: Connection,
    field: SimpleField,
    id: Value
  ): Promise<any> {
    const table = this.db.table(field.model);
    return field.column.nullable
      ? table._update(connection, { [field.name]: null }, { [field.name]: id })
      : table._delete(connection, { [field.name]: id });
  }

  _connectThrough(
    connection: Connection,
    related: RelatedField,
    value: Value,
    args: Document[]
  ): Promise<any> {
    const table = this.db.table(related.throughField.referencedField.model);
    const mapping = this.db.table(related.throughField.model);
    const promises = args.map(arg =>
      table._get(connection, arg).then(row =>
        row
          ? mapping._create(connection, {
              [related.referencingField.name]: value,
              [related.throughField.name]: row[table.model.keyField().name]
            })
          : Promise.resolve(null)
      )
    );
    return Promise.all(promises);
  }

  private _createThrough(
    connection: Connection,
    related: RelatedField,
    value: Value,
    args: Document[]
  ): Promise<any> {
    const table = this.db.table(related.throughField.referencedField.model);
    const mapping = this.db.table(related.throughField.model);
    const promises = args.map(arg =>
      table._create(connection, arg).then(row =>
        mapping._create(connection, {
          [related.referencingField.name]: value,
          [related.throughField.name]: row[table.model.keyField().name]
        })
      )
    );
    return Promise.all(promises);
  }

  private _upsertThrough(
    connection: Connection,
    related: RelatedField,
    value: Value,
    args: Document[]
  ): Promise<any> {
    const table = this.db.table(related.throughField.referencedField.model);
    const mapping = this.db.table(related.throughField.model);
    const promises = args.map(arg =>
      table
        ._upsert(connection, arg.create as Document, arg.update as Document)
        .then(row =>
          mapping._upsert(connection, {
            [related.referencingField.name]: value,
            [related.throughField.name]: row[table.model.keyField().name]
          })
        )
    );
    return Promise.all(promises);
  }

  private _updateThrough(
    connection: Connection,
    related: RelatedField,
    value: Value,
    args: Document[]
  ): Promise<any> {
    const model = related.throughField.referencedField.model;
    const promises = args.map(arg => {
      let where;
      if (related.throughField.relatedField.throughField) {
        where = {
          [related.throughField.relatedField.name]: {
            [related.model.keyField().name]: value
          },
          ...(arg.where as object)
        };
      } else {
        where = {
          [related.throughField.relatedField.name]: {
            [related.referencingField.name]: value
          },
          ...(arg.where as object)
        };
      }
      return this.db
        .table(model)
        ._modify(connection, arg.data as Document, where);
    });
    return Promise.all(promises);
  }

  _deleteThrough(
    connection: Connection,
    related: RelatedField,
    value: Value,
    args: Document[]
  ): Promise<any> {
    const mapping = this.db.table(related.throughField.model);
    const table = this.db.table(related.throughField.referencedField.model);
    return mapping
      ._select(connection, '*', {
        where: {
          [related.referencingField.name]: value,
          [related.throughField.name]: args
        }
      })
      .then(rows => {
        if (rows.length === 0) return Promise.resolve(0);
        const values = rows.map(
          row => row[related.throughField.name][table.model.keyField().name]
        );
        return mapping._delete(connection, rows).then(() =>
          table._delete(connection, {
            [table.model.keyField().name]: values
          })
        );
      });
  }

  _disconnectThrough(
    connection: Connection,
    related: RelatedField,
    value: Value,
    args: Document[]
  ): Promise<any> {
    const mapping = this.db.table(related.throughField.model);
    return mapping._delete(connection, {
      [related.referencingField.name]: value,
      [related.throughField.name]: args
    });
  }

  claim(filter: Filter, data: Document, orderBy?: string[]): Promise<Document> {
    const self = this;

    return new Promise(resolve => {
      function _select() {
        self.select('*', { where: filter, limit: 10, orderBy }).then(rows => {
          if (rows.length === 0) {
            resolve(null);
          } else {
            const row = rows[Math.floor(Math.random() * rows.length)];
            _update(row);
          }
        });
      }

      function _update(row) {
        self.update(data, self.model.getUniqueFields(row)).then(result => {
          if (result.changedRows === 1) {
            resolve(row);
          } else {
            setTimeout(_select, Math.random() * 1000);
          }
        });
      }

      _select();
    });
  }

  append(data?: { [key: string]: any } | any[]): Record {
    const record = new Proxy(new Record(this), RecordProxy);
    Object.assign(record, data);
    const existing = this._mapGet(record);
    if (!existing) {
      this.recordList.push(record);
      this._mapPut(record);
      return record;
    }
    for (const name in data) {
      if (existing[name] != data[name]) {
        const field = record.__table.model.field(name);
        if (field instanceof ForeignKeyField) {
          const model = field.referencedField.model;
          if (existing[name] !== undefined) {
            if (pkOf(model, existing[name]) == pkOf(model, data[name])) {
              continue;
            }
          }
        }
        existing[name] = data[name];
      }
    }
    return existing;
  }

  clear() {
    this.recordList = [];
    this._initMap();
  }

  getDirtyCount(): number {
    let dirtyCount = 0;
    for (const record of this.recordList) {
      if (record.__dirty() && !record.__state.merged) {
        dirtyCount++;
      }
    }
    return dirtyCount;
  }

  json() {
    return this.recordList.map(record => record.__json());
  }

  _mapGet(record: Record): Record {
    let existing: Record;
    for (const uc of this.model.uniqueKeys) {
      const value = record.__valueOf(uc);
      if (value !== undefined) {
        const record = this.recordMap[uc.name()][value];
        if (record) {
          if (existing && existing !== record) {
            throw Error(`Inconsistent unique constraint values`);
          }
          existing = record;
        }
      }
    }
    return existing;
  }

  _mapPut(record: Record) {
    for (const uc of this.model.uniqueKeys) {
      const value = record.__valueOf(uc);
      if (value !== undefined) {
        this.recordMap[uc.name()][value] = record;
      }
    }
  }

  _initMap() {
    this.recordMap = this.model.uniqueKeys.reduce((map, uc) => {
      map[uc.name()] = {};
      return map;
    }, {});
  }

  _selectRelated(
    connection: Connection,
    field: RelatedField,
    values: Value[],
    fields,
    selectOptions: SelectOptions
  ) {
    const table = this.db.table(field.referencingField.model);
    const name = field.referencingField.name;

    if (selectOptions.limit) {
      const promises = [];
      if (field.throughField) {
        for (const value of values) {
          const options = Object.assign({}, selectOptions);
          options.where = {
            [field.throughField.name]: options.where,
            [field.referencingField.name]: value
          };
          if (options.orderBy) {
            const prefix = field.throughField.name;
            options.orderBy = toArray(options.orderBy).map(
              name => `${prefix}.${name}`
            );
          }
          promises.push(
            table
              .select(
                { [field.throughField.name]: fields },
                options,
                undefined,
                connection
              )
              .then(rows => rows.map(row => row[field.throughField.name]))
          );
        }
      } else {
        for (const value of values) {
          const options = Object.assign({}, selectOptions);
          options.where = Object.assign({}, options.where, { [name]: value });
          promises.push(
            table.select(fields, options, undefined, connection).then(rows => {
              if (field.referencingField.isUnique()) {
                const row = rows[0];
                if (row) {
                  delete row[name];
                }
                return row;
              } else {
                return rows.map(row => {
                  delete row[name];
                  return row;
                });
              }
            })
          );
        }
      }
      return Promise.all(promises);
    }

    const options = Object.assign({ where: {} }, selectOptions);

    if (field.throughField) {
      options.where = { [field.throughField.name]: options.where };
      options.where[field.referencingField.name] = values;
      if (options.orderBy) {
        const prefix = field.throughField.name;
        options.orderBy = toArray(options.orderBy).map(
          name => `${prefix}.${name}`
        );
      }
      return table
        .select(
          { [field.throughField.name]: fields },
          options,
          undefined,
          connection
        )
        .then(rows => {
          const id = field.referencingField.model.keyField().name;
          if (field.referencingField.isUnique()) {
            return values.map(key => {
              const row = rows.find(row => row[name][id] === key);
              return row ? row[field.throughField.name] : undefined;
            });
          } else {
            return values
              .map(key => rows.filter(row => row[name][id] === key))
              .map(docs => docs.map(doc => doc[field.throughField.name]));
          }
        });
    } else {
      options.where[name] = values;
      return table.select(fields, options, undefined, connection).then(rows => {
        const id = field.referencingField.model.keyField().name;
        if (field.referencingField.isUnique()) {
          return values.map(key => {
            const row = rows.find(row => row[name] && row[name][id] === key);
            if (row) {
              delete row[name];
            }
            return row;
          });
        } else {
          return values
            .map(key => rows.filter(row => row[name][id] === key))
            .map(docs => {
              for (const doc of docs) {
                delete doc[name];
              }
              return docs;
            });
        }
      });
    }
  }

  // It is strongly recommended to call db.clear() before calling this method!
  xappend(
    data: Document | Document[],
    config: RecordConfig,
    defaults?: Document
  ): Promise<any> {
    return loadTable(this, data, config, defaults);
  }

  xselect(
    config: RecordConfig,
    options: SelectOptions = {}
  ): Promise<Document[]> {
    config = { ...config };

    let attrs;
    if (config['*']) {
      attrs = config['*'];
      delete config['*'];
    }

    const fields = recordConfigToDocument(this, config);

    return this.select(fields, options).then(docs => {
      if (attrs) {
        const options = parseRelatedOption(attrs);
        const table = this.db.table(
          (this.model.field(options.name) as RelatedField).referencingField
            .model
        );
        const range = docs.map(doc => this.model.keyValue(doc));
        const field = table.model.getForeignKeyOf(this.model);
        return table
          .select('*', { where: { [field.name]: range } })
          .then(rows => {
            const result = [];
            for (const doc of docs) {
              const kv = this.model.keyValue(doc);
              const rs = (rows as Document[]).filter(
                row => this.model.keyValue(row[field.name] as Document) === kv
              );
              for (const d of mapDocument(doc, config)) {
                for (const r of rs) {
                  const name = r[options.key] as string;
                  const value = r[options.value];
                  d[name] = value;
                }
                result.push(d);
              }
            }
            return result;
          });
      } else {
        return [].concat.apply([], docs.map(doc => mapDocument(doc, config)));
      }
    });
  }

  async selectTree(
    filter: Filter,
    options?: FieldOptions
  ): Promise<Document[] | null> {
    const data = await (options
      ? selectTree(this, filter, options)
      : selectTree2(this, filter));
    const serialiser = new JsonSerialiser(data);
    return serialiser.serialise(this.model);
  }
}

export function _toCamel(value: Value, field: SimpleField): Value {
  if (value === null || value === undefined) return null;

  if (value instanceof Record) return value;

  if (/text|string/i.test(field.column.type)) {
    return value;
  }

  if (/date|time/i.test(field.column.type)) {
    // MUST BE IN ISO 8601 FORMAT!
    return new Date(value as string).toISOString();
  }

  if (/int|long/i.test(field.column.type)) {
    return typeof value === 'boolean'
      ? value
        ? 1
        : 0
      : parseInt(value as string);
  }

  if (/float|double/i.test(field.column.type)) {
    return parseFloat(value as string);
  }

  if (/^bool/i.test(field.column.type)) {
    if (typeof value === 'boolean') {
      return value;
    }
    if (typeof value === 'string') {
      return /^false|0$/i.test(value) ? false : true;
    }
    return !!value;
  }

  if (field.uniqueKey) {
    // mysql ...
    return (value + '').trim();
  }

  return value;
}

export function toRow(value: Value, field: SimpleField): Value {
  if (value && /date|time/i.test(field.column.type)) {
    return new Date(value as string)
      .toISOString()
      .slice(0, 23) // datetime(3)
      .replace('T', ' ');
  }
  return value;
}

function setNullForeignKeys(result: Document, model: Model): Document {
  if (model.keyValue(result) === null) {
    return null;
  }

  for (const field of model.fields) {
    if (field instanceof ForeignKeyField && result[field.name]) {
      result[field.name] = setNullForeignKeys(
        result[field.name] as Document,
        field.referencedField.model
      );
    }
  }

  return result;
}

export function toDocument(row: Row, model: Model, fieldMap = {}): Document {
  const result = {};
  for (const key in row) {
    const fieldNames = key.split('__');
    let currentResult = result;
    let currentModel = model;

    for (let i = 0; i < fieldNames.length - 1; i++) {
      const fieldName = fieldNames[i];
      if (!currentResult[fieldName]) {
        currentResult[fieldName] = {};
      }
      const field = currentModel.field(fieldName);
      if (!(field instanceof ForeignKeyField)) {
        throw Error(`Not a foreign key: ${key}`);
      }
      currentResult = currentResult[fieldName];
      currentModel = field.referencedField.model;
    }

    const field = currentModel.field(fieldNames[fieldNames.length - 1]);

    const fieldName = fieldMap[key] || field.name;
    if (field instanceof SimpleField) {
      const value = _toCamel(row[key], field);
      if (field instanceof ForeignKeyField) {
        if (value !== null) {
          if (!(field.name in currentResult)) {
            currentResult[fieldName] = {};
          }
          let keyField = field.referencedField.model.keyField();
          let result = currentResult[fieldName];
          while (keyField instanceof ForeignKeyField) {
            result[keyField.name] = {};
            result = result[keyField.name];
            keyField = keyField.referencedField.model.keyField();
          }
          result[keyField.name] = value;
        } else {
          currentResult[fieldName] = null;
        }
      } else {
        currentResult[fieldName] = value;
      }
    }
  }

  return setNullForeignKeys(result, model);
}

export function isEmpty(value: Value | Record | any) {
  if (value === undefined) {
    return true;
  }

  if (value instanceof Record) {
    while (value.__state.merged) {
      value = value.__state.merged;
    }
    if (value.__primaryKeyDirty()) return true;
    return isEmpty(value.__primaryKey());
  }

  return false;
}

export function isValue(value): boolean {
  if (value === null) return true;

  const type = typeof value;
  if (type === 'string' || type === 'number' || type === 'boolean') {
    return true;
  }

  return value instanceof Date;
}

function pkOf(model: Model, data: unknown): Value {
  if (isValue(data)) return <Value>data;
  const pk = model.keyField();
  if (pk instanceof ForeignKeyField) {
    return pkOf(pk.referencedField.model, (<Document>data)[pk.name]);
  }
  return <Value>(<Document>data)[pk.name];
}

export function shouldSelectSeparately(
  model: Model,
  fields: string | Document
) {
  if (typeof fields === 'string') {
    return false;
  }

  for (const name in fields) {
    if (model.field(name) instanceof RelatedField) {
      return true;
    }
  }

  return false;
}
