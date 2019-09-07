import { getInformationSchema } from './information_schema';
import { Table, _toCamel } from '../database';
import { Model, Value, SimpleField } from 'sqlex';

export interface ConnectionInfo {
  dialect: string;
  connection: any;
}

export type Row = {
  [key: string]: Value;
};

export class QueryCounter {
  total: number = 0;
}

export type TransactionCallback = (
  connection: Connection
) => Promise<any> | void;

export interface Dialect {
  dialect: string;
  escape: (unsafe: any) => string;
  escapeId: (unsafe: string) => string;
}

export abstract class Connection implements Dialect {
  dialect: string;
  connection: any;
  name: string;
  queryCounter: QueryCounter;

  abstract query(sql: string, pk?: string): Promise<any>;

  beginTransaction(): Promise<void> {
    return this.query('begin');
  }

  commit(): Promise<void> {
    return this.query('commit');
  }

  rollback(): Promise<void> {
    return this.query('rollback');
  }

  abstract end(): Promise<void>;
  abstract release();

  abstract escape(s: string): string;
  abstract escapeId(name: string): string;

  async transaction(callback: TransactionCallback) {
    await this.beginTransaction();
    try {
      const promise = callback(this);
      if (promise instanceof Promise) {
        const result = await promise;
        await this.commit();
        return result;
      }
      // else: caller has dealt with the transaction
    } catch (error) {
      await this.rollback();
      throw error;
    }
  }

  private escapeInsertValues(
    model: Model,
    columns: string[],
    values: Value[]
  ): string {
    if (values.length !== columns.length) {
      throw Error('Columns and values do not match');
    }
    return (
      '(' +
      columns
        .map((column, index) => {
          const field = model.field(column);
          const value = _toCamel(values[index], field as SimpleField);
          return typeof value === 'string' ? this.escape(value) : value;
        })
        .join(',') +
      ')'
    );
  }

  insert(table: Table, columns: string[], values: Value[][] | Value[]) {
    if (columns.length === 0 || values.length === 0) {
      throw Error('Missing columns/values');
    }
    const tab = this.escapeId(table.name);
    const cols = columns.map(this.escapeId).join(',');
    const vals = Array.isArray(values[0])
      ? (values as Value[][]).map(row =>
          this.escapeInsertValues(table.model, columns, row)
        )
      : this.escapeInsertValues(table.model, columns, values as Value[]);
    const sql = `insert into ${tab} (${cols}) values ${vals}`;
    if (this.dialect === 'mssql') {
      const pk = table.model.keyField().column;
      if (pk.autoIncrement && columns.indexOf(pk.name) !== -1) {
        const on = `set identity_insert ${tab} on`;
        const ext = 'select scope_identity() as insertId';
        const off = `set identity_insert ${tab} off`;
        return this.query(`${on};${sql};${ext};${off}`).then(
          res => res.rows[0].insertId
        );
      }
    }
    return this.query(sql);
  }
}

export abstract class ConnectionPool implements Dialect {
  dialect: string;
  name: string;

  abstract getConnection(): Promise<Connection>;
  abstract end(): Promise<void>;

  abstract escape(s: string): string;
  abstract escapeId(name: string): string;
}

export function createConnectionPool(
  dialect: string,
  connection: any
): ConnectionPool {
  if (dialect === 'mysql') {
    const result = require('./mysql').default.createConnectionPool(connection);
    result.name = connection.database;
    return result;
  }

  if (dialect === 'sqlite3') {
    return require('./sqlite3').default.createConnectionPool(connection);
  }

  if (dialect === 'postgres') {
    return require('./postgres').default.createConnectionPool(connection);
  }

  if (dialect === 'mssql') {
    return require('./mssql').default.createConnectionPool(connection);
  }

  throw Error(`Unsupported engine type: ${dialect}`);
}

export function createConnection(dialect: string, connection: any): Connection {
  if (dialect === 'mysql') {
    const result = require('./mysql').default.createConnection(connection);
    result.name = connection.database;
    return result;
  }

  if (dialect === 'sqlite3') {
    return require('./sqlite3').default.createConnection(connection);
  }

  if (dialect === 'postgres') {
    return require('./postgres').default.createConnection(connection);
  }

  if (dialect === 'mssql') {
    return require('./mssql').default.createConnection(connection);
  }

  throw Error(`Unsupported engine type: ${dialect}`);
}

export { getInformationSchema };
