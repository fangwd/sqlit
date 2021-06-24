import { getInformationSchema } from './information_schema';
import { Value } from 'sqlex';

export type Dialect = 'mysql' | 'postgres' | 'mssql' | 'oracle' | 'sqlite3';

export interface ConnectionInfo {
  dialect: Dialect;
  connection: { [key: string]: any };
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

export interface DialectEncoder {
  escape: (unsafe: any) => string;
  escapeId: (unsafe: string) => string;
}

export abstract class Connection implements DialectEncoder {
  dialect: Dialect;
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
        this.commit();
        return result;
      }
      // else: caller has dealt with the transaction
    } catch (error) {
      this.rollback();
      throw error;
    }
  }
}

export abstract class ConnectionPool implements DialectEncoder {
  dialect: Dialect;
  name: string;

  abstract getConnection(): Promise<Connection>;
  abstract end(): Promise<void>;

  abstract escape(s: string): string;
  abstract escapeId(name: string): string;
}

export function createConnectionPool(
  dialect: Dialect,
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

  throw Error(`Unsupported engine type: ${dialect}`);
}

export { getInformationSchema };
