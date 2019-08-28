import { Connection, QueryCounter, ConnectionPool } from '.';

import { Pool, Client, PoolClient, PoolConfig, ClientConfig } from 'pg';
import { Value } from 'sqlex';

export class _ConnectionPool extends ConnectionPool {
  pool: Pool;

  constructor(options: PoolConfig) {
    super();
    this.pool = new Pool(options);
  }

  getConnection(): Promise<Connection> {
    return this.pool
      .connect()
      .then(connection => new _Connection(connection, true));
  }

  end(): Promise<void> {
    return this.pool.end();
  }

  escape(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  escapeId(name: string) {
    return `"${name}"`;
  }
}

class _Connection extends Connection {
  dialect: string = 'postgres';
  connection: Client | PoolClient;
  queryCounter: QueryCounter = new QueryCounter();

  constructor(options: ClientConfig | PoolClient, connected?: boolean) {
    super();
    if (connected) {
      this.connection = options as PoolClient;
    } else {
      this.connection = new Client(options as ClientConfig);
      this.connection.connect();
    }
  }

  release() {
    const client = this.connection as PoolClient;
    if (typeof client.release === 'function') {
      client.release();
    }
  }

  async query(sql: string, pk?: string): Promise<any[] | any> {
    this.queryCounter.total++;
    console.log(sql);
    if (/^\s*insert\s/i.test(sql) && pk) {
      sql = `${sql} returning ${this.escapeId(pk)}`;
    }
    return this.connection
      .query(sql)
      .then(result => {
        switch (result.command) {
          case 'SELECT':
            return result.rows;
          case 'INSERT':
            return pk ? valueOf(result.rows[0][pk]) : undefined;
          default:
            return {
              changedRows: result.rowCount,
              affectedRows: result.rowCount
            };
        }
      })
      .catch(error => {
        throw error;
      });
  }

  end(): Promise<void> {
    const client = this.connection as PoolClient;
    if (typeof client.release !== 'function') {
      return (this.connection as Client).end();
    }
  }

  escape(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  escapeId(name: string) {
    return `"${name}"`;
  }
}

export default {
  createConnectionPool: (options): ConnectionPool => {
    return new _ConnectionPool(options);
  },
  createConnection: (options): Connection => {
    return new _Connection(options);
  }
};

function valueOf(obj: any): Value {
  if (!obj || typeof obj !== 'object' || obj instanceof Date) {
    return obj;
  }
  return valueOf(obj[Object.keys(obj)[0]]);
}
