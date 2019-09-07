import { Connection, QueryCounter, ConnectionPool } from '.';
import Pool from './pool';

import * as tds from 'tedious';

class _ConnectionPool extends ConnectionPool {
  private pool: Pool<tds.Connection>;

  constructor(options) {
    super();
    this.pool = new Pool({
      connect: () => {
        return new Promise(resolve => {
          const connection = new tds.Connection(options);
          connection.on('connect', error => {
            if (error) throw error;
            resolve(connection);
          });
        });
      },
      close: (connection: tds.Connection) => {
        connection.close();
      }
    });
  }

  getConnection(): Promise<Connection> {
    return this.pool
      .acquire()
      .then(connection => new _Connection(connection, this.pool));
  }

  end(): Promise<void> {
    return this.pool.close();
  }

  escape(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  escapeId(name: string) {
    return `[${name}]`;
  }
}

class _Connection extends Connection {
  dialect: string = 'mssql';
  connection: tds.Connection;
  connected: boolean;
  private pool?: Pool<tds.Connection>;

  queryCounter: QueryCounter = new QueryCounter();

  constructor(connection: tds.Connection, pool?: Pool<tds.Connection>) {
    super();
    this.connection = connection;
    if (pool) {
      this.pool = pool;
      this.connected = true;
    } else {
      this.connected = false;
    }
  }

  release() {
    if (this.pool) {
      this.pool.release(this.connection);
    }
  }

  private _connect(): Promise<tds.Connection> {
    if (this.connected) {
      return Promise.resolve(this.connection);
    } else {
      return new Promise(resolve => {
        this.connection.on('connect', () => {
          this.connected = true;
          resolve(this.connection);
        });
      });
    }
  }

  private _query(sql: string): Promise<any> {
    console.log('--', sql);
    const rows = [];
    return new Promise((resolve, reject) => {
      const request = new tds.Request(sql, (err, rowCount) => {
        if (err) {
          reject(err);
        } else {
          resolve({ rowCount, rows });
        }
      });
      request.on('row', row => {
        rows.push(
          row.reduce((res, col) => {
            res[col.metadata.colName] = col.value;
            return res;
          }, {})
        );
      });
      this._connect().then(connection => connection.execSql(request));
    });
  }

  beginTransaction(): Promise<void> {
    return this._connect().then(
      connection =>
        new Promise((resolve, reject) => {
          connection.beginTransaction(error => {
            if (error) reject(error);
            else resolve();
          });
        })
    );
  }

  commit(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.commitTransaction(error => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  rollback(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.rollbackTransaction(error => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  async query(sql: string): Promise<any[] | any> {
    this.queryCounter.total++;

    if (/^\s*select\s/i.test(sql)) {
      return this._query(sql).then(res => res.rows);
    }

    if (/^\s*insert\s/i.test(sql)) {
      const query =
        sql.replace(/;\s*/, '') + ';' + 'select SCOPE_IDENTITY() as insertId';
      return this._query(query).then(
        res => res.rows[0] && res.rows[0].insertId
      );
    }

    if (/^\s*(update|delete)\s/i.test(sql)) {
      return this._query(sql).then(res => ({
        affectedRows: res.rowCount,
        changedRows: res.rowCount
      }));
    }

    return this._query(sql);
  }

  end(): Promise<void> {
    if (this.pool) {
      this.pool.release(this.connection);
    } else {
      this.connection.close();
    }
    return Promise.resolve();
  }

  escape(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  escapeId(name: string) {
    return `[${name}]`;
  }
}

export default {
  createConnectionPool: (options): ConnectionPool => {
    return new _ConnectionPool(options);
  },
  createConnection: (options): Connection => {
    const connection = new tds.Connection(options);
    return new _Connection(connection);
  }
};
