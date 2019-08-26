import {
  Connection,
  TransactionCallback,
  QueryCounter,
  ConnectionPool
} from '.';

import * as sqlite3 from 'sqlite3';

class _ConnectionPool extends ConnectionPool {
  private options: any;
  private connection: _Connection;
  private queue: [any, any][];

  constructor(options) {
    super();
    this.options = options;
    this.queue = [];
    this.connection = new _Connection(this.options);
    this.connection._pool = this;
  }

  getConnection(): Promise<Connection> {
    return new Promise<Connection>((resolve, reject) => {
      this.queue.push([resolve, reject]);
      if (this.queue.length === 1) {
        resolve(this.connection);
      }
    });
  }

  reclaim() {
    this.queue.shift();
    if (this.queue.length > 0) {
      this.queue[0][0](this.connection);
    }
  }

  end(): Promise<void> {
    return Promise.resolve();
  }

  escape(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  escapeId(name: string) {
    return `"${name}"`;
  }
}

class _Connection extends Connection {
  _pool: _ConnectionPool;

  dialect: string = 'sqlite3';
  connection: sqlite3.Database;
  queryCounter: QueryCounter = new QueryCounter();

  constructor(options, connected?: boolean) {
    super();
    if (connected) {
      this.connection = options;
    } else {
      this.connection = new sqlite3.Database(options.database);
    }
  }

  release() {
    if (this._pool) {
      this._pool.reclaim();
      return Promise.resolve();
    }
    return new Promise(resolve =>
      this.connection.close(err => {
        if (err) throw err;
        resolve();
      })
    );
  }

  query(sql: string): Promise<any[] | any> {
    this.queryCounter.total++;
    return new Promise((resolve, reject) => {
      if (/^\s*select\s/i.test(sql)) {
        this.connection.all(sql, function(err, rows) {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      } else {
        this.connection.run(sql, function(error) {
          if (error) {
            return reject(error);
          }
          if (/^\s*insert\s/i.test(sql)) {
            resolve(this.lastID);
          } else {
            resolve({
              changedRows: this.changes,
              affectedRows: this.changes
            });
          }
        });
      }
    });
  }

  private beginTransaction(callback: (error) => void) {
    return this.connection.run('BEGIN TRANSACTION', callback);
  }

  transaction(callback: TransactionCallback): Promise<any> {
    return new Promise((resolve, reject) => {
      return this.beginTransaction(error => {
        if (error) return reject(error);
        let promise;
        try {
          promise = callback(this);
        } catch (error) {
          return this.rollback().then(() => reject(error));
        }
        if (promise instanceof Promise) {
          return promise
            .then(result =>
              this.commit()
                .then(() => resolve(result))
                .catch(error => {
                  return this.rollback().then(() => {
                    reject(error);
                  });
                })
            )
            .catch(reason => {
              this.rollback().then(() => {
                reject(reason);
              });
            });
        } else {
          resolve();
        }
      });
    });
  }

  commit(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.run('COMMIT', error => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  rollback(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.run('ROLLBACK', error => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  end(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.close(err => {
        if (err) reject(err);
        else resolve();
      });
    });
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
