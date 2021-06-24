import { Connection, QueryCounter, ConnectionPool, Dialect } from '.';
import { Pool, Client, PoolClient, PoolConfig, ClientConfig } from 'pg';
import {
  Database as SchemaInfo,
  Table as TableInfo,
  Column as ColumnInfo,
  Constraint as ConstraintInfo,
  Value,
} from 'sqlex';
import {lower, queryInformationSchema as query } from './util';

export class _ConnectionPool extends ConnectionPool {
  pool: Pool;

  constructor(options: PoolConfig) {
    super();
    this.pool = new Pool(options);
  }

  async getConnection(): Promise<Connection> {
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
  dialect: Dialect = 'postgres';
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
    else {
      client.release();
    }
  }

  escape(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
  }

  escapeId(name: string) {
    return `"${name}"`;
  }
}

function valueOf(obj: any): Value {
  if (!obj || typeof obj !== 'object' || obj instanceof Date) {
    return obj;
  }
  return valueOf(obj[Object.keys(obj)[0]]);
}

type ColumnUsage = {
  column: string,
  // position_in_unique_constraint: for a foreign-key constraint, ordinal position
  // of the referenced column within its unique constraint; otherwise null.
  position: number | null,
}
type ColumnUsageMap = { [key: string]: { [key: string]: ColumnUsage[] } };
type ForeignKeyMap = { [key: string]: { [key: string]: { table: string; constraint: string } } };

class SchemaBuilder {
  connection: Connection;
  catalogName: string;
  escapedCatalogName: string;

  constructor(connection: Connection, catalogName: string) {
    this.connection = connection;
    this.catalogName = catalogName;
    this.escapedCatalogName = connection.escape(catalogName);
  }

  get dialet() {
    return this.connection.dialect;
  }

  async getResult(): Promise<SchemaInfo> {
    const tableColumnsMap = await this.getColumns();
    const tableConstraintMap = await this.getTableConstraints();
    const columnUsageMap = await this.getKeyColumnUsage();
    const foreignKeyMap = await this.getForeignKeyMap();

    const schemaInfo = {
      name: this.catalogName,
      tables: [],
    };

    for (const tableName in tableColumnsMap) {
      const tableInfo: TableInfo = {
        name: tableName,
        columns: tableColumnsMap[tableName],
        constraints: [],
      };

      for (const constraintName in tableConstraintMap[tableName]) {
        const type = tableConstraintMap[tableName][constraintName];
        if (!/^(PRIMARY|UNIQUE|FOREIGN)/.test(type)) {
          continue;
        }
        const constraint: ConstraintInfo = {
          name: constraintName,
          columns: columnUsageMap[tableName][constraintName].map(e => e.column),
        };
        switch (type) {
          case 'PRIMARY KEY':
            constraint.primaryKey = true;
            break;
          case 'UNIQUE':
            constraint.unique = true;
            break;
          case 'FOREIGN KEY':
            const { table, constraint: unique } = foreignKeyMap[tableName][constraintName];
            const columns = columnUsageMap[table][unique].map(e => e.column);
            constraint.references = { table, columns };
            break;
        }
        tableInfo.constraints.push(constraint);
      }
      schemaInfo.tables.push(tableInfo);
    }
    return schemaInfo;
  }

  async getColumns(): Promise<{ [key: string]: ColumnInfo[] }> {
    const enumMap = await this.getEnumMap();
    const rows = await query(this.connection, `
      select table_name, column_name, ordinal_position, column_default,
      is_nullable, data_type, character_maximum_length, udt_name
      from information_schema.columns
      where table_catalog = ${this.escapedCatalogName} and table_schema = 'public';
    `);
    const map = {};
    for (const row of rows) {
      map[row.table_name] = map[row.table_name] || [];
      const columnInfo: ColumnInfo = {
        name: row.column_name,
        type: row.data_type.split(/\s/)[0],
        nullable: row.is_nullable === 'YES',
      };
      if (/char|text/i.exec(columnInfo.type)) {
        columnInfo.size = row.character_maximum_length;
      }
      else if (/USER-DEFINED/i.test(columnInfo.type)) {
        columnInfo.type = 'varchar';
        if (enumMap[row.udt_name]) {
          const values = enumMap[row.udt_name];
          columnInfo.size = Math.max(...(values.map(value => value.length)));
          (columnInfo as any).udt = {
            type: 'enum',
            name: row.udt_name,
            values,
          }
        }
        else {
          columnInfo.size = 255;
          (columnInfo as any).udt = row.udt_name;
        }
      }
      else if (/^(big)?(int|long)/i.test(columnInfo.type)) {
        if (/^nextval\(/i.exec(row.column_default)) {
          columnInfo.autoIncrement = true;
        }
      }
      map[row.table_name].push([row.ordinal_position, columnInfo]);
    }
    for (const tableName in map) {
      const columns = map[tableName];
      map[tableName] = columns.sort((a, b) => a[0] - b[0]).map((r) => r[1]);
    }
    return map;
  }

  // table_name => constraint_name => constraint_type
  async getTableConstraints(): Promise<{ [key: string]: { [key: string]: string } }> {
   const rows = await query(this.connection, `
      select table_name, constraint_name, constraint_type
      from information_schema.table_constraints
      where table_catalog = ${this.escapedCatalogName} and table_schema = 'public'
    `);
    const map = {};
    for (let row of rows) {
      row = lower(row);
      map[row.table_name] = map[row.table_name] || {};
      map[row.table_name][row.constraint_name] = row.constraint_type;
    }
    return map;
  }

  // table_name -> constraint_name -> [{ column, position(_in_unique_constraint) }]
  async getKeyColumnUsage() : Promise<ColumnUsageMap> {
    const rows = await query(this.connection, `
      select constraint_name, table_name, column_name,
             position_in_unique_constraint - 1 as position_in_unique_constraint
      from information_schema.key_column_usage
      where table_catalog = ${this.escapedCatalogName} and table_schema = 'public'
      order by table_name, constraint_name, ordinal_position;
    `);
    const result: ColumnUsageMap = {};
    for (const row of rows) {
      const constraintsMap = result[row.table_name];
      const column = {
        column: row.column_name,
        position: row.position_in_unique_constraint,
      };
      if (!constraintsMap) {
        result[row.table_name] = { [row.constraint_name]: [column] };
      }
      else {
        constraintsMap[row.constraint_name] = constraintsMap[row.constraint_name] || [];
        constraintsMap[row.constraint_name].push(column);
      }
    }
    return result;
  }

  // table_name -> constraint_name -> [{ table, constraint }]
  async getForeignKeyMap() : Promise<ForeignKeyMap> {
    const rows = await query(this.connection, `
      select distinct fk.table_name as table_name, rc.constraint_name,
          pk.table_name as foreign_table_name, rc.unique_constraint_name
      from
          information_schema.referential_constraints rc
          join information_schema.table_constraints fk on
              rc.constraint_name = fk.constraint_name and rc.constraint_schema = fk.table_schema
          join information_schema.table_constraints pk on
              rc.unique_constraint_name = pk.constraint_name and rc.unique_constraint_schema = pk.table_schema
      where
          fk.table_catalog=${this.escapedCatalogName} and fk.table_schema = 'public';
    `);
    const result: ForeignKeyMap = {};
    for (const row of rows) {
      result[row.table_name] = result[row.table_name] || {};
      result[row.table_name][row.constraint_name] = {
        table: row.foreign_table_name,
        constraint: row.unique_constraint_name,
      };
    }
    return result;
  }

  async getEnumMap(): Promise<{[key:string]: string[]}> {
    const rows = await query(this.connection, `
      select n.nspname as enum_schema, t.typname as enum_name, e.enumlabel as enum_value
      from pg_type t
          join pg_enum e on t.oid = e.enumtypid
          join pg_catalog.pg_namespace n ON n.oid = t.typnamespace
      where n.nspname NOT IN ('pg_catalog', 'information_schema')
    `)
    const result = {};
    for (const row of rows) {
      result[row.enum_name] = result[row.enum_name] || [];
      result[row.enum_name].push(row.enum_value);
    }
    return result;
  }

}

export default {
  createConnectionPool: (options): ConnectionPool => {
    return new _ConnectionPool(options);
  },
  createConnection: (options): Connection => {
    return new _Connection(options);
  },
  SchemaBuilder,
};
