import { Connection } from '.';
import {
  Database as SchemaInfo,
  Table as TableInfo,
  Column as ColumnInfo,
  Constraint as ConstraintInfo
} from 'sqlex';

export function getInformationSchema(
  connection: Connection,
  schemaName: string
): Promise<SchemaInfo> {
  return new Builder(connection, schemaName).getResult();
}

class Builder {
  connection: Connection;
  schemaName: string;
  escapedSchemaName: string;

  constructor(connection: Connection, schemaName: string) {
    this.connection = connection;
    this.schemaName = schemaName;
    this.escapedSchemaName = connection.escape(schemaName);
  }

  getResult(): Promise<SchemaInfo> {
    return Promise.all([
      this.getTables(),
      this.getColumns(),
      this.getTableConstraints(),
      this.getKeyColumnUsage()
    ]).then(result => {
      const [
        tableSet,
        tableColumnsMap,
        tableConstraintMap,
        tableConstraintColumnsMap
      ] = result;

      const schemaInfo = {
        name: this.schemaName,
        tables: []
      };

      for (const tableName in tableColumnsMap) {
        if (!tableSet.has(tableName)) continue;
        const tableInfo: TableInfo = {
          name: tableName,
          columns: tableColumnsMap[tableName],
          constraints: []
        };

        for (const constraintName in tableConstraintMap[tableName]) {
          const type = tableConstraintMap[tableName][constraintName];
          const columns = tableConstraintColumnsMap[tableName][constraintName];
          const constraint: ConstraintInfo = {
            name: constraintName,
            columns: columns.map(entry => entry[0])
          };
          switch (type) {
            case 'PRIMARY KEY':
              constraint.primaryKey = true;
              break;
            case 'UNIQUE':
              constraint.unique = true;
              break;
            case 'FOREIGN KEY':
              constraint.references = {
                table: columns[0][1][0],
                columns: columns.map(entry => entry[1][1])
              };
              break;
          }
          tableInfo.constraints.push(constraint);
        }
        schemaInfo.tables.push(tableInfo);
      }

      return schemaInfo;
    });
  }

  getTables(): Promise<Set<string>> {
    return this.connection
      .query(
        `
        select table_name from information_schema.tables
        where table_schema = ${
          this.escapedSchemaName
        } and table_type = 'BASE TABLE'
        `
      )
      .then(rows => {
        const set = new Set();
        for (const row of rows) {
          set.add(row.table_name);
        }
        return set;
      });
  }

  getColumns(): Promise<{ [key: string]: ColumnInfo[] }> {
    return this.connection
      .query(
        `
        select table_name, column_name, ordinal_position, column_default,
        is_nullable, data_type, character_maximum_length, extra
        from information_schema.columns
        where table_schema = ${this.escapedSchemaName}`
      )
      .then(rows => {
        const map = {};
        for (const row of rows) {
          map[row.table_name] = map[row.table_name] || [];
          const columnInfo: ColumnInfo = {
            name: row.column_name,
            type: row.data_type,
            nullable: row.is_nullable === 'YES'
          };
          if (/char|text/i.exec(columnInfo.type)) {
            columnInfo.size = row.character_maximum_length;
          }
          if (/auto_increment/i.exec(row.extra)) {
            columnInfo.autoIncrement = true;
          }
          map[row.table_name].push([row.ordinal_position, columnInfo]);
        }
        for (const tableName in map) {
          const columns = map[tableName];
          map[tableName] = columns.sort((a, b) => a[0] - b[0]).map(r => r[1]);
        }
        return map;
      });
  }

  // table_name => constraint_name => constraint_type
  getTableConstraints(): Promise<{ [key: string]: { [key: string]: string } }> {
    return this.connection
      .query(
        `
        select table_name, constraint_name, constraint_type
        from information_schema.table_constraints
        where table_schema = ${this.escapedSchemaName}`
      )
      .then(rows => {
        const map = {};
        for (const row of rows) {
          map[row.table_name] = map[row.table_name] || {};
          map[row.table_name][row.constraint_name] = row.constraint_type;
        }
        return map;
      });
  }

  // table_name => constraint_name => column_name[]
  getKeyColumnUsage(): Promise<{ [key: string]: { [key: string]: string[] } }> {
    return this.connection
      .query(
        `
        select table_name, constraint_name, column_name, ordinal_position,
        referenced_table_name, referenced_column_name
        from information_schema.key_column_usage
        where table_schema = ${this.escapedSchemaName}`
      )
      .then(rows => {
        const map = {};
        for (const row of rows) {
          map[row.table_name] = map[row.table_name] || {};
          map[row.table_name][row.constraint_name] =
            map[row.table_name][row.constraint_name] || [];
          map[row.table_name][row.constraint_name].push([
            row.ordinal_position,
            row.column_name,
            [row.referenced_table_name, row.referenced_column_name]
          ]);
        }
        for (const tableName in map) {
          for (const constraintName in map[tableName]) {
            const columns = map[tableName][constraintName];
            map[tableName][constraintName] = columns
              .sort((a, b) => a[0] - b[0])
              .map(r => [r[1], r[2]]);
          }
        }
        return map;
      });
  }
}
