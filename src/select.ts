import { Table, Filter, Document } from './database';
import { ForeignKeyField, RelatedField, Field, SimpleField } from './model';
import { Value } from './engine';

export type Result = { [key: string]: Map<Value, Document> };

export type FieldOptions =
  | ''
  | '*'
  | '**'
  | {
      [key: string]: FieldOptions;
    };

export async function selectTree(
  table: Table,
  filter: Filter,
  options: FieldOptions = {}
): Promise<Result> {
  const result = {};
  const querySet: Set<string> = new Set();
  return _selectTree(table, filter, options, result, null, querySet);
}

async function _selectTree(
  table: Table,
  filter: Filter,
  options: FieldOptions = {},
  result: Result = {},
  entry: ForeignKeyField | null,
  querySet: Set<string>
): Promise<Result> {
  const rows = await table.select('*', { where: filter });

  if (rows.length === 0) return result;

  const map = merge(result, table, rows);
  const model = table.model;
  const db = table.db;

  for (const field of table.model.fields) {
    if (field instanceof ForeignKeyField) {
      let option: FieldOptions;
      if (typeof options === 'string') {
        if (options !== '**') continue;
        option = '**';
      } else {
        option = options[field.name];
      }
      if (!option) continue;
      const table = db.table(field.referencedField);
      const map = merge(result, table);
      const values = rows
        .map(r => model.valueOf(r, field))
        .filter(value => !map.has(value));
      if (values.length > 0) {
        if (mayQuery(querySet, [table.model.keyField()], values)) {
          const filter = { [table.model.keyField().name]: values };
          await _selectTree(table, filter, option, result, field, querySet);
        }
      }
    }
  }

  const values = [...map.keys()];

  for (const table of db.tableList) {
    const fields: [SimpleField, FieldOptions][] = [];
    const fieldsThrough: [ForeignKeyField, FieldOptions][] = [];

    for (const field of table.model.fields) {
      if (!(field instanceof ForeignKeyField)) continue;
      if (!(field.referencedField.model === model)) continue;
      if (!field.relatedField) continue;
      if (field === entry) continue;
      const option =
        typeof options === 'string'
          ? options
          : options[field.relatedField.name];
      if (option === '') continue;
      if (field.relatedField.throughField) {
        fieldsThrough.push([field, option]);
      } else {
        fields.push([field, option]);
      }
    }

    if (fieldsThrough.length > 0) {
      const filter = fieldsThrough.map(field => ({ [field[0].name]: values }));
      const rows = await table.select('*', { where: filter });
      merge(result, table, rows);
      for (const [field, option] of fieldsThrough) {
        const key = field.relatedField.throughField;
        const table = db.table(key.referencedField);
        const map = merge(result, table);
        const values = rows
          .map(r => table.model.valueOf(r, key))
          .filter(value => !map.has(value));
        if (values.length > 0) {
          if (mayQuery(querySet, [table.model.keyField()], values)) {
            const filter = { [table.model.keyField().name]: values };
            await _selectTree(table, filter, option, result, key, querySet);
          }
        }
      }
    }

    for (const [field, option] of fields) {
      if (mayQuery(querySet, [field], values)) {
        const filter = { [field.name]: values };
        await _selectTree(table, filter, option, result, null, querySet);
      }
    }
  }

  return result;
}

function merge(result: Result, table: Table, rows?: Document[]) {
  let map = result[table.name];

  if (!map) {
    map = new Map();
    result[table.name] = map;
  }
  if (rows) {
    const model = table.model;
    const key = model.keyField();
    for (const row of rows) {
      map.set(model.valueOf(row, key), row);
    }
  }
  return map;
}

function mayQuery(querySet: Set<string>, fields: Field[], values: Value[]) {
  const key = fields
    .map(field => field.displayName() + JSON.stringify(values))
    .join('/');
  if (!querySet.has(key)) {
    querySet.add(key);
    return true;
  }
  return false;
}

export async function selectTree2(
  table: Table,
  filter: Filter
): Promise<Result> {
  const result: Result = {};
  const rows = await table.select('*', { where: filter });

  merge(result, table, rows);

  const selected: Map<Table, number> = new Map([[table, 0]]);

  const db = table.db;

  while (true) {
    let min: number = Infinity;
    let next: {
      parent: Table;
      child: Table;
      keys: ForeignKeyField[];
    };

    for (const child of db.tableList) {
      if (selected.has(child)) continue;
      selected.forEach((distance, parent) => {
        const keys = getForeignKeys(child, parent);
        if (keys.length > 0 && distance < min) {
          next = { parent, child, keys };
          min = distance;
        }
      });
    }

    if (!next) break;

    const values = [...result[next.parent.name].keys()];

    if (values.length > 0) {
      const filter = next.keys.map(key => ({
        [key.name]: values
      }));
      const rows = await next.child.select('*', { where: filter });
      merge(result, next.child, rows);
      if (next.keys.length === 1) {
        const key = next.keys[0];
        if (key.relatedField && key.relatedField.throughField) {
          const model = next.child.model;
          let values = rows.map(row =>
            model.valueOf(row, key.relatedField.throughField)
          );
          const table = db.table(key.relatedField.throughField.referencedField);
          const map = result[table.name];
          if (map) {
            values = values.filter(pk => !map.has(pk));
          }
          if (values.length > 0) {
            const key = table.model.keyField().name;
            const rows = await table.select('*', { where: { [key]: values } });
            merge(result, table, rows);
            selected.set(table, min + 1);
          }
        }
      }
    } else {
      merge(result, next.child, []);
    }
    selected.set(next.child, min + 1);
  }

  return result;
}

function getForeignKeys(child: Table, parent: Table): ForeignKeyField[] {
  const result = [];
  for (const field of child.model.fields) {
    if (field instanceof ForeignKeyField) {
      if (field.referencedField.model === parent.model) {
        result.push(field);
      }
    }
  }
  return result;
}
