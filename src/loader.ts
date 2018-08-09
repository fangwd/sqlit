import { Document, Table } from './database';
import { Record } from './record';
import { SimpleField, ForeignKeyField, RelatedField } from './model';

export interface RecordConfig {
  [key: string]: string;
}

function append(
  table: Table,
  data: Document,
  config: RecordConfig,
  defaults?: Document
): Record {
  const db = table.db;
  const model = table.model;
  const row = table.append();
  for (const key in data) {
    const value = data[key];
    if (key in config) {
      const field = model.field(config[key]);
      if (field) {
        // "categoryName": "name"
        if (field instanceof ForeignKeyField) {
          row[field.name] = value || null;
        } else if (field instanceof SimpleField) {
          row[field.name] = value;
        }
      } else {
        // "parent_parent_name": "parent.parent.name"
        const names = config[key].split('.');
        let m = model;
        let r = row;
        for (let i = 0; i < names.length - 1; i++) {
          const field = m.field(names[i]) as ForeignKeyField;
          if (!r[field.name]) {
            r[field.name] = db.table(field.referencedField).append();
          }
          m = field.referencedField.model;
          r = r[field.name];
        }
        r[names[names.length - 1]] = value || null;
      }
    } else {
      // "*": "categoryAttributes[name,value]"
      const option = parseRelatedOption(config['*']);
      const field = model.field(option.name);
      if (field instanceof RelatedField) {
        const table = db.table(field.referencingField.model);
        const record = table.append();
        record[field.referencingField.name] = row;
        record[option.key] = key;
        record[option.value] = value;
      } else {
        throw Error(`Invalid field: ${model.name}.${key}`);
      }
    }
  }

  if (defaults) {
    setDefaults(row, defaults);
  }

  return row;
}

function setDefaults(row: Record, values: Document) {
  for (const name in values) {
    const field = row.__table.model.field(name);
    const value = values[name];

    if (field instanceof ForeignKeyField) {
      if (row[name] === undefined) {
        if (value === null) {
          row[name] = null;
          continue;
        }
        row[name] = row.__table.db.table(field.referencedField.model).append();
      }
      if (typeof value === 'object' && !(value instanceof Date)) {
        setDefaults(row[name], value as Document);
      } else {
        row[name].__setPrimaryKey(value);
      }
    } else {
      if (row[name] === undefined) {
        row[name] = value;
      }
    }
  }
}

function parseRelatedOption(spec: string) {
  // "categoryAttributes[name,value]"
  const [name, optional] = spec.split('[');

  if (optional) {
    const parts = optional.replace(/\]\s*$/, '').split(',');
    return {
      name,
      key: parts[0].trim(),
      value: parts[1].trim()
    };
  }

  return {
    name,
    key: 'name',
    value: 'value'
  };
}

export function loadTable(
  table: Table,
  data: Document | Document[],
  config: RecordConfig,
  defaults?: Document
): Promise<any> {
  if (Array.isArray(data)) {
    for (const row of data) {
      append(table, row, config, defaults);
    }
  } else {
    append(table, data, config, defaults);
  }
  return table.db.flush();
}
