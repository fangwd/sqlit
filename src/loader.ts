import { Document, Table, Filter } from './database';
import { Record } from './record';
import { ForeignKeyField, RelatedField, Field, SimpleField } from './model';

export interface RecordConfig {
  [key: string]: string | string[];
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
  const defaultMap: Map<Record, Document> = new Map();
  const rowMap: Map<string, Record> = new Map();

  for (const key in data) {
    const value = data[key];

    if (key in config) {
      const setField = selector => {
        let _model = model;
        let _row = row;
        let _defaults = defaults;

        const names = selector.split('.');

        for (let i = 0; i < names.length - 1; i++) {
          _defaults = _defaults && (_defaults[names[i]] as Document);
          const path = names.slice(0, i + 1).join('.');
          const row = rowMap.get(path);
          if (row) {
            _row = row;
          } else {
            _row = getRecordField(_row, _model.field(names[i]));
            rowMap.set(path, _row);
            if (_model.field(names[i]) instanceof RelatedField && _defaults) {
              defaultMap.set(_row, _defaults);
            }
          }
          _model = _row.__table.model;
        }

        const field = _model.field(names[names.length - 1]);

        if (field instanceof ForeignKeyField) {
          _row[names[names.length - 1]] = value || null;
        } else {
          _row[names[names.length - 1]] = value;
        }
      };

      if (Array.isArray(config[key])) {
        for (const name of config[key]) {
          setField(name);
        }
      } else {
        setField(config[key]);
      }
    } else {
      // "*": "categoryAttributes[name,value]"
      if (!config['*']) {
        throw Error(`Unknown field: ${key}`);
      }
      const option = parseRelatedOption(config['*'] as string);
      const field = model.field(option.name);
      if (field instanceof RelatedField) {
        const table = db.table(field.referencingField.model);
        let record = table.append();
        record[field.referencingField.name] = row;

        if (option.value) {
          record[option.key] = key;
          record[option.value] = value;
        } else {
          if (!field.throughField) {
            record[option.key] = value;
          } else {
            const referencedField = field.throughField.referencedField;
            const referencedTable = db.table(referencedField.model);
            const referencedRecord = referencedTable.append();
            referencedRecord[option.key] = value;
            record[field.throughField.name] = referencedRecord;
            record = referencedRecord;
          }
        }

        const _defaults = defaults && (defaults[field.name] as Document);
        if (_defaults) {
          setDefaults(record, _defaults);
        }
      } else {
        throw Error(`Invalid field: ${model.name}.${key}`);
      }
    }
  }

  if (defaults) {
    setDefaults(row, defaults);
    defaultMap.forEach((defaults, row) => {
      setDefaults(row, defaults);
    });
  }

  return row;
}

function getRecordField(row: Record, field: Field): Record {
  const db = row.__table.db;

  if (field instanceof ForeignKeyField) {
    if (!row[field.name]) {
      row[field.name] = db.table(field.referencedField).append();
    }
    return row[field.name];
  } else if (field instanceof RelatedField) {
    if (field.throughField) {
      const record = db
        .table(field.throughField.referencedField.model)
        .append();
      const bridge = db.table(field.referencingField.model).append();
      bridge[field.referencingField.name] = row;
      bridge[field.throughField.name] = record;
      return record;
    } else {
      const table = db.table(field.referencingField.model);
      const record = table.append();
      record[field.referencingField.name] = row;
      return record;
    }
  }

  throw Error(`Invalid field: ${field && field.displayName()}`);
}

function setDefaults(row: Record, defaults: Document) {
  for (const name in defaults) {
    const field = row.__table.model.field(name);
    const value = defaults[name];

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
    } else if (field instanceof SimpleField) {
      if (row[name] === undefined) {
        row[name] = value;
      }
    }
  }
}

export function parseRelatedOption(spec: string) {
  // "categoryAttributes[name,value]"
  const [name, optional] = spec.split('[');

  if (optional) {
    const parts = optional.replace(/\]\s*$/, '').split(',');
    return {
      name,
      key: parts[0].trim(),
      value: parts[1] && parts[1].trim()
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

export function recordConfigToDocument(
  table: Table,
  config: RecordConfig
): Document {
  const result = {};

  for (const name in config) {
    let selector: string;

    if (Array.isArray(config[name])) {
      selector = config[name][0];
    } else {
      selector = config[name] as string;
    }

    let names = selector.split('.');
    let model = table.model;
    let fields = result;

    for (let i = 0; i < names.length - 1; i++) {
      const field = model.field(names[i]);
      if (field instanceof ForeignKeyField) {
        model = field.referencedField.model;
      } else {
        const related = field as RelatedField;
        if (related.throughField) {
          model = related.throughField.referencedField.model;
        } else {
          model = related.referencingField.model;
        }
      }
      if (field instanceof RelatedField) {
        if (!fields[field.name]) {
          fields[field.name] = { fields: {} };
        }
        fields = fields[field.name].fields;
      } else {
        if (!fields[field.name]) {
          fields[field.name] = {};
        }
        fields = fields[field.name];
      }
    }

    fields[names[names.length - 1]] = names[names.length - 1];
  }

  return result;
}

export function mapDocument(doc: Document, config: RecordConfig): Document[] {
  const result = [];
  for (const entry of flatten(doc)) {
    const item = {};
    for (const name in config) {
      const path = Array.isArray(config[name]) ? config[name][0] : config[name];
      if (entry[path as string] !== undefined) {
        item[name] = entry[path as string];
      }
    }
    result.push(item);
  }
  return result;
}

function flatten(doc: Document) {
  let result = [{}];

  for (const name in doc) {
    const value = doc[name];
    if (Array.isArray(value)) {
      const next = [];
      if (value.length > 0) {
        for (const val of value) {
          const docs = flatten(val as Document);
          for (const doc of docs) {
            for (const res of result) {
              const ent = { ...res };
              for (const key in doc) {
                ent[`${name}.${key}`] = doc[key];
              }
              next.push(ent);
            }
          }
        }
        result = next;
      } else {
        // Keep
      }
    } else if (value && typeof value === 'object' && !(value instanceof Date)) {
      const docs = flatten(value as Document);
      const next = [];
      for (const doc of docs) {
        for (const res of result) {
          const ent = { ...res };
          for (const key in doc) {
            ent[`${name}.${key}`] = doc[key];
          }
          next.push(ent);
        }
      }
      result = next;
    } else {
      for (const entry of result) {
        entry[name] = value;
      }
    }
  }

  return result;
}
