import { Table, Document, toRow, isEmpty, isValue } from './database';
import {
  SimpleField,
  ForeignKeyField,
  UniqueKey,
  Field,
  RelatedField
} from './model';
import { FlushState, FlushMethod, flushRecord } from './flush';
import { Row, Value } from './engine';
import { copyRecord } from './copy';

export type FieldValue = Value | Record;

export const RecordProxy = {
  set: function(record: Record, name: string, value: any) {
    if (!/^__/.test(name)) {
      if (value === undefined) {
        throw Error(`Assigning undefined to ${name}`);
      }
      const model = record.__table.model;
      const field = model.field(name);
      if (field instanceof SimpleField) {
        // user.email = 'user@example.com'
        record.__data[name] = value;
        record.__state.dirty.add(name);
      } else if (field instanceof RelatedField) {
        throw Error(`Not assignable: ${model.name}.${name}`);
      } else {
        throw Error(`Invalid field: ${model.name}.${name}`);
      }
    } else {
      record[name] = value;
    }
    return true;
  },

  get: function(record: Record, name: string) {
    if (typeof name === 'string' && !/^__/.test(name)) {
      if (typeof record[name] !== 'function') {
        const model = record.__table.model;
        const field = model.field(name);
        if (field instanceof SimpleField) {
          return record.__data[name];
        } else if (field instanceof RelatedField) {
          let recordSet = record.__related[name];
          if (!recordSet) {
            recordSet = new RecordSet(record, field);
            record.__related[name] = recordSet;
          }
          return recordSet;
        }
      }
    }
    return record[name];
  }
};

export class Record {
  __table: Table;
  __data: { [key: string]: FieldValue };
  __state: FlushState;
  __related: { [key: string]: RecordSet };

  constructor(table: Table) {
    this.__table = table;
    this.__data = {};
    this.__state = new FlushState();
    this.__related = {};

    return new Proxy(this, RecordProxy);
  }

  get(name: string): FieldValue | undefined {
    return this.__data[name];
  }

  save(): Promise<any> {
    if (!this.__dirty()) {
      return Promise.resolve(this);
    }
    return this.__table.db.pool.getConnection().then(connection =>
      connection.transaction(() =>
        flushRecord(connection, this).then(result => {
          connection.release();
          return result;
        })
      )
    );
  }

  update(data: Row = {}): Promise<any> {
    for (const key in data) {
      this[key] = data[key];
    }
    this.__state.method = FlushMethod.UPDATE;
    return this.save();
  }

  delete(): Promise<any> {
    const filter = this.__table.model.getUniqueFields(this.__data);
    return this.__table.delete(filter);
  }

  copy(data: Document, options?) {
    return copyRecord(this, data, options);
  }

  __dirty(): boolean {
    return this.__state.dirty.size > 0;
  }

  __flushable(perfect?: boolean): boolean {
    if (this.__state.merged) {
      return false;
    }

    const data = this.__data;

    if (!this.__table.model.checkUniqueKey(data, isEmpty)) {
      return false;
    }

    if (this.__state.method === FlushMethod.DELETE) {
      return true;
    }

    let flushable = 0;

    this.__state.dirty.forEach(key => {
      if (!isEmpty(data[key])) {
        flushable++;
      }
    });

    if (flushable === 0) return false;

    return perfect ? flushable === this.__state.dirty.size : true;
  }

  __fields(): Row {
    const fields = {};
    this.__state.dirty.forEach(key => {
      if (!isEmpty(this.__data[key])) {
        fields[key] = this.__getValue(key);
      }
    });
    return fields;
  }

  __remove_dirty(keys: string | string[]) {
    if (typeof keys === 'string') {
      this.__state.dirty.delete(keys);
    } else {
      for (const key of keys) {
        this.__state.dirty.delete(key);
      }
    }
  }

  __getValue(name: string): Value {
    if (this.__data[name] instanceof Record) {
      let parent = this.__data[name] as Record;
      while (parent.__state.merged) {
        parent = parent.__state.merged;
      }
      return parent.__primaryKey();
    }
    return this.__data[name] as Value;
  }

  __primaryKey(): Value {
    const name = this.__table.model.primaryKey.fields[0].name;
    const value = this.__data[name];
    if (value instanceof Record) {
      return value.__primaryKey();
    }
    return value;
  }

  __setPrimaryKey(value: Value) {
    const name = this.__table.model.primaryKey.fields[0].name;
    this.__data[name] = value;
  }

  __filter(): Row {
    const self = this;
    const data = Object.keys(this.__data).reduce(function(acc, cur, i) {
      acc[cur] = self.__getValue(cur);
      return acc;
    }, {});
    return this.__table.model.getUniqueFields(data);
  }

  __match(row: Document): boolean {
    const model = this.__table.model;
    const fields = this.__filter();
    for (const name in fields) {
      const lhs = model.valueOf(fields, name);
      const rhs = model.valueOf(row, name);
      const field = model.field(name) as SimpleField;
      if (toRow(lhs, field) != toRow(rhs, field)) {
        return false;
      }
    }
    return true;
  }

  __valueOf(uc: UniqueKey): string {
    const values = [];
    for (const field of uc.fields) {
      let value = this.__getValue(field.name);
      if (value === undefined) return undefined;
      if (field instanceof ForeignKeyField) {
        let key = field;
        while (!isValue(value)) {
          value = value[key.referencedField.name];
          key = key.referencedField as ForeignKeyField;
        }
      }
      values.push(value);
    }
    return JSON.stringify(values);
  }

  __merge() {
    let root = this.__state.merged;
    while (root.__state.merged) {
      root = root.__state.merged;
    }
    const self = this;
    this.__state.dirty.forEach(name => {
      root.__data[name] = self.__data[name];
      root.__state.dirty.add(name);
    });
  }

  __json() {
    const result = {};
    for (const field of this.__table.model.fields) {
      result[field.name] = this.__getValue(field.name);
    }
    return result;
  }

  __dump() {
    const data = { __state: this.__state.json() };
    for (const field of this.__table.model.fields) {
      let name = field.name;
      const value = this.__data[name];
      if (value !== undefined) {
        if (this.__state.merged) {
          name = '!' + name;
        } else if (this.__state.dirty.has(name)) {
          name = '*' + name;
        }
        if (isValue(value)) {
          data[name] = value;
        } else {
          const record = value as Record;
          data[name] = record.__repr();
        }
      }
    }
    return data;
  }

  __repr() {
    const model = this.__table.model;
    const value = this.__data[model.keyField().name];
    if (value === undefined || isValue(value)) {
      return `${model.name}(${value})`;
    } else {
      const record = value as Record;
      return `${model.name}(${record.__repr()})`;
    }
  }
}

class RecordSet {
  record: Record;
  field: RelatedField;

  constructor(record: Record, field: RelatedField) {
    this.record = record;
    this.field = field;
  }

  // user.groups.add(admin)
  add(record) {
    const data = { [this.field.name]: { upsert: { create: record.__data } } };
    const filter = this.record.__filter();
    return this.record.__table.modify(data, filter);
  }

  // user.groups.replaceWith([admin, customer])
  replaceWith() {}

  // user.groups.remove(admin)
  remove(record: Record) {
    const data = { [this.field.name]: { delete: [record.__filter()] } };
    const filter = this.record.__filter();
    return this.record.__table.modify(data, filter);
  }
}

export function getModel(table: Table, bulk: boolean = false) {
  const model = function(data) {
    if (bulk) return table.append(data);
    const record = new Proxy(new Record(table), RecordProxy);
    Object.assign(record, data);
    return record;
  };
  return model;
}
