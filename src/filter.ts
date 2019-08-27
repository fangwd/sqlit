import {
  Filter,
  OrderBy,
  toRow,
  shouldSelectSeparately,
  getUniqueFields
} from './database';
import { Record } from './record';

import {
  Model,
  Field,
  SimpleField,
  ForeignKeyField,
  RelatedField,
  Document,
  Value,
  isValue
} from 'sqlex';

import { Dialect } from './engine';
import { toArray } from './misc';

export interface AliasEntry {
  name: string;
  model: Model;
}

export interface SelectQuery {
  fields: string;
  tables: string;
  where?: string;
  orderBy?: string;
}

export class Context {
  private counter: number;

  aliasMap: { [key: string]: AliasEntry } = {};

  constructor() {
    this.counter = 0;
  }

  getAlias(builder: QueryBuilder): string {
    const model = builder.model;
    const field = builder.field;
    const names: string[] = [];
    while (builder.parent) {
      names.unshift(builder.field.name);
      builder = builder.parent;
    }
    const alias = 't' + this.counter++;
    if (field instanceof ForeignKeyField) {
      this.aliasMap[names.join('.')] = { name: alias, model: model };
    }
    return alias;
  }
}

export class QueryBuilder {
  model: Model;
  field?: Field;
  parent?: QueryBuilder;

  dialect: Dialect;
  context?: Context;
  alias?: string;

  froms?: string[];

  fieldMap = {};

  getFroms() {
    let builder: QueryBuilder = this;
    while (builder && builder.field instanceof ForeignKeyField) {
      builder = builder.parent;
    }
    return builder.froms;
  }

  // (model, dialect), or (parent, field)
  constructor(model: Model | QueryBuilder, dialect: Dialect | Field) {
    if (model instanceof Model) {
      this.model = model;
      this.dialect = dialect as Dialect;
      this.context = new Context();
    } else {
      this.parent = model;
      this.field = dialect as Field;
      this.dialect = this.parent.dialect;
      this.context = this.parent.context;
      if (dialect instanceof ForeignKeyField) {
        this.model = dialect.referencedField.model;
      } else if (dialect instanceof RelatedField) {
        this.model = dialect.referencingField.model;
      }
      this.alias = this.context.getAlias(this);
    }
  }

  where(args: Filter): string {
    if (!args) return '';
    args = plainify(args);
    if (Array.isArray(args)) {
      return this.or(args);
    } else {
      return this.and(args as Filter);
    }
  }

  private or(args: Filter[]): string {
    const exprs = args.map(arg => this.and(arg));
    return exprs.length === 0
      ? ''
      : exprs.length === 1
      ? exprs[0]
      : exprs.map(x => `(${x})`).join(' or ');
  }

  private and(args: Filter): string {
    const exprs: string[] = [];
    for (const key in args) {
      const [name, operator] = splitKey(key);
      const field = this.model.field(name);
      const value = args[key];
      if (field instanceof ForeignKeyField) {
        const query = value as Filter;
        if (query === null || typeof query !== 'object') {
          exprs.push(this.expr(field, '=', value));
        } else if (Array.isArray(query)) {
          const values = [];
          const filter = [];
          for (const arg of query) {
            if (arg === null || typeof arg !== 'object') {
              values.push(arg);
            } else {
              filter.push(arg);
            }
          }
          let expr =
            values.length > 0 ? this.expr(field, 'in', values) : 'false';
          if (filter.length > 0) {
            if (expr.length > 0) expr += ' or ';
            expr += this._join(field, filter);
          }
          if (expr.length > 0) {
            exprs.push(`${expr}`);
          }
        } else {
          const keys = Object.keys(query);
          if (keys.length === 1) {
            const [name, operator] = splitKey(keys[0] as string);
            if (name === field.referencedField.name) {
              const value = query[keys[0]] as Value;
              if (isValue(value)) {
                exprs.push(this.expr(field, operator, value));
                continue;
              }
            }
            const relatedField = field.referencedField.model.field(name);
            if (relatedField instanceof RelatedField) {
              const referencingField = relatedField.referencingField;
              const builder = new QueryBuilder(
                referencingField.model,
                this.dialect
              );
              let filter = query[keys[0]] as Filter;
              if (relatedField.throughField && relatedField.name === keys[0]) {
                filter = {
                  [relatedField.throughField.name]: filter
                };
              }
              const rhs = builder.select(referencingField, filter);
              const lhs = this.alias
                ? `${this.escapeId(this.alias)}.${this.escapeId(field)}`
                : this.escapeId(field);
              exprs.push(`${lhs} in (${rhs})`);
              continue;
            }
          }
          const expr = this._join(field, query);
          if (expr.length > 0) {
            exprs.push(`${expr}`);
          }
        }
      } else if (field instanceof SimpleField) {
        exprs.push(this.expr(field, operator, value));
      } else if (field instanceof RelatedField) {
        const filter = value === '*' ? {} : (value as Filter);
        exprs.push(this.exists(field, operator, filter));
      } else if (key === AND) {
        // { and: [{name_like: "%Apple%"}, {price_lt: 6}] }
        exprs.push(value.map(c => this.and(c)).join(' and '));
      } else if (key === OR) {
        /*
         { or: [
                { name_like: "%Apple%" },
                { productCategories_some:
                  { category: { name: 'Banana' } }
                }
              ]
         }
         */
        exprs.push('(' + value.map(c => this.and(c)).join(' or ') + ')');
      } else if (key === NOT) {
        /*
         { not: [
                { name_like: "%Apple%" },
                { productCategories_some:
                  { category: { name: 'Banana' } }
                }
              ]
         }
         */
        const filters = toArray(value);
        exprs.push('not (' + filters.map(c => this.and(c)).join(' or ') + ')');
      } else if (name !== '*') {
        throw Error(`Bad field: ${this.model.name}.${name}`);
      }
    }
    return exprs.length === 0
      ? ''
      : exprs.length === 1
      ? exprs[0]
      : exprs.map(x => `(${x})`).join(' and ');
  }

  private expr(field: SimpleField, operator: string, value: Value | Value[]) {
    const lhs = this.encodeField(field.column.name);
    if (Array.isArray(value)) {
      if (!operator || operator === 'in') {
        const values = value
          .filter(value => value !== null)
          .map(value => this.escape(field, value));
        if (values.length < value.length) {
          return values.length === 0
            ? `${lhs} is null`
            : `(${lhs} is null or ${lhs} in (${values.join(', ')}))`;
        } else {
          if (values.length === 0) return 'false';
          return `${lhs} in (${values.join(', ')})`;
        }
      } else {
        throw Error(`Bad value: ${JSON.stringify(value)}`);
      }
    }

    operator = operator || '=';

    if (operator === '=' && value === null) {
      return `${lhs} is null`;
    }

    if (operator === 'null') {
      return value ? `${lhs} is null` : `${lhs} is not null`;
    }

    return `${lhs} ${operator} ${this.escape(field, value)}`;
  }

  _extendFilter(filter: Filter, orderBy: OrderBy): Filter {
    for (const entry of toArray(orderBy)) {
      const fields = entry.replace(/^-/, '').split('.');
      let result = filter;
      let model = this.model;
      for (let i = 0; i < fields.length - 1; i++) {
        const name = fields[i];
        const field = model.field(name);
        if (!(field instanceof ForeignKeyField)) {
          throw Error(`Not a foreign key: ${entry}`);
        }
        if (
          i < fields.length - 2 ||
          (i === fields.length - 2 &&
            fields[i + 1] !== field.referencedField.model.primaryKey.name())
        ) {
          if (!result[name]) {
            result[name] = {};
          }
          result = result[name];
          model = field.referencedField.model;
        }
      }
    }
    return filter;
  }

  _pushField(fields: string[], path: string) {
    const aliasMap = this.context.aliasMap;
    let alias: string, field: Field;

    const match = /^(.+)\.([^\.]+)/.exec(path);

    if (match) {
      const entry = aliasMap[match[1]];
      if (entry) {
        alias = entry.name;
        field = entry.model.field(match[2]);
      } else {
        alias = this.alias || this.model.table.name;
        field = this.model.field(path.split('.')[0]);
      }
    } else {
      alias = this.alias || this.model.table.name;
      field = this.model.field(path);
    }

    if (field instanceof SimpleField) {
      const column = `${this.escapeId(alias)}.${this.escapeId(field)}`;
      if (match) {
        const name = this.escapeId(path.replace(/\./g, '__'));
        fields.push(`${column} as ${name}`);
      } else {
        fields.push(`${column}`);
      }
      return column;
    }

    throw new Error(`Invalid field: ${path}`);
  }

  _select(
    name: string | SimpleField | Document,
    filter: Filter = {},
    orderBy?: OrderBy
  ): SelectQuery {
    this.froms = [`${this.escapeId(this.model)} ${this.alias || ''}`];

    if (!(name instanceof Field || typeof name === 'string')) {
      // { code: 'ID', user: { firstName: 'name' } }
      extendFilter(this.model, filter, name as Document);
    }

    if (orderBy) {
      // orderBy: ['-user.email']
      filter = this._extendFilter(filter, orderBy);
    }

    const where = this.where(filter).trim();

    let fields = [];
    if (name instanceof Field || typeof name === 'string') {
      fields.push(this.encodeField(name));
    } else if (Array.isArray(name)) {
      name.forEach(name =>
        fields.push(this.encodeField(this.model.field(name) as SimpleField))
      );
    } else {
      const names = getFields(this.model, name, this.fieldMap);
      for (const key of names) {
        this._pushField(fields, key);
      }
    }

    if (orderBy) {
      orderBy = toArray(orderBy).map((order: string) => {
        const [path, direction] =
          order[0] === '-' ? [order.substr(1), 'DESC'] : [order, 'ASC'];
        const column = this._pushField(fields, path);
        return `${column} ${direction}`;
      });
    }

    return {
      fields: fields.join(', '),
      tables: this.froms.join(' left join '),
      where,
      orderBy: orderBy ? (orderBy as string[]).join(', ') : null
    };
  }

  select(
    name: string | SimpleField | Document,
    filter?: Filter,
    orderBy?: OrderBy,
    filterThunk?: (QueryBuilder) => string
  ): string {
    const query = this._select(name, filter, orderBy);
    let sql = `select ${query.fields} from ${query.tables}`;
    if (query.where) {
      sql += ` where ${query.where}`;
    }
    if (filterThunk) {
      const extra = filterThunk(this);
      if (extra) {
        if (!query.where) {
          sql += ' where ';
        } else {
          sql += ' and ';
        }
        sql += `(${extra})`;
      }
    }
    if (query.orderBy) {
      sql += ` order by ${query.orderBy}`;
    }
    return sql;
  }

  column(): string {
    return this.encodeField(this.model.keyField().column.name);
  }

  encodeField(name: string | SimpleField): string {
    if (name instanceof SimpleField) {
      name = name.column.name;
    }

    if (/count\(\*\)/i.test(name)) {
      return name;
    } else if (name !== '*') {
      name = this.escapeId(name);
    }

    const alias = this.alias || this.escapeId(this.model.table.name);

    return `${alias}.${name}`;
  }

  private _join(field: ForeignKeyField, args: Filter) {
    if (!this.getFroms()) return this._in(field, args);

    const builder = new QueryBuilder(this, field);
    const model = field.referencedField.model;
    const keys = Object.keys(args);
    if (keys.length === 1 && keys[0] === model.keyField().name) {
      if (isValue(args[keys[0]])) {
        return this.expr(field, null, args[keys[0]]);
      }
    }

    const name = `${this.escapeId(model.table.name)} ${builder.alias}`;
    const lhs = this.encodeField(field);
    const rhs = builder.encodeField(model.keyField());

    this.getFroms().push(`${name} on ${lhs}=${rhs}`);

    return builder.where(args);
  }

  private _in(field: ForeignKeyField, args: Filter) {
    const builder = new QueryBuilder(this, field);
    const model = field.referencedField.model;
    const keys = Object.keys(args);
    if (keys.length === 1 && keys[0] === model.keyField().name) {
      return this.expr(field, null, args[keys[0]]);
    }
    const lhs = this.encodeField(field.column.name);
    const rhs = builder.select(model.keyField().column.name, args);
    return `${lhs} in (${rhs})`;
  }

  private exists(field: RelatedField, operator: string, args: Filter) {
    const builder = new QueryBuilder(this, field);

    const where = field.throughField
      ? builder._in(field.throughField, args)
      : builder.where(args);

    const scope =
      builder.select('*') +
      ' where ' +
      builder.encodeField(field.referencingField.column.name) +
      '=' +
      this.encodeField(this.model.keyField().name);

    const exists = operator === 'none' ? 'not exists' : 'exists';

    return where.length > 0
      ? `${exists} (${scope} and ${where})`
      : `${exists} (${scope})`;
  }

  private escape(field: SimpleField, value: Value): string {
    if (/^bool/i.test(field.column.type)) {
      return value ? 'true' : 'false';
    }
    if (/int|float|double|number/i.test(field.column.type)) {
      if (typeof value === 'number') {
        return value + '';
      }
    }
    return this.dialect.escape(toRow(value, field) + '');
  }

  private escapeId(name: string | SimpleField | Model): string {
    if (name instanceof SimpleField) {
      name = name.column.name;
    } else if (name instanceof Model) {
      name = name.table.name;
    }
    return this.dialect.escapeId(name);
  }
}

export function encodeFilter(
  args: Filter,
  model: Model,
  escape: Dialect
): string {
  const builder = new QueryBuilder(model, escape);
  return builder.where(args);
}

export const AND = 'and';
export const OR = 'or';
export const NOT = 'not';
export const LT = 'lt';
export const LE = 'le';
export const GE = 'ge';
export const GT = 'gt';
export const NE = 'ne';
export const IN = 'in';
export const LIKE = 'like';
export const NULL = 'null';
export const SOME = 'some';
export const NONE = 'none';

const OPERATOR_MAP = {
  [LT]: '<',
  [LE]: '<=',
  [GE]: '>=',
  [GT]: '>',
  [NE]: '<>',
  [IN]: 'in',
  [LIKE]: 'like'
};

export function splitKey(arg: string): string[] {
  const match = /^(.+?)_([^_]+)$/.exec(arg);
  if (match) {
    const op = match[2] in OPERATOR_MAP ? OPERATOR_MAP[match[2]] : match[2];
    return [match[1], op];
  }
  return [arg];
}

export function plainify(value) {
  if (Array.isArray(value)) {
    return value
      .filter(entry => entry !== undefined)
      .map(entry => plainify(entry));
  } else if (isValue(value)) {
    return value;
  } else if (value instanceof Record) {
    const model = value.__table.model;
    if (value.__primaryKey()) {
      return { [model.keyField().name]: value.__primaryKey() };
    } else {
      return getUniqueFields(model, value.__data);
    }
  } else {
    const result = {};
    for (const key in value) {
      result[key] = plainify(value[key]);
    }
    return result;
  }
}

function pkOnly(doc: Document, model: Model) {
  if (doc && !isValue(doc)) {
    const keys = Object.keys(doc);
    return keys.length === 1 && keys[0] === model.keyField().name;
  }
  return false;
}

// Extends the filter to include foreign key fields
function extendFilter(model: Model, filter: Filter, fields: Document) {
  for (const name in fields) {
    const value = fields[name] === '*' ? {} : fields[name];
    if (value && typeof value === 'object') {
      const field = model.field(name);
      if (field instanceof ForeignKeyField) {
        if (!filter[name]) {
          filter[name] = {};
        }
        if (
          fields[name] === '*' ||
          !pkOnly(value as Document, field.referencedField.model)
        ) {
          filter[name]['*'] = true;
        }
        extendFilter(
          field.referencedField.model,
          filter[name] as Filter,
          value as Document
        );
      }
    }
  }
}

function getFields(
  model: Model,
  input: string | Document,
  fieldMap,
  prefix?: string
) {
  let result = [];

  const getKey = (name: string) => (prefix ? `${prefix}.${name}` : name);

  // By default, all fields in a model are included in the query result
  for (const field of model.fields) {
    if (field instanceof SimpleField) {
      result.push(getKey(field.name));
    }
  }

  if (typeof input === 'string') {
    if (input === '*') return result;
  }

  for (const name in input as Document) {
    const key = getKey(name);
    const value = input[name];

    if (!value) {
      const index = result.indexOf(key);
      if (index > -1) {
        result.splice(index, 1);
      }
    } else {
      const field = model.field(name);
      if (field instanceof ForeignKeyField) {
        const model = field.referencedField.model;
        if (!shouldSelectSeparately(model, value as Document)) {
          const fields = getFields(model, value as Document, fieldMap, key);
          result = result.concat(fields);
        }
      } else if (typeof value === 'string') {
        fieldMap[key.replace(/\./g, '__')] = value;
      }
    }
  }

  return result;
}
