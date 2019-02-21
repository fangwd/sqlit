import { Schema, SchemaInfo, Model, Field } from './model';
import { SimpleField, ForeignKeyField, RelatedField } from '.';
import { writeFileSync } from 'fs';
import { join } from 'path';

export function printSchema(
  schema: Schema | SchemaInfo,
  keyword: string = 'interface'
): string {
  if (!(schema instanceof Schema)) {
    schema = new Schema(schema);
  }

  const lines = [];
  for (const model of schema.models) {
    lines.push(`export ${keyword} ${model.name}`);
    lines.push(`{`);
    for (const field of model.fields) {
      let typeName;
      if (field instanceof ForeignKeyField) {
        typeName = field.referencedField.model.name;
      } else if (field instanceof SimpleField) {
        typeName = getTypeName(field.column.type);
      } else {
        const relatedField = field as RelatedField;
        if (relatedField.referencingField.isUnique()) {
          typeName = relatedField.referencingField.model.name;
        } else {
          typeName = relatedField.referencingField.model.name + '[]';
        }
      }
      let flag = '';
      if (field instanceof SimpleField && !field.column.nullable) {
        flag = '?';
      }
      lines.push(`${field.name}${flag}: ${typeName};`);
    }
    lines.push(`}`);
    lines.push('');
  }

  return lines.join('\n');
}

export function printSchemaTypeScript(
  schema: Schema | SchemaInfo,
  base: string = 'Model',
  column: string = 'Column'
): string {
  if (!(schema instanceof Schema)) {
    schema = new Schema(schema);
  }

  const lines = [
    `import {Filter, Value, ForeignKeyField} from 'sqlit'`,
    `import {${base}, ${column}, db} from './${base.toLowerCase()}'`,
    ''
  ];

  for (const model of schema.models) {
    lines.push(`export class ${model.name} extends ${base}`);
    lines.push(`{`);

    const table = model.table;

    for (const field of model.fields) {
      lines.push('');
      let typeName;
      if (field instanceof ForeignKeyField) {
        lines.push(`@${column}()`);
        typeName = field.referencedField.model.name;
      } else if (field instanceof SimpleField) {
        lines.push(`@${column}()`);
        typeName = getTypeName(field.config.userType || field.column.type);
      } else {
        const relatedField = field as RelatedField;
        typeName = relatedField.throughField
          ? relatedField.throughField.referencedField.model.name
          : relatedField.referencingField.model.name;
        if (!relatedField.referencingField.isUnique()) {
          typeName += '[]';
        }
      }
      lines.push(`${field.name}!: ${typeName};`);
    }

    lines.push('');
    lines.push(`constructor(data?: Partial<${model.name}>)`);
    lines.push('{');
    lines.push(`super(db.table('${table.name}'), data);`);
    lines.push('}');

    lines.push('');

    const type = `Promise<${model.name} | null>`;

    lines.push(`static async get(key: Value | Filter):${type} {
      const row = await db.table('${table.name}').get(key);
      return row ? new ${model.name}(row) : null;
    }`);

    lines.push('');

    lines.push(`
    __set(data: { [key: string]: any }) {
      for (const name in data) {
        const value = data[name];
        if (value === null || value === undefined) {
          this._set(name, null);
          continue;
        }
        switch(name) {
    `);

    for (const field of model.fields) {
      if (field instanceof ForeignKeyField) {
        lines.push(`case '${field.name}':
          this.${field.name} = new ${field.referencedField.model.name}(value);
          break;
        `);
      }
    }

    lines.push(`default:
          this._set(name, value);
          break;
        }
      }
    }`);

    for (const field of model.fields) {
      if (field instanceof ForeignKeyField) {
        lines.push('');
        const name = field.name.charAt(0).toUpperCase() + field.name.slice(1);
        const type = field.referencedField.model.name;
        const promise = `Promise<${type}|null>`;
        lines.push(`async get${name}():${promise} {
          const field = this.table.model.field('${field.name}');
          const row = await super.get(field as ForeignKeyField);
          if (row) {
            this.${field.name} =  new ${type}(row);
            return this.${field.name};
          }
          return null;
        }`);
      }
    }

    lines.push(`}`);
    lines.push('');
  }

  return lines.join('\n');
}

function getTypeName(name: string) {
  if (/date|time/i.test(name)) {
    return 'Date';
  }

  if (/char|text|string/i.test(name)) {
    return 'string';
  }

  if (/int|long/i.test(name)) {
    return 'number';
  }

  if (/float|double/i.test(name)) {
    return 'number';
  }

  if (/^bool/i.test(name)) {
    return 'boolean';
  }

  throw Error(`Unknown type '${name}'`);
}

interface ExportOptions {
  path?: string;
  package?: string;
  types?: string[];
}

export function shouldSkip(entry: Model | Field, options: ExportOptions) {
  if (!options.types || options.types.length === 0) {
    return false;
  }

  if (entry instanceof Model) {
    return (
      options.types.indexOf(entry.table.name) === -1 &&
      options.types.indexOf(entry.name) === -1
    );
  }

  if (entry instanceof ForeignKeyField) {
    return shouldSkip(entry.referencedField.model, options);
  }

  if (entry instanceof RelatedField) {
    if (entry.throughField) {
      return shouldSkip(entry.throughField.referencedField.model, options);
    } else {
      return shouldSkip(entry.referencingField.model, options);
    }
  }

  return false;
}

export function exportSchemaJava(
  schema: Schema | SchemaInfo,
  options: ExportOptions
) {
  if (!(schema instanceof Schema)) {
    schema = new Schema(schema);
  }

  options = { path: '.', package: '', types: [], ...options };

  for (const model of schema.models) {
    if (!shouldSkip(model, options)) {
      writeModelJava(model, options);
    }
  }

  printDateTimeConverter(options);
}

function writeModelJava(model: Model, options: ExportOptions) {
  const imports: Set<string> = new Set();
  const members: [string, string][] = [];
  imports.add('com.thoughtworks.xstream.annotations.XStreamAlias');
  for (const field of model.fields) {
    if (shouldSkip(field, options)) continue;
    let typeName;
    if (field instanceof ForeignKeyField) {
      typeName = field.referencedField.model.name;
    } else if (field instanceof SimpleField) {
      typeName = getTypeNameJava(field.column.type);
      if (/Date/.test(typeName)) {
        imports.add(`java.time.${typeName}`);
        imports.add('com.thoughtworks.xstream.annotations.XStreamConverter');
      }
    } else {
      const relatedField = field as RelatedField;
      if (relatedField.referencingField.isUnique()) {
        typeName = relatedField.referencingField.model.name;
      } else {
        const relatedField = field as RelatedField;
        const name = relatedField.throughField
          ? relatedField.throughField.referencedField.model.name
          : relatedField.referencingField.model.name;
        if (relatedField.referencingField.isUnique()) {
          typeName = name;
        } else {
          typeName = `List<${name}>`;
          imports.add('java.util.List');
        }
      }
    }
    members.push([typeName, field.name]);
  }

  const lines = [];

  for (const name of imports) {
    lines.push(`import ${name};`);
  }

  const alias = model.name[0].toLowerCase() + model.name.slice(1);
  lines.push(`@XStreamAlias("${alias}")`);
  lines.push(`public class ${model.name} {`);

  for (const [type, name] of members) {
    if (/Date/.test(type)) {
      lines.push('@XStreamConverter(DateTimeConverter.class)');
    }
    lines.push(`private ${type} ${name}`);
  }

  for (const [type, name] of members) {
    const getter = 'get' + name[0].toUpperCase() + name.slice(1);
    lines.push(`public ${type} ${getter}() {`);
    lines.push(`return ${name}`);
    lines.push('}');

    const setter = 'set' + name[0].toUpperCase() + name.slice(1);
    lines.push(`public void ${setter}(${type} ${name}) {`);
    lines.push(`this.${name}=${name}`);
    lines.push('}');
  }

  lines.push('}');

  const code = lines
    .join(';\n')
    .replace(/\{;/g, '{')
    .replace(/\};/g, '}')
    .replace(/(@.+?);/g, '$1');

  writeFileJava(model.name, code, options);
}

function getTypeNameJava(name: string) {
  if (/date|time/i.test(name)) {
    return 'LocalDateTime';
  }

  if (/char|text|string/i.test(name)) {
    return 'String';
  }

  if (/int|long/i.test(name)) {
    return 'int';
  }

  if (/float|double/i.test(name)) {
    return 'double';
  }

  if (/^bool/i.test(name)) {
    return 'boolean';
  }

  throw Error(`Unknown type '${name}'`);
}

// LocalDateTime.ofInstant(Instant.parse(s), ZoneOffset.of("+10:30"));
function printDateTimeConverter(options: ExportOptions) {
  const code = `
  import com.thoughtworks.xstream.converters.Converter;
  import com.thoughtworks.xstream.converters.MarshallingContext;
  import com.thoughtworks.xstream.converters.UnmarshallingContext;
  import com.thoughtworks.xstream.io.HierarchicalStreamReader;
  import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
  import java.time.Instant;
  import java.time.LocalDateTime;
  import java.time.ZoneOffset;

  public class DateTimeConverter implements Converter {

    public boolean canConvert(Class clazz) {
      return clazz.equals(LocalDateTime.class);
    }

    public void marshal(Object value, HierarchicalStreamWriter writer, MarshallingContext context) {
      LocalDateTime dateTime = (LocalDateTime) value;
      writer.setValue(dateTime.toInstant(ZoneOffset.UTC).toString());
    }

    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
      LocalDateTime dateTime =
          LocalDateTime.ofInstant(Instant.parse(reader.getValue()), ZoneOffset.UTC);
      return dateTime;
    }
  }
  `;

  writeFileJava('DateTimeConverter', code, options);
}

function writeFileJava(
  className: string,
  code: string,
  options: ExportOptions
) {
  const path = join(
    options.path,
    options.package.replace(/\./g, '/'),
    `${className}.java`
  );
  writeFileSync(
    path,
    options.package ? `package ${options.package};\n${code}` : code
  );
}
