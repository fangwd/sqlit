import { Schema, SchemaInfo, Model } from './model';
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

export function printSchemaJava(
  schema: Schema | SchemaInfo,
  path: string,
  packageName: string
) {
  if (!(schema instanceof Schema)) {
    schema = new Schema(schema);
  }

  for (const model of schema.models) {
    printModelJava(model, path, packageName);
  }

  printDateTimeConverter(path, packageName);
}

function printModelJava(model: Model, path: string, packageName: string) {
  const imports: Set<string> = new Set();
  const members: [string, string][] = [];
  imports.add('com.thoughtworks.xstream.annotations.XStreamAlias');
  for (const field of model.fields) {
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

  lines.push(`package ${packageName}`);

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

  path = join(path, packageName.replace(/\./g, '/'), model.name + '.java');

  const code = lines
    .join(';\n')
    .replace(/\{;/g, '{')
    .replace(/\};/g, '}')
    .replace(/(@.+?);/g, '$1');

  writeFileSync(path, code);
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
function printDateTimeConverter(path: string, packageName: string) {
  const code = `
  package ${packageName};

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
  path = join(path, packageName.replace(/\./g, '/'), 'DateTimeConverter.java');
  writeFileSync(path, code);
}
