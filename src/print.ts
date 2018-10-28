import { Schema, SchemaInfo } from './model';
import { SimpleField, ForeignKeyField, RelatedField } from '.';

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
