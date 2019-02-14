import { Result as SelectResult } from './select';
import { Model, ForeignKeyField, RelatedField } from './model';
import { Document, isValue } from './database';
import { Value } from './engine';
import { lcfirst } from './model';

class DocumentMap {
  map: Map<Model, Map<Value, number>>;
  next: number;

  constructor() {
    this.map = new Map();
    this.next = 1;
  }

  has(model: Model, data: Value | Document) {
    const value = toValue(model, data);
    const map = this.map.get(model);
    return map ? map.has(value) : false;
  }

  add(model: Model, value: Value) {
    let map = this.map.get(model);
    if (!map) {
      map = new Map();
      this.map.set(model, map);
    }
    map.set(value, this.next);
    return this.next++;
  }

  get(model: Model, data: Value | Document) {
    const value = toValue(model, data);
    return this.map.get(model).get(value);
  }
}

interface Task {
  model: Model;
  root: Document;
}

export class JsonSerialiser {
  data: SelectResult;
  map: DocumentMap;
  tasks: Task[];

  constructor(data: SelectResult) {
    this.data = data;
    this.map = new DocumentMap();
    this.tasks = [];
  }

  serialise(model: Model): Document[] | null {
    if (!this.data[model.name]) return null;

    const result: Document[] = [];

    this.data[model.name].forEach((doc, value) => {
      const root = { ...doc };
      this.tasks.push({ model, root });
      result.push(root);
    });

    while (this.tasks.length > 0) {
      const task = this.tasks.shift();
      this.processTask(task);
    }

    return result;
  }

  private processTask(task: Task) {
    const rootModel = task.model;
    const root = task.root;

    const pk = rootModel.keyValue(root);

    for (const field of rootModel.fields) {
      if (field instanceof ForeignKeyField) {
        if (!root[field.name]) continue;
        const model = field.referencedField.model;
        const value = model.keyValue(root[field.name] as Document);
        if (this.map.has(model, value)) {
          root[field.name] = { [model.keyField().name]: value };
        } else {
          if (this.data[model.name]) {
            const row = this.data[model.name].get(value);
            if (row) {
              const doc = { ...row };
              this.tasks.push({ model, root: doc });
              root[field.name] = doc;
              this.map.add(model, value);
            }
          }
        }
      } else if (field instanceof RelatedField) {
        const model = field.referencingField.model;

        if (!this.data[model.name]) continue;

        const rows = [];

        this.data[model.name].forEach((doc, value) => {
          if (model.valueOf(doc, field.referencingField) === pk) {
            if (field.throughField) {
              const model2 = field.throughField.referencedField.model;
              const value2 = model.valueOf(doc, field.throughField);
              if (this.map.has(model2, value2)) {
                rows.push({ [model2.keyField().name]: value2 });
              } else {
                const root = { ...this.data[model2.name].get(value2) };
                this.tasks.push({ model: model2, root });
                this.map.add(model2, value2);
                rows.push(root);
              }
            } else {
              if (this.map.has(model, value)) {
                rows.push({ [model.keyField().name]: value });
              } else {
                const root = { ...doc };
                this.tasks.push({ model, root });
                this.map.add(model, value);
                rows.push(root);
              }
            }
          }
        });

        if (field.referencingField.isUnique()) {
          root[field.name] = rows[0] || null;
        } else {
          root[field.name] = rows;
        }
      }
    }
  }
}

/* Example Usage

import com.thoughtworks.xstream.XStream;
import java.io.File;

public class Application {

  public static void main(String[] args) {
    XStream xstream = new XStream();
    xstream.setMode(XStream.ID_REFERENCES);
    XStream.setupDefaultSecurity(xstream);
    xstream.allowTypesByWildcard(new String[] {"com.example.domain.*"});
    xstream.processAnnotations(com.example.domain.Example.class);
    Example example = (Example) xstream.fromXML(new File("example.xml"));
    // ...
  }
}
*/
export class XstreamSerialiser {
  data: SelectResult;
  map: DocumentMap;
  lines: string[];

  constructor(data: SelectResult) {
    this.data = data;
    this.map = new DocumentMap();
    this.lines = [];
  }

  serialise(model: Model): string {
    if (!this.data[model.name]) return '';
    this.data[model.name].forEach((doc, value) => {
      const id = this.map.add(model, value);
      this.lines.push(`<${lcfirst(model.name)} id="${id}">`);
      this.pushFields(model, doc);
      this.lines.push(`</${lcfirst(model.name)}>`);
    });
    return this.lines.join('\n');
  }

  private pushFields(rootModel: Model, root: Document) {
    for (const field of rootModel.fields) {
      if (field instanceof ForeignKeyField) {
        if (!root[field.name]) continue;

        const model = field.referencedField.model;
        const doc = root[field.name] as Document;
        const value = model.keyValue(doc);

        if (this.map.has(model, value)) {
          const id = this.map.get(model, value);
          this.lines.push(`<${field.name} reference="${id}"/>`);
        } else {
          if (this.data[model.name]) {
            const doc = this.data[model.name].get(value);
            if (doc) {
              const id = this.map.add(model, value);
              this.lines.push(`<${field.name} id="${id}">`);
              this.pushFields(model, doc);
              this.lines.push(`</${field.name}>`);
            }
          }
        }
      } else if (field instanceof RelatedField) {
        const model = field.referencingField.model;

        if (!this.data[model.name]) continue;

        const pk = rootModel.keyValue(root);

        const unique = field.referencingField.isUnique();

        if (!unique) {
          this.lines.push(`<${field.name}>`);
        }

        this.data[model.name].forEach((doc, value) => {
          if (model.valueOf(doc, field.referencingField) === pk) {
            if (field.throughField) {
              const model2 = field.throughField.referencedField.model;
              const value2 = model.valueOf(doc, field.throughField);
              const name = unique ? field.name : lcfirst(model2.name);
              if (this.map.has(model2, value2)) {
                const id = this.map.get(model2, value2);
                this.lines.push(`<${name} reference="${id}"/>`);
              } else {
                const id = this.map.add(model2, value2);
                this.lines.push(`<${name} id="${id}">`);
                this.pushFields(model2, this.data[model2.name].get(value2));
                this.lines.push(`</${name}>`);
              }
            } else {
              const name = unique ? field.name : lcfirst(model.name);
              if (this.map.has(model, value)) {
                const id = this.map.get(model, value);
                this.lines.push(`<${name} reference="${id}"/>`);
              } else {
                const id = this.map.add(model, value);
                this.lines.push(`<${name} id="${id}">`);
                this.pushFields(model, doc);
                this.lines.push(`</${name}>`);
              }
            }
          }
        });

        if (!unique) {
          this.lines.push(`</${field.name}>`);
        }
      } else if (root[field.name] !== null) {
        this.lines.push(`<${field.name}>${root[field.name]}</${field.name}>`);
      }
    }
  }
}

function isRef(model: Model, doc: Document) {
  return Object.keys(doc).length < model.fields.length;
}

function toValue(model: Model, data: Document | Value): Value {
  return isValue(data) ? (data as Value) : model.keyValue(data as Document);
}
