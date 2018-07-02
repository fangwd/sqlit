import { Schema } from '../src/model';
import { printSchema } from '../src/print';
import * as helper from './helper';

test('print', () => {
  const schema = new Schema(helper.getExampleData());
  const result = printSchema(schema);
  expect(/\bname:\s+string;/.test(result)).toBe(true);
  expect(/\border:\s+Order;/.test(result)).toBe(true);
  expect(/\borderItems:\s+OrderItem\[\];/.test(result)).toBe(true);
  expect(/\borderShipping:\s+OrderShipping;/.test(result)).toBe(true);
});
