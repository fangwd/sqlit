import {
  pluralise,
  setPluralForm,
  setPluralForms,
  toCamelCase,
  toPascalCase,
  promiseAll,
  pluraliser
} from '../src/misc';

test('pluralise', () => {
  expect(pluralise('category')).toBe('categories');
  expect(pluralise('hierarchy')).toBe('hierarchies');
  expect(pluralise('property')).toBe('properties');
  expect(pluralise('guy')).toBe('guys');
  expect(pluralise('child')).toBe('children');
  expect(pluralise('equipmentChild')).toBe('equipmentChildren');
  expect(pluralise('class')).toBe('classes');
});

test('customise plural forms', () => {
  expect(pluralise('foot')).toBe('foots');
  setPluralForm('foot', 'feet');
  expect(pluralise('totalFoot')).toBe('totalFeet');
  expect(pluralise('special_equipment')).toBe('special_equipments');
  setPluralForms({ tooth: 'teeth', equipment: 'equipment' });
  expect(pluralise('blueTooth')).toBe('blueTeeth');
  expect(pluralise('special_equipment')).toBe('special_equipment');
});

test('pluralise - java', () => {
  pluraliser.style = 'java';
  pluraliser.forms['foo'] = 'fooSet';
  expect(pluralise('foo')).toBe('fooSet');
  expect(pluralise('bar')).toBe('barList');
});

test('camel/pascal cases', () => {
  expect(toCamelCase('special_equipment')).toBe('specialEquipment');
  expect(toPascalCase('special_equipment')).toBe('SpecialEquipment');
});

test('promiseAll', done => {
  const results = Array.apply(null, { length: 3 });

  function createResolve(n) {
    return new Promise(resolve => {
      setTimeout(() => {
        results[n - 1] = true;
        resolve(n);
      }, n * 1000);
    });
  }

  function createReject(n) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        results[n - 1] = false;
        reject(n);
      }, n * 1000);
    });
  }

  promiseAll([createResolve(1), createReject(2), createResolve(3)]).catch(
    error => {
      expect(results[0]).toBe(true);
      expect(results[1]).toBe(false);
      expect(results[2]).toBe(true);
      expect(error).toBe(2);
      done();
    }
  );
});
