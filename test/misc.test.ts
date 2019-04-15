import { promiseAll } from '../src/misc';

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
