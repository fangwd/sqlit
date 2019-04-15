export function toArray(args): Array<any> {
  return Array.isArray(args) ? args : [args];
}

const reflect = p => p.then(value => ({ value }), error => ({ error }));

export function promiseAll(promises: Promise<any>[]) {
  return Promise.all(promises.map(reflect)).then(results => {
    const error = results.find(result => 'error' in result);
    if (error) {
      throw error.error;
    } else {
      return results.map(result => result.value);
    }
  });
}
