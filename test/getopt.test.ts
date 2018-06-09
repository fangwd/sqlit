const getopt = require('../lib/getopt');

test('getopt', () => {
  const specs = [
    ['-u', '--user'],
    ['-p', '--password', 'secret'],
    ['-S', '--socket'],
    ['-v', '--verbose', true],
    ['-b', '--brief', 'verbose', false],
    ['', '--include']
  ];

  let options;

  options = getopt(specs, ['-uroot', '--password=secret', '-v']);

  expect(options.user).toBe('root');
  expect(options.password).toBe(undefined);
  expect(options.secret).toBe('secret');
  expect(options.verbose).toBe(true);
  expect(getopt(specs, ['-b']).verbose).toBe(false);

  options = getopt(specs, ['two', '--include', 'one', 'three']);

  expect(options.include).toBe('one');
  expect(options.argv.length).toBe(2);
  expect(options.argv[1]).toBe('three');

  expect(() => getopt(specs, ['-S'])).toThrow();
  expect(() => getopt(specs, ['--socket'])).toThrow();
  expect(() => getopt(specs, ['-X'])).toThrow();
  expect(() => getopt(specs, ['--unknown'])).toThrow();
});
