'use strict';

function getopt(specs, argv, i, result) {
  if (typeof argv === 'object' && !Array.isArray(argv)) {
    result = argv;
    argv = undefined;
  }

  if (argv === undefined) {
    argv = process.argv;
    i = 2;
  }

  result = result || { argv: [] };
  result.argv = result.argv || [];

  i = i || 0;

  while (i < argv.length) {
    const arg = argv[i++];
    let j;
    for (j = 0; j < specs.length; j++) {
      const spec = specs[j];
      if (_startsWith(arg, spec[0]) || _startsWith(arg, spec[1])) {
        result[spec[0]] = result[spec[1]] = true;
        if (typeof spec[2] === 'boolean' || typeof spec[3] === 'boolean') {
          if (typeof spec[2] === 'boolean') {
            result[spec[1].substr(2)] = spec[2];
          } else {
            result[spec[2]] = spec[3];
          }
        } else {
          const key = typeof spec[2] === 'string' ? spec[2] : spec[1].substr(2);
          if (_startsWith(arg, spec[0])) {
            if (arg.length > spec[0].length) {
              result[key] = arg.substr(2).replace(/^\s*=\s*/, '');
            } else {
              result[key] = argv[i++];
            }
          } else {
            const len = spec[1].length;
            if (arg.length > len) {
              result[key] = arg.substr(len).replace(/^\s*=\s*/, '');
            } else {
              result[key] = argv[i++];
            }
          }
          if (result[key] === undefined) {
            const desc =
              spec[0] && spec[1] ? `${spec[0]}/${spec[1]}` : spec[0] || spec[1];
            throw Error(`${desc} requires an argument`);
          }
        }
        break;
      }
    }
    if (j === specs.length) {
      if (arg[0] === '-') {
        throw Error(`Unknown option: ${arg}`);
      } else {
        result.argv.push(arg);
      }
    }
  }
  return result;
}

function _startsWith(str, prefix) {
  if (prefix.trim() === '') return false;
  return str.startsWith(prefix);
}

module.exports = getopt;
