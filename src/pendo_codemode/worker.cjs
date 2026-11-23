'use strict';

// JSON-lines bridge. The MCP connection and Pendo credentials stay in Python;
// this process only ever sees tool names, arguments and results.
// node:vm is NOT a security boundary: trusted-local proof of concept only.
const vm = require('node:vm');
const readline = require('node:readline');

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
const pending = new Map();
const logs = [];
let logBytes = 0;
let nextId = 1;
let resultsSeen = 0;
let started = false;
let finished = false;

function send(message) {
  process.stdout.write(JSON.stringify(message) + '\n');
}

function finish(message) {
  if (finished) return;
  finished = true;
  send({ ...message, logs });
}

function format(value) {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_error) {
    return String(value);
  }
}

function log(...parts) {
  const line = parts.map(format).join(' ');
  if (logBytes > 8000) return;
  logBytes += line.length;
  logs.push(line.slice(0, 2000));
}

function makeApi(tools) {
  const known = new Set(tools);
  const call = (tool) => (args) => {
    if (args !== undefined && (args === null || typeof args !== 'object' || Array.isArray(args))) {
      return Promise.reject(new TypeError(`pendo.${tool} takes one arguments object`));
    }
    const id = nextId++;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      // `after` lets the host tell which calls were issued together (same value = same round).
      send({ type: 'call', id, tool, after: resultsSeen, args: args === undefined ? {} : args });
    });
  };
  return new Proxy(Object.create(null), {
    get(_target, name) {
      if (typeof name !== 'string' || name === 'then') return undefined;
      // pendo.help('toolName' | 'keywords') resolves to signatures; pendo.help() to the index.
      if (name === 'help' && !known.has('help')) return (topic) => call('$help')({ topic: topic === undefined ? '' : String(topic) });
      if (!known.has(name)) {
        throw new Error(`Unknown Pendo tool "${name}". Available: ${tools.join(', ')}`);
      }
      return call(name);
    },
    has: (_target, name) => known.has(name),
    ownKeys: () => tools,
    getOwnPropertyDescriptor: (_target, name) =>
      known.has(name) ? { enumerable: true, configurable: true, value: call(name) } : undefined,
    set: () => false
  });
}

// Helpers that run inside the sandbox. `table` drops repeated keys from row lists.
const PRELUDE = `
function full(value) { return { $full: value }; }
function table(rows, columns) {
  const cols = columns || [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return { columns: cols, rows: rows.map((row) => cols.map((c) => (row[c] === undefined ? null : row[c]))) };
}
`;

async function execute(start) {
  const console_ = Object.freeze({ log, info: log, warn: log, error: log, debug: log });
  const context = vm.createContext({ pendo: makeApi(start.tools.map(String)), console: console_ }, {
    name: 'pendo-code-mode',
    codeGeneration: { strings: false, wasm: false }
  });
  const run = (source) => vm.runInContext(source, context, { timeout: 5000, displayErrors: false });
  try {
    run(PRELUDE);
    if (start.stdlib) run(start.stdlib);  // defines `lib`, built on `pendo`
    // `state` survives between programs of one session; it is plain JSON on both sides.
    context.state = run(`JSON.parse(${JSON.stringify(JSON.stringify(start.state || {}))})`);
    context.recipes = run('Object.create(null)');
    for (const [name, source] of Object.entries(start.recipes || {})) {
      try {
        const body = run(`(async (params) => {\n'use strict';\n${source}\n})`);
        context.recipes[name] = (params) => body(params === undefined ? {} : params);
      } catch (error) {
        context.recipes[name] = () => Promise.reject(new Error(`recipe ${name} does not compile: ${error.message}`));
      }
    }
    // The timeout bounds synchronous work; the Python host bounds the whole run.
    const result = await run(`(async (params) => {\n'use strict';\n${start.code}\n})({})`);
    const state = JSON.parse(run('JSON.stringify(state === undefined ? {} : state)') || '{}');
    finish({ type: 'done', value: result === undefined ? null : result, state });
  } catch (error) {
    finish({ type: 'error', message: String((error && error.message) || error) });
  }
}

input.on('line', (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch (_error) {
    finish({ type: 'error', message: 'Invalid JSON bridge input' });
    return;
  }
  if (!started) {
    if (message.type !== 'start' || typeof message.code !== 'string' || !Array.isArray(message.tools)) {
      finish({ type: 'error', message: 'Expected start with code and tools' });
      return;
    }
    started = true;
    void execute(message);
    return;
  }
  if (finished || message.type !== 'result' || !pending.has(message.id)) return;
  const request = pending.get(message.id);
  pending.delete(message.id);
  resultsSeen += 1;
  if (Object.prototype.hasOwnProperty.call(message, 'error')) {
    request.reject(new Error(message.error));
  } else {
    request.resolve(message.value);
  }
});
