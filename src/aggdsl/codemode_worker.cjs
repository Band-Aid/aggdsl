'use strict';

// Deliberately small JSON-lines bridge. Pendo keys and HTTP are owned by Python.
// vm is NOT a security boundary; this is a trusted-user PoC only.
const vm = require('node:vm');
const readline = require('node:readline');

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
const pending = new Map();
let nextId = 1;
let started = false;
let finished = false;

function send(message) {
  process.stdout.write(JSON.stringify(message) + '\n');
}

const api = Object.freeze({
  aggdsl: Object.freeze({
    run(dsl) {
      if (typeof dsl !== 'string' || !dsl.trim()) {
        return Promise.reject(new TypeError('aggdsl.run requires a DSL string'));
      }
      const id = nextId++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        send({ type: 'call', method: 'aggdsl.run', id, dsl });
      });
    }
  })
});

async function execute(code) {
  const context = vm.createContext({ pendo: api }, {
    name: 'aggdsl-code-mode',
    codeGeneration: { strings: false, wasm: false }
  });
  try {
    // The script returns a JSON-serializable value. Await aggregation promises.
    const result = await vm.runInContext(`(async () => {\n'use strict';\n${code}\n})()`, context, {
      timeout: 1000,
      displayErrors: false
    });
    if (!finished) {
      finished = true;
      send({ type: 'done', value: result === undefined ? null : result });
    }
  } catch (error) {
    if (!finished) {
      finished = true;
      send({ type: 'error', message: String(error && error.message || error) });
    }
  }
}

input.on('line', (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch (_error) {
    send({ type: 'error', message: 'Invalid JSON bridge input' });
    return;
  }
  if (!started) {
    if (message.type !== 'start' || typeof message.code !== 'string') {
      send({ type: 'error', message: 'Expected start with code' });
      return;
    }
    started = true;
    void execute(message.code);
    return;
  }
  if (finished || message.type !== 'result' || !pending.has(message.id)) return;
  const request = pending.get(message.id);
  pending.delete(message.id);
  if (Object.prototype.hasOwnProperty.call(message, 'error')) {
    request.reject(new Error(message.error));
  } else {
    request.resolve(message.value);
  }
});
