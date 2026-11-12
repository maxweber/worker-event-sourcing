// The Stream Object on real workerd: its four methods over RPC, with
// transit text at the boundary, as a Worker would call them. Every test
// takes a stream of its own.
import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import transit from 'transit-js';

const writer = transit.writer('json');
const reader = transit.reader('json');
const kw = transit.keyword;

// a command as transit text; `id` is its :command/id when given
const add = (text, id) =>
  writer.write(transit.map([kw('command/type'), kw('todo/add'), kw('todo/text'), text, ...(id ? [kw('command/id'), id] : [])]));
const complete = (todoId) =>
  writer.write(transit.map([kw('command/type'), kw('todo/complete'), kw('todo/id'), todoId]));
const read = (text) => reader.read(text);
const stream = (name = crypto.randomUUID()) => env.STREAM.get(env.STREAM.idFromName(name));

describe('a stream', () => {
  it('appends and projects a command, and answers a retry with the same events', async () => {
    const acme = stream();
    const first = read(await acme.execute(add('Ship it', 'req-1')));
    const events = first.get(kw('events'));
    expect(events.length).toBe(1);
    expect(events[0].get(kw('event/number'))).toBe(1);
    expect(events[0].get(kw('event/command-id'))).toBe('req-1');

    const again = read(await acme.execute(add('Ship it', 'req-1')));
    expect(again.get(kw('events'))[0].get(kw('event/number'))).toBe(1);

    const todos = read(await acme.query('todos'));
    expect(todos.length).toBe(1);
    expect(todos[0].get(kw('text'))).toBe('Ship it');
    expect(todos[0].get(kw('done'))).toBe(0);

    const done = read(await acme.execute(complete(todos[0].get(kw('id')))));
    expect(done.get(kw('events'))[0].get(kw('event/number'))).toBe(2);
    expect(read(await acme.query('todos'))[0].get(kw('done'))).toBe(1);
  });

  it('rejects without writing, and decides a retried rejection afresh', async () => {
    const acme = stream();
    expect(read(await acme.execute(add(' ', 'req-2'))).get(kw('error'))).toBe('A todo needs a text.');
    expect(read(await acme.execute(add(' ', 'req-2'))).get(kw('error'))).toBe('A todo needs a text.');
    expect(read(await acme.events(0)).length).toBe(0);
  });

  it('hands out the log after a cursor', async () => {
    const acme = stream();
    await acme.execute(add('one'));
    await acme.execute(add('two'));
    expect(read(await acme.events(0)).map((e) => e.get(kw('event/number')))).toEqual([1, 2]);
    expect(read(await acme.events(1)).map((e) => e.get(kw('event/number')))).toEqual([2]);
    expect(read(await acme.events(2)).length).toBe(0);
  });

  it('refuses a bad cursor, an unknown query and a bad command id', async () => {
    const acme = stream();
    await expect(acme.events('abc')).rejects.toThrow('non-negative number');
    await expect(acme.query('nothing')).rejects.toThrow('No such query');
    await expect(acme.execute(add('x', 'a\nb'))).rejects.toThrow('control characters');
  });

  it('keeps streams apart and survives a rebuild', async () => {
    const acme = stream();
    const other = stream();
    await acme.execute(add('mine'));
    expect(read(await other.query('todos')).length).toBe(0);
    await acme.rebuild();
    expect(read(await acme.query('todos'))[0].get(kw('text'))).toBe('mine');
  });
});
