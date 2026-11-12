# worker-event-sourcing

Event sourcing on a Cloudflare Worker, in the SQLite of a Durable Object.
A minimal example to learn the recipe from. ClojureScript compiled by
[clojurific](https://github.com/WhimsicalCode/clojurific), bundled by Vite,
deployed by wrangler.

Mostly written by Claude Code.

## The recipe

1. **One Durable Object per stream.** A stream is one history: an
   organization, a customer, a device. The Object runs one request at a
   time, so it is the stream's single writer, and nothing else writes.
   That holds as long as nothing in `execute` or `decide` awaits;
   anything external is a consumer's job. `idFromName(stream)` finds
   it.

2. **Events are the truth.** The `events` table holds one event per row
   as [transit](https://github.com/cognitect/transit-format) JSON,
   numbered without a gap, never updated, never deleted. The store adds
   `:event/number` and `:event/at`, and `:event/command-id` when the
   command carried an id; the domain owns the rest, `:event/type`
   included. Events stay small: a blob goes to object
   storage and its name into the event. Transit rather than EDN because
   it reads an order of magnitude faster on V8
   ([docs/codec-benchmark.md](docs/codec-benchmark.md)).

3. **One transaction per command.** `decide` reads the read model and
   answers with events or an error. The events are appended and projected
   into the read model, and the transaction commits (`transactionSync`).
   An error writes nothing, even if `decide` touched a table: the
   transaction ends in a rollback. A projection that throws rolls the
   events back with it. `{:events []}` is a command that changed nothing.

4. **The read model is derived.** `project` turns one event into the
   statements that fold it into the tables, SQL text and parameters as
   data, and the store runs them in the command's transaction. It is a
   function of the event alone, tested without a database. A statement
   binds text, finite numbers, nil or bytes; a keyword, a boolean or a
   date is converted by the projection, in the open, or the statement
   throws. `project` sees every event ever written: an old shape is
   handled there, a retired type keeps a clause answering nil as long
   as its events exist, and an unknown type throws. The tables can be
   dropped and derived again from the events at any time (`rebuild`): a
   changed projection means a rebuild, never a migration of the tables
   in place. A changed projection is a new `version` in the domain,
   committed with the change; the Object records the version it was
   built with and the names of the tables it built, so a stream built
   under another version derives its read model again at its first use
   after the deploy, before it answers, dropping those tables and no
   other. A
   rebuild reads the log a page at a time, as any consumer does, in one
   transaction. A rebuild that fails, a projection that throws or a
   stream past the Object's CPU budget, leaves the log and the old read
   model intact and fails every request of that stream until a
   corrected projection, or a larger `limits.cpu_ms`, is deployed.

5. **Others read the log.** `events` hands out events in order after a
   number. A consumer keeps the last number it saw as its cursor and asks
   again from there: another Object with its own read model, a server, a
   copy of the log kept elsewhere. One protocol for all of them.

6. **Outside the Object.** A side effect is recorded as an event and
   performed by a consumer with its own cursor, never while deciding. A
   request that may be retried carries `:command/id`, a string of at
   most 128 characters: the events it produced record it, and a retry
   is answered with them again, without deciding. A rejection records
   nothing, so its retry is decided afresh, and a flood of rejected
   commands costs CPU, never storage.

## Layout

| file | what |
| --- | --- |
| `src/event_sourcing/store.cljs` | the recipe: the events table, `execute!`, `events-after`, `rebuild!`, the codec |
| `src/event_sourcing/todo.cljs` | the example domain: `version`, `tables`, `decide`, `project`, `queries` |
| `src/event_sourcing/stream.cljs` | the Durable Object: the store behind `execute`, `events`, `query`, `rebuild` |
| `test/event_sourcing/` | the store exercised on a `node:sqlite` stand-in of the Object's storage |
| `test/worker/` | the Object on real workerd, through the Cloudflare vitest plugin |
| `docs/codec-benchmark.md` | transit against EDN, measured |
| `docs/sql-strings.md` | SQL strings with bindings, not a query DSL: why |
| `docs/projections-as-data.md` | a projection answers its statements as data: why |

## From a Worker

Commands, events and query results cross the call as transit JSON text;
`events` takes the cursor as a number, `query` the query's name as a
string, and `rebuild` answers nothing. With the `STREAM` binding of
`wrangler.jsonc`:

```clojure
(def writer (transit/writer :json))
(def reader (transit/reader :json))

(let [^js streams (.-STREAM env)
      ^js stream (.get streams (.idFromName streams "acme"))]
  (transit/read reader (await (.execute stream (transit/write writer {:command/id "req-1" :command/type :todo/add :todo/text "Ship it"}))))
  ;; => {:events [{:event/type :todo/added, :todo/id "…", :todo/text "Ship it", :event/number 1, :event/at "…", :event/command-id "req-1"}]}
  (transit/read reader (await (.query stream "todos")))
  ;; => [{:id "…", :text "Ship it", :done 0}]
  (transit/read reader (await (.events stream 0))))
  ;; => the events after number 0, up to 100
```

## Run

```sh
npm ci
npm test             # the store under Node, then the Object inside workerd; no network
npm run test:store   # the store alone
npm run test:worker  # the Object alone
npm run deploy       # wrangler deploy
```

## License

MIT
