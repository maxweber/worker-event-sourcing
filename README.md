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
   `idFromName(stream)` finds it.

2. **Events are the truth.** The `events` table holds one event per row
   as [transit](https://github.com/cognitect/transit-format) JSON,
   numbered without a gap, never updated, never deleted. The store adds
   `:event/number` and `:event/at`; the domain owns the rest,
   `:event/type` included. Events stay small: a blob goes to object
   storage and its name into the event. Transit rather than EDN because
   it reads an order of magnitude faster on V8
   ([docs/codec-benchmark.md](docs/codec-benchmark.md)).

3. **One transaction per command.** `decide` reads the read model and
   answers with events or an error. The events are appended and projected
   into the read model, and the transaction commits (`transactionSync`).
   An error writes nothing. A projection that throws rolls the events
   back with it. `{:events []}` is a command that changed nothing.

4. **The read model is derived.** `project` turns one event into the
   statements that fold it into the tables, SQL text and parameters as
   data, and the store runs them in the command's transaction. It is a
   function of the event alone, tested without a database. A statement
   binds text, finite numbers, nil or bytes; a keyword, a boolean or a
   date is converted by the projection, in the open, or the statement
   throws. The tables
   can be dropped and derived again from the events at any time
   (`rebuild`): a changed projection means a rebuild, never a migration
   of the tables in place. `project` sees every event ever written, so
   an old shape is handled there. A changed projection is a new
   `version` in the domain, committed with the change; the Object
   records the version it was built with, so a stream built under
   another one derives its read model again at its first use after the
   deploy, before it answers. A rebuild reads the log a page at a time,
   as any consumer does; it is one transaction and the Object has
   limited CPU per request, so a very long stream rebuilds in slices.

5. **Others read the log.** `events` hands out events in order after a
   number. A consumer keeps the last number it saw as its cursor and asks
   again from there: another Object with its own read model, a server, a
   copy of the log kept elsewhere. One protocol for all of them.

6. **Outside the Object.** A side effect is recorded as an event and
   performed by a consumer with its own cursor, never while deciding. A
   request that may be retried carries an id, and the id is decided like
   any other fact.

## Layout

| file | what |
| --- | --- |
| `src/event_sourcing/store.cljs` | the recipe: the events table, `execute!`, `events-after`, `rebuild!`, the codec |
| `src/event_sourcing/todo.cljs` | the example domain: `version`, `tables`, `decide`, `project`, `queries` |
| `src/event_sourcing/stream.cljs` | the Durable Object: the store behind `execute`, `events`, `query`, `rebuild` |
| `test/event_sourcing/` | the store exercised on a `node:sqlite` stand-in of the Object's storage |
| `docs/codec-benchmark.md` | transit against EDN, measured |
| `docs/sql-strings.md` | SQL strings with bindings, not a query DSL: why |
| `docs/projections-as-data.md` | a projection answers its statements as data: why |

## From a Worker

The Object's methods take and answer transit JSON text. With the `STREAM`
binding of `wrangler.jsonc`:

```clojure
(def writer (transit/writer :json))
(def reader (transit/reader :json))

(let [^js streams (.-STREAM env)
      ^js stream (.get streams (.idFromName streams "acme"))]
  (transit/read reader (await (.execute stream (transit/write writer {:command/type :todo/add :todo/text "Ship it"}))))
  ;; => {:events [{:event/type :todo/added, :todo/id "…", :todo/text "Ship it", :event/number 1, :event/at "…"}]}
  (transit/read reader (await (.query stream "todos")))
  ;; => [{:id "…", :text "Ship it", :done 0}]
  (transit/read reader (await (.events stream 0))))
  ;; => the events after number 0, up to 100
```

## Run

```sh
npm ci
npm test          # the store under Node, no network
npm run deploy    # wrangler deploy
```

## License

MIT
