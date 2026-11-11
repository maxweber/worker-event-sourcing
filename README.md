# worker-event-sourcing

Event sourcing on Cloudflare Workers, in the SQLite of Durable Objects:
one Object holds a stream's log, one Object per projection version holds
a read model derived from it. A minimal example to learn the recipe from.
ClojureScript compiled by
[clojurific](https://github.com/WhimsicalCode/clojurific), bundled by Vite,
deployed by wrangler.

Mostly written by Claude Code.

## The recipe

1. **One log Object per stream.** A stream is one history: an
   organization, a customer, a device. `Log`, named after the stream,
   holds the `events` table and nothing else. `append` takes the number
   its caller believes is the head and appends only if it still is;
   `events` hands out a page after a number. The Object runs one request
   at a time, so the check and the append are one step. Its code never
   changes.

2. **Events are the truth.** One event per row as
   [transit](https://github.com/cognitect/transit-format) JSON, numbered
   without a gap, never updated, never deleted. The log adds
   `:event/number` and `:event/at`; the domain owns the rest,
   `:event/type` included. Events stay small: a blob goes to object
   storage and its name into the event. Transit rather than EDN because
   it reads an order of magnitude faster on V8
   ([docs/codec-benchmark.md](docs/codec-benchmark.md)).

3. **A command is decided on a read model and appended optimistically.**
   `Projection`, named `<stream>/<version>`, holds a read model and its
   cursor, the number of the last event folded in. For a command it
   catches up with the log, `decide` reads the read model and answers
   events or an error, and the events go to the log with the cursor as
   the expected head. Appended, they are folded into the read model with
   the new cursor in one local transaction; refused because the head
   moved in between, the projection catches up and decides again. An
   error appends nothing. `{:events []}` is a command that changed
   nothing. One command at a time per projection, under
   `blockConcurrencyWhile`.

4. **The read model is derived.** `project` turns one event into the
   statements that fold it into the tables, SQL text and parameters as
   data ([docs/projections-as-data.md](docs/projections-as-data.md)). It
   is a function of the event alone, tested without a database, and it
   sees every event ever written, so an old shape is handled there. A
   changed projection is a new `version` in the domain, so a new Object:
   it catches up from the start of the log at its first use while the
   old one keeps serving, cutover is addressing it, rollback is
   addressing the old one. Nothing is rebuilt in place, and the log is
   never touched.

5. **Others read the log.** `events` on the log Object hands out pages
   after a number. A consumer keeps the last number it saw as its cursor
   and asks again from there: a projection, a server, a copy of the log
   kept elsewhere. A projection catches up before it answers a query, so
   a reader sees every event appended before it asked, whichever
   projection appended it.

6. **Outside the Objects.** A side effect is recorded as an event and
   performed by a consumer with its own cursor, never while deciding. A
   request that may be retried carries an id, and the id is decided like
   any other fact.

## Layout

| file | what |
| --- | --- |
| `src/event_sourcing/store.cljs` | what both Objects share: `exec`, the codec, the page size |
| `src/event_sourcing/log.cljs` | the log: `append!` guarded by the expected head, `events-after` |
| `src/event_sourcing/projection.cljs` | the read model: `apply!`, `catch-up!`, `execute!` with the optimistic loop |
| `src/event_sourcing/todo.cljs` | the example domain: `version`, `tables`, `decide`, `project`, `queries` |
| `src/event_sourcing/objects.cljs` | the Durable Objects `Log` and `Projection`, transit at the boundary |
| `test/event_sourcing/` | the log, and two projections on one log, on `node:sqlite` stand-ins |
| `docs/log-and-projection-objects.md` | why two Objects, what it costs, what it opens |
| `docs/codec-benchmark.md` | transit against EDN, measured |
| `docs/sql-strings.md` | SQL strings with bindings, not a query DSL: why |
| `docs/projections-as-data.md` | a projection answers its statements as data: why |

## From a Worker

The Objects' methods take and answer transit JSON text. With the
`PROJECTION` and `LOG` bindings of `wrangler.jsonc`:

```clojure
(def writer (transit/writer :json))
(def reader (transit/reader :json))

(let [^js projections (.-PROJECTION env)
      ^js todos (.get projections (.idFromName projections (str "acme/" todo/version)))]
  (transit/read reader (await (.execute todos (transit/write writer {:command/type :todo/add :todo/text "Ship it"}))))
  ;; => {:events [{:event/type :todo/added, :todo/id "…", :todo/text "Ship it", :event/number 1, :event/at "…"}]}
  (transit/read reader (await (.query todos "todos"))))
  ;; => [{:id "…", :text "Ship it", :done 0}]

(let [^js logs (.-LOG env)
      ^js log (.get logs (.idFromName logs "acme"))]
  (transit/read reader (await (.events log 0))))
  ;; => the events after number 0, up to 100
```

## Run

```sh
npm ci
npm test          # the log and the projections under Node, no network
npm run deploy    # wrangler deploy
```

## License

MIT
