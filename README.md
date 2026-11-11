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

2. **Events are the truth.** The `events` table holds one EDN map per row,
   numbered without a gap, never updated, never deleted. The store adds
   `:event/number` and `:event/at`; the domain owns the rest,
   `:event/type` included. Events stay small: a blob goes to object
   storage and its name into the event.

3. **One transaction per command.** `decide` reads the read model and
   answers with events or an error. The events are appended and projected
   into the read model, and the transaction commits (`transactionSync`).
   An error writes nothing. A projection that throws rolls the events
   back with it. `{:events []}` is a command that changed nothing.

4. **The read model is derived.** `project` folds one event into the
   tables. They can be dropped and derived again from the events at any
   time (`rebuild`): a changed projection means a rebuild, never a
   migration of the tables in place. `project` reads every event ever
   written, so an old shape is handled there. A rebuild is one
   transaction and the Object has limited CPU per request, so a long
   stream rebuilds in slices.

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
| `src/event_sourcing/store.cljs` | the recipe: the events table, `execute!`, `events-after`, `rebuild!` |
| `src/event_sourcing/todo.cljs` | the example domain: `tables`, `decide`, `project`, `queries` |
| `src/event_sourcing/stream.cljs` | the Durable Object: the store behind `execute`, `events`, `query`, `rebuild` |
| `test/event_sourcing/` | the store exercised on a `node:sqlite` stand-in of the Object's storage |

## From a Worker

The Object's methods take and answer EDN text. With the `STREAM` binding
of `wrangler.jsonc`:

```clojure
(let [^js streams (.-STREAM env)
      ^js stream (.get streams (.idFromName streams "acme"))]
  (reader/read-string (await (.execute stream (pr-str {:command/type :todo/add :todo/text "Ship it"}))))
  ;; => {:events [{:event/type :todo/added, :todo/id "…", :todo/text "Ship it", :event/number 1, :event/at "…"}]}
  (reader/read-string (await (.query stream "todos")))
  ;; => [{:id "…", :text "Ship it", :done 0}]
  (reader/read-string (await (.events stream 0))))
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
