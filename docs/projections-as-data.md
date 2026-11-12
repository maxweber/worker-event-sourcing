# A projection answers data

Why `project` returns the statements that fold an event into the read
model instead of running them. Decided 2026-11-11.

## The contract

`project` is a function of the event alone. It answers a sequence of
statements, each a vector of SQL text and its parameters, in order; `nil`
or an empty sequence for an event that touches no table; an unknown type
throws, and a retired type gets a clause that answers `nil`. The store
runs the statements with `exec!`, inside the transaction that appends the
event, and again on a rebuild.

```clojure
(defn project [event]
  (case (:event/type event)
    :todo/added [["insert into todos (id, text) values (?, ?)" (:todo/id event) (:todo/text event)]]
    :todo/completed [["update todos set done = 1 where id = ?" (:todo/id event)]]))
```

## Why

- **Purity.** A projection is tested by an equality check on the
  statements, with no database. It reads like the recipe's other data:
  an event in, statements out.
- **One place runs SQL.** The store executes every projection, so it can
  name the event number and the statement that failed, count what a
  rebuild did, or print what a rebuild would do. A projection that runs
  its own statements spreads that over every domain.
- **HoneySQL costs nothing to opt into.** `sql/format` answers exactly
  such a vector, so a domain that wants it returns what `format` gives
  ([sql-strings.md](sql-strings.md)).
- **The same data runs elsewhere.** A consumer on a server can fold the
  same statements into its own SQLite.

## The cost

A projection cannot read before it writes. A projection that needs the
current row says so in SQL, `update ... where`, `insert ... on
conflict`, a subquery, which is set-based and replays identically. Reads
stay in `decide` and in the queries, which keep their `db`.
