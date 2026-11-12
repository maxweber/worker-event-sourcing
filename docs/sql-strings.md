# SQL strings, not a query DSL

Why the store and the domain write SQL as strings with `?` bindings
rather than as [HoneySQL](https://github.com/seancorfield/honeysql) maps.
Decided 2026-11-11.

## The decision

A statement is a vector of SQL text and its parameters. A projection
answers them as data, and the store runs them with `exec!`:

```clojure
[["update todos set done = 1 where id = ?" (:todo/id event)]]
```

Values travel as bindings, never spliced into the string. The only string
the store composes is the `drop table` of names the domain itself
declares.

## Why

- **Fluency.** SQL is the query language a coding agent has read most. A
  query DSL is a second notation for the same statement: the agent
  translates before it can check the statement against SQLite's error or
  try it in `sqlite3`, and every mistake in the translation is one it
  would not have made in SQL.
- **Nothing to compose.** A DSL pays off when queries are assembled at
  run time, an optional clause here, a join there. The statements of this
  recipe are fixed: a projection is one insert or update, a decision one
  select, the schema one `create table`. DDL is also where HoneySQL's
  coverage is thinnest.
- **Cost on the Object.** Formatting a map into a string would run for
  every projected event on the single-threaded Object, and add a
  dependency to every Worker's bundle. The string form has neither cost.
- **Safety is equal.** Parameters go through bindings either way. What
  can be bound is text, a finite number, nil or bytes; `select` and `exec!` throw on
  anything else, because workerd would otherwise store the value's text,
  `true` as `"true"`, a keyword as `":image"`, and nothing would fail.

## Opting in

`sql/format` answers exactly such a vector, so a domain with genuinely
dynamic statements can format them with HoneySQL and hand them over,
without the store knowing:

```clojure
(defn project [event]
  (case (:event/type event)
    :todo/completed [(sql/format {:update :todos :set {:done 1} :where [:= :id (:todo/id event)]})]))
```
