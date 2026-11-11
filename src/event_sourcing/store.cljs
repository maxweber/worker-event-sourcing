(ns event-sourcing.store
  "What the log and the projections share: `exec` on the SQLite of a
   Durable Object, the transit codec, the page size.

   `db` is an Object's SQLite, `ctx.storage.sql`: embedded, mutable,
   synchronous, one per Object."
  (:require [cognitect.transit :as transit]))

(def ^:private writer (transit/writer :json))
(def ^:private reader (transit/reader :json))

(defn encode
  "Data as transit JSON text: how an event is stored, and how commands,
   events and query results cross an Object's boundary. Transit, not
   EDN: it reads an order of magnitude faster on V8 (docs/codec-benchmark.md)."
  [data]
  (transit/write writer data))

(defn decode
  [text]
  (transit/read reader text))

(def page
  "How many events one read of the log hands out."
  100)

(defn exec
  "One SQL statement with its parameters; the rows as maps."
  [^js db statement & params]
  (let [^js cursor (.apply (.-exec db) db (to-array (cons statement params)))]
    (mapv (fn [^js row]
            (into {} (map (fn [k] [(keyword k) (aget row k)])) (js/Object.keys row)))
          (.toArray cursor))))
