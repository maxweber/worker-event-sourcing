(ns event-sourcing.store
  "Event sourcing in the SQLite of a Durable Object. The Object runs one
   request at a time, so it is the single writer of its stream. The
   `events` table is the truth: one event per row as transit JSON,
   numbered without a gap, never updated, never deleted. The read model
   is derived from it by the domain's `project`, in the transaction that
   appends the events, and can be dropped and derived again at any time.

   A domain is a map:
     :version  a UUID literal: a new one, committed with the change,
               whenever `tables` or `project` change; a stream built
               under another version is derived again at its first use
     :tables   table name to its `create table if not exists` statement;
               none of them may be the store's own `events` or
               `projection`
     :decide   (fn [db command]) reading the read model and answering
               `{:events [...]}` or `{:error ...}`; it writes nothing
     :project  (fn [event]) answering the statements that fold one event
               into the read model, `[[sql & params] ...]`, nil for none
   `db` is the Object's SQLite, `ctx.storage.sql`: embedded, mutable,
   synchronous, one per Object. The store adds `:event/number` and
   `:event/at` to every event; the domain owns the rest, `:event/type`
   included."
  (:require [clojure.string :as str]
            [cognitect.transit :as transit]))

(def ^:private writer (transit/writer :json))
(def ^:private reader (transit/reader :json))

(defn encode
  "Data as transit JSON text: how an event is stored, and how commands,
   events and query results cross the Object's boundary. Transit, not
   EDN: it reads an order of magnitude faster on V8 (docs/codec-benchmark.md)."
  [data]
  (transit/write writer data))

(defn decode
  [text]
  (transit/read reader text))

(def page
  "How many events one read of the log hands out."
  100)

(defn- binding?
  "What SQLite can bind: text, a finite number, nil, or bytes."
  [value]
  (or (string? value)
      (and (number? value) (js/Number.isFinite value))
      (nil? value)
      (instance? js/Uint8Array value)
      (instance? js/ArrayBuffer value)))

(defn exec
  "One SQL statement with its parameters; the rows as maps. A parameter
   that SQLite cannot bind throws here, in the fake and on workerd
   alike: workerd would otherwise store its text, `true` as \"true\"
   and a keyword as \":image\", and nothing would fail."
  [^js db statement & params]
  (doseq [[i value] (map-indexed vector params)]
    (when-not (binding? value)
      (throw (ex-info (str "Parameter " (inc i) " of \"" statement "\" cannot be bound: " (pr-str value)
                           ". Bind text, a finite number, nil or bytes.")
                      {:statement statement :index (inc i) :value value}))))
  (let [^js cursor (.apply (.-exec db) db (to-array (cons statement params)))]
    (mapv (fn [^js row]
            (into {} (map (fn [k] [(keyword k) (aget row k)])) (js/Object.keys row)))
          (.toArray cursor))))

(defn- read-model-tables
  "The domain's tables, which the store creates and drops; it refuses a
   domain that names the store's own `events` or `projection`, so that
   no rebuild can drop the log. SQLite ignores the case of a table name,
   so does this."
  [{:keys [tables]}]
  (when (some #(contains? #{"events" "projection"} (str/lower-case %)) (keys tables))
    (throw (ex-info "The events and projection tables are the store's; a domain cannot declare them." {:tables (keys tables)})))
  tables)

(defn- create-tables!
  [db domain]
  (doseq [[_ ddl] (read-model-tables domain)]
    (exec db ddl)))

(declare rebuild!)

(defn schema!
  "The store's tables where they are missing, and the read model at the
   domain's version: a stream built under another version, or not yet,
   has its read model derived from the events before anything else is
   answered. Called when the Object is constructed, so a changed
   projection takes effect at a stream's first use after the deploy."
  [^js storage domain]
  (let [db (.-sql storage)]
    (exec db "create table if not exists events (number integer primary key, event text not null)")
    (exec db "create table if not exists projection (version text not null)")
    (if (= (:version domain) (:version (first (exec db "select version from projection"))))
      (create-tables! db domain)
      (rebuild! storage domain))))

(defn- run!
  "The statements of a projection, each SQL text and its parameters, in order."
  [db statements]
  (doseq [statement statements]
    (apply exec db statement)))

(defn- append!
  "`events` appended after the last one and projected, inside the
   caller's transaction; the events with their numbers."
  [db {:keys [project]} events]
  (let [last (:n (first (exec db "select coalesce(max(number), 0) as n from events")))
        at (.toISOString (js/Date.))]
    (reduce (fn [appended event]
              (let [event (assoc event :event/number (+ last (count appended) 1) :event/at at)]
                (exec db "insert into events (number, event) values (?, ?)"
                      (:event/number event) (encode event))
                (run! db (project event))
                (conj appended event)))
            []
            events)))

(defn execute!
  "One command in one transaction: `decide` reads the read model and
   answers with events or an error; the events are appended and
   projected; the transaction commits. An error writes nothing. A
   projection that throws rolls the events back with it."
  [^js storage {:keys [decide] :as domain} command]
  (.transactionSync storage
    (fn []
      (let [db (.-sql storage)
            decision (decide db command)]
        (if (contains? decision :events)
          {:events (append! db domain (:events decision))}
          decision)))))

(defn events-after
  "Up to `limit` events after number `after`, in order; `after` 0 is the
   start. A consumer keeps the last number it saw as its cursor and asks
   again from there."
  [^js storage after limit]
  (when-not (and (number? after) (js/Number.isFinite after) (not (neg? after)))
    (throw (ex-info (str "The cursor to read after must be a non-negative number, not " (pr-str after))
                    {:after after})))
  (mapv #(decode (:event %))
        (exec (.-sql storage)
              "select event from events where number > ? order by number limit ?"
              after limit)))

(defn rebuild!
  "The read model dropped and derived again from every event, and the
   domain's version recorded, in one transaction: after a projection
   changed, or when in doubt. It reads its own log as any consumer
   does, a page after a cursor at a time."
  [^js storage {:keys [project version] :as domain}]
  (.transactionSync storage
    (fn []
      (let [db (.-sql storage)]
        (doseq [[table _] (read-model-tables domain)]
          (exec db (str "drop table if exists " table)))
        (create-tables! db domain)
        (loop [after 0]
          (let [events (events-after storage after page)]
            (doseq [event events]
              (run! db (project event)))
            (when (= page (count events))
              (recur (:event/number (peek events))))))
        (exec db "delete from projection")
        (exec db "insert into projection (version) values (?)" version)))))
