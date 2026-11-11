(ns event-sourcing.store
  "Event sourcing in the SQLite of a Durable Object. The Object runs one
   request at a time, so it is the single writer of its stream. The
   `events` table is the truth: one EDN map per row, numbered without a
   gap, never updated, never deleted. The read model is derived from it
   by the domain's `project`, in the transaction that appends the
   events, and can be dropped and derived again at any time.

   A domain is a map:
     :tables   table name to its `create table if not exists` statement
     :decide   (fn [sql command]) reading the read model and answering
               `{:events [...]}` or `{:error ...}`; it writes nothing
     :project  (fn [sql event]) folding one event into the read model
   The store adds `:event/number` and `:event/at` to every event; the
   domain owns the rest, `:event/type` included."
  (:require [cljs.reader :as reader]))

(defn exec
  "One SQL statement with its parameters; the rows as maps."
  [^js sql statement & params]
  (let [^js cursor (.apply (.-exec sql) sql (to-array (cons statement params)))]
    (mapv (fn [^js row]
            (into {} (map (fn [k] [(keyword k) (aget row k)])) (js/Object.keys row)))
          (.toArray cursor))))

(defn schema!
  "The events table and the domain's tables, where they are missing."
  [^js storage {:keys [tables]}]
  (let [sql (.-sql storage)]
    (exec sql "create table if not exists events (number integer primary key, event text not null)")
    (doseq [[_ ddl] tables]
      (exec sql ddl))))

(defn- append!
  "`events` appended after the last one and projected, inside the
   caller's transaction; the events with their numbers."
  [sql {:keys [project]} events]
  (let [last (:n (first (exec sql "select coalesce(max(number), 0) as n from events")))
        at (.toISOString (js/Date.))]
    (vec (map-indexed
           (fn [i event]
             (let [event (assoc event :event/number (+ last i 1) :event/at at)]
               (exec sql "insert into events (number, event) values (?, ?)"
                     (:event/number event) (pr-str event))
               (project sql event)
               event))
           events))))

(defn execute!
  "One command in one transaction: `decide` reads the read model and
   answers with events or an error; the events are appended and
   projected; the transaction commits. An error writes nothing. A
   projection that throws rolls the events back with it."
  [^js storage {:keys [decide] :as domain} command]
  (.transactionSync storage
    (fn []
      (let [sql (.-sql storage)
            decision (decide sql command)]
        (if (contains? decision :events)
          {:events (append! sql domain (:events decision))}
          decision)))))

(defn events-after
  "Up to `limit` events after number `after`, in order; `after` 0 is the
   start, `limit` -1 every one. A consumer keeps the last number it saw
   as its cursor and asks again from there."
  [^js storage after limit]
  (mapv #(reader/read-string (:event %))
        (exec (.-sql storage)
              "select event from events where number > ? order by number limit ?"
              after limit)))

(defn rebuild!
  "The read model dropped and derived again from every event, in one
   transaction: after a projection changed, or when in doubt."
  [^js storage {:keys [tables project] :as domain}]
  (.transactionSync storage
    (fn []
      (let [sql (.-sql storage)]
        (doseq [[table _] tables]
          (exec sql (str "drop table if exists " table)))
        (schema! storage domain)
        (doseq [event (events-after storage 0 -1)]
          (project sql event))))))
