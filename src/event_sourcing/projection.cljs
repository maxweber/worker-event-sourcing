(ns event-sourcing.projection
  "A read model derived from a stream's log, in the SQLite of its own
   Durable Object, one Object per projection version. It holds the
   domain's tables and its cursor, the number of the last event folded
   in. It reaches the log through two functions it is handed, both
   answering promises: `append`, of the expected head and the events,
   answering `{:events [...]}` or `{:conflict head}`; `source`, of a
   number, answering a page of events after it.

   A domain is a map:
     :version  a UUID literal, part of the Object's name; a new one,
               committed with the change, whenever `tables` or
               `project` change
     :tables   table name to its `create table if not exists` statement;
               none of them may be the projection's own `cursor`
     :decide   (fn [db command]) reading the read model and answering
               `{:events [...]}` or `{:error ...}`; it writes nothing
     :project  (fn [event]) answering the statements that fold one event
               into the read model, `[[sql & params] ...]`, nil for none
     :queries  query name to (fn [db]) answering data"
  (:require [clojure.string :as str]
            [event-sourcing.store :refer [exec page]]))

(defn- read-model-tables
  "The domain's tables; a domain that names the projection's own
   `cursor` table is refused. SQLite ignores the case of a table name,
   so does this."
  [{:keys [tables]}]
  (when (some #(= "cursor" (str/lower-case %)) (keys tables))
    (throw (ex-info "The cursor table is the projection's; a domain cannot declare it." {:tables (keys tables)})))
  tables)

(defn schema!
  "The cursor at 0 and the domain's tables, where they are missing."
  [^js storage domain]
  (let [db (.-sql storage)
        tables (read-model-tables domain)]
    (exec db "create table if not exists cursor (number integer not null)")
    (when (empty? (exec db "select number from cursor"))
      (exec db "insert into cursor (number) values (0)"))
    (doseq [[_ ddl] tables]
      (exec db ddl))))

(defn cursor
  "The number of the last event folded into the read model, 0 for none."
  [^js storage]
  (:number (first (exec (.-sql storage) "select number from cursor"))))

(defn- run!
  "The statements of a projection, each SQL text and its parameters, in order."
  [db statements]
  (doseq [statement statements]
    (apply exec db statement)))

(defn apply!
  "`events`, numbered and continuing the cursor, folded into the read
   model together with the new cursor, in one transaction."
  [^js storage {:keys [project]} events]
  (when (seq events)
    (.transactionSync storage
      (fn []
        (let [db (.-sql storage)
              cursor (cursor storage)]
          (when (not= (inc cursor) (:event/number (first events)))
            (throw (ex-info "The events do not continue the cursor."
                            {:cursor cursor :first (:event/number (first events))})))
          (doseq [event events]
            (run! db (project event)))
          (exec db "update cursor set number = ?" (:event/number (peek events))))))))

(defn ^:async catch-up!
  "Every event after the cursor pulled from `source`, a page at a time,
   and applied; the cursor then. The read model is fresh as of the log
   the last page was read from."
  [storage domain source]
  (loop []
    (let [events (await (source (cursor storage)))]
      (apply! storage domain events)
      (if (= page (count events))
        (recur)
        (cursor storage)))))

(defn ^:async execute!
  "One command: the projection catches up with the log, `decide` reads
   the read model at the cursor and answers events or an error, and the
   events go to the log with the cursor as the expected head. Appended,
   they are folded into the read model with the new cursor; refused
   because the head moved between the catch-up and the append, the
   projection catches up and decides again. An error appends nothing.
   Run one command at a time per projection: the Object does that
   under `blockConcurrencyWhile`."
  [storage {:keys [decide] :as domain} append source command]
  (loop []
    (await (catch-up! storage domain source))
    (let [decision (decide (.-sql storage) command)]
      (if (or (not (contains? decision :events)) (empty? (:events decision)))
        decision
        (let [answer (await (append (cursor storage) (:events decision)))]
          (if (contains? answer :conflict)
            (recur)
            (do (apply! storage domain (:events answer))
                answer)))))))
