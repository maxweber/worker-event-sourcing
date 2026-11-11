(ns event-sourcing.log
  "The log of one stream, in the SQLite of its own Durable Object. The
   `events` table is the truth: one event per row as transit JSON,
   numbered without a gap, never updated, never deleted. The Object runs
   one request at a time, so checking the head and appending after it
   are one step. The log adds `:event/number` and `:event/at` to every
   event; the domain owns the rest, `:event/type` included."
  (:require [event-sourcing.store :refer [decode encode exec]]))

(defn schema!
  "The events table, where it is missing."
  [^js storage]
  (exec (.-sql storage) "create table if not exists events (number integer primary key, event text not null)"))

(defn head
  "The number of the last event, 0 for an empty log."
  [db]
  (:n (first (exec db "select coalesce(max(number), 0) as n from events"))))

(defn append!
  "`events` appended after number `expected`, in one transaction, when
   `expected` is still the head: `{:events [...]}`, the events with
   their numbers. When the head has moved, `{:conflict head}` and
   nothing written: the caller decided on a state that is no longer the
   latest, catches up and decides again."
  [^js storage expected events]
  (.transactionSync storage
    (fn []
      (let [db (.-sql storage)
            head (head db)]
        (if (not= head expected)
          {:conflict head}
          (let [at (.toISOString (js/Date.))]
            {:events (reduce (fn [appended event]
                               (let [event (assoc event :event/number (+ head (count appended) 1) :event/at at)]
                                 (exec db "insert into events (number, event) values (?, ?)"
                                       (:event/number event) (encode event))
                                 (conj appended event)))
                             []
                             events)}))))))

(defn events-after
  "Up to `limit` events after number `after`, in order; `after` 0 is the
   start. A consumer keeps the last number it saw as its cursor and asks
   again from there."
  [^js storage after limit]
  (mapv #(decode (:event %))
        (exec (.-sql storage)
              "select event from events where number > ? order by number limit ?"
              after limit)))
