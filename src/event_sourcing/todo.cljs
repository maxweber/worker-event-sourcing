(ns event-sourcing.todo
  "The example domain, todos: commands decided against the read model,
   events folded into it. Replace it with yours; the store does not
   know it."
  (:require [clojure.string :as str]
            [event-sourcing.store :refer [exec]]))

(def tables
  {"todos" "create table if not exists todos (id text primary key, text text not null, done integer not null default 0)"})

(defn decide
  "The events of a command, or `{:error ...}`. Reads the read model
   through `db`, writes nothing."
  [db command]
  (case (:command/type command)
    :todo/add
    (if (str/blank? (:todo/text command))
      {:error "A todo needs a text."}
      {:events [{:event/type :todo/added
                 :todo/id (str (random-uuid))
                 :todo/text (str/trim (:todo/text command))}]})

    :todo/complete
    (let [todo (first (exec db "select done from todos where id = ?" (:todo/id command)))]
      (cond
        (nil? todo) {:error "No such todo."}
        (= 1 (:done todo)) {:events []}
        :else {:events [{:event/type :todo/completed
                         :todo/id (:todo/id command)}]}))

    {:error (str "Unknown command " (pr-str (:command/type command)))}))

(defn project
  "One event folded into the read model. It reads every event ever
   written, so an old shape is handled here; an unknown type throws,
   and a retired type gets a clause that ignores it."
  [db event]
  (case (:event/type event)
    :todo/added (exec db "insert into todos (id, text) values (?, ?)" (:todo/id event) (:todo/text event))
    :todo/completed (exec db "update todos set done = 1 where id = ?" (:todo/id event))))

(def queries
  "The read model's queries, by the path they answer on."
  {"todos" (fn [db] (exec db "select id, text, done from todos order by rowid"))})

(def domain
  {:tables tables
   :decide decide
   :project project
   :queries queries})
