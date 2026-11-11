(ns event-sourcing.todo
  "The example domain, todos: commands decided against the read model,
   events folded into it as statements. Replace it with yours; the log
   and the projection do not know it."
  (:require [clojure.string :as str]
            [event-sourcing.store :refer [exec]]))

(def version
  "Part of the projection Object's name. A new UUID, committed with the
   change, whenever `tables` or `project` change."
  "2d9c7fe0-cd07-4a9b-9b3a-65fde5a89396")

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
  "The statements that fold one event into the read model, each SQL text
   and its parameters: a function of the event alone. It sees every
   event ever written, so an old shape is handled here; an unknown type
   throws, and a retired type gets a clause that answers nil."
  [event]
  (case (:event/type event)
    :todo/added [["insert into todos (id, text) values (?, ?)" (:todo/id event) (:todo/text event)]]
    :todo/completed [["update todos set done = 1 where id = ?" (:todo/id event)]]))

(def queries
  "The read model's queries, by name."
  {"todos" (fn [db] (exec db "select id, text, done from todos order by rowid"))})

(def domain
  {:version version
   :tables tables
   :decide decide
   :project project
   :queries queries})
