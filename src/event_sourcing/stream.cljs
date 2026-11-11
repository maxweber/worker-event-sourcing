(ns event-sourcing.stream
  "The Durable Object of one stream: an organization, a customer, a
   device, whatever owns one history. A Worker reaches it through the
   binding, `(.get streams (.idFromName streams name))`, and calls the
   methods below on the stub. Commands, events and query results cross
   the call as EDN text, so both sides keep plain Clojure data."
  (:require ["cloudflare:workers" :refer [DurableObject]]
            [cljf.x :refer [defclass]]
            [cljs.reader :as reader]
            [event-sourcing.store :as store]
            [event-sourcing.todo :as todo]))

(def domain
  todo/domain)

(def page
  "How many events one read of the log hands out."
  100)

(defclass Stream
  (extends DurableObject)
  (constructor [this ctx env]
    (super ctx env)
    (store/schema! (.-storage ctx) domain))
  Object
  ;; one command, EDN; its events, or `{:error ...}`, EDN
  (execute [this command]
    (pr-str (store/execute! (.. this -ctx -storage) domain (reader/read-string command))))
  ;; up to a page of events after number `after`, EDN
  (events [this after]
    (pr-str (store/events-after (.. this -ctx -storage) after page)))
  ;; the read model's query `name` (`todo/queries`), EDN
  (query [this name]
    (pr-str ((get (:queries domain) name) (.. this -ctx -storage -sql))))
  ;; the read model derived again from the events
  (rebuild [this]
    (store/rebuild! (.. this -ctx -storage) domain)
    nil))
