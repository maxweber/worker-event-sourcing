(ns event-sourcing.stream
  "The Durable Object of one stream: an organization, a customer, a
   device, whatever owns one history. A Worker reaches it through the
   binding, `(.get streams (.idFromName streams name))`, and calls the
   methods below on the stub. Commands, events and query results cross
   the call as transit JSON text, so both sides keep plain Clojure data."
  (:require ["cloudflare:workers" :refer [DurableObject]]
            [cljf.x :refer [defclass]]
            [event-sourcing.store :as store]
            [event-sourcing.todo :as todo]))

(def domain
  todo/domain)

(defclass Stream
  (extends DurableObject)
  (constructor [this ctx env]
    (super ctx env)
    (store/schema! (.-storage ctx) domain))
  Object
  ;; one command, transit; its events, or `{:error ...}`, transit
  (execute [this command]
    (store/encode (store/execute! (.. this -ctx -storage) domain (store/decode command))))
  ;; up to a page of events after number `after`, transit
  (events [this after]
    (store/encode (store/events-after (.. this -ctx -storage) after store/page)))
  ;; the read model's query `name` (`todo/queries`), transit
  (query [this name]
    (if-some [query (get (:queries domain) name)]
      (store/encode (query (.. this -ctx -storage -sql)))
      (throw (ex-info (str "No such query: " (pr-str name)) {:name name}))))
  ;; the read model derived again from the events
  (rebuild [this]
    (store/rebuild! (.. this -ctx -storage) domain)
    nil))
