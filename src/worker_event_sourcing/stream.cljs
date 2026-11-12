(ns worker-event-sourcing.stream
  "The Durable Object of one stream: an organization, a customer, a
   device, whatever owns one history. `(defstream Stream domain)`
   defines the class; a Worker reaches an Object through the binding,
   `(.get streams (.idFromName streams name))`, and calls the methods
   on the stub:

     execute  one command, transit; its events, or `{:error ...}`, transit
     events   up to a page of events after a number, transit
     query    the read model's query by name, transit
     rebuild  the read model derived again from the events

   Commands, events and query results cross the call as transit JSON
   text, so both sides keep plain Clojure data. Nothing here awaits:
   that is what keeps the Object the stream's single writer."
  (:require-macros [worker-event-sourcing.stream])
  (:require ["cloudflare:workers" :as cf]
            [cljf.x]
            [worker-event-sourcing.store :as store]))

(def durable-object
  "The base class a stream extends."
  cf/DurableObject)

(defn init
  "At construction: the store's tables, and the read model at the
   domain's version."
  [storage domain]
  (store/schema! storage domain))

(defn execute
  [storage domain command]
  (store/encode (store/execute! storage domain (store/decode command))))

(defn events
  [storage after]
  (store/encode (store/events-after storage after store/page)))

(defn query
  [storage domain name]
  (if-some [query (get (:queries domain) name)]
    (store/encode (query (.-sql storage)))
    (throw (ex-info (str "No such query: " (pr-str name)) {:name name}))))

(defn rebuild
  [storage domain]
  (store/rebuild! storage domain)
  nil)
