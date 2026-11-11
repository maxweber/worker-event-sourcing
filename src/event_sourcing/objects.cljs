(ns event-sourcing.objects
  "The two Durable Objects. `Log`, named after its stream, holds the
   stream's log and nothing else; its code never changes. `Projection`,
   named `<stream>/<version>`, holds one read model of that log: it
   decides commands, appends to its stream's log through the `LOG`
   binding, and catches up from it before it answers a query. A Worker
   reaches either through its binding, `(.get objects (.idFromName
   objects name))`, and calls the methods below on the stub. Commands,
   events and query results cross the call as transit JSON text, so
   both sides keep plain Clojure data."
  (:require ["cloudflare:workers" :refer [DurableObject]]
            [cljf.x :refer [defclass]]
            [clojure.string :as str]
            [event-sourcing.log :as log]
            [event-sourcing.projection :as projection]
            [event-sourcing.store :as store :refer [decode encode]]
            [event-sourcing.todo :as todo]))

(defclass Log
  (extends DurableObject)
  (constructor [this ctx env]
    (super ctx env)
    (log/schema! (.-storage ctx)))
  Object
  ;; `events`, transit, appended after `expected` when it is still the
  ;; head: `{:events [...]}`, else `{:conflict head}`; transit
  (append [this expected events]
    (encode (log/append! (.. this -ctx -storage) expected (decode events))))
  ;; up to a page of events after number `after`, transit
  (events [this after]
    (encode (log/events-after (.. this -ctx -storage) after store/page))))

(def domain
  todo/domain)

(defn- stream-of
  "The stream of a projection Object's name, `<stream>/<version>`."
  [^js ctx]
  (let [name (.. ctx -id -name)]
    (when-not (and (string? name) (str/includes? name "/"))
      (throw (ex-info "A projection Object is named <stream>/<version>." {:name name})))
    (subs name 0 (str/last-index-of name "/"))))

(defn- log-of
  "The stub of the projection's log, and the two functions the
   projection takes: `append` and `source`, over the stub's methods."
  [^js ctx ^js env]
  (let [^js logs (.-LOG env)
        ^js stub (.get logs (.idFromName logs (stream-of ctx)))]
    {:append (fn [expected events] (.then (.append stub expected (encode events)) decode))
     :source (fn [after] (.then (.events stub after) decode))}))

(defclass Projection
  (extends DurableObject)
  (constructor [this ctx env]
    (super ctx env)
    (projection/schema! (.-storage ctx) domain))
  Object
  ;; one command, transit; its events, or `{:error ...}`, transit; one
  ;; at a time, so that two decisions never race each other to the log
  (execute [this command]
    (let [^js ctx (.-ctx this)
          {:keys [append source]} (log-of ctx (.-env this))]
      (.blockConcurrencyWhile ctx
        (fn []
          (.then (projection/execute! (.-storage ctx) domain append source (decode command))
                 encode)))))
  ;; the read model's query `name` (`todo/queries`), transit, after
  ;; catching up with the log
  (query [this name]
    (let [^js ctx (.-ctx this)
          {:keys [source]} (log-of ctx (.-env this))]
      (.blockConcurrencyWhile ctx
        (fn []
          (.then (projection/catch-up! (.-storage ctx) domain source)
                 (fn [_] (encode ((get (:queries domain) name) (.. ctx -storage -sql)))))))))
  ;; every event after the cursor folded in; the cursor then
  (catchUp [this]
    (let [^js ctx (.-ctx this)
          {:keys [source]} (log-of ctx (.-env this))]
      (.blockConcurrencyWhile ctx
        (fn []
          (projection/catch-up! (.-storage ctx) domain source))))))
