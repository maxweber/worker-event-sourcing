(ns event-sourcing.worker
  "The Worker and the Durable Object. `handler` is the module's default
   export: a request to `/<stream>/...` goes to the Object named
   `<stream>` (an organization, a customer, a device: whatever owns one
   history) with the rest of the path. `Stream` is the Object: it
   answers the stream's API from its own SQLite through the store."
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [event-sourcing.store :as store]
            [event-sourcing.todo :as todo]))

(def domain
  todo/domain)

(def page
  "How many events one read of the log hands out."
  100)

(defn edn-response
  [status data]
  (js/Response. (pr-str data)
                #js {:status status
                     :headers #js {"content-type" "application/edn"}}))

(defn after-param
  "The `after` of a URL, 0 when missing or not a number."
  [^js url]
  (let [n (js/Number (or (.get (.-searchParams url) "after") 0))]
    (if (js/Number.isInteger n) n 0)))

(defn ^:async serve-stream
  "The stream's API, answered by its Object:
     POST /command         an EDN command; its events, or the error
     GET  /events?after=N  up to a page of events after number N
     GET  /<query>         a query of the read model, `todo/queries`
     POST /rebuild         the read model derived again from the events"
  [^js storage ^js request]
  (let [url (js/URL. (.-url request))
        path (.-pathname url)
        method (.-method request)
        query (get (:queries domain) (subs path 1))]
    (store/schema! storage domain)
    (cond
      (and (= "POST" method) (= "/command" path))
      (let [result (store/execute! storage domain (reader/read-string (await (.text request))))]
        (edn-response (if (:error result) 400 200) result))

      (and (= "GET" method) (= "/events" path))
      (edn-response 200 (store/events-after storage (after-param url) page))

      (and (= "POST" method) (= "/rebuild" path))
      (do (store/rebuild! storage domain)
          (edn-response 200 {:rebuilt true}))

      (and (= "GET" method) query)
      (edn-response 200 (query (.-sql storage)))

      :else
      (edn-response 404 {:error "Not found."}))))

(deftype Stream [^js ctx ^js env]
  Object
  (fetch [_ request]
    (serve-stream (.-storage ctx) request)))

(defn ^:async serve
  "`/<stream>/<rest>` to the Object named `<stream>`, as `/<rest>`."
  [^js request ^js env]
  (let [url (js/URL. (.-url request))
        [stream & rest] (remove str/blank? (str/split (.-pathname url) #"/"))]
    (if-not stream
      (edn-response 404 {:error "The path starts with the stream's name."})
      (let [^js streams (.-STREAM env)
            ^js stub (.get streams (.idFromName streams stream))]
        (set! (.-pathname url) (str "/" (str/join "/" rest)))
        (await (.fetch stub (js/Request. (.toString url) request)))))))

(def handler
  "The module's default export."
  #js {:fetch (fn [request env _ctx]
                (.catch (serve request env)
                        (fn [e]
                          (js/console.error "worker-event-sourcing failed" (str e) (.-stack e))
                          (edn-response 500 {:error "The worker failed; the log has the reason."}))))})
