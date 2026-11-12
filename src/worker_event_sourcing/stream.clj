(ns worker-event-sourcing.stream
  "The macro side of `worker-event-sourcing.stream`: `defstream`."
  (:require [cljf.x]))

(defmacro defstream
  "A Durable Object class `name` for `domain`, with the stream's
   methods over RPC: `execute`, `events`, `query`, `rebuild`. Export
   it from the Worker's module and name it in wrangler.jsonc as a
   SQLite-backed class."
  [name domain]
  `(cljf.x/defclass ~name
     (~'extends worker-event-sourcing.stream/durable-object)
     (~'constructor [this# ctx# env#]
       (~'super ctx# env#)
       (worker-event-sourcing.stream/init (.-storage ctx#) ~domain))
     ~'Object
     (~'execute [this# command#]
       (worker-event-sourcing.stream/execute (.-storage (.-ctx this#)) ~domain command#))
     (~'events [this# after#]
       (worker-event-sourcing.stream/events (.-storage (.-ctx this#)) after#))
     (~'query [this# name#]
       (worker-event-sourcing.stream/query (.-storage (.-ctx this#)) ~domain name#))
     (~'rebuild [this#]
       (worker-event-sourcing.stream/rebuild (.-storage (.-ctx this#)) ~domain))))
