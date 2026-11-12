(ns example.worker
  "The Worker's module: one Durable Object class, `Stream`, for the todo
   domain. vite.worker.config.mjs exports it; wrangler.jsonc names it."
  (:require [example.todo :as todo]
            [worker-event-sourcing.stream :refer [defstream]]))

(defstream Stream todo/domain)
