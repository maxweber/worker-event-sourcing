(ns event-sourcing.fake
  "The Object's storage stood in by node:sqlite: `sql.exec` and
   `transactionSync`, which is all the store uses."
  (:require ["node:sqlite" :as sqlite]))

(defn storage
  []
  (let [^js db (new (.-DatabaseSync sqlite) ":memory:")]
    #js {:sql #js {:exec (fn [statement & params]
                           (let [^js prepared (.prepare db statement)
                                 rows (.apply (.-all prepared) prepared (to-array params))]
                             #js {:toArray (fn [] rows)}))}
         :transactionSync (fn [f]
                            (.exec db "begin")
                            (try
                              (let [result (f)]
                                (.exec db "commit")
                                result)
                              (catch :default e
                                (.exec db "rollback")
                                (throw e))))}))
