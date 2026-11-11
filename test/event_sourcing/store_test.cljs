(ns event-sourcing.store-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.fake :as fake]
            [event-sourcing.store :as store]
            [event-sourcing.todo :as todo]))

(defn todos
  [^js storage]
  ((get todo/queries "todos") (.-sql storage)))

(deftest a-stream
  (let [storage (fake/storage)
        _ (store/schema! storage todo/domain)
        {[added] :events} (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Ship it"})
        id (:todo/id added)]
    (testing "a command's events are appended and projected"
      (is (= 1 (:event/number added)))
      (is (= :todo/added (:event/type added)))
      (is (string? (:event/at added)))
      (is (= [{:id id :text "Ship it" :done 0}] (todos storage))))
    (testing "a rejected command writes nothing"
      (is (= {:error "A todo needs a text."}
             (store/execute! storage todo/domain {:command/type :todo/add :todo/text " "})))
      (is (= {:error "No such todo."}
             (store/execute! storage todo/domain {:command/type :todo/complete :todo/id "x"})))
      (is (= 1 (count (store/events-after storage 0 100)))))
    (testing "the next command continues the numbering"
      (is (= 2 (-> (store/execute! storage todo/domain {:command/type :todo/complete :todo/id id})
                   :events first :event/number)))
      (is (= [{:id id :text "Ship it" :done 1}] (todos storage))))
    (testing "a command that changes nothing has no event"
      (is (= {:events []} (store/execute! storage todo/domain {:command/type :todo/complete :todo/id id}))))
    (testing "the log is read after a cursor"
      (is (= [1 2] (map :event/number (store/events-after storage 0 100))))
      (is (= [2] (map :event/number (store/events-after storage 1 100))))
      (is (= [] (store/events-after storage 2 100))))
    (testing "a rebuild derives the same read model"
      (store/rebuild! storage todo/domain)
      (is (= [{:id id :text "Ship it" :done 1}] (todos storage))))
    (testing "a projection that throws takes its events with it"
      (is (thrown? js/Error
                   (store/execute! storage
                                   (assoc todo/domain :decide (fn [_ _] {:events [{:event/type :todo/unknown}]}))
                                   {})))
      (is (= 2 (count (store/events-after storage 0 100)))))))
