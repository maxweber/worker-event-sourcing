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
    (testing "the log is read after a cursor, each event as it was appended"
      (is (= added (first (store/events-after storage 0 100))))
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

(deftest a-rebuild-pages-through-its-log
  (let [storage (fake/storage)
        n (+ (* 2 store/page) 50)]
    (store/schema! storage todo/domain)
    (dotimes [i n]
      (store/execute! storage todo/domain {:command/type :todo/add :todo/text (str "todo " i)}))
    (store/rebuild! storage todo/domain)
    (is (= n (count (todos storage))))
    (is (= n (:event/number (peek (store/events-after storage (* 2 store/page) store/page)))))))


(deftest a-projection-is-data
  (is (= [["insert into todos (id, text) values (?, ?)" "t1" "Ship it"]]
         (todo/project {:event/type :todo/added :todo/id "t1" :todo/text "Ship it"})))
  (is (= [["update todos set done = 1 where id = ?" "t1"]]
         (todo/project {:event/type :todo/completed :todo/id "t1"}))))
