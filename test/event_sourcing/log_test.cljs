(ns event-sourcing.log-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.fake :as fake]
            [event-sourcing.log :as log]))

(deftest a-log
  (let [storage (fake/storage)
        _ (log/schema! storage)
        {[a b] :events} (log/append! storage 0 [{:event/type :a} {:event/type :b}])]
    (testing "events are numbered after the expected head"
      (is (= [1 2] [(:event/number a) (:event/number b)]))
      (is (string? (:event/at a))))
    (testing "an append on a stale head is refused and writes nothing"
      (is (= {:conflict 2} (log/append! storage 1 [{:event/type :c}])))
      (is (= 2 (count (log/events-after storage 0 100)))))
    (testing "the log is read after a cursor, each event as appended"
      (is (= a (first (log/events-after storage 0 100))))
      (is (= [2] (map :event/number (log/events-after storage 1 100))))
      (is (= [1] (map :event/number (log/events-after storage 0 1))))
      (is (= [] (log/events-after storage 2 100))))
    (testing "an append on the current head continues the numbering"
      (is (= [3] (map :event/number (:events (log/append! storage 2 [{:event/type :c}]))))))))
