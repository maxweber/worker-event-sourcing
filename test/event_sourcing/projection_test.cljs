(ns event-sourcing.projection-test
  (:require [clojure.test :refer [async deftest is testing]]
            [event-sourcing.fake :as fake]
            [event-sourcing.log :as log]
            [event-sourcing.projection :as projection]
            [event-sourcing.store :as store]
            [event-sourcing.todo :as todo]))

(defn a-log
  "A log on a fake storage, as the two functions a projection takes."
  []
  (let [storage (fake/storage)]
    (log/schema! storage)
    {:storage storage
     :append (fn [expected events] (js/Promise.resolve (log/append! storage expected events)))
     :source (fn [after] (js/Promise.resolve (log/events-after storage after store/page)))}))

(defn a-projection
  []
  (let [storage (fake/storage)]
    (projection/schema! storage todo/domain)
    storage))

(defn todos
  [^js storage]
  ((get todo/queries "todos") (.-sql storage)))

(defn ^:async two-projections-on-one-log
  []
  (let [{:keys [append source]} (a-log)
        a (a-projection)
        b (a-projection)
        execute (fn [storage command] (projection/execute! storage todo/domain append source command))
        {[added] :events} (await (execute a {:command/type :todo/add :todo/text "Ship it"}))
        id (:todo/id added)]
    (testing "a decided command lands in the log and in the deciding projection"
      (is (= 1 (:event/number added)))
      (is (= 1 (projection/cursor a)))
      (is (= [{:id id :text "Ship it" :done 0}] (todos a))))
    (testing "a rejected command appends nothing"
      (is (= {:error "A todo needs a text."} (await (execute a {:command/type :todo/add :todo/text " "}))))
      (is (= 1 (projection/cursor a))))
    (testing "a stale projection is refused, catches up and decides again"
      (is (= 0 (projection/cursor b)))
      (let [{[completed] :events} (await (execute b {:command/type :todo/complete :todo/id id}))]
        (is (= 2 (:event/number completed)))
        (is (= 2 (projection/cursor b)))
        (is (= [{:id id :text "Ship it" :done 1}] (todos b)))))
    (testing "the other projection sees it once it has caught up"
      (is (= [{:id id :text "Ship it" :done 0}] (todos a)))
      (is (= 2 (await (projection/catch-up! a todo/domain source))))
      (is (= [{:id id :text "Ship it" :done 1}] (todos a))))
    (testing "a command that changes nothing appends nothing"
      (is (= {:events []} (await (execute a {:command/type :todo/complete :todo/id id})))))
    (testing "a new projection version is a fresh projection catching up from the start"
      (let [c (a-projection)]
        (is (= 2 (await (projection/catch-up! c todo/domain source))))
        (is (= (todos a) (todos c)))))))

(deftest projections-on-one-log
  (async done
    (-> (two-projections-on-one-log)
        (.catch (fn [e] (is false (str e))))
        (.then (fn [] (done))))))

(defn ^:async catching-up-pages
  []
  (let [{:keys [storage source]} (a-log)
        n (+ (* 2 store/page) 50)]
    (dotimes [i n]
      (log/append! storage i [{:event/type :todo/added :todo/id (str i) :todo/text (str "todo " i)}]))
    (let [p (a-projection)]
      (is (= n (await (projection/catch-up! p todo/domain source))))
      (is (= n (count (todos p)))))))

(deftest a-catch-up-pages-through-the-log
  (async done
    (-> (catching-up-pages)
        (.catch (fn [e] (is false (str e))))
        (.then (fn [] (done))))))

(deftest a-projection-is-data
  (is (= [["insert into todos (id, text) values (?, ?)" "t1" "Ship it"]]
         (todo/project {:event/type :todo/added :todo/id "t1" :todo/text "Ship it"})))
  (is (= [["update todos set done = 1 where id = ?" "t1"]]
         (todo/project {:event/type :todo/completed :todo/id "t1"}))))

(deftest the-projection-guards-its-cursor
  (let [p (a-projection)]
    (is (thrown? js/Error (projection/apply! p todo/domain [{:event/type :todo/added :event/number 2 :todo/id "x" :todo/text "x"}])))
    (is (= 0 (projection/cursor p)))
    (is (thrown? js/Error (projection/schema! p (assoc-in todo/domain [:tables "Cursor"] "create table if not exists Cursor (x)"))))))
