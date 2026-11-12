(ns event-sourcing.store-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
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
      (is (thrown? js/Error (store/events-after storage nil 100)))
      (is (thrown? js/Error (store/events-after storage "1" 100)))
      (is (thrown? js/Error (store/events-after storage -1 100)))
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


(deftest the-log-cannot-be-dropped
  (let [storage (fake/storage)
        _ (store/schema! storage todo/domain)
        _ (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Keep me"})
        careless (assoc-in todo/domain [:tables "Events"] "create table if not exists Events (x)")]
    (is (thrown? js/Error (store/schema! storage careless)))
    (is (thrown? js/Error (store/rebuild! storage careless)))
    (is (thrown? js/Error (store/schema! storage (assoc-in todo/domain [:tables "projection"] "create table if not exists projection (x)"))))
    (is (= 1 (count (store/events-after storage 0 100))))))

(deftest a-new-version-derives-the-read-model-again
  (let [storage (fake/storage)
        _ (store/schema! storage todo/domain)
        {[added] :events} (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Ship it"})
        id (:todo/id added)
        tamper! #(store/exec (.-sql storage) "update todos set text = 'tampered'")
        v2 (-> todo/domain
               (assoc :version "a-new-uuid")
               (assoc-in [:tables "todos"] "create table if not exists todos (id text primary key, text text not null, done integer not null default 0, upper text)")
               (assoc :project (fn [event]
                                 (case (:event/type event)
                                   :todo/added [["insert into todos (id, text, upper) values (?, ?, ?)" (:todo/id event) (:todo/text event) (str/upper-case (:todo/text event))]]
                                   :todo/completed [["update todos set done = 1 where id = ?" (:todo/id event)]]))))]
    (testing "the same version leaves the read model alone"
      (tamper!)
      (store/schema! storage todo/domain)
      (is (= [{:id id :text "tampered" :done 0}] (todos storage))))
    (testing "a new version derives the read model again, in its new shape"
      (store/schema! storage v2)
      (is (= [{:id id :text "Ship it" :done 0 :upper "SHIP IT"}]
             (store/exec (.-sql storage) "select id, text, done, upper from todos"))))
    (testing "the version is recorded, so the next construction does nothing, a uuid literal included"
      (tamper!)
      (store/schema! storage v2)
      (store/schema! storage (assoc v2 :version (uuid "a-new-uuid")))
      (is (= [{:id id :text "tampered" :done 0 :upper "SHIP IT"}]
             (store/exec (.-sql storage) "select id, text, done, upper from todos"))))))

(deftest a-parameter-is-text-a-number-nil-or-bytes
  (let [storage (fake/storage)
        db (.-sql storage)]
    (store/exec db "create table t (v)")
    (is (= [] (store/exec db "insert into t (v) values (?)" "text")))
    (is (= [] (store/exec db "insert into t (v) values (?)" 1.5)))
    (is (= [] (store/exec db "insert into t (v) values (?)" nil)))
    (is (= [] (store/exec db "insert into t (v) values (?)" (js/Uint8Array. #js [1 2]))))
    (is (thrown? js/Error (store/exec db "insert into t (v) values (?)" :image)))
    (is (thrown? js/Error (store/exec db "insert into t (v) values (?)" true)))
    (is (thrown? js/Error (store/exec db "insert into t (v) values (?)" (js/Date.))))
    (is (thrown? js/Error (store/exec db "insert into t (v) values (?)" js/NaN)))
    (is (= 4 (:n (first (store/exec db "select count(*) as n from t")))))))

(deftest a-rejection-rolls-back-what-decide-touched
  (let [storage (fake/storage)
        _ (store/schema! storage todo/domain)
        _ (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Ship it"})
        careless (assoc todo/domain :decide (fn [db _]
                                              (store/exec db "update todos set text = 'touched'")
                                              {:error "No."}))]
    (is (= {:error "No."} (store/execute! storage careless {})))
    (is (= ["Ship it"] (map :text (todos storage))))
    (is (thrown? js/Error (store/execute! storage (assoc todo/domain :decide (fn [_ _] nil)) {})))
    (is (thrown? js/Error (store/execute! storage (assoc todo/domain :decide (fn [_ _] [{:event/type :todo/added}])) {})))
    (is (thrown? js/Error (store/execute! storage (assoc todo/domain :decide (fn [_ _] {:events [] :error "both"})) {})))
    (is (= 1 (count (store/events-after storage 0 100))))))

(deftest a-rebuild-drops-the-previous-version-s-tables-and-no-other
  (let [storage (fake/storage)
        db (.-sql storage)
        _ (store/schema! storage todo/domain)
        _ (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Ship it"})
        _ (store/exec db "create table notes (note text)")
        renamed (-> todo/domain
                    (assoc :version "renamed")
                    (assoc :tables {"items" "create table if not exists items (id text primary key, text text not null)"})
                    (assoc :project (fn [event]
                                      (case (:event/type event)
                                        :todo/added [["insert into items (id, text) values (?, ?)" (:todo/id event) (:todo/text event)]]
                                        :todo/completed nil))))
        tables #(set (map :name (store/exec db "select name from sqlite_master where type = 'table'")))]
    (testing "the renamed version drops todos, builds items, leaves notes alone"
      (store/schema! storage renamed)
      (is (= #{"events" "projection" "items" "notes"} (tables)))
      (is (= ["Ship it"] (map :text (store/exec db "select text from items")))))
    (testing "a key is a name and nothing else"
      (store/schema! storage (-> renamed (assoc :version "tricky") (assoc-in [:tables "x; drop table events"] "create table if not exists x (y)")))
      (store/schema! storage (assoc renamed :version "after"))
      (is (= #{"events" "projection" "items" "notes" "x"} (tables)))
      (is (= 1 (count (store/events-after storage 0 100)))))))

(deftest a-retried-command-answers-what-it-did-the-first-time
  (let [storage (fake/storage)
        _ (store/schema! storage todo/domain)
        command {:command/id "req-1" :command/type :todo/add :todo/text "Ship it"}
        first-answer (store/execute! storage todo/domain command)
        again (store/execute! storage todo/domain command)]
    (is (= first-answer again))
    (is (= "req-1" (-> first-answer :events first :event/command-id)))
    (is (= 1 (count (todos storage))))
    (is (= 1 (count (store/events-after storage 0 100))))
    (testing "a rejection leaves no trace, so its retry is decided afresh"
      (is (= {:error "A todo needs a text."}
             (store/execute! storage todo/domain {:command/id "req-2" :command/type :todo/add :todo/text " "})))
      (is (= [{:id (:todo/id (first (:events first-answer))) :text "Ship it" :done 0}] (todos storage))))
    (testing "an id is a string of 1 to 128 characters without control characters"
      (doseq [id [42 "" (apply str (repeat 129 "a")) "a\nb"]]
        (is (thrown? js/Error (store/execute! storage todo/domain {:command/id id :command/type :todo/add :todo/text "x"}))))
      (is (= 1 (count (store/events-after storage 0 100)))))
    (testing "a command without an id is decided every time"
      (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Again"})
      (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Again"})
      (is (= 3 (count (todos storage)))))))

(deftest a-failed-rebuild-leaves-the-old-read-model-and-version
  (let [storage (fake/storage)
        db (.-sql storage)
        _ (store/schema! storage todo/domain)
        _ (store/execute! storage todo/domain {:command/type :todo/add :todo/text "Ship it"})
        broken (-> todo/domain (assoc :version "broken") (assoc :project (fn [_] [["insert into nowhere (x) values (1)"]])))]
    (is (thrown? js/Error (store/schema! storage broken)))
    (is (= ["Ship it"] (map :text (todos storage))))
    (is (= (str todo/version) (:version (first (store/exec db "select version from projection")))))))

(deftest the-store-owns-the-number-and-the-time-of-an-event
  (let [storage (fake/storage)
        _ (store/schema! storage todo/domain)
        sneaky (assoc todo/domain :decide (fn [_ _] {:events [{:event/type :todo/added :todo/id "s" :todo/text "s"
                                                               :event/number 99 :event/at "yesterday"}]}))
        {[event] :events} (store/execute! storage sneaky {})]
    (is (= 1 (:event/number event)))
    (is (not= "yesterday" (:event/at event)))))

(deftest a-log-of-exactly-two-pages-rebuilds
  (let [storage (fake/storage)
        n (* 2 store/page)]
    (store/schema! storage todo/domain)
    (dotimes [i n]
      (store/execute! storage todo/domain {:command/type :todo/add :todo/text (str i)}))
    (store/rebuild! storage todo/domain)
    (is (= n (count (todos storage))))))

(deftest an-unreadable-row-fails-construction-and-stays
  (let [storage (fake/storage)
        db (.-sql storage)
        _ (store/schema! storage todo/domain)
        _ (store/exec db "insert into events (number, event) values (1, 'not transit')")]
    (is (thrown? js/Error (store/schema! storage (assoc todo/domain :version "next"))))
    (is (= 1 (count (store/exec db "select number from events"))))))
