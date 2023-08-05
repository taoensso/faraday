(ns taoensso.faraday.tests.main
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [taoensso.encore :as encore]
   [taoensso.faraday :as far]
   [taoensso.nippy :as nippy])

  (:import
   [com.amazonaws.auth BasicAWSCredentials]
   [com.amazonaws.internal StaticCredentialsProvider]
   [com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient AmazonDynamoDBStreamsClient]
   [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException TransactionCanceledException]
   [com.amazonaws AmazonServiceException]
   [java.util Date]
   [org.testcontainers.containers GenericContainer]))

(defmethod clojure.test/report :begin-test-var [m]
  (println "\u001B[32mTesting" (-> m :var meta :name) "\u001B[0m"))

;;;; Private var aliases

(def index-status-watch #'far/index-status-watch)

;;;; Config & setup

(def ^:dynamic *client-opts*
  {:access-key (or (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY") "test")
   :secret-key (or (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY") "test")
   :endpoint (or (get (System/getenv) "AWS_DYNAMODB_ENDPOINT") "http://localhost:6798")})

(defn- dynamodb-local
  [t]
  (let [dynamodb-port 8000
        container (doto (GenericContainer. "amazon/dynamodb-local:2.0.0")
                    (.addExposedPort (int dynamodb-port))
                    (.start))
        local-port (.getMappedPort container dynamodb-port)]
    (with-open [_ container]
      (with-bindings {#'*client-opts* (assoc *client-opts* :endpoint (format "http://localhost:%d" local-port))}
        (t)))))

(use-fixtures :once dynamodb-local)

(def ttable :faraday.tests.main)
(def range-table :faraday.tests.range)
(def book-table :faraday.tests.books)
(def bulk-table :faraday.tests.bulk)

(defn with-some-tables
  [f]
  (assert (and (:access-key *client-opts*)
               (:secret-key *client-opts*)))

  (far/ensure-table *client-opts* ttable
                    [:id :n]
                    {:throughput {:read 1 :write 1}
                     :block? true})

  (far/ensure-table *client-opts* range-table
                    [:title :s]
                    {:range-keydef [:number :n]
                     :throughput {:read 1 :write 1}
                     :block? true})

  (far/ensure-table *client-opts* book-table
                    [:author :s]
                    {:range-keydef [:name :s]
                     :throughput {:read 1 :write 1}
                     :block? true})

  (far/ensure-table *client-opts* bulk-table
                    [:group :s]
                    {:range-keydef [:id :n]
                     :throughput {:read 1 :write 1}
                     :block? true})

  (f)
  (far/delete-table *client-opts* ttable)
  (far/delete-table *client-opts* range-table)
  (far/delete-table *client-opts* book-table)
  (far/delete-table *client-opts* bulk-table))

(use-fixtures :each with-some-tables)

(deftest client-creation
  (is (#'far/db-client (assoc *client-opts* :region "us-east-1")))
  (is (#'far/db-client (assoc *client-opts* :endpoint "http://localhost:6798")))
  (is (#'far/db-client (assoc *client-opts* :protocol "HTTP")))
  (is (#'far/db-client (assoc *client-opts* :protocol "HTTPS")))
  (is (thrown? IllegalArgumentException
               (#'far/db-client (assoc *client-opts* :protocol "random")))))

(deftest basic-api
  (let [i0 {:id 0 :name "foo"}
        i1 {:id 1 :name "bar"}]

    (far/batch-write-item *client-opts*
                          {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})

    (testing "Batch put"
      (is (= [i0 i1 nil]
             (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                 [(far/get-item *client-opts* ttable {:id 0})
                  (far/get-item *client-opts* ttable {:id 1})
                  (far/get-item *client-opts* ttable {:id -1})]))))

    (testing "Batch get"
      (is (= (set [i0 i1])
             (->> (far/batch-get-item *client-opts*
                                      {ttable {:prim-kvs {:id [0 1]}
                                               :consistent? true}})
                  ttable set))))

    (testing "We only get existing items"
      (is (= [i1]
             (->> (far/batch-get-item *client-opts*
                                      {ttable {:prim-kvs {:id [1 2 3 4]}
                                               :consistent? true}})
                  ttable))))

    (testing "The call does not fail if there are no items at all"
      (is (= []
             (->> (far/batch-get-item *client-opts*
                                      {ttable {:prim-kvs {:id [2 3 4]}
                                               :consistent? true}})
                  ttable))))

    (testing "Batch get, with :attrs"
      (is (= (set [(dissoc i0 :name) (dissoc i1 :name)])
             (->> (far/batch-get-item *client-opts*
                                      {ttable {:prim-kvs {:id [0 1]}
                                               :attrs [:id]
                                               :consistent? true}})
                  ttable set))))

    (testing "Batch delete"
      (is (= [nil nil]
             (do (far/batch-write-item *client-opts* {ttable {:delete {:id [0 1]}}})
                 [(far/get-item *client-opts* ttable {:id 0})
                  (far/get-item *client-opts* ttable {:id 1})]))))))

(deftest string-table-name
  (let [bulk-table-str (name bulk-table)
        i0             {:id 0 :group "group"}
        i1             {:id 1 :group "group"}]
    (far/batch-write-item *client-opts*
                          {bulk-table {:delete [i0 i1]}})
    (far/batch-write-item *client-opts*
                          {bulk-table {:put [i0 i1]}})
    (testing "query & scan requests can be made when table name given as a string"
      (is (= #{i0 i1}
             (->> (far/query *client-opts*
                             bulk-table-str
                             {:group [:eq "group"]
                              :id    [:lt 3]}
                             {:consistent? true
                              :limit       100
                              :span-reqs   {:max 25}
                              :return      #{:id :group}})
                  set)))
      (is (= #{i0 i1}
             (->> (far/scan *client-opts*
                            bulk-table-str
                            {:consistent? true
                             :limit       100
                             :span-reqs   {:max 25}
                             :return      #{:id :group}})
                  set))))))

(deftest bulk-queries
  (let [num-items 100]
    (doseq [batch (partition 25 (range num-items))]
      (far/batch-write-item *client-opts*
                            {bulk-table {:delete (map (fn [i] {:group "group" :id i}) batch)}}))
    (is (= [100 100 100]
           (do (let [long-text (apply str (repeatedly 300000 (constantly "n")))]
                 (doseq [i (range num-items)]
                   (far/put-item *client-opts* bulk-table {:group "group"
                                                           :id i
                                                           :text long-text})))
               [(->> (far/batch-get-item *client-opts*
                                         {bulk-table {:prim-kvs {:group "group"
                                                                 :id (range num-items)}
                                                      :attrs [:id]
                                                      :consistent? true}}
                                         {:span-reqs {:max 2}})
                     bulk-table set count)
                (->> (far/query *client-opts*
                                bulk-table {:group [:eq "group"]
                                            :id [:lt num-items]}
                                {:consistent? true
                                 :limit 100
                                 :span-reqs {:max 25}
                                 :return #{:id}})
                     set count)
                (->> (far/scan *client-opts*
                               bulk-table
                               {:consistent? true
                                :limit 100
                                :span-reqs {:max 25}
                                :return #{:id}})
                     set count)])))))

(deftest updating-items
  (let [i {:id 10 :name "update me"}]
    (far/delete-item *client-opts* ttable {:id 10})
    (is (=
         {:id 10 :name "baz"}
         (do
           (far/put-item *client-opts* ttable i)
           (far/update-item
            *client-opts* ttable {:id 10} {:update-map {:name [:put "baz"]}
                                           :return :all-new}))))

    (is (thrown? ConditionalCheckFailedException
                 (far/update-item *client-opts* ttable
                                  {:id 10}
                                  {:update-map {:name [:put "baz"]}
                                   :expected {:name "garbage"}})))))

(deftest expressions-support
  (let [i {:id 10 :name "update me"}]
    (far/delete-item *client-opts* ttable {:id 10})

    (testing "Update expression support in update-item"
      (is (=
           {:id 10 :name "foo"}
           (far/update-item *client-opts* ttable
                            {:id 10}
                            {:update-expr "SET #name = :name"
                             :expr-attr-names {"#name" "name"}
                             :expr-attr-vals {":name" "foo"}
                             :return :all-new}))))

    (testing "We can add a set item"
      (is (=
           {:id 10 :name "foo" :someset #{"a" "b" "c"}}
           (far/update-item *client-opts* ttable
                            {:id 10}
                            {:update-expr "SET someset = :toset"
                             :expr-attr-vals {":toset" #{"a" "b" "c"}}
                             :return :all-new}))))

    (testing "We can add update a set"
      (is (=
           {:id 10 :name "foo" :someset #{"a" "b" "c" "d" "e"}}
           (far/update-item *client-opts* ttable
                            {:id 10}
                            {:update-expr "ADD someset :toset"
                             :expr-attr-vals {":toset" #{"d" "e"}}
                             :return :all-new}))))

    (testing "Updating an existing element to a set does not duplicate it"
      (is (=
           {:id 10 :name "foo" :someset #{"a" "b" "c" "d" "e" "f"}}
           (far/update-item *client-opts* ttable
                            {:id 10}
                            {:update-expr "ADD someset :toset"
                             :expr-attr-vals {":toset" #{"e" "f"}}
                             :return :all-new}))))

    (testing "We can remove elements from a set"
      (is (=
           {:id 10 :name "foo" :someset #{"a" "c" "e" "f"}}
           (far/update-item *client-opts* ttable
                            {:id 10}
                            {:update-expr "DELETE someset :todel"
                             :expr-attr-vals {":todel" #{"b" "d"}}
                             :return :all-new}))))


    (testing "Condition expression support in update-item"
      (is (thrown? ConditionalCheckFailedException
                   (far/update-item *client-opts* ttable
                                    {:id 10}
                                    {:cond-expr "#name <> :name"
                                     :update-expr "SET #name = :name"
                                     :expr-attr-names {"#name" "name"}
                                     :expr-attr-vals {":name" "foo"}
                                     :return :all-new})))))

  (testing "Condition expression support in put-item"
    (is (thrown? ConditionalCheckFailedException
                 (far/put-item *client-opts* ttable
                               {:id 10}
                               {:cond-expr "attribute_not_exists(id) AND #name <> :name"
                                :expr-attr-names {"#name" "name"}
                                :expr-attr-vals {":name" "foo"}})))))

(deftest batch-writes
  (let [items [{:id 11 :name "eleven" :test "batch"}
               {:id 12 :name "twelve" :test "batch"}
               {:id 13 :name "thirteen" :test "batch"}]
        [i1 i2 i3] items]

    (far/batch-write-item
     *client-opts* {ttable {:delete (map #(select-keys % #{:id}) items)}})

    (is (=
         [i1]
         (do (far/batch-write-item *client-opts* {ttable {:put items}})
             (far/scan *client-opts* ttable
                       {:attr-conds {:name [:eq "eleven"]}}))))

    (is (=
         #{i1 i3}
         (into #{} (far/scan *client-opts* ttable
                             {:attr-conds {:name [:ne "twelve"]
                                           :test [:eq "batch"]}}))))

    (is (=
         (repeat 3 {:test "batch"})
         (far/scan *client-opts* ttable {:attr-conds {:test [:eq "batch"]}
                                         :return [:test]})))))

;; Test projection expressions
(deftest projection-expressions
  (let [i0 {:id 1984 :name "Nineteen Eighty-Four"
            :author "George Orwell"
            :details {:tags ["dystopia" "surveillance"]
                      :characters ["Winston Smith" "Julia" "O'Brien"]}}
        i1 {:id 2001 :name "2001: A Space Odyssey"
            :author "Arthur C. Clarke"
            :details {:tags ["science fiction" "evolution" "artificial intelligence"]
                      :characters ["David Bowman" "Francis Poole" "HAL 9000"]}}]

    (far/batch-write-item *client-opts*
                          {ttable {:delete [{:id 1984} {:id 2001}]}})

    (testing "Batch put"
      (is (= [i0 i1 nil]
             (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                 [(far/get-item *client-opts* ttable {:id 1984})
                  (far/get-item *client-opts* ttable {:id 2001})
                  (far/get-item *client-opts* ttable {:id 0})]))))

    (testing "Batch get"
      (is (= (set [i0 i1])
             (->> (far/batch-get-item *client-opts*
                                      {ttable {:prim-kvs {:id [1984 2001]}
                                               :consistent? true}})
                  ttable set))))

    (testing "Get without projections"
      (is (= i0
             (far/get-item *client-opts* ttable {:id 1984}))))

    (testing "Simple projection expression, equivalent to getting the attributes"
      (is (= {:author "Arthur C. Clarke"}
             (far/get-item *client-opts* ttable {:id 2001} {:proj-expr "author"}))))

    (testing "Getting the tags for 1984"
      (is (= {:details {:tags ["dystopia" "surveillance"]}}
             (far/get-item *client-opts* ttable {:id 1984} {:proj-expr "details.tags"}))))

    (testing "Getting a specific character for 2001"
      (is (= {:details {:characters ["HAL 9000"]}}
             (far/get-item *client-opts* ttable {:id 2001} {:proj-expr "details.characters[2]"}))))

    (testing "Getting multiple items with projections from the same list of 2001 characters"
      (is (= {:details {:characters ["David Bowman" "HAL 9000"]}}
             (far/get-item *client-opts* ttable
                           {:id 2001}
                           {:proj-expr "details.characters[0], details.characters[2]"}))))

    (testing "Expression attribute names, necessary since 'name' is a reserved keyword"
      (is (= {:author "Arthur C. Clarke" :name "2001: A Space Odyssey"}
             (far/get-item *client-opts* ttable
                           {:id 2001}
                           {:proj-expr "#n, author"
                            :expr-attr-names {"#n" "name"}}))))

    (testing "Batch delete"
      (is (= [nil nil]
             (do (far/batch-write-item *client-opts* {ttable {:delete {:id [1984 2001]}}})
                 [(far/get-item *client-opts* ttable {:id 1984})
                  (far/get-item *client-opts* ttable {:id 2001})]))))))

(defmacro update-t
  [& cmds]
  `(do
     (far/put-item *client-opts* ttable ~'t)
     (far/update-item *client-opts* ttable {:id (:id ~'t)} ~@cmds)
     (far/get-item *client-opts* ttable {:id (:id ~'t)})))

(deftest type-check
  (let [t {:id 14
           :boolT true
           :boolF false
           :string "string"
           :num 1
           :null nil
           :numset #{4 12 6 13}
           :strset #{"a" "b" "c"}
           :map {:k1 "v1" :k2 "v2" :k3 "v3"}
           :vec ["a" 1 false nil]}
        take-care-of-map-keys {:id 15
                               :map {:key "val" "key" 5}}]

    (far/put-item *client-opts* ttable take-care-of-map-keys)

    (is (= t
           (do
             (far/put-item *client-opts* ttable t)
             (far/get-item *client-opts* ttable {:id (:id t)}))))

    (is (= {:id 15 :map {:key 5}}
           (far/get-item *client-opts* ttable {:id (:id take-care-of-map-keys)})))

    (is (= (update-in t [:strset] #(conj % "d"))
           (update-t {:update-map {:strset [:add #{"d"}]}})))

    (is (= (update-in t [:strset] #(disj % "c"))
           (update-t {:update-map {:strset [:delete #{"c"}]}})))

    (is (= (assoc t :strset #{"d"})
           (update-t {:update-map {:strset [:put #{"d"}]}})))

    (is (= (assoc t :num 2)
           (update-t {:update-map {:num [:add 1]}})))

    (is (= (dissoc t :map)
           (update-t {:update-map {:map [:delete]}})))

    (is (= (assoc t :boolT false)
           (update-t {:update-map {:boolT [:put false]}})))

    (is (= (assoc t :boolT nil)
           (update-t {:update-map {:boolT [:put nil]}})))

    (extend-protocol far/ISerializable Date
      (serialize [x]
        (far/serialize (pr-str x))))

    (is (= (assoc t :date "#inst \"1970-01-01T00:00:00.000-00:00\"")
           (update-t {:update-map {:date [:put (Date. (long 0))]}})))

    (is (= (assoc-in t [:map-new :new] "x")
           (update-t {:update-map {:map-new [:put {:new "x"}]}})))))

(deftest expectations
  (let [t {:id 16
           :val 1
           :str "abc"}]

    (is (= (update-in t [:val] inc)
           (update-t {:update-map {:val [:add 1]}
                      :expected {:val :exists}})))

    (is (= (update-in t [:val] inc)
           (update-t
            {:update-map {:val [:add 1]}
             :expected {:blah :not-exists}})))

    (is (= (update-in t [:val] inc)
           (update-t
            {:update-map {:val [:add 1]}
             :expected {:val [:< 5]}})))

    (is (= (update-in t [:val] inc)
           (update-t
            {:update-map {:val [:add 1]}
             :expected {:val [:eq 1]}})))

    (is (= (update-in t [:val] inc)
           (update-t
            {:update-map {:val [:add 1]}
             :expected {:str [:begins-with "a"]}})))

    (is (= (update-in t [:val] inc)
           (update-t
            {:update-map {:val [:add 1]}
             :expected {:val [:between 0 2]}})))

    (is (thrown? ConditionalCheckFailedException
                 (update-t
                  {:update-map {:val [:add 1]}
                   :expected {:val [:> 5]}})))

    (is (thrown? ConditionalCheckFailedException
                 (update-t
                  {:update-map {:val [:add 1]}
                   :expected {:val [:= 2]}})))))

(deftest query-and-delete-item
  (let [i0 {:name "Nineteen Eighty-Four"
            :author "George Orwell"
            :year 1949
            :details {:tags ["dystopia" "surveillance"]
                      :characters ["Winston Smith" "Julia" "O'Brien"]}}
        i1 {:name "2001: A Space Odyssey"
            :author "Arthur C. Clarke"
            :year 1968
            :read? true
            :details {:tags ["science fiction" "evolution" "artificial intelligence"]
                      :characters ["David Bowman" "Francis Poole" "HAL 9000"]}}
        i2 {:name "2010: Odissey Two"
            :author "Arthur C. Clarke"
            :year 1982
            :details {:tags ["science fiction" "evolution" "artificial intelligence"]
                      :characters ["David Bowman" "Heywood Floyd" "HAL 9000" "Dr. Chandra"]}}
        i3 {:name "3001: The Final Odissey"
            :author "Arthur C. Clarke"
            :year 1997
            :details {:tags ["science fiction" "evolution" "artificial intelligence"]
                      :characters ["Francis Poole"]}}
        i4 {:name "Animal Farm"
            :author "George Orwell"
            :year 1945
            :read? true}
        books [i0 i1 i2 i3 i4]]

    (far/batch-write-item *client-opts*
                          {book-table {:delete (mapv (fn [m] (select-keys m [:author :name])) books)}})
    (far/batch-write-item *client-opts* {book-table {:put books}})

    (testing "Books are returned ordered by the sort key"
      (is (= [i4 i0] (far/query *client-opts* book-table {:author [:eq "George Orwell"]})))
      (is (= [i1 i2 i3]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}))))

    (testing "We can get only the books starting with a string"
      (is (= [i1 i2]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]
                                                  :name [:begins-with "20"]}))))

    (testing "We can request only some attributes on the return list"
      (is (= (mapv #(select-keys % [:name :year]) [i1 i2])
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]
                                                  :name [:begins-with "20"]}
                        {:return [:name :year]}))))

    (testing "Test query filters"
      (is (= [i2 i3]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:query-filter {:year [:gt 1980]}})))

      (is (= [i4]
             (far/query *client-opts* book-table {:author [:eq "George Orwell"]}
                        {:query-filter {:year [:le 1945]}}))))

    (testing "Test query filter expressions"
      (is (= [i2]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(details.characters) >= :cnt"
                         :expr-attr-vals {":cnt" 4}})))
      (is (= [i1 i3]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(details.characters) < :cnt"
                         :expr-attr-vals {":cnt" 4}})))
      (is (= [i1 i3]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(details.characters) < :cnt"
                         :expr-attr-vals {":cnt" 4}}))))

    (testing "We cannot combine query-filter and filter expressions, it's either-or"
      (is (= [i1]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(details.characters) < :cnt and #y < :year"
                         :expr-attr-names {"#y" "year"}
                         :expr-attr-vals {":cnt" 4 ":year" 1990}}))))

    (testing "We can use expression attribute names even when going for a nested expression"
      (is (= [i1]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(#d.characters) < :cnt and #y < :year"
                         :expr-attr-names {"#y" "year"
                                           "#d" "details"}
                         :expr-attr-vals {":cnt" 4 ":year" 1990}})))
      (is (= [i1]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(#d.#c) < :cnt and #y < :year"
                         :expr-attr-names {"#y" "year"
                                           "#d" "details"
                                           "#c" "characters"}
                         :expr-attr-vals {":cnt" 4 ":year" 1990}}))))
    (testing "Confirm we can query and filter by booleans with question marks on the name"
      (is (= [i4]
             (far/query *client-opts* book-table {:author [:eq "George Orwell"]}
                        {:filter-expr "#r = :r"
                         :expr-attr-names {"#r" "read?"}
                         :expr-attr-vals {":r" true}})))
      (is (= [i4 i1]
             (far/scan *client-opts* book-table {:attr-conds {:read? [:eq true]}})))

      (is (= [i4 i1]
             (far/scan *client-opts* book-table {:filter-expr "#r = :r"
                                                 :expr-attr-names {"#r" "read?"}
                                                 :expr-attr-vals {":r" true}}))))
    (testing "Limits are respected, but we need to send in :span-reqs"
      (is (= 3
             (count (far/scan *client-opts* book-table {:limit 3
                                                        :span-reqs {:max 1}}))))
      (is (= [i4]
             (far/scan *client-opts* book-table {:filter-expr "#r = :r"
                                                 :expr-attr-names {"#r" "read?"}
                                                 :limit 1
                                                 :span-reqs {:max 1}
                                                 :expr-attr-vals {":r" true}}))))
    (testing "Paging metadata is returned"
      (is (= {:cc-units nil, :last-prim-kvs {:author "George Orwell", :name "Animal Farm"}, :count 1, :scanned-count 1}
             (meta (far/scan *client-opts* book-table {:filter-expr "#r = :r"
                                                       :expr-attr-names {"#r" "read?"}
                                                       :limit 1
                                                       :span-reqs {:max 1}
                                                       :expr-attr-vals {":r" true}})))))

    (testing "Confirm we combine projection and filter expressions on scan"
      (is (= ["George Orwell" "Arthur C. Clarke"]
             (map :author
                  (far/scan *client-opts* book-table {:proj-expr "author"
                                                      :filter-expr "#r = :r"
                                                      :expr-attr-names {"#r" "read?"}
                                                      :expr-attr-vals {":r" true}})))))
    (testing ":attr-conds and :filter-expr are mutually exclusive"
      (is (thrown? AssertionError
                   (far/scan *client-opts* book-table {:attr-conds {:author [:eq "George Orwell"]}
                                                       :filter-expr "#r = :r"
                                                       :expr-attr-names {"#r" "read?"}
                                                       :expr-attr-vals {":r" true}}))))


    (testing "Test projection expressions"
      (is (= [{:year 1968
               :details {:characters ["David Bowman"]}}
              {:year 1997
               :details {:characters ["Francis Poole"]}}]
             (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                        {:filter-expr "size(#d.characters) < :cnt"
                         :proj-expr "#y, details.characters[0]"
                         :expr-attr-names {"#y" "year"
                                           "#d" "details"}
                         :expr-attr-vals {":cnt" 4}}))))

    (testing "If we specify a condition that isn't fulfilled, we get an exception when deleting"
      (is (thrown? AmazonServiceException
                   (far/delete-item *client-opts* book-table
                                    {:name "Nineteen Eighty-Four"
                                     :author "George Orwell"}
                                    {:cond-expr "#y > :y"
                                     :expr-attr-names {"#y" "year"}
                                     :expr-attr-vals {":y" 1984}})))
      (is (= i0
             (far/get-item *client-opts* book-table
                           {:name "Nineteen Eighty-Four"
                            :author "George Orwell"}
                           ))))
    (testing "If a condition is fulfilled, then the item is deleted as expected"
      (is nil?
          (far/delete-item *client-opts* book-table
                           {:name "Nineteen Eighty-Four"
                            :author "George Orwell"}
                           {:cond-expr "#y = :y"
                            :expr-attr-names {"#y" "year"}
                            :expr-attr-vals {":y" 1949}}))
      (is nil?
          (far/get-item *client-opts* book-table
                        {:name "Nineteen Eighty-Four"
                         :author "George Orwell"}
                        )))
    (testing "But we can't just embed the value in the condition expression"
      (is (thrown? AmazonServiceException
                   (far/delete-item *client-opts* book-table
                                    {:name "Animal Farm"
                                     :author "George Orwell"}
                                    {:cond-expr "#y = 1945"
                                     :expr-attr-names {"#y" "year"}}))))))


(deftest range-queries
  (let [j0 {:title "One" :number 0}
        j1 {:title "One" :number 1}
        k0 {:title "Two" :number 0}
        k1 {:title "Two" :number 1}
        k2 {:title "Two" :number 2}
        k3 {:title "Two" :number 3}]

    (far/batch-write-item *client-opts* {range-table {:put [j0 j1 k0 k1 k2 k3]}})

    (testing "Query, normal ordering"
      (is (= [j0 j1]
             (far/query *client-opts* range-table {:title [:eq "One"]}))))

    (testing "Query, reverse ordering"
      (is (= [j1 j0]
             (far/query *client-opts* range-table {:title [:eq "One"]}
                        {:order :desc}))))

    (testing "Query with :limit"
      (is (= [j0]
             (far/query *client-opts* range-table {:title [:eq "One"]}
                        {:limit 1 :span-reqs {:max 1}}))))

    (testing "Query with more results"
      (is (= {:cc-units nil, :last-prim-kvs {:number 0N, :title "One"}, :count 1}
             (meta (far/query *client-opts* range-table {:title [:eq "One"]}
                              {:limit 1 :span-reqs {:max 1}})))))

    (testing "Query, with range"
      (is (= [k1 k2]
             (far/query *client-opts* range-table {:title [:eq "Two"]
                                                   :number [:between [1 2]]}))))

    (testing "Verify the :le lt :ge :gt options"
      (is (= [k0 k1 k2]
             (far/query *client-opts* range-table {:title [:eq "Two"]
                                                   :number [:lt 3]})))

      (is (= [k0 k1 k2]
             (far/query *client-opts* range-table {:title [:eq "Two"]
                                                   :number [:le 2]})))

      (is (= [k2 k3]
             (far/query *client-opts* range-table {:title [:eq "Two"]
                                                   :number [:gt 1]})))

      (is (= [k1 k2 k3]
             (far/query *client-opts* range-table {:title [:eq "Two"]
                                                   :number [:ge 1]})))

      (is (= [k1 k2]
             (far/query *client-opts* range-table {:title [:eq "Two"]
                                                   :number [:ge 1]}
                        {:limit 2 :span-reqs {:max 1}}))))))


(deftest serialization
  (let [data nippy/stress-data-comparable]
    (is (= {:id 10 :nippy-data data}
           (do (far/put-item *client-opts* ttable {:id 10 :nippy-data (far/freeze data)})
               (far/get-item *client-opts* ttable {:id 10}))))))

(deftest unserialized-bytes
  (let [data (byte-array (mapv byte [0 1 2]))]
    (far/put-item *client-opts* ttable {:id 11 :ba-data data})
    (is (encore/ba= data (:ba-data (far/get-item *client-opts* ttable {:id 11}))))))

(deftest put-item-contitions
  (let [i0 {:id 0 :name "foo"}
        i1 {:id 1 :name "bar"}
        i2 {:id 2 :name "baz"}]

    (testing "Throw for bad conds"
      (is (thrown? ConditionalCheckFailedException
                   (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                       (far/put-item *client-opts* ttable i1 {:expected {:id false}})))))

    (testing "Proceed for good conds"
      (is nil? (far/put-item *client-opts* ttable i1 {:expected {:id 1}}))
      (is nil? (far/put-item *client-opts* ttable i2 {:expected {:id :not-exists}}))
      (is nil? (far/put-item *client-opts* ttable i2 {:expected {:id 2
                                                                 :dummy :not-exists}})))))


(deftest aws-credentials-provider
  (when-let [endpoint (:endpoint *client-opts*)]
    (let [i0 {:id 0 :name "foo"}
          i1 {:id 1 :name "bar"}
          i2 {:id 2 :name "baz"}
          creds (BasicAWSCredentials. (:access-key *client-opts*)
                                      (:secret-key *client-opts*))
          provider (StaticCredentialsProvider. creds)]

      (binding [*client-opts* {:provider provider
                               :endpoint endpoint}]

        (testing "Batch put"
          (is (= [i0 i1 nil]
                 (do
                   (far/batch-write-item *client-opts*
                                         {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})
                   (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                   [(far/get-item *client-opts* ttable {:id 0})
                    (far/get-item *client-opts* ttable {:id 1})
                    (far/get-item *client-opts* ttable {:id -1})]))))))))

(defn get-temp-table []
  (keyword (str "temp_table_" (.getTime (Date.)))))

(defmacro do-with-temp-table
  [bindings & cmds]
  (concat
   (list 'let (into ['temp-table (get-temp-table)] bindings))
   cmds
   ['(far/delete-table *client-opts* temp-table)]))

(deftest table-and-index-creation
  (testing "Basic table creation"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= temp-table (:name created)))
     (is nil? (:lsindexes created))
     (is nil? (:gsindexes created))
     (is (= {:artist {:key-type :hash, :data-type :s}} (:prim-keys created)))))

  (testing "Table creation with range key"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:range-keydef [:song-title :s]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= temp-table (:name created)))
     (is nil? (:lsindexes created))
     (is nil? (:gsindexes created))
     (is (= {:artist {:key-type :hash, :data-type :s},
             :song-title {:key-type :range, :data-type :s}} (:prim-keys created)))))

  (testing "Test creating a global secondary index, without a sort key, :projection defaults to :all"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:gsindexes [{:name "genre-index"
                                              :hash-keydef [:genre :s]
                                              :throughput {:read 1 :write 1}}]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= temp-table (:name created)))
     (is nil? (:lsindexes created))
     (is (= [{:name :genre-index
              :size 0
              :item-count 0
              :key-schema [{:name :genre :type :hash}]
              :projection {:projection-type "ALL"
                           :non-key-attributes nil}
              :throughput {:read 1 :write 1 :last-decrease nil
                           :last-increase nil :num-decreases-today nil}}]
            (:gsindexes created)))
     (is (= {:artist {:key-type :hash :data-type :s}
             :genre {:data-type :s}} (:prim-keys created)))))

  (testing "Test creating a global secondary index and sort key, projection defaults to :all"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:gsindexes [{:name "genre-index"
                                              :hash-keydef [:genre :s]
                                              :range-keydef [:year :n]
                                              :throughput {:read 1 :write 1}}]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= temp-table (:name created)))
     (is nil? (:lsindexes created))
     (is (= [{:name :genre-index
              :size 0
              :item-count 0
              :key-schema [{:name :genre :type :hash}
                           {:name :year :type :range}]
              :projection {:projection-type "ALL"
                           :non-key-attributes nil}
              :throughput {:read 1 :write 1 :last-decrease nil
                           :last-increase nil :num-decreases-today nil}}]
            (:gsindexes created)))
     (is (= {:artist {:key-type :hash :data-type :s}
             :genre {:data-type :s}
             :year {:data-type :n}} (:prim-keys created)))))

  (testing "Test creating multiple global secondary indexes, verify :projection defaults to :all"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:gsindexes [{:name "genre-index"
                                              :hash-keydef [:genre :s]
                                              :throughput {:read 1 :write 1}}
                                             {:name "label-index"
                                              :hash-keydef [:label :s]
                                              :throughput {:read 2 :write 3}
                                              :projection :keys-only}
                                             ]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= temp-table (:name created)))
     (is nil? (:lsindexes created))
     (is (= [{:name :label-index
              :size 0
              :item-count 0
              :key-schema [{:name :label :type :hash}]
              :projection {:projection-type "KEYS_ONLY" :non-key-attributes nil}
              :throughput
              {:read 2
               :write 3
               :last-decrease nil
               :last-increase nil
               :num-decreases-today nil}}
             {:name :genre-index
              :size 0
              :item-count 0
              :key-schema [{:name :genre :type :hash}]
              :projection {:projection-type "ALL" :non-key-attributes nil}
              :throughput
              {:read 1
               :write 1
               :last-decrease nil
               :last-increase nil
               :num-decreases-today nil}}]
            (:gsindexes created)))
     (is (= {:artist {:key-type :hash :data-type :s}
             :genre {:data-type :s}
             :label {:data-type :s}} (:prim-keys created)))))

  (testing "Test creating a local secondary index and sort key, projection defaults to :all"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:range-keydef [:song-title :s] ; Need a range key for it to accept a local secondary index
                                 :lsindexes [{:name "year-index"
                                              :range-keydef [:year :n]}]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is nil? (:gsindexes created))
     (is (= [{:name :year-index
              :size 0
              :item-count 0
              :key-schema [{:type :hash :name :artist} {:name :year :type :range}]
              :projection {:projection-type "ALL"
                           :non-key-attributes nil}}]
            (:lsindexes created)))
     (is (= {:artist {:key-type :hash :data-type :s}
             :song-title {:key-type :range, :data-type :s}
             :year {:data-type :n}}
            (:prim-keys created)))))

  (testing "hash-keydef and throughput are ignored on local secondary index"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:range-keydef [:song-title :s] ; Need a range key for it to accept a local secondary index
                                 :lsindexes [{:name "year-index"
                                              :hash-keydef [:genre :s]
                                              :range-keydef [:year :n]
                                              :throughput {:read 1 :write 1}}]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= [{:name :year-index
              :size 0
              :item-count 0
              :key-schema [{:type :hash :name :artist} {:name :year :type :range}]
              :projection {:projection-type "ALL"
                           :non-key-attributes nil}}]
            (:lsindexes created)))))

  (testing "We can combine local secondary and global secondary indexes"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:range-keydef [:song-title :s] ; Need a range key for it to accept a local secondary index
                                 :gsindexes [{:name "genre-index"
                                              :hash-keydef [:genre :s]
                                              :throughput {:read 1 :write 2}}
                                             {:name "label-index"
                                              :hash-keydef [:label :s]
                                              :throughput {:read 3 :write 4}
                                              :projection :keys-only}
                                             ]
                                 :lsindexes [{:name "year-index"
                                              :range-keydef [:year :n]}]
                                 :throughput {:read 1 :write 1}
                                 :block? true})]
     (is (= [{:name :label-index
              :size 0
              :item-count 0
              :key-schema [{:name :label :type :hash}]
              :projection {:projection-type "KEYS_ONLY" :non-key-attributes nil}
              :throughput
              {:read 3
               :write 4
               :last-decrease nil
               :last-increase nil
               :num-decreases-today nil}}
             {:name :genre-index
              :size 0
              :item-count 0
              :key-schema [{:name :genre :type :hash}]
              :projection {:projection-type "ALL" :non-key-attributes nil}
              :throughput
              {:read 1
               :write 2
               :last-decrease nil
               :last-increase nil
               :num-decreases-today nil}}]
            (:gsindexes created)))
     (is (= [{:name :year-index
              :size 0
              :item-count 0
              :key-schema [{:type :hash :name :artist} {:name :year :type :range}]
              :projection {:projection-type "ALL"
                           :non-key-attributes nil}}]
            (:lsindexes created)))))

  (testing "Increasing throughput"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:throughput {:read 1 :write 1}
                                 :block? true})
      updated @(far/update-table *client-opts* temp-table {:throughput {:read 16 :write 16}})
      again @(far/update-table *client-opts* temp-table {:throughput {:read 256 :write 256}})]
     (testing "Both table descriptions are the same other than the throughput"
       (is (= (dissoc created :throughput)
              (dissoc updated :throughput)))
       (is (= (dissoc updated :throughput)
              (dissoc again :throughput))))
     (testing "Throughput was updated"
       (is (= {:read 16 :write 16}
              (-> updated
                  :throughput
                  (select-keys #{:read :write}))))
       (is (= {:read 256 :write 256}
              (-> again
                  :throughput
                  (select-keys #{:read :write})))))))

  (testing "Test decreasing throughput"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:throughput {:read 256 :write 256}
                                 :block? true})
      updated @(far/update-table *client-opts* temp-table {:throughput {:read 16 :write 16}})
      again @(far/update-table *client-opts* temp-table {:throughput {:read 1 :write 1}})]
     (testing "Both table descriptions are the same other than the throughput"
       (is (= (dissoc created :throughput)
              (dissoc updated :throughput)))
       (is (= (dissoc updated :throughput)
              (dissoc again :throughput))))
     (testing "Throughput was updated"
       (is (= {:read 16 :write 16}
              (-> updated
                  :throughput
                  (select-keys #{:read :write}))))
       (is (= {:read 1 :write 1}
              (-> again
                  :throughput
                  (select-keys #{:read :write})))))))

  (testing "Sending the same throughput as what the table has should have no effect"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:throughput {:read 1 :write 1}
                                 :block? true})
      updated @(far/update-table *client-opts* temp-table {:throughput {:read 1 :write 1}})]
     (testing "Both table descriptions are the same"
       (is (= created updated)))))

  (testing "Sending an empty parameter set should have no effect"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:throughput {:read 1 :write 1}
                                 :block? true})
      updated @(far/update-table *client-opts* temp-table {})]
     (testing "Both table descriptions are the same"
       (is (= created updated)))))

  (testing "Global secondary index creation"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:throughput {:read 1 :write 1}
                                 :block? true})
      new-idx @(far/update-table *client-opts* temp-table
                                 {:gsindexes {:operation :create
                                              :name "genre-index"
                                              :hash-keydef [:genre :s]
                                              :throughput {:read 4 :write 2}
                                              }})
      ;; We need to wait until the index is created before updating it, or the call will fail
      _ @(index-status-watch *client-opts* temp-table :gsindexes "genre-index")
      inc-idx @(far/update-table *client-opts* temp-table
                                 {:gsindexes {:operation :update
                                              :name "genre-index"
                                              :throughput {:read 6 :write 6}
                                              }})
      ;; We can create a second index right after
      amt-idx @(far/update-table *client-opts* temp-table
                                 {:gsindexes {:operation :create
                                              :name "amount-index"
                                              :hash-keydef [:amount :n]
                                              :throughput {:read 1 :write 1}
                                              }})
      ;; Let's wait until amount-index is created before deleting genre-index,
      ;; so that we can consistently evaluate the result (otherwise we might not
      ;; know if size/item-count are 0 or nil.
      _ @(index-status-watch *client-opts* temp-table :gsindexes "amount-index")
      del-idx @(far/update-table *client-opts* temp-table
                                 {:gsindexes {:operation :delete
                                              :name "genre-index"}})
      _ @(index-status-watch *client-opts* temp-table :gsindexes "genre-index")
      ;; And get the final state
      fin-idx (far/describe-table *client-opts* temp-table)]

     (testing "Tables are the same other than the global indexes"
       (is (= (dissoc created :gsindexes :prim-keys)
              (dissoc new-idx :gsindexes :prim-keys))))
     (testing "We have a new index"
       (is (= [{:name :genre-index
                :key-schema [{:name :genre :type :hash}]
                :projection {:projection-type "ALL" :non-key-attributes nil}
                :throughput {:read 4 :write 2 :last-decrease nil :last-increase nil :num-decreases-today nil}}]
              (->> (:gsindexes new-idx) (map #(dissoc % :size :item-count))))))
     (testing "The updated index has the new throughput values, as well as a size and item-count since it was already created"
       (is (= [{:name :genre-index
                :size 0
                :item-count 0
                :key-schema [{:name :genre :type :hash}]
                :projection {:projection-type "ALL" :non-key-attributes nil}
                :throughput {:read 6 :write 6 :last-decrease nil :last-increase nil :num-decreases-today nil}}]
              (:gsindexes inc-idx))))
     (testing "The second index created comes back"
       (is (= #{{:name :amount-index
                 :key-schema [{:name :amount :type :hash}]
                 :projection {:projection-type "ALL" :non-key-attributes nil}
                 :throughput {:read 1 :write 1 :last-decrease nil :last-increase nil :num-decreases-today nil}}
                {:name :genre-index
                 :key-schema [{:name :genre :type :hash}]
                 :projection {:projection-type "ALL" :non-key-attributes nil}
                 :throughput {:read 6 :write 6 :last-decrease nil :last-increase nil :num-decreases-today nil}}}
              (set (->> (:gsindexes amt-idx) (map #(dissoc % :size :item-count)))))))
     (testing "When we request that the genre index be deleted, it returns that it's being destroyed"
       (is (= #{{:name :amount-index
                 :key-schema [{:name :amount :type :hash}]
                 :projection {:projection-type "ALL" :non-key-attributes nil}
                 :throughput {:read 1 :write 1 :last-decrease nil :last-increase nil :num-decreases-today nil}}
                {:name :genre-index
                 :key-schema [{:name :genre :type :hash}]
                 :projection {:projection-type "ALL" :non-key-attributes nil}
                 :throughput {:read 6 :write 6 :last-decrease nil :last-increase nil :num-decreases-today nil}}}
              (set (->> (:gsindexes del-idx) (map #(dissoc % :size :item-count)))))))
     (testing "And finally, we were left only with the amount index"
       (is (= [{:name :amount-index
                :size 0
                :item-count 0
                :key-schema [{:name :amount :type :hash}]
                :projection {:projection-type "ALL" :non-key-attributes nil}
                :throughput {:read 1 :write 1 :last-decrease nil :last-increase nil :num-decreases-today nil}}]
              (:gsindexes fin-idx)))))))

(deftest querying-indexes
  (testing "We can scan with an index, and do projections"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:artist :s]
                                {:range-keydef [:song-title :s] ; Need a range key for it to accept a local secondary index
                                 :gsindexes [{:name "genre-index"
                                              :hash-keydef [:genre :s]
                                              :throughput {:read 1 :write 2}}
                                             {:name "label-index"
                                              :hash-keydef [:label :s]
                                              :throughput {:read 3 :write 4}
                                              :projection :keys-only}
                                             ]
                                 :lsindexes [{:name "year-index"
                                              :range-keydef [:year :n]}]
                                 :throughput {:read 1 :write 1}
                                 :block? true})
      items [{:artist "Carpenter Brut"
              :song-title "Le Perv"
              :genre "Electro"
              :label "Unknown"
              :year 2012}
             {:artist "The Mars Volta"
              :song-title "Eriatarka"
              :genre "Progressive Rock"
              :label "Universal Records"
              :year 2003}
             {:artist "The Darkest of the Hillside Thickets"
              :song-title "The Shadow Out of Tim"
              :genre "Rock"
              :label "Divine Industries"
              :year 2007}
             {:artist "The Mars Volta"
              :song-title "Cassandra Gemini"
              :genre "Progressive Rock"
              :label "Universal Records"
              :year 2005}]
      _ (doall (map #(far/put-item *client-opts* temp-table %) items))
      scanned (far/scan *client-opts* temp-table {:attr-conds {:year [:ge 2005]}
                                                  :index "year-index"})
      projected (far/scan *client-opts* temp-table {:proj-expr "genre, artist"
                                                    :index "genre-index"})
      with-name (far/scan *client-opts* temp-table {:proj-expr "genre, #y"
                                                    :index "year-index"
                                                    :expr-attr-names {"#y" "year"}})]
     (testing "Querying for a range key returns items sorted"
       (is (= [{:artist "The Mars Volta"
                :song-title "Cassandra Gemini"
                :genre "Progressive Rock"
                :label "Universal Records"
                :year 2005}
               {:artist "The Darkest of the Hillside Thickets"
                :song-title "The Shadow Out of Tim"
                :genre "Rock"
                :label "Divine Industries"
                :year 2007}
               {:artist "Carpenter Brut"
                :song-title "Le Perv"
                :genre "Electro"
                :label "Unknown"
                :year 2012}]
              scanned)))
     (testing "We can't rely on items being in a particular order unless we use the item with a sort key"
       (is (= #{{:artist "The Mars Volta"
                 :genre "Progressive Rock"}
                {:artist "The Darkest of the Hillside Thickets"
                 :genre "Rock"}
                {:artist "Carpenter Brut"
                 :genre "Electro"}}
              (set projected)))
       (is (= 4 (count projected))))
     (testing "We can request projections with expression attribute names"
       (is (= [{:genre "Progressive Rock"
                :year 2003}
               {:genre "Progressive Rock"
                :year 2005}
               {:genre "Rock"
                :year 2007}
               {:genre "Electro"
                :year 2012}]
              with-name))))))

(deftest listing-tables-with-lazy-sequence
  ;; Generate > 100 tables to exceed the batch size limit:
  (let [tables (map #(keyword (str "test_" %)) (range 102))]
    (doseq [table tables]
      (far/ensure-table *client-opts* table [:id :n]
                        {:throughput {:read 1 :write 1}
                         :block? true}))
    (let [table-count (count (far/list-tables *client-opts*))]
      (doseq [table tables]
        (far/delete-table *client-opts* table))
      (is (> table-count 100))))

  (let [update-t :faraday.tests.update-table]
    (when (far/describe-table *client-opts* update-t)
      (far/delete-table *client-opts* update-t))
    (far/create-table
     *client-opts* update-t [:id :n]
     {:throughput {:read 1 :write 1} :block? true})

    (is (= {:read 2 :write 2}
           (-> (far/update-table *client-opts* update-t {:throughput {:read 2 :write 2}})
               deref
               :throughput
               (select-keys #{:read :write}))))))

(deftest empty-string
  (let [item {:id 1 :name ""}]
    (is (= item
           (do
             (far/put-item *client-opts* ttable item)
             (far/get-item *client-opts* ttable {:id (:id item)}))))))

(deftest removing-empty-attributes
  (is (= {:b [{:a "b"}], :empt-str "", :e #{""}, :f false, :g "    "}
         (far/remove-empty-attr-vals
          {:b [{:a "b" :c [[]] :d #{}}, {}] :a nil :empt-str "" :e #{""} :f false :g "    "}))))

(deftest update-table-stream-spec
  (do-with-temp-table
   [created (far/create-table *client-opts* temp-table
                              [:title :s]
                              {:throughput {:read 1 :write 1}
                               :block? true})
    updated @(far/update-table *client-opts* temp-table
                               {:stream-spec {:enabled? true
                                              :view-type :keys-only}})
    updated2 @(far/update-table *client-opts* temp-table
                                {:stream-spec {:enabled? false}})]
   (is nil? (:stream-spec created))
   (is (= {:enabled? true
           :view-type :keys-only}
          (:stream-spec updated)))
   (is nil? (:stream-spec updated2))))

(deftest streams
  (do-with-temp-table
   [created (far/create-table *client-opts* temp-table
                              [:title :s]
                              {:throughput {:read 10 :write 10}
                               :stream-spec {:enabled? true
                                             :view-type :new-and-old-images}
                               :block? true})
    [{:keys [stream-arn] :as list-stream-response}] (far/list-streams *client-opts* {:table-name temp-table})
    items (mapv #(hash-map :title (str "title" %)
                           :serial-num (rand-int 100)) (range 100))]
   (is (= temp-table (-> list-stream-response :table-name keyword)))
   (is string? stream-arn)
   (doseq [chunk (partition 10 items)]
     (far/batch-write-item *client-opts* {temp-table {:put chunk}}))
   (let [stream (far/describe-stream *client-opts* stream-arn)
         shard-id (get-in stream [:shards 0 :shard-id])
         _ (is string? shard-id)
         shard-iterator (far/shard-iterator *client-opts* stream-arn shard-id :trim-horizon)
         records-result (far/get-stream-records *client-opts* shard-iterator)
         records (mapv :stream-record (:records records-result))]
     (is (= 100 (count (:records records-result))))
     (is string? (:next-shard-iterator records-result))
     (doseq [[item record] (mapv vector items records)]
       (is (= {:old-image {}
               :new-image item}
              (select-keys record [:old-image :new-image])))))))

(deftest billing-mode
  (do-with-temp-table
   [created (far/create-table *client-opts* temp-table
                              [:title :s]
                              {:billing-mode :provisioned
                               :block? true
                               :throughput {:read 10 :write 10}})]
   (let [create-described (far/describe-table *client-opts* temp-table)]
     (is (= :provisioned (-> create-described :billing-mode :name)))
     @(far/update-table *client-opts* temp-table
                        {:billing-mode :pay-per-request}))
   (let [update-described (far/describe-table *client-opts* temp-table)]
     (is (= :pay-per-request (-> update-described :billing-mode :name)))))

  (do-with-temp-table
   [created (far/create-table *client-opts* temp-table
                              [:title :s]
                              {:billing-mode :pay-per-request
                               :gsindexes [{:name "gsi"
                                            :hash-keydef [:some_id :s]
                                            :projection :all}]
                               :block? true})]
   (let [described (far/describe-table *client-opts* temp-table)]
     (is (= :pay-per-request (-> described :billing-mode :name))))
   @(far/update-table *client-opts* temp-table
                     {:gsindexes {:name "new_gsi"
                                  :hash-keydef [:new_id :s]
                                  :operation :create
                                  :projection :all}})
   (let [described (far/describe-table *client-opts* temp-table)]
     (is (= 2 (-> described :gsindexes count))))))

(deftest custom-client
  (let [calls (atom 0)
        client (proxy [AmazonDynamoDBClient] [(BasicAWSCredentials. (:access-key *client-opts*)
                                                                    (:secret-key *client-opts*))]
                 (describeTable [describe-table-request]
                   (swap! calls inc)
                   (proxy-super describeTable describe-table-request)))]
    (.setEndpoint client (:endpoint *client-opts*))
    (far/describe-table (assoc *client-opts* :client client) ttable)
    (is (= 1 @calls)))
  (let [calls (atom 0)
        client (proxy [AmazonDynamoDBStreamsClient] [(BasicAWSCredentials. (:access-key *client-opts*)
                                                                           (:secret-key *client-opts*))]
                 (listStreams [list-streams-request]
                   (swap! calls inc)
                   (proxy-super listStreams list-streams-request)))]
    (doall (far/list-streams (assoc *client-opts* :client client) {:table-name ttable}))
    (is (= 1 @calls))))

(deftest attribute-not-exists
  ;;https://github.com/Taoensso/faraday/issues/106

  (far/put-item *client-opts* range-table {:title "Three" :number 33})

  (is (thrown?
       ConditionalCheckFailedException
       (far/put-item *client-opts* range-table {:title "Three" :number 33}
                     {:cond-expr "attribute_not_exists(#t) AND attribute_not_exists(#n)"
                      ;; note typo in the issue. 'name' should be 'number'
                      :expr-attr-names {"#n" "name"
                                        "#t" "title"}})))

  (is (thrown?
       ConditionalCheckFailedException
       (far/put-item *client-opts* range-table {:title "Three" :number 33}
                     {:cond-expr "attribute_not_exists(#t) AND attribute_not_exists(#n)"
                      ;; note typo in the issue: fixed here
                      :expr-attr-names {"#n" "number"
                                        "#t" "title"}})))

  (is (thrown? ConditionalCheckFailedException
               (far/put-item *client-opts* range-table {:title "Three" :number 33}
                             {:cond-expr "attribute_not_exists(#t)"
                              :expr-attr-names {"#t" "title"}})))

  (is (thrown? ConditionalCheckFailedException
               (far/put-item *client-opts* range-table {:title "Three" :number 33}
                             {:cond-expr "attribute_not_exists(#n)"
                              :expr-attr-names {"#n" "number"}})))

  (far/put-item *client-opts* range-table {:title "Three" :number 35}
                {:cond-expr "attribute_not_exists(#t)"
                 :expr-attr-names {"#t" "title"}}))

(deftest lazy-seqs

  (testing "Lazy seq that is not realized cannot be serialized"
      (is (thrown? IllegalArgumentException
                   (far/put-item *client-opts* ttable {:id 10 :items (map str (range 5))}))))

  (testing "Lazy seqs that are realized are okay"
    (far/put-item *client-opts* ttable {:id 10 :items (doall (map str (range 5)))})
    (is (= ["0" "1" "2" "3" "4"] (:items (far/get-item *client-opts* ttable {:id 10}))))

    (far/put-item *client-opts* ttable {:id 10 :items (mapv str (range 5))})
    (is (= ["0" "1" "2" "3" "4"] (:items (far/get-item *client-opts* ttable {:id 10}))))))

(deftest ttl
  (testing "activate and deactivate ttl using update-ttl"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:id :n]
                                {:throughput {:read 1 :write 1}
                                 :block? true})
      before (far/describe-ttl *client-opts* temp-table)
      result (far/update-ttl *client-opts* temp-table true :ttl)
      after  (far/describe-ttl *client-opts* temp-table)]
     (is (= {:status :disabled :attribute-name nil} before))
     (is (= {:enabled? true :attribute-name "ttl"} result))
     (is (= {:status :enabled :attribute-name "ttl"} after))

     (let [result (far/update-ttl *client-opts* temp-table false :ttl)
           after  (far/describe-ttl *client-opts* temp-table)]
       (is (= {:enabled? false :attribute-name "ttl"} result))
       (is (= {:status :disabled :attribute-name nil} after)))))

  (testing "activate ttl using ensure-ttl"
    (do-with-temp-table
     [created (far/create-table *client-opts* temp-table
                                [:id :n]
                                {:throughput {:read 1 :write 1}
                                 :block? true})
      before (far/describe-ttl *client-opts* temp-table)
      result (far/ensure-ttl *client-opts* temp-table :ttl)
      after  (far/describe-ttl *client-opts* temp-table)]
     (is (= {:status :disabled :attribute-name nil} before))
     (is (= {:enabled? true :attribute-name "ttl"} result))
     (is (= {:status :enabled :attribute-name "ttl"} after))

     (let [result (far/ensure-ttl *client-opts* temp-table :ttl)
           after  (far/describe-ttl *client-opts* temp-table)]
       (is (nil? result))
       (is (= {:status :enabled :attribute-name "ttl"} after))))))

(deftest transaction-support
  (let [i0 {:id 300 :name "foo"}
        i1 {:id 301 :name "bar"}
        i2 {:id 302 :name "baz"}
        i3 {:id 303 :name "qux"}
        i4 {:id 304 :name "quux"}
        i5 {:id 305 :name "corge"}
        i6 {:id 306 :name "grault"}]

    (far/batch-write-item *client-opts*
                          {ttable {:delete [{:id 300} {:id 301} {:id 302} {:id 303}
                                            {:id 304} {:id 305} {:id 306}]}})
    (testing "Batch put"
      (is (= [i0 i1 i5 i6]
             (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1 i5 i6]}})
                 [(far/get-item *client-opts* ttable {:id 300})
                  (far/get-item *client-opts* ttable {:id 301})
                  (far/get-item *client-opts* ttable {:id 305})
                  (far/get-item *client-opts* ttable {:id 306})]))))

    (testing "Condition Check"
      (is (= {:cc-units {}}
             (far/transact-write-items *client-opts*
                                       {:items [[:cond-check {:table-name ttable
                                                              :prim-kvs {:id 300}
                                                              :cond-expr "attribute_exists(#id)"
                                                              :expr-attr-names {"#id" "id"}}]
                                                [:cond-check {:table-name ttable
                                                              :prim-kvs {:id 301}
                                                              :cond-expr "attribute_exists(#id)"
                                                              :expr-attr-names {"#id" "id"}}]]}))))

    (testing "Condition Check Fail"
      (is (thrown? TransactionCanceledException
                   (far/transact-write-items *client-opts*
                                             {:items [[:cond-check {:table-name ttable
                                                                    :prim-kvs {:id 30001}
                                                                    :cond-expr "attribute_exists(#id)"
                                                                    :expr-attr-names {"#id" "id"}}]]}))))

    (testing "Put"
      (is (= {:cc-units {}}
             (far/transact-write-items *client-opts*
                                       {:items [[:put {:table-name ttable
                                                       :item i2
                                                       :cond-expr "attribute_not_exists(#id)"
                                                       :expr-attr-names {"#id" "id"}}]
                                                [:put {:table-name ttable
                                                       :item i3
                                                       :cond-expr "attribute_not_exists(#id)"
                                                       :expr-attr-names {"#id" "id"}}]]}))))

    (testing "Verify put results"
      (is (= [i2 i3]
             [(far/get-item *client-opts* ttable {:id 302})
              (far/get-item *client-opts* ttable {:id 303})])))


    (testing "Put transaction should fail"
      (is (thrown? TransactionCanceledException
                   (far/transact-write-items *client-opts*
                                             {:items [[:put {:table-name ttable
                                                             :item i4
                                                             :cond-expr "attribute_not_exists(#id)"
                                                             :expr-attr-names {"#id" "id"}}]
                                                      [:put {:table-name ttable
                                                             :item i3 ;; This already exists
                                                             :cond-expr "attribute_not_exists(#id)"
                                                             :expr-attr-names {"#id" "id"}}]]}))))

    (testing "Verify that Put failed (it should not have been written)"
      (is (nil?
           (far/get-item *client-opts* ttable {:id 304}))))

    (testing "Delete"
      (is (= {:cc-units {}}
             (far/transact-write-items *client-opts*
                                       {:items [[:delete {:table-name ttable
                                                          :prim-kvs {:id 302}
                                                          :cond-expr "attribute_exists(#id)"
                                                          :expr-attr-names {"#id" "id"}}]
                                                [:delete {:table-name ttable
                                                          :prim-kvs {:id 303}
                                                          :cond-expr "attribute_exists(#id)"
                                                          :expr-attr-names {"#id" "id"}}]]}))))

    (testing "Verify delete results"
      (is (= [nil nil]
             [(far/get-item *client-opts* ttable {:id 302})
              (far/get-item *client-opts* ttable {:id 303})])))

    (testing "Update"
      (is (= {:cc-units {:faraday.tests.main 4.0}}
             (far/transact-write-items *client-opts*
                                       {:return-cc? true
                                        :items [[:update {:table-name ttable
                                                          :prim-kvs {:id 300}
                                                          :update-expr "SET #name = :name"
                                                          :cond-expr "attribute_exists(#id) AND #name = :oldname"
                                                          :expr-attr-names {"#id" "id", "#name" "name"}
                                                          :expr-attr-vals {":oldname" "foo", ":name" "foofoo"}}]]}))))

    (testing "Second update should fail"
      (is (thrown? TransactionCanceledException
                   (far/transact-write-items *client-opts*
                                             {:items [[:update {:table-name ttable
                                                                :prim-kvs {:id 300}
                                                                :update-expr "SET #name = :name"
                                                                :cond-expr "attribute_exists(#id) AND #name = :oldname"
                                                                :expr-attr-names {"#id" "id", "#name" "name"}
                                                                :expr-attr-vals {":oldname" "foo", ":name" "foobar"}}]]}))))

    (testing "Verify first update results"
      (is (= "foofoo"
             (:name (far/get-item *client-opts* ttable {:id 300})))))



    (testing "Transact Get Items"
      (is (= {:cc-units {:faraday.tests.main 4.0}
              :items [i5 i6]}
             (far/transact-get-items *client-opts*
                                     {:return-cc? true
                                      :items [{:table-name ttable
                                               :prim-kvs {:id 305}}
                                              {:table-name ttable
                                               :prim-kvs {:id 306}}]}))))))

(deftype MyType [^long val])

(extend-protocol
  far/ISerializable
  MyType
  (serialize [c]
    (str "<MyType " (.val c) ">")))

(deftest clj-item->db-item-extensions
  (testing "Serialize custom types"
    (is (= (far/clj-item->db-item {:item (MyType. 17)})
           {"item" "<MyType 17>"}))))
