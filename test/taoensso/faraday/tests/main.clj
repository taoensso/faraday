(ns taoensso.faraday.tests.main
  (:require [expectations     :as test :refer :all]
            [taoensso.encore  :as encore]
            [taoensso.faraday :as far]
            [taoensso.nippy   :as nippy])
  (:import  [com.amazonaws.auth BasicAWSCredentials]
            [com.amazonaws.internal StaticCredentialsProvider]
            [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException]))

;; TODO LOTS of tests still outstanding, PRs very, very welcome!!

(comment (test/run-tests '[taoensso.faraday.tests.main]))

;;;; Config & setup

(def ^:dynamic *client-opts*
  {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
   :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")
   :endpoint   (get (System/getenv) "AWS_DYNAMODB_ENDPOINT")})

(def ttable :faraday.tests.main)
(def range-table :faraday.tests.range)
(def book-table :faraday.tests.books)


(def run-after-setup (atom #{}))

(defn- after-setup! [thunk]
  (swap! run-after-setup conj thunk))

(defn- before-run {:expectations-options :before-run} []
  (assert (and (:access-key *client-opts*)
               (:secret-key *client-opts*)))
  (println "Setting up testing environment...")
  (far/ensure-table *client-opts* ttable [:id :n]
                    {:throughput  {:read 1 :write 1}
                     :block?      true})
  (far/ensure-table *client-opts* range-table [:title :s]
                    {:range-keydef [:number :n]
                     :throughput   {:read 1 :write 1}
                     :block?       true})
  (far/ensure-table *client-opts* book-table
                    [:author :s]
                    {:range-keydef [:name :s]
                     :throughput   {:read 1 :write 1}
                     :block?       true})
  (doseq [thunk @run-after-setup]
    (thunk))

  (println "Ready to roll..."))

(defn after-run {:expectations-options :after-run} [])

(comment (far/delete-table *client-opts* ttable))

;;;; Basic API

(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}]

  (after-setup!
   #(far/batch-write-item *client-opts*
                          {ttable {:delete [{:id 0} {:id 1} {:id 2}]}}))

  (expect ; Batch put
   [i0 i1 nil] (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                   [(far/get-item *client-opts* ttable {:id  0})
                    (far/get-item *client-opts* ttable {:id  1})
                    (far/get-item *client-opts* ttable {:id -1})]))

  (expect ; Batch get
   (set [i0 i1]) (->> (far/batch-get-item *client-opts*
                        {ttable {:prim-kvs    {:id [0 1]}
                                 :consistent? true}})
                      ttable set))

  (expect ; Batch get, with :attrs
   (set [(dissoc i0 :name) (dissoc i1 :name)])
   (->> (far/batch-get-item *client-opts*
          {ttable {:prim-kvs    {:id [0 1]}
                   :attrs       [:id]
                   :consistent? true}})
        ttable set))

  (expect ; Batch delete
   [nil nil] (do (far/batch-write-item *client-opts* {ttable {:delete {:id [0 1]}}})
                 [(far/get-item *client-opts* ttable {:id 0})
                  (far/get-item *client-opts* ttable {:id 1})])))

(let [i {:id 10 :name "update me"}]

  (after-setup!
    #(far/delete-item *client-opts* ttable {:id 10}))

  (expect
   {:id 10 :name "baz"}
   (do
     (far/put-item *client-opts* ttable i)
     (far/update-item
        *client-opts* ttable {:id 10} {:name [:put "baz"]} {:return :all-new})))

  (expect
   #=(far/ex :conditional-check-failed)
   (far/update-item *client-opts* ttable
       {:id 10} {:name [:put "baz"]}
       {:expected {:name "garbage"}})))

;;;; Expressions support

(let [i {:id 10 :name "update me"}]
  (after-setup!
   #(far/delete-item *client-opts* ttable {:id 10}))

  (expect ; Update expression support in update-item
   {:id 10 :name "foo"}
   (do
     (far/update-item *client-opts* ttable
       {:id 10}
       {}
       {:update-expr     "SET #name = :name"
        :expr-attr-names {"#name" "name"}
        :expr-attr-vals  {":name" "foo"}
        :return          :all-new})))

  (expect ; Condition expression support in update-item
   #=(far/ex :conditional-check-failed)
   (do
     (far/update-item *client-opts* ttable
       {:id 10}
       {}
       {:cond-expr       "#name <> :name"
        :update-expr     "SET #name = :name"
        :expr-attr-names {"#name" "name"}
        :expr-attr-vals  {":name" "foo"}
        :return          :all-new})))

  (expect ; Condition expression support in put-item
   #=(far/ex :conditional-check-failed)
   (do
     (far/put-item *client-opts* ttable i
       {:cond-expr "attribute_not_exists(id) AND #name <> :name"
        :expr-attr-names {"#name" "name"}
        :expr-attr-vals  {":name" "foo"}}))))

(let [items [{:id 11 :name "eleven" :test "batch"}
             {:id 12 :name "twelve" :test "batch"}
             {:id 13 :name "thirteen" :test "batch"}]
      [i1 i2 i3] items]

  (after-setup!
   (fn [] (far/batch-write-item
          *client-opts* {ttable {:delete (map #(select-keys % #{:id}) items)}})))

  (expect
   [i1]
   (do (far/batch-write-item *client-opts* {ttable {:put items}})
       (far/scan *client-opts* ttable
                 {:attr-conds {:name [:eq "eleven"]}})))

  (expect
   #{i1 i3}
   (into #{} (far/scan *client-opts* ttable
                       {:attr-conds {:name [:ne "twelve"]
                                     :test [:eq "batch"]}})))

  (expect
   (repeat 3 {:test "batch"})
   (far/scan *client-opts* ttable {:attr-conds {:test [:eq "batch"]}
                                   :return [:test]})))

;; Test projection expressions
(let [i0 {:id 1984 :name "Nineteen Eighty-Four"
          :author "George Orwell"
          :details {:tags ["dystopia" "surveillance"]
                    :characters ["Winston Smith" "Julia" "O'Brien"]}}
      i1 {:id 2001 :name "2001: A Space Odyssey"
          :author "Arthur C. Clarke"
          :details {:tags ["science fiction" "evolution" "artificial intelligence"]
                    :characters ["David Bowman" "Francis Poole" "HAL 9000"]}}]


  (after-setup!
    #(far/batch-write-item *client-opts*
                           {ttable {:delete [{:id 1984} {:id 2001}]}}))

  (expect ; Batch put
    [i0 i1 nil] (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                    [(far/get-item *client-opts* ttable {:id 1984})
                     (far/get-item *client-opts* ttable {:id 2001})
                     (far/get-item *client-opts* ttable {:id 0})]))

  (expect ; Batch get
    (set [i0 i1]) (->> (far/batch-get-item *client-opts*
                                           {ttable {:prim-kvs    {:id [1984 2001]}
                                                    :consistent? true}})
                       ttable set))

  (expect ; Get without projections
    i0 (far/get-item *client-opts* ttable {:id 1984}))

  (expect ; Simple projection expression, equivalent to getting the attributes
    {:author "Arthur C. Clarke"} (far/get-item *client-opts* ttable {:id 2001} {:projection "author"}))

  (expect ; Getting the tags for 1984
    {:details {:tags ["dystopia" "surveillance"]}}
    (far/get-item *client-opts* ttable {:id 1984} {:projection "details.tags"}))

  (expect ; Getting a specific character for 2001
    {:details {:characters ["HAL 9000"]}}
    (far/get-item *client-opts* ttable {:id 2001} {:projection "details.characters[2]"}))

  (expect ; Getting multiple items with projections from the same list of 2001 characters
    {:details {:characters ["David Bowman" "HAL 9000"]}}
    (far/get-item *client-opts* ttable
                  {:id 2001}
                  {:projection "details.characters[0], details.characters[2]"}))

  (expect ; Expression attribute names, necessary since 'name' is a reserved keyword
    {:author "Arthur C. Clarke" :name "2001: A Space Odyssey"}
    (far/get-item *client-opts* ttable
                  {:id 2001}
                  {:projection      "#n, author"
                   :expr-attr-names {"#n" "name"}}))

  (expect ; Batch delete
    [nil nil] (do (far/batch-write-item *client-opts* {ttable {:delete {:id [1984 2001]}}})
                  [(far/get-item *client-opts* ttable {:id 1984})
                   (far/get-item *client-opts* ttable {:id 2001})])))



(defmacro update-t
  [& cmds]
  `(do
     (far/put-item *client-opts* ttable ~'t)
     (far/update-item *client-opts* ttable {:id (:id ~'t)} ~@cmds)
     (far/get-item *client-opts* ttable {:id (:id ~'t)})))

;;;; type check
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

  (after-setup!
   #(far/put-item *client-opts* ttable take-care-of-map-keys))

  (expect
   t
   (do
     (far/put-item *client-opts* ttable t)
     (far/get-item *client-opts* ttable {:id (:id t)})))

  (expect
   {:id 15
    :map {:key 5}}
   (far/get-item *client-opts* ttable {:id (:id take-care-of-map-keys)}))

  (expect
   (update-in t [:strset] #(conj % "d"))
   (update-t
    {:strset [:add #{"d"}]}))

  (expect
   (update-in t [:strset] #(disj % "c"))
   (update-t
    {:strset [:delete #{"c"}]}))

  (expect
   (assoc t :strset #{"d"})
   (update-t
    {:strset [:put #{"d"}]}))

  (expect
   (assoc t :num 2)
   (update-t
    {:num [:add 1]}))

  (expect
   (dissoc t :map)
   (update-t
    {:map [:delete]}))

  (expect
   (assoc t :boolT false)
   (update-t
    {:boolT [:put false]}))

  (expect
   (assoc t :boolT nil)
   (update-t
    {:boolT [:put nil]}))

  (expect
   (assoc-in t [:map-new :new] "x")
   (update-t
    {:map-new [:put {:new "x"}]})))

;;;; expectation tests
(let [t {:id 16
         :val 1
         :str "abc"}]

  (expect
   (update-in t [:val] inc)
   (update-t
    {:val [:add 1]}
    {:expected {:val :exists}}))

  (expect
   (update-in t [:val] inc)
   (update-t
    {:val [:add 1]}
    {:expected {:blah :not-exists}}))

  (expect
   (update-in t [:val] inc)
   (update-t
    {:val [:add 1]}
    {:expected {:val [:< 5]}}))

  (expect
   (update-in t [:val] inc)
   (update-t
    {:val [:add 1]}
    {:expected {:val [:eq 1]}}))

  (expect
   (update-in t [:val] inc)
   (update-t
    {:val [:add 1]}
    {:expected {:str [:begins-with "a"]}}))

  (expect
   (update-in t [:val] inc)
   (update-t
    {:val [:add 1]}
    {:expected {:val [:between 0 2]}}))

  (expect
   ConditionalCheckFailedException
   (update-t
    {:val [:add 1]}
    {:expected {:val [:> 5]}}))

  (expect
   ConditionalCheckFailedException
   (update-t
    {:val [:add 1]}
    {:expected {:val [:= 2]}}))
  )

;;; Query tests
(let [i0    {:name    "Nineteen Eighty-Four"
             :author  "George Orwell"
             :year    1949
             :details {:tags       ["dystopia" "surveillance"]
                       :characters ["Winston Smith" "Julia" "O'Brien"]}}
      i1    {:name    "2001: A Space Odyssey"
             :author  "Arthur C. Clarke"
             :year    1968
             :details {:tags       ["science fiction" "evolution" "artificial intelligence"]
                       :characters ["David Bowman" "Francis Poole" "HAL 9000"]}}
      i2    {:name    "2010: Odissey Two"
             :author  "Arthur C. Clarke"
             :year    1982
             :details {:tags       ["science fiction" "evolution" "artificial intelligence"]
                       :characters ["David Bowman" "Heywood Floyd" "HAL 9000" "Dr. Chandra"]}}
      i3    {:name    "3001: The Final Odissey"
             :author  "Arthur C. Clarke"
             :year    1997
             :details {:tags       ["science fiction" "evolution" "artificial intelligence"]
                       :characters ["Francis Poole"]}}
      i4    {:name   "Animal Farm"
             :author "George Orwell"
             :year   1945}
      books [i0 i1 i2 i3 i4]
      ]

  (after-setup!
    #(do
      (far/batch-write-item *client-opts*
                            {book-table {:delete (mapv (fn [m] (select-keys m [:author :name])) books)}})
      (far/batch-write-item *client-opts* {book-table {:put books}})
      ))

  ;; Books are returned ordered by the sort key
  (expect
    [i4 i0] (far/query *client-opts* book-table {:author [:eq "George Orwell"]}))
  (expect
    [i1 i2 i3] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}))

  ;; We can get only the books starting with a string
  (expect
    [i1 i2] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]
                                                 :name   [:begins-with "20"]}))

  ;; We can request only some attributes on the return list
  (expect
    (mapv #(select-keys % [:name :year]) [i1 i2])
    (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]
                                         :name   [:begins-with "20"]}
               {:return [:name :year]}))

  ;; Test query filters
  (expect
    [i2 i3] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                       {:query-filter {:year [:gt 1980]}}))

  (expect
    [i4] (far/query *client-opts* book-table {:author [:eq "George Orwell"]}
                    {:query-filter {:year [:le 1945]}}))

  ;; Test query filter expressions
  (expect
    [i2] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                    {:filter         "size(details.characters) >= :cnt"
                     :expr-attr-vals {":cnt" 4}}))
  (expect
    [i1 i3] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                       {:filter         "size(details.characters) < :cnt"
                        :expr-attr-vals {":cnt" 4}}))
  (expect
    [i1 i3] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                       {:filter         "size(details.characters) < :cnt"
                        :expr-attr-vals {":cnt" 4}}))

  ;; Test expression attribute names
  ;; We cannot combine query-filter and filter expressions, it's either-or
  (expect
    [i1] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                    {:filter          "size(details.characters) < :cnt and #y < :year"
                     :expr-attr-names {"#y" "year"}
                     :expr-attr-vals  {":cnt" 4 ":year" 1990}}))

  ;; Test that we can use expression attribute names even when going for a nested expression
  (expect
    [i1] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                    {:filter          "size(#d.characters) < :cnt and #y < :year"
                     :expr-attr-names {"#y" "year"
                                       "#d" "details"}
                     :expr-attr-vals  {":cnt" 4 ":year" 1990}}))
  (expect
    [i1] (far/query *client-opts* book-table {:author [:eq "Arthur C. Clarke"]}
                    {:filter          "size(#d.#c) < :cnt and #y < :year"
                     :expr-attr-names {"#y" "year"
                                       "#d" "details"
                                       "#c" "characters"}
                     :expr-attr-vals  {":cnt" 4 ":year" 1990}}))

  )


;;;; range queries
(let [j0 {:title "One" :number 0}
      j1 {:title "One" :number 1}
      k0 {:title "Two" :number 0}
      k1 {:title "Two" :number 1}
      k2 {:title "Two" :number 2}
      k3 {:title "Two" :number 3}
      ]

  (after-setup!
    #(far/batch-write-item *client-opts* {range-table {:put [j0 j1 k0 k1 k2 k3]}}))

  (expect ; Query, normal ordering
    [j0 j1] (far/query *client-opts* range-table {:title [:eq "One"]}))

  (expect ; Query, reverse ordering
    [j1 j0] (far/query *client-opts* range-table {:title [:eq "One"]}
              {:order :desc}))

  (expect ; Query with :limit
    [j0] (far/query *client-opts* range-table {:title [:eq "One"]}
           {:limit 1 :span-reqs {:max 1}}))

  (expect ; Query, with range
    [k1 k2] (far/query *client-opts* range-table {:title  [:eq "Two"]
                                                  :number [:between [1 2]]}))

  ;; Verify the :le lt :ge :gt options
  (expect
    [k0 k1 k2] (far/query *client-opts* range-table {:title  [:eq "Two"]
                                                     :number [:lt 3]}))

  (expect
    [k0 k1 k2] (far/query *client-opts* range-table {:title  [:eq "Two"]
                                                     :number [:le 2]}))

  (expect
    [k2 k3] (far/query *client-opts* range-table {:title  [:eq "Two"]
                                                  :number [:gt 1]}))

  (expect
    [k1 k2 k3] (far/query *client-opts* range-table {:title  [:eq "Two"]
                                                     :number [:ge 1]}))

  (expect
    [k1 k2] (far/query *client-opts* range-table {:title  [:eq "Two"]
                                                  :number [:ge 1]}
                       {:limit 2 :span-reqs {:max 1}}))

  )


(expect-let ; Serialization
 ;; Dissoc'ing :bytes, :throwable, :ex-info, and :exception because Object#equals()
 ;; is reference-based and not structural. `expect` falls back to Java equality,
 ;; and so will fail when presented with different Java objects that don't themselves
 ;; implement #equals() - such as arrays and Exceptions - despite having identical data.
 [data ;; nippy/stress-data-comparable ; Awaiting Nippy v2.6
  (dissoc nippy/stress-data :bytes :throwable :exception :ex-info)]
 {:id 10 :nippy-data data}
 (do (far/put-item *client-opts* ttable {:id 10 :nippy-data (far/freeze data)})
     (far/get-item *client-opts* ttable {:id 10})))

(expect-let ; "Unserialized" bytes
 [data (byte-array (mapv byte [0 1 2]))]
 #(encore/ba= data %)
 (do (far/put-item *client-opts* ttable {:id 11 :ba-data data})
     (:ba-data (far/get-item *client-opts* ttable {:id 11}))))

(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}
      i2 {:id 2 :name "baz"}]

  (expect ; Throw for bad conds
   #=(far/ex :conditional-check-failed)
   (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
       (far/put-item *client-opts* ttable i1 {:expected {:id false}})))

  ;;; Proceed for good conds
  (expect nil? (far/put-item *client-opts* ttable i1 {:expected {:id 1}}))
  (expect nil? (far/put-item *client-opts* ttable i2 {:expected {:id :not-exists}}))
  (expect nil? (far/put-item *client-opts* ttable i2 {:expected {:id 2
                                                               :dummy :not-exists}})))

;; (expect (interaction (println anything&)) (println 5))
;; (expect (interaction (println Long))      (println 5))

;;; Test AWSCredentialProvider
(when-let [endpoint (:endpoint *client-opts*)]
  (let [i0 {:id 0 :name "foo"}
        i1 {:id 1 :name "bar"}
        i2 {:id 2 :name "baz"}
        creds    (BasicAWSCredentials. (:access-key *client-opts*)
                   (:secret-key *client-opts*))
        provider (StaticCredentialsProvider. creds)]

    (binding [*client-opts* {:provider provider
                             :endpoint endpoint}]

      (expect ; Batch put
        [i0 i1 nil]
        (do
          (far/batch-write-item *client-opts*
            {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})
          ;;
          (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
          [(far/get-item *client-opts* ttable {:id  0})
           (far/get-item *client-opts* ttable {:id  1})
           (far/get-item *client-opts* ttable {:id -1})])))))

;;; Test `list-tables` lazy sequence
;; Creates a _large_ number of tables so only run locally
(when-let [endpoint (:endpoint *client-opts*)]
  (when (.contains ^String endpoint "localhost")
    (expect
     (let [ ;; Generate > 100 tables to exceed the batch size limit:
           tables (map #(keyword (str "test_" %)) (range 102))]
       (doseq [table tables]
         (far/ensure-table *client-opts* table [:id :n]
                           {:throughput  {:read 1 :write 1}
                            :block?      true}))
       (let [table-count (count (far/list-tables *client-opts*))]
         (doseq [table tables]
           (far/delete-table *client-opts* table))
         (> table-count 100))))

    (let [update-t :faraday.tests.update-table]
      (after-setup!
       #(do
          (when (far/describe-table *client-opts* update-t)
            (far/delete-table *client-opts* update-t))
          (far/create-table
           *client-opts* update-t [:id :n]
           {:throughput {:read 1 :write 1} :block? true})))

      (expect
       {:read 2 :write 2}
       (-> (far/update-table *client-opts* update-t {:read 2 :write 2})
           deref
           :throughput
           (select-keys #{:read :write}))))))

(expect {:b [{:a "b"}], :f false, :g "    "}
  (far/remove-empty-attr-vals
    {:b [{:a "b" :c [[]] :d #{}}, {}] :a nil :empt-str "" :e #{""} :f false :g "    "}))
