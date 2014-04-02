(ns taoensso.faraday.tests.main
  (:require [expectations           :as test :refer :all]
            [taoensso.encore        :as encore]
            [taoensso.faraday       :as far]
            [taoensso.nippy         :as nippy])
  (:import [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.internal StaticCredentialsProvider]))

;; TODO LOTS of tests still outstanding, PRs very, very welcome!!

(comment (test/run-tests '[taoensso.faraday.tests.main]))

;;;; Config & setup

(def ^:dynamic *client-opts*
  (merge {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
          :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")}
         (when-let [endpoint (get (System/getenv) "AWS_DYNAMODB_ENDPOINT")] {:endpoint endpoint})))

(def ttable :faraday.tests.main)

(defn- before-run {:expectations-options :before-run} []
  (println "Setting up testing environment...")
  (far/ensure-table *client-opts* ttable [:id :n]
    {:throughput  {:read 1 :write 1}
     :block?      true})
  (println "Ready to roll..."))

(defn- after-run {:expectations-options :after-run} [])

(comment (far/delete-table *client-opts* ttable))

;;;; Basic API

(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}]

  (far/batch-write-item *client-opts* {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})

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
  (expect nil? (far/put-item *client-opts* ttable i2 {:expected {:id false}}))
  (expect nil? (far/put-item *client-opts* ttable i2 {:expected {:id 2
                                                               :dummy false}})))

;; (expect (interaction (println anything&)) (println 5))
;; (expect (interaction (println Long))      (println 5))


;; Test AWSCredentialProvider
(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}
      i2 {:id 2 :name "baz"}
      creds        (BasicAWSCredentials. (:access-key *client-opts*) (:secret-key *client-opts*))
      provider     (StaticCredentialsProvider. creds)
      endpoint     (get (System/getenv) "AWS_DYNAMODB_ENDPOINT")]
  (binding [*client-opts* (merge {:provider provider} (when endpoint {:endpoint endpoint}))]
    (far/batch-write-item *client-opts* {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})
    (expect                             ; Batch put
     [i0 i1 nil] (do (far/batch-write-item *client-opts* {ttable {:put [i0 i1]}})
                     [(far/get-item *client-opts* ttable {:id  0})
                      (far/get-item *client-opts* ttable {:id  1})
                      (far/get-item *client-opts* ttable {:id -1})]))

    ;; test list-tables lazy sequence
    ;; generate more than 100 tables to hit the batch size limit
    ;; of list-tables
    ;; since this creates a large number of tables, only run this
    ;; when the endpoint matches localhost
    (when (.contains endpoint "localhost")
      (let [tables (map keyword (map #(str "test_" %) (range 102)))]
        (doseq [table tables] (far/ensure-table *client-opts* table
                                                [:id :n]
                                                {:throughput  {:read 1 :write 1}
                                                 :block?      true}))
        (expect true (> (count (far/list-tables *client-opts*)) 100))
        (doseq [table tables] (far/delete-table *client-opts* table))))))
