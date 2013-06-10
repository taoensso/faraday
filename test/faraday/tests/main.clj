(ns faraday.tests.main
  (:require [expectations     :as test :refer :all]
            [taoensso.faraday :as far]
            [taoensso.nippy   :as nippy]))

;; TODO LOTS of tests still outstanding, PRs very, very welcome!!

(defonce creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
                :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def ttable :faraday.tests.main)

(defn- setup {:expectations-options :before-run} []
  (println "Setting up testing environment...")

  (when (far/ensure-table creds
        {:name        ttable
         :hash-keydef {:name :id :type :n}
         :throughput  {:read 1 :write 1}}) ; Free tier: {:read 10 :write 10}
    (println "Sleeping 90s to allow for table creation...")
    (Thread/sleep 45000)
    (println "45s left...")
    (Thread/sleep 45000))

  (far/batch-write-item creds {ttable {:delete [{:id 0} {:id 1} {:id 2}]}}))

(comment (far/delete-table creds ttable))

(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}]

  (expect ; Batch put
   [i0 i1 nil] (do (far/batch-write-item creds {ttable {:put [i0 i1]}})
                   [(far/get-item creds ttable {:id  0})
                    (far/get-item creds ttable {:id  1})
                    (far/get-item creds ttable {:id -1})]))

  (expect ; Batch get
   (set [i0 i1]) (->> (far/batch-get-item creds
                        {ttable {:prim-kvs    {:id [0 1]}
                                 :consistent? true}})
                      ttable set))

  (expect ; Batch get, with :attrs
   (set [(dissoc i0 :name) (dissoc i1 :name)])
   (->> (far/batch-get-item creds
          {ttable {:prim-kvs    {:id [0 1]}
                   :attrs       [:id]
                   :consistent? true}})
        ttable set))

  (expect ; Batch delete
   [nil nil] (do (far/batch-write-item creds {ttable {:delete [{:id 0} {:id 1}]}})
                 [(far/get-item creds ttable {:id 0})
                  (far/get-item creds ttable {:id 1})])))

(expect-let ; Serialization
 [data (dissoc nippy/stress-data :bytes)]
 {:id 10 :nippy-data data}
 (do (far/put-item creds ttable {:id 10 :nippy-data (far/freeze data)})
     (far/get-item creds ttable {:id 10})))

(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}
      i2 {:id 2 :name "baz"}]

  (expect ; Throw for bad conds
   #=(far/ex :conditional-check-failed)
   (do (far/batch-write-item creds {ttable {:put [i0 i1]}})
       (far/put-item creds ttable i1 {:expected {:id false}})))

  ;;; Proceed for good conds
  (expect nil? (far/put-item creds ttable i1 {:expected {:id 1}}))
  (expect nil? (far/put-item creds ttable i2 {:expected {:id false}}))
  (expect nil? (far/put-item creds ttable i2 {:expected {:id 2
                                                         :dummy false}})))

;; (expect (interaction (println anything&)) (println 5))
;; (expect (interaction (println Long))      (println 5))