(ns faraday.tests.main
  (:require [expectations     :as test :refer :all]
            [taoensso.faraday :as far]
            [taoensso.carmine :as car]
            [taoensso.tundra  :as tundra]
            [taoensso.timbre  :as timbre]
            [taoensso.nippy   :as nippy]))

;; TODO LOTS of tests still outstanding, PRs very, very welcome!!

;;;; Config & setup

(defonce creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
                :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def ttable :faraday.tests.main)

(defn- setup {:expectations-options :before-run} []
  (println "Setting up testing environment...")

  (when (or (far/ensure-table creds
              {:name        ttable
               :hash-keydef {:name :id :type :n}
               :throughput  {:read 1 :write 1}})
            (tundra/ensure-table creds {:read 1 :write 1}))
    (println "Sleeping 90s for table creation (only need to do this once!)...")
    (Thread/sleep 45000)
    (println "45s left...")
    (Thread/sleep 45000)
    (println "Ready to roll...")))

(comment (far/delete-table creds ttable))

;;;; Basic API

(let [i0 {:id 0 :name "foo"}
      i1 {:id 1 :name "bar"}]

  (far/batch-write-item creds {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})

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
   [nil nil] (do (far/batch-write-item creds {ttable {:delete {:id [0 1]}}})
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

;;;; Tundra

(defmacro wcar [& body] `(car/with-conn nil nil ~@body))

(let [ttable  tundra/ttable
      tworker :test-worker
      tkey (partial car/kname "carmine" "tundra" "temp" "test")
      [k1 k2 k3 k4 :as ks] (mapv tkey ["k1 k2 k3 k4"])]

  (wcar (apply car/del ks))
  (far/batch-write-item creds {ttable {:delete [{:worker   (name tworker)
                                                 :redis-key k1}]}})

  ;; TODO
  ;;(far/scan creds tundra/ttable {:attr-conds {:worker [:eq ["default"]]}})
  )