(ns taoensso.faraday.tests.main
  (:require [expectations           :as test :refer :all]
            [taoensso.faraday       :as far]
            [taoensso.faraday.utils :as utils]
            [taoensso.nippy         :as nippy]))

;; TODO LOTS of tests still outstanding, PRs very, very welcome!!

;;;; Config & setup

(defonce creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
                :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def ttable :faraday.tests.main)

(defn- before-run {:expectations-options :before-run} []
  (println "Setting up testing environment...")
  (far/ensure-table creds ttable [:id :n]
    {:throughput  {:read 1 :write 1}
     :block?      true})
  (println "Ready to roll..."))

(defn- after-run {:expectations-options :after-run} [])

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
 ;; Dissoc'ing :bytes, :throwable, :ex-info, and :exception because Object#equals()
 ;; is reference-based and not structural. `expect` falls back to Java equality,
 ;; and so will fail when presented with different Java objects that don't themselves
 ;; implement #equals() - such as arrays and Exceptions - despite having identical data.
 [data (dissoc nippy/stress-data :throwable :ex-info :exception)]
 {:id 10 :nippy-data data}
 (do (far/put-item creds ttable {:id 10 :nippy-data (far/freeze data)})
     (far/get-item creds ttable {:id 10})))

(expect-let ; "Unserialized" bytes
 [data (byte-array (mapv byte [0 1 2]))]
 #(utils/ba= data %)
 (do (far/put-item creds ttable {:id 11 :ba-data data})
     (:ba-data (far/get-item creds ttable {:id 11}))))

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
