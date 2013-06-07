(ns faraday.tests.main
  (:use     [clojure.test])
  (:require [taoensso.faraday :as far]
            [taoensso.nippy   :as nippy]))

;; TODO LOTS of tests still outstanding, PRs very, very welcome!!

(def creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
            :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def ttable :faraday.tests.main)

(use-fixtures :each
  (fn [f]
    (far/ensure-table creds
      {:name        ttable
       :hash-keydef {:name :id :type :n}
       :throughput  {:read 1 :write 1}}) ; Free tier: {:read 10 :write 10}
    (f)
    (far/batch-write-item creds {ttable {:delete [{:id 0} {:id 1} {:id 2}]}})
    (comment (far/delete-ttable creds ttable))))

(deftest batch-simple
  (let [i0 {:id 0 :name "foo"}
        i1 {:id 1 :name "bar"}]

    (far/batch-write-item creds {ttable {:put [i0 i1]}})

    (is (and (= (far/get-item creds ttable {:id 0}) i0)
             (= (far/get-item creds ttable {:id 1}) i1))
        "batch-write-item :put works")

    (let [r1 (far/batch-get-item creds
               {ttable {:prim-kvs    {:id [0 1]}
                       ;; :attrs    [] ; All attrs by default
                       :consistent? true}})

          r2 (far/batch-get-item creds
               {ttable {:prim-kvs    {:id [0 1]}
                       :attrs       [:id]
                       :consistent? true}})]

      (is (and (= (->> r1 ttable set) (set [i0 i1]))
               (= (->> r2 ttable set) (set [(dissoc i0 :name)
                                                  (dissoc i1 :name)])))
          "batch-write-item :put works with :attrs option"))

    (far/batch-write-item creds {ttable {:delete [{:id 0} {:id 1}]}})
    (is (and (not (far/get-item creds ttable {:id 0}))
             (not (far/get-item creds ttable {:id 1})))
        "batch-write-item :delete works")))

(deftest serialization
  (let [data (dissoc nippy/stress-data :bytes)]
    (far/put-item creds ttable {:id 10 :nippy-data (far/freeze data)})
    (is (= (far/get-item creds ttable {:id 10}) {:id 10 :nippy-data data})
        "Nippy (serialized) stress data survives round-trip")))

(deftest conditional-put
  (let [i0 {:id 0 :name "foo"}
        i1 {:id 1 :name "bar"}
        i2 {:id 2 :name "baz"}]

    (far/batch-write-item creds {ttable {:put [i0 i1]}})

    (is (thrown? #=(far/ex :conditional-check-failed)
                 (far/put-item creds ttable i1 {:expected {:id false}}))
        "Put with bad conds throws exceptions")

    (is (and (= (far/put-item creds ttable i1 {:expected {:id 1}})        nil)
             (= (far/put-item creds ttable i2 {:expected {:id false}})    nil)
             (= (far/put-item creds ttable i2 {:expected {:id    2
                                                         :dummy false}}) nil))
        "Put with good conds proceed without exceptions")))