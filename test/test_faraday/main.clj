(ns test-faraday.main
  (:use     [clojure.test])
  (:require [taoensso.faraday :as far]
            [taoensso.nippy   :as nippy]))

;; TODO These tests are pretty messy. Also LOTS of tests still outstanding.
;; Test PRs very, very welcome!

(def creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
            :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def table :test-faraday.main)
(def id    :test-id)
(def attr  :test-attr)

(defn setup-table!
  []
  (far/ensure-table creds
    {:name       table
     :hash-key   {:name id :type :s}
     :throughput {:read 1 :write 1}})

  (far/batch-write-item creds
    {table {:delete [{id "1"} {id "2"} {id "3"} {id "4"}]}})

  (far/batch-write-item creds
    {table {:put [{id "1" attr "foo"}
                  {id "2" attr "bar"}
                  {id "3" attr "baz"}
                  {id "4" attr "foobar"}]}}))

(deftest test-batch-simple
  (setup-table!)

  (let [result
        (far/batch-get-item creds
          {table {:prim-kvs {id ["1" "2" "3" "4"]}
                  :consistent? true ; TODO Should be false?
                  }})
        consis
        (far/batch-get-item creds
          {table {:prim-kvs {id ["1" "2" "3" "4"]}
                  :consistent? true}})
        attrs
        (far/batch-get-item creds
          {table {:prim-kvs {id ["1" "2" "3" "4"]}
                  :attrs    [attr]
                  :consistent? true}})

        items  (get-in result [:responses table])
        item-1 (far/get-item creds table {id "1"})
        item-2 (far/get-item creds table {id "2"})
        item-3 (far/get-item creds table {id "3"})
        item-4 (far/get-item creds table {id "4"})]

    (is (= "foo"    (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar"    (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz"    (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    (is (= true (some #(= (% attr) "foo") items)))
    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs  [:responses table]))))
    (is (= true (some #(= (% attr) "foobar") items)))

    (far/batch-write-item creds
      {table {:delete [{id "1"} {id "2"} {id "3"} {id "4"}]}})

    (is (= nil (far/get-item creds table {id "1"})) "batch-write-item :delete failed")
    (is (= nil (far/get-item creds table {id "2"})) "batch-write-item :delete failed")
    (is (= nil (far/get-item creds table {id "3"})) "batch-write-item :delete failed")
    (is (= nil (far/get-item creds table {id "4"})) "batch-write-item :delete failed")))

(deftest conditional-put
  (far/batch-write-item creds
    {table {:delete [{id "42"} {id "9"} {id "6"} {id "23"}]}})

  (far/batch-write-item creds
    {table {:put [{id "42" attr "foo"}
                  {id "6"  attr "foobar"}
                  {id "9"  attr "foobaz"}]}})

  ;; Should update item 42 to have attr bar
  (far/put-item creds table {id "42" attr "bar"} {:expected {attr "foo"}})
  (is (= "bar" ((far/get-item creds table {id "42"}) attr)))

  ;; Should fail to update item 6
  (is (thrown? #=(far/ex :conditional-check-failed)
        (far/put-item creds table {id "6" attr "baz"} {:expected {id false}})))
  (is (not= "baz" ((far/get-item creds table {id "6"}) attr)))

  ;; Should upate item 9 to have attr baz
  (far/put-item creds table {id "9" attr "baz"} {:expected {attr "foobaz"}})
  (is (= "baz" ((far/get-item creds table {id "9"}) attr)))

  ;; Should add item 23
  (far/put-item creds table {id "23" attr "bar"} {:expected {id false}})
  (is (not= nil (far/get-item creds table {id "23"}))))

(deftest serialization
  (let [data (dissoc nippy/stress-data :bytes)]
    (far/put-item creds table {id "nippy" attr (far/freeze data)})
    (is (= (far/get-item creds table {id "nippy"}) {id "nippy" attr data})
        "Nippy (serialized) stress data survives round-trip")))