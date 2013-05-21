(ns test-faraday.main
  (:use     [clojure.test]
            [taoensso.faraday])
  (:require [taoensso.faraday :as far]) ; TODO
  )

(def creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
            :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def table "rotary-dev-test")
(def id    "test-id")
(def attr  "test-attr")

(ensure-table creds {:name       table
                     :hash-key   {:name id :type :s}
                     :throughput {:write 1 :read 1}})

(deftest test-batch-simple
  (batch-write-item creds
    [:delete table {:hash-key "1"}]
    [:delete table {:hash-key "2"}]
    [:delete table {:hash-key "3"}]
    [:delete table {:hash-key "4"}])

  (batch-write-item creds
    [:put table {id "1" attr "foo"}]
    [:put table {id "2" attr "bar"}]
    [:put table {id "3" attr "baz"}]
    [:put table {id "4" attr "foobar"}])

  (let [;; result (batch-get-item creds table '("1" "2" "3" "4")) ; TODO PR
        consis (batch-get-item creds {
                 table {
                   :consistent true
                   :keys ["1" "2" "3" "4"]}})
        attrs  (batch-get-item creds {
                 table {
                   :consistent true
                   :attrs [attr]
                   :keys ["1" "2" "3" "4"]}})
        ;; items  (get-in result [:responses table :items]) ; TODO PR
        item-1 (get-item creds table "1")
        item-2 (get-item creds table "2")
        item-3 (get-item creds table "3")
        item-4 (get-item creds table "4")]

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar" (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz" (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    ;; (is (= true (some #(= (% attr) "foo") items))) ; TODO PR
    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table :items]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs [:responses table :items]))))
    ;; (is (= true (some #(= (% attr) "foobar") items))) ; TODO PR

    (batch-write-item creds
      [:delete table {:hash-key "1"}]
      [:delete table {:hash-key "2"}]
      [:delete table {:hash-key "3"}]
      [:delete table {:hash-key "4"}])

    (is (= nil (get-item creds table "1")) "batch-write-item :delete failed")
    (is (= nil (get-item creds table "2")) "batch-write-item :delete failed")
    (is (= nil (get-item creds table "3")) "batch-write-item :delete failed")
    (is (= nil (get-item creds table "4")) "batch-write-item :delete failed")))
