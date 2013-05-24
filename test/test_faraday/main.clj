(ns test-faraday.main
  (:use     [clojure.test]
            [taoensso.faraday])
  (:require [taoensso.faraday :as far]) ; TODO
  (:import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException))

(def creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
            :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def table "rotary-dev-test")
(def id    "test-id")
(def attr  "test-attr")

(ensure-table creds {:name       table
                     :hash-key   {:name id :type :s}
                     :throughput {:write 1 :read 1}})

(defn setup-table
  []
  (batch-write-item creds
    [:delete table {id "1"}]
    [:delete table {id "2"}]
    [:delete table {id "3"}]
    [:delete table {id "4"}])

  (batch-write-item creds
    [:put table {id "1" attr "foo"}]
    [:put table {id "2" attr "bar"}]
    [:put table {id "3" attr "baz"}]
    [:put table {id "4" attr "foobar"}]))

(deftest test-batch-simple
  (setup-table)

  (let [result
        (batch-get-item
         creds {table {:key-name "test-id"
                       :keys ["1" "2" "3" "4"]
                       :consistent true}})
        consis
        (batch-get-item
         creds {table {:key-name "test-id"
                       :keys ["1" "2" "3" "4"]
                       :consistent true}})
        attrs
        (batch-get-item
         creds {table {:key-name "test-id"
                       :keys ["1" "2" "3" "4"]
                       :consistent true
                       :attrs [attr]}})

        items  (get-in result [:responses table])
        item-1 (get-item creds table {id "1"})
        item-2 (get-item creds table {id "2"})
        item-3 (get-item creds table {id "3"})
        item-4 (get-item creds table {id "4"})]

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar" (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz" (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    (is (= true (some #(= (% attr) "foo") items)))
    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs [:responses table]))))
    (is (= true (some #(= (% attr) "foobar") items)))

    (batch-write-item creds
      [:delete table {id "1"}]
      [:delete table {id "2"}]
      [:delete table {id "3"}]
      [:delete table {id "4"}])

    (is (= nil (get-item creds table {id "1"})) "batch-write-item :delete failed")
    (is (= nil (get-item creds table {id "2"})) "batch-write-item :delete failed")
    (is (= nil (get-item creds table {id "3"})) "batch-write-item :delete failed")
    (is (= nil (get-item creds table {id "4"})) "batch-write-item :delete failed")))

(deftest conditional-put
  (batch-write-item
   creds
   [:delete table {id "42"}]
   [:delete table {id "9"}]
   [:delete table {id "6"}]
   [:delete table {id "23"}])

  (batch-write-item
   creds
   [:put table {id "42" attr "foo"}]
   [:put table {id "6" attr "foobar"}]
   [:put table {id "9" attr "foobaz"}])

  ;; ;; Should update item 42 to have attr bar
  (put-item creds table {id "42" attr "bar"} :expected {attr "foo"})
  (is (= "bar" ((get-item creds table {id "42"}) attr)))

  ;; Should fail to update item 6
  (is (thrown? ConditionalCheckFailedException
               (put-item creds table {id "6" attr "baz"} :expected {id false})))
  (is (not (= "baz" ((get-item creds table {id "6"}) attr))))

  ;; Should upate item 9 to have attr baz
  (put-item creds table {id "9" attr "baz"} :expected {attr "foobaz"})
  (is (= "baz" ((get-item creds table {id "9"}) attr)))

  ;; Should add item 23
  (put-item creds table {id "23" attr "bar"} :expected {id false})
  (is (not (= nil (get-item creds table {id "23"})))))