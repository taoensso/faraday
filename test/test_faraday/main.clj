(ns test-faraday.main
  (:use [clojure.test]
        [taoensso.faraday])
  ;; (:require [taoensso.faraday :as far]) ; TODO
  )

(def cred {:access-key "",
           :secret-key ""})

(def table "")
(def id "")
(def attr "")

(deftest test-batch-simple
  (batch-write-item cred
    [:delete table {:hash-key "1"}]
    [:delete table {:hash-key "2"}]
    [:delete table {:hash-key "3"}]
    [:delete table {:hash-key "4"}])

  (batch-write-item cred
    [:put table {id "1" attr "foo"}]
    [:put table {id "2" attr "bar"}]
    [:put table {id "3" attr "baz"}]
    [:put table {id "4" attr "foobar"}])

  (let [result (batch-get-item cred table '("1" "2" "3" "4"))
        consis (batch-get-item cred {
                 table {
                   :consistent true
                   :keys ["1" "2" "3" "4"]}})
        attrs  (batch-get-item cred {
                 table {
                   :consistent true
                   :attrs [attr]
                   :keys ["1" "2" "3" "4"]}})
        items  (get-in result [:responses table :items])
        item-1 (get-item cred table "1")
        item-2 (get-item cred table "2")
        item-3 (get-item cred table "3")
        item-4 (get-item cred table "4")]

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar" (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz" (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    (is (= true (some #(= (% attr) "foo") items)))
    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table :items]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs [:responses table :items]))))
    (is (= true (some #(= (% attr) "foobar") items)))

    (batch-write-item cred
      [:delete table {:hash-key "1"}]
      [:delete table {:hash-key "2"}]
      [:delete table {:hash-key "3"}]
      [:delete table {:hash-key "4"}])

    (is (= nil (get-item cred table "1")) "batch-write-item :delete failed")
    (is (= nil (get-item cred table "2")) "batch-write-item :delete failed")
    (is (= nil (get-item cred table "3")) "batch-write-item :delete failed")
    (is (= nil (get-item cred table "4")) "batch-write-item :delete failed")))