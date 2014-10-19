(ns taoensso.faraday.tests.requests
  (:require [expectations     :as test :refer :all]
            [taoensso.faraday.requests :as reqs])
  (:import [com.amazonaws.services.dynamodbv2.model
            DescribeTableRequest
            ProvisionedThroughput
            KeyType
            CreateTableRequest
            KeySchemaElement
            LocalSecondaryIndex
            GlobalSecondaryIndex
            Projection
            ProjectionType
            UpdateTableRequest
            GetItemRequest
            AttributeValue
            PutItemRequest
            ReturnValue
            ExpectedAttributeValue
            UpdateItemRequest
            DeleteItemRequest
            BatchGetItemRequest
            BatchWriteItemRequest
            AttributeValueUpdate
            AttributeAction
            KeysAndAttributes
            WriteRequest
            PutRequest
            DeleteRequest]))

(expect
 "describe-table-name"
 (.getTableName ^DescribeTableRequest
                (reqs/describe-table-request :describe-table-name)))

(let [req ^CreateTableRequest
      (reqs/create-table-request
       :create-table-name [:hash-keydef :n]
       {:range-keydef [:range-keydef :n]
        :throughput {:read 5 :write 10}
        :lsindexes [{:name "local-secondary"
                     :range-keydef [:ls-range-keydef :n]
                     :projection [:ls-projection]}]
        :gsindexes [{:name "global-secondary"
                     :hash-keydef [:gs-hash-keydef :n]
                     :range-keydef [:gs-range-keydef :n]
                     :projection :keys-only
                     :throughput {:read 10 :write 2}}]})]
  (expect "create-table-name" (.getTableName req))

  (expect
   (ProvisionedThroughput. 5 10)
   (.getProvisionedThroughput req))

  (expect
   #{(KeySchemaElement. "hash-keydef" KeyType/HASH)
     (KeySchemaElement. "range-keydef" KeyType/RANGE)}
   (into #{} (.getKeySchema req)))

  (let [[^LocalSecondaryIndex lsindex & rest] (.getLocalSecondaryIndexes req)]
    (expect nil? rest)
    (expect "local-secondary" (.getIndexName lsindex))
    (expect
     (doto (Projection.)
       (.setProjectionType ProjectionType/INCLUDE)
       (.setNonKeyAttributes ["ls-projection"]))
     (.getProjection lsindex))
    (expect
     #{(KeySchemaElement. "hash-keydef" KeyType/HASH)
       (KeySchemaElement. "ls-range-keydef" KeyType/RANGE)}
     (into #{} (.getKeySchema lsindex))))

  (let [[^GlobalSecondaryIndex gsindex & rest] (.getGlobalSecondaryIndexes req)]
    (expect nil? rest)
    (expect "global-secondary" (.getIndexName gsindex))
    (expect
     (doto (Projection.)
       (.setProjectionType ProjectionType/KEYS_ONLY))
     (.getProjection gsindex))
    (expect
     #{(KeySchemaElement. "gs-range-keydef" KeyType/RANGE)
       (KeySchemaElement. "gs-hash-keydef" KeyType/HASH)}
     (into #{} (.getKeySchema gsindex)))
    (expect
     (ProvisionedThroughput. 10 2)
     (.getProvisionedThroughput gsindex))))

(expect
 "update-table"
 (.getTableName
  ^UpdateTableRequest
  (reqs/update-table-request :update-table {:read 1 :write 1})))

(expect
 (ProvisionedThroughput. 15 7)
 (.getProvisionedThroughput
  ^UpdateTableRequest
  (reqs/update-table-request :update-table {:read 15 :write 7})))

(expect
 "get-item"
 (.getTableName
  ^GetItemRequest
  (reqs/get-item-request :get-item {:x 1})))

(expect
 not
 (.getConsistentRead
  ^GetItemRequest
  (reqs/get-item-request :get-item {:x 1} {:consistent? false})))

(expect
 true?
 (.getConsistentRead
  ^GetItemRequest
  (reqs/get-item-request :get-item {:x 1} {:consistent? true})))

(let [req ^GetItemRequest (reqs/get-item-request
                           :get-item-table-name
                           {:hash "y" :range 2}
                           {:attrs [:j1 :j2]})]
  (expect {"hash" (AttributeValue. "y")
           "range" (doto (AttributeValue.)
                     (.setN "2"))}
          (.getKey req))

  (expect #{"j1" "j2"}
          (into #{} (.getAttributesToGet req))))

(expect
 "put-item-table-name"
 (.getTableName
  ^PutItemRequest
  (reqs/put-item-request :put-item-table-name {:x 1})))

(let [req
      ^PutItemRequest (reqs/put-item-request
                       :put-item-table-name
                       {:c1 "hey" :c2 1}
                       {:return :updated-new
                        :expected {:e1 "expected value"
                                   :e2 false}})]
  (expect (str ReturnValue/UPDATED_NEW) (.getReturnValues req))
  (expect {"c1" (AttributeValue. "hey")
           "c2" (doto (AttributeValue.)
                  (.setN "1"))}
          (.getItem req))
  (expect {"e1" (doto (ExpectedAttributeValue.)
                  (.setValue (AttributeValue. "expected value")))
           "e2" (ExpectedAttributeValue. false)}
          (.getExpected req)))

(let [req
      ^UpdateItemRequest (reqs/update-item-request
                          :update-item
                          {:x 1}
                          {:y [:put 2]
                           :z [:add "xyz"]
                           :a [:delete]}
                          {:expected {:e1 "expected!"}
                           :return :updated-old})]
  
  (expect "update-item" (.getTableName req))
  (expect {"x" (doto (AttributeValue.)
                 (.setN "1"))}
          (.getKey req))
  (expect {"y" (AttributeValueUpdate.
                (doto (AttributeValue.)
                  (.setN "2"))
                (str AttributeAction/PUT))
           "z" (AttributeValueUpdate.
                (AttributeValue. "xyz")
                AttributeAction/ADD)
           "a" (AttributeValueUpdate. nil AttributeAction/DELETE)}
          (.getAttributeUpdates req))
  (expect (str ReturnValue/UPDATED_OLD) (.getReturnValues req))
  (expect {"e1" (doto (ExpectedAttributeValue.)
                  (.setValue (AttributeValue. "expected!")))}
          (.getExpected req)))

(let [req
      ^DeleteItemRequest (reqs/delete-item-request
                          :delete-item
                          {:k1 "val" :r1 -3}
                          {:return :all-new
                           :expected {:e1 1}})]
  
  (expect "delete-item" (.getTableName req))
  (expect {"k1" (AttributeValue. "val")
           "r1" (doto (AttributeValue.)
                  (.setN "-3"))}
          (.getKey req))
  (expect {"e1" (doto (ExpectedAttributeValue.)
                  (.setValue (doto (AttributeValue.)
                               (.setN "1"))))}
          (.getExpected req))
  (expect (str ReturnValue/ALL_NEW) (.getReturnValues req)))

(let [req
      ^BatchGetItemRequest
      (reqs/batch-get-item-request
       false
       (reqs/batch-request-items
        {:t1 {:prim-kvs {:t1-k1 -10}
              :attrs [:some-other-guy]}
         :t2 {:prim-kvs {:t2-k1 ["x" "y" "z"]}}}))]
  (expect
   {"t1" (doto (KeysAndAttributes.)
           (.setKeys [{"t1-k1" (doto (AttributeValue.)
                                 (.setN "-10"))}])
           (.setAttributesToGet ["some-other-guy"]))
    "t2" (doto (KeysAndAttributes.)
           (.setKeys [{"t2-k1" (AttributeValue. "x")}
                      {"t2-k1" (AttributeValue. "y")}
                      {"t2-k1" (AttributeValue. "z")}]))}
   (.getRequestItems req)))

(let [req
      ^BatchWriteItemRequest
      (reqs/batch-write-item-request
       false
       {"t1" (map
              #(reqs/write-request :put %)
              (reqs/attr-multi-vs {:k ["x" "y"]}))
        "t2" (map
              #(reqs/write-request :delete %)
              (reqs/attr-multi-vs {:k [0]}))})]
  (expect
   {"t1" [(WriteRequest.
           (PutRequest. {"k" (AttributeValue. "x")}))
          (WriteRequest.
           (PutRequest. {"k" (AttributeValue. "y")}))]
    "t2" [(WriteRequest.
           (DeleteRequest. {"k" (doto (AttributeValue.)
                                  (.setN "0"))}))]}
   (.getRequestItems req)))
