(ns taoensso.faraday2.tests.requests
  (:require [clojure.test :refer :all]
            [taoensso.faraday2 :as far]
            [taoensso.faraday2.utils :as utils])
  (:import [java.util Collection]
           [software.amazon.awssdk.services.dynamodb.model AttributeAction
                                                           AttributeValue
                                                           AttributeValueUpdate
                                                           BatchGetItemRequest
                                                           BatchWriteItemRequest
                                                           ComparisonOperator
                                                           Condition
                                                           CreateTableRequest
                                                           DeleteItemRequest
                                                           DeleteRequest
                                                           DescribeStreamRequest
                                                           DescribeTableRequest
                                                           DescribeTimeToLiveRequest
                                                           ExpectedAttributeValue
                                                           GetItemRequest
                                                           GetRecordsRequest
                                                           GetShardIteratorRequest
                                                           GlobalSecondaryIndex
                                                           GlobalSecondaryIndexUpdate
                                                           KeySchemaElement
                                                           KeyType
                                                           KeysAndAttributes
                                                           ListStreamsRequest
                                                           LocalSecondaryIndex
                                                           Projection
                                                           ProjectionType
                                                           ProvisionedThroughput
                                                           PutItemRequest
                                                           PutRequest
                                                           QueryRequest
                                                           ReturnValue
                                                           ScanRequest
                                                           Select
                                                           ShardIteratorType
                                                           StreamViewType
                                                           UpdateItemRequest
                                                           UpdateTableRequest
                                                           UpdateTimeToLiveRequest
                                                           WriteRequest]))

;;;; Private var aliases

(def describe-table-request #'far/describe-table-request)
(def create-table-request #'far/create-table-request)
(def update-table-request #'far/update-table-request)
(def get-item-request #'far/get-item-request)
(def put-item-request #'far/put-item-request)
(def update-item-request #'far/update-item-request)
(def delete-item-request #'far/delete-item-request)
(def batch-get-item-request #'far/batch-get-item-request)
(def batch-request-items #'far/batch-request-items)
(def batch-write-item-request #'far/batch-write-item-request)
(def attr-multi-vs #'far/attr-multi-vs)
(def query-request #'far/query-request)
(def write-request #'far/write-request)
(def scan-request #'far/scan-request)
(def list-stream-request #'far/list-streams-request)
(def describe-stream-request #'far/describe-stream-request)
(def get-shard-iterator-request #'far/get-shard-iterator-request)
(def get-records-request #'far/get-records-request)
(def describe-ttl-request #'far/describe-ttl-request)
(def update-ttl-request #'far/update-ttl-request)

(deftest describe-table-request-creation
  (is (= "describe-table-name"
         (.tableName ^DescribeTableRequest
                     (describe-table-request :describe-table-name)))))

(deftest create-table-request-creation
  (let [req ^CreateTableRequest
            (create-table-request
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
                           :throughput {:read 10 :write 2}}]
              :stream-spec {:enabled? true
                            :view-type :new-image}})]
    (is (= "create-table-name" (.tableName req)))

    (is (= (-> (ProvisionedThroughput/builder)
               (.readCapacityUnits 5)
               (.writeCapacityUnits 10)
               (.build))
           (.provisionedThroughput req)))

    (is (= #{(-> (KeySchemaElement/builder) (.attributeName "hash-keydef") (.keyType KeyType/HASH) (.build))
             (-> (KeySchemaElement/builder) (.attributeName "range-keydef") (.keyType KeyType/RANGE) (.build))}
           (into #{} (.keySchema req))))

    (let [[^LocalSecondaryIndex lsindex & rest] (.localSecondaryIndexes req)]
      (is nil? rest)
      (is (= "local-secondary" (.indexName lsindex)))
      (is (= (-> (Projection/builder)
                 (.projectionType ProjectionType/INCLUDE)
                 (.nonKeyAttributes ["ls-projection"])
                 (.build))
             (.projection lsindex)))
      (is (= #{(-> (KeySchemaElement/builder) (.attributeName "hash-keydef") (.keyType KeyType/HASH) (.build))
               (-> (KeySchemaElement/builder) (.attributeName "ls-range-keydef") (.keyType KeyType/RANGE) (.build))}
             (into #{} (.keySchema lsindex)))))

    (let [[^GlobalSecondaryIndex gsindex & rest] (.globalSecondaryIndexes req)]
      (is nil? rest)
      (is (= "global-secondary" (.indexName gsindex)))
      (is (= (-> (Projection/builder)
                 (.projectionType ProjectionType/KEYS_ONLY)
                 (.build))
             (.projection gsindex)))
      (is (= #{(-> (KeySchemaElement/builder) (.attributeName "gs-range-keydef") (.keyType KeyType/RANGE) (.build))
               (-> (KeySchemaElement/builder) (.attributeName "gs-hash-keydef") (.keyType KeyType/HASH) (.build))}
             (into #{} (.keySchema gsindex))))
      (is (= (-> (ProvisionedThroughput/builder)
                 (.readCapacityUnits 10)
                 (.writeCapacityUnits 2)
                 (.build))
             (.provisionedThroughput gsindex))))

    (let [stream-spec (.streamSpecification req)]
      (is true? (.streamEnabled stream-spec))
      (is (= StreamViewType/NEW_IMAGE (.streamViewType stream-spec)))))

  (is (thrown? AssertionError
               (create-table-request
                :create-table-name [:hash-keydef :n]
                {:range-keydef [:range-keydef :n]
                 :billing-mode :pay-per-request
                 :throughput {:read 5 :write 10}})))

  (is (thrown? AssertionError
               (create-table-request
                :create-table-name [:hash-keydef :n]
                {:range-keydef [:range-keydef :n]
                 :billing-mode :pay-per-request
                 :gsindexes [{:name "global-secondary"
                              :hash-keydef [:gs-hash-keydef :n]
                              :range-keydef [:gs-range-keydef :n]
                              :projection :keys-only
                              :throughput {:read 10 :write 2}}]})))

  (let [req ^CreateTableRequest
            (create-table-request
             :create-table-name [:hash-keydef :n]
             {:range-keydef [:range-keydef :n]
              :billing-mode :pay-per-request
              :gsindexes [{:name "global-secondary"
                           :hash-keydef [:gs-hash-keydef :n]
                           :range-keydef [:gs-range-keydef :n]
                           :projection :keys-only}]})]

    (is nil? (.provisionedThroughput req))

    (is (= (utils/enum :pay-per-request) (.toString (.billingMode req))))
    (let [[^GlobalSecondaryIndex gsindex & rest] (.globalSecondaryIndexes req)]
      (is nil? (.provisionedThroughput gsindex))))

  (let [req ^CreateTableRequest
            (create-table-request
             :create-table-name [:hash-keydef :n]
             {:range-keydef [:range-keydef :n]
              :gsindexes [{:name "global-secondary"
                           :hash-keydef [:gs-hash-keydef :n]
                           :range-keydef [:gs-range-keydef :n]
                           :projection :keys-only}]})]

    (is nil? (.provisionedThroughput req))
    (is (= (utils/enum :provisioned) (.toString (.billingMode req))))))

(deftest update-tables-request-creation
  (is (= "update-table"
         (.tableName
          ^UpdateTableRequest
          (update-table-request :update-table {} {:throughput {:read 1 :write 1}}))))

  (is (= (-> (ProvisionedThroughput/builder)
             (.readCapacityUnits 15)
             (.writeCapacityUnits 7)
             (.build))
         (.provisionedThroughput
          ^UpdateTableRequest
          (update-table-request :update-table {} {:throughput {:read 15 :write 7}}))))

  (let [req ^UpdateTableRequest
            (update-table-request
             :update-table
             {}
             {:gsindexes {:name "global-secondary"
                          :operation :create
                          :hash-keydef [:gs-hash-keydef :n]
                          :range-keydef [:gs-range-keydef :n]
                          :projection :keys-only
                          :throughput {:read 10 :write 2}}})]
    (is (= "update-table" (.tableName req)))

    (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.globalSecondaryIndexUpdates req)
          create-action (.create gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (.indexName create-action)))
      (is (= (-> (Projection/builder)
                 (.projectionType ProjectionType/KEYS_ONLY)
                 (.build))
             (.projection create-action)))
      (is (= #{(-> (KeySchemaElement/builder) (.attributeName "gs-range-keydef") (.keyType KeyType/RANGE) (.build))
               (-> (KeySchemaElement/builder) (.attributeName "gs-hash-keydef") (.keyType KeyType/HASH) (.build))}
             (into #{} (.keySchema create-action))))
      (is (= (-> (ProvisionedThroughput/builder)
                 (.readCapacityUnits 10)
                 (.writeCapacityUnits 2)
                 (.build))
             (.provisionedThroughput create-action)))))

  (let [req ^UpdateTableRequest
            (update-table-request
             :update-table
             {}
             {:gsindexes {:name "global-secondary"
                          :operation :update
                          :throughput {:read 4 :write 2}}})]
    (is (= "update-table" (.tableName req)))

    (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.globalSecondaryIndexUpdates req)
          update-action (.update gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (.indexName update-action)))
      (is (= (-> (ProvisionedThroughput/builder)
                 (.readCapacityUnits 4)
                 (.writeCapacityUnits 2)
                 (.build))
             (.provisionedThroughput update-action)))))

  (let [req ^UpdateTableRequest
            (update-table-request
             :update-table
             {}
             {:gsindexes {:name "global-secondary"
                          :operation :delete}})]
    (is (= "update-table" (.tableName req)))

    (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.globalSecondaryIndexUpdates req)
          action (.delete gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (.indexName action)))))

  (let [req ^UpdateTableRequest (update-table-request
                                 :update-table
                                 {}
                                 {:stream-spec {:enabled? false}})
        stream-spec (.streamSpecification req)]
    (is false? (.streamEnabled stream-spec))
    (is nil? (.streamViewType stream-spec)))

  (testing "updating billing mode"
    (is (= (utils/enum :pay-per-request)
           (.toString (.billingMode
                        ^UpdateTableRequest
                        (update-table-request :update-table {} {:billing-mode :pay-per-request})))))

    (is (thrown? AssertionError
                 (update-table-request
                  :update-table
                  {}
                  {:throughput {:read 4 :write 2}
                   :billing-mode :pay-per-request}))))

  (testing "If main table is pay-per-request, then can't specify throughput when adding a GSI"
    (is (thrown? AssertionError
                 (update-table-request
                  :update-table
                  {:billing-mode {:name :pay-per-request}}
                  {:gsindexes {:name "new-global-secondary"
                               :operation :create
                               :hash-keydef [:id :s]
                               :projection :keys-only
                               :throughput {:read 10 :write 9}}})))  )

  (testing "If parent table is pay-per-request, then no need to specify billing mode on new GSIs"
    (let [req ^UpdateTableRequest
              (update-table-request
               :update-table
               {:billing-mode {:name :pay-per-request}}
               {:gsindexes {:name "new-global-secondary"
                            :operation :create
                            :hash-keydef [:id :s]
                            :projection :keys-only}})]
      (is (= "update-table" (.tableName req))))))

(deftest get-item-request-creation
  (is (= "get-item"
         (.tableName
          ^GetItemRequest
          (get-item-request :get-item {:x 1}))))

  (is (not (.consistentRead
            ^GetItemRequest
            (get-item-request :get-item {:x 1} {:consistent? false}))))

  (is
   true?
   (.consistentRead
    ^GetItemRequest
    (get-item-request :get-item {:x 1} {:consistent? true})))

  (let [req ^GetItemRequest (get-item-request
                             :get-item-table-name
                             {:hash "y" :range 2}
                             {:attrs [:j1 :j2]})]
    (is (= {"hash" (-> (AttributeValue/builder) (.s "y") (.build))
            "range" (-> (AttributeValue/builder) (.n "2") (.build))}
           (.key req)))

    (is (= #{"j1" "j2"}
           (into #{} (.attributesToGet req))))))

(deftest put-item-request-creation
  (is (= "put-item-table-name"
         (.tableName
          ^PutItemRequest
          (put-item-request :put-item-table-name {:x 1}))))

  (let [req ^PutItemRequest (put-item-request
                             :put-item-table-name
                             {:c1 "hey" :c2 1}
                             {:return :updated-new
                              :expected {:e1 "expected value"
                                         :e2 false}})]
    (is (= ReturnValue/UPDATED_NEW (.returnValues req)))
    (is (= {"c1" (-> (AttributeValue/builder) (.s "hey") (.build))
            "c2" (-> (AttributeValue/builder) (.n "1") (.build))}
           (.item req)))
    (is (= {"e1" (-> (ExpectedAttributeValue/builder)
                     (.value (-> (AttributeValue/builder) (.s "expected value") ^AttributeValue (.build)))
                     (.build))
            "e2" (-> (ExpectedAttributeValue/builder)
                     (.value (-> (AttributeValue/builder) (.bool false) ^AttributeValue (.build)))
                     (.build))}
           (.expected req)))))

(deftest update-item-request-creation
  (let [req ^UpdateItemRequest (update-item-request
                                :update-item
                                {:x 1}
                                {:update-map {:y [:put 2]
                                              :z [:add "xyz"]
                                              :a [:delete]}
                                 :expected {:e1 "expected!"}
                                 :return :updated-old})]

    (is (= "update-item" (.tableName req)))
    (is (= {"x" (-> (AttributeValue/builder) (.n "1") (.build))}
           (.key req)))
    (is (= {"y" (-> (AttributeValueUpdate/builder)
                    (.value (-> (AttributeValue/builder) (.n "2") ^AttributeValue (.build)))
                    (.action AttributeAction/PUT)
                    (.build))
            "z" (-> (AttributeValueUpdate/builder)
                    (.value (-> (AttributeValue/builder) (.s "xyz") ^AttributeValue (.build)))
                    (.action AttributeAction/ADD)
                    (.build))
            "a" (-> (AttributeValueUpdate/builder)
                    (.action AttributeAction/DELETE)
                    (.build))}
           (.attributeUpdates req)))
    (is (= ReturnValue/UPDATED_OLD (.returnValues req)))
    (is (= {"e1" (-> (ExpectedAttributeValue/builder)
                     (.value (-> (AttributeValue/builder) (.s "expected!") ^AttributeValue (.build)))
                     (.build))}
           (.expected req))))

  (let [req ^UpdateItemRequest (update-item-request
                                :update-item
                                {:x 1}
                                {:update-expr "SET #p = :price REMOVE details.tags[2]"
                                 :expr-attr-vals {":price" 0.89}
                                 :expr-attr-names {"#p" "price"}
                                 :expected {:e1 "expected!"}
                                 :return :updated-old})]

    (is (= "update-item" (.tableName req)))
    (is (= {"x" (-> (AttributeValue/builder) (.n "1") (.build))}
           (.key req)))
    (is (= "SET #p = :price REMOVE details.tags[2]" (.updateExpression req)))
    (is (= {":price" (-> (AttributeValue/builder) (.n "0.89") (.build))}
           (.expressionAttributeValues req)))
    (is (= {"#p" "price"} (.expressionAttributeNames req)))
    (is (= ReturnValue/UPDATED_OLD (.returnValues req)))
    (is (= {"e1" (-> (ExpectedAttributeValue/builder)
                     (.value (-> (AttributeValue/builder) (.s "expected!") ^AttributeValue (.build)))
                     (.build))}
           (.expected req)))))

(deftest delete-item-request-creation
  (let [req ^DeleteItemRequest (delete-item-request
                                :delete-item
                                {:k1 "val" :r1 -3}
                                {:return :all-new
                                 :cond-expr "another = :a AND #n = :name"
                                 :expr-attr-vals {":a" 1 ":name" "joe"}
                                 :expr-attr-names {"#n" "name"}
                                 :expected {:e1 1}})]

    (is (= "delete-item" (.tableName req)))
    (is (= {"k1" (-> (AttributeValue/builder) (.s "val") (.build))
            "r1" (-> (AttributeValue/builder) (.n "-3") (.build))}
           (.key req)))
    (is (= {"e1" (-> (ExpectedAttributeValue/builder)
                     (.value (-> (AttributeValue/builder) (.n "1") ^AttributeValue (.build)))
                     (.build))}
           (.expected req)))
    (is (= "another = :a AND #n = :name" (.conditionExpression req)))
    (is (= 2 (count (.expressionAttributeValues req))))
    (is (= {"#n" "name"} (.expressionAttributeNames req)))
    (is (= ReturnValue/ALL_NEW (.returnValues req)))))

(deftest batch-get-item-request-creation
  (let [req
        ^BatchGetItemRequest
        (batch-get-item-request
         false
         (batch-request-items
          {:t1 {:prim-kvs {:t1-k1 -10}
                :attrs [:some-other-guy]}
           :t2 {:prim-kvs {:t2-k1 ["x" "y" "z"]}}}))]

    (is (= {"t1" (-> (KeysAndAttributes/builder)
                     (.keys ^Collection (list {"t1-k1" (-> (AttributeValue/builder) (.n "-10") (.build))}))
                     (.attributesToGet ["some-other-guy"])
                     (.build))
            "t2" (-> (KeysAndAttributes/builder)
                     (.keys ^Collection (list {"t2-k1" (-> (AttributeValue/builder) (.s "x") (.build))}
                                              {"t2-k1" (-> (AttributeValue/builder) (.s "y") (.build))}
                                              {"t2-k1" (-> (AttributeValue/builder) (.s "z") (.build))}))
                     (.build))}
           (.requestItems req)))))

(deftest batch-write-item-request-creation
    (let [req
          ^BatchWriteItemRequest
          (batch-write-item-request
            false
            {"t1" (map
                    #(write-request :put %)
                    (attr-multi-vs {:k ["x" "y"]}))
             "t2" (map
                    #(write-request :delete %)
                (attr-multi-vs {:k [0]}))})]
    (is (= {"t1" [(-> (WriteRequest/builder)
                      (.putRequest (-> (PutRequest/builder)
                                       (.item {"k" (-> (AttributeValue/builder) (.s "x") (.build))})
                                       ^PutRequest (.build)))
                      (.build))
                  (-> (WriteRequest/builder)
                      (.putRequest (-> (PutRequest/builder)
                                       (.item {"k" (-> (AttributeValue/builder) (.s "y") (.build))})
                                       ^PutRequest (.build)))
                      (.build))]
            "t2" [(-> (WriteRequest/builder)
                      (.deleteRequest (-> (DeleteRequest/builder)
                                          (.key {"k" (-> (AttributeValue/builder) (.n "0") (.build))})
                                          ^DeleteRequest (.build)))
                      (.build))]}
           (.requestItems req)))))

(deftest query-request-creation
    (let [req ^QueryRequest (query-request
                              :query
                              {:name [:eq "Steve"]
                               :age [:between [10 30]]}
                              {:return :all-projected-attributes
                               :index :lsindex
                               :order :desc
                               :limit 2})]
      (is (= "query" (.tableName req)))
      (is (= {"name" (-> (Condition/builder)
                         (.comparisonOperator ComparisonOperator/EQ)
                         (.attributeValueList ^Collection (list (-> (AttributeValue/builder) (.s "Steve") (.build))))
                         (.build))
              "age" (-> (Condition/builder)
                        (.comparisonOperator ComparisonOperator/BETWEEN)
                        (.attributeValueList ^Collection (list (-> (AttributeValue/builder) (.n "10") (.build))
                                                               (-> (AttributeValue/builder) (.n "30") (.build))))
                        (.build))}
             (.keyConditions req)))
      (is (= Select/ALL_PROJECTED_ATTRIBUTES (.select req)))
      (is (= "lsindex" (.indexName req)))
      (is false? (.scanIndexForward req))
      (is (= 2 (.limit req)))))

(deftest scan-request-creation
  (let [req ^ScanRequest (scan-request
                           :scan
                           {:attr-conds {:age [:in [24 27]]}
                            :index :age-index
                            :proj-expr "age, #t"
                           :expr-attr-names {"#t" "year"}
                           :return :count
                           :limit 10})]
    (is (= "scan" (.tableName req)))
    (is (= 10 (.limit req)))
    (is (= {"age" (-> (Condition/builder)
                      (.comparisonOperator ComparisonOperator/IN)
                      (.attributeValueList ^Collection (list (-> (AttributeValue/builder) (.n "24") (.build))
                                                             (-> (AttributeValue/builder) (.n "27") (.build))))
                      (.build))}
           (.scanFilter req)))
    (is (= Select/COUNT (.select req)))
    (is (= "age-index" (.indexName req)))
    (is (= "age, #t" (.projectionExpression req)))
    (is (= {"#t" "year"} (.expressionAttributeNames req))))

  (let [req ^ScanRequest (scan-request
                          :scan
                          {:filter-expr "age < 25"
                           :index "age-index"
                           :limit 5
                           :consistent? true})]
    (is (= "scan" (.tableName req)))
    (is (= 5 (.limit req)))
    (is (= "age < 25" (.filterExpression req)))
    (is (= "age-index" (.indexName req)))
    (is (= (.consistentRead req)))))

(deftest list-streams-request-creation
  (let [req ^ListStreamsRequest (list-stream-request
                                 {:table-name "stream-table-name"
                                  :limit 42
                                  :start-arn "arn:aws:dynamodb:ddblocal:0:table/etc"})]
    (is (= "stream-table-name" (.tableName req)))
    (is (= 42 (.limit req)))
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (.exclusiveStartStreamArn req)))))

(deftest describe-streams-request-creation
  (let [req ^DescribeStreamRequest (describe-stream-request
                                    "arn:aws:dynamodb:ddblocal:0:table/etc"
                                    {:limit 20
                                     :start-shard-id "01"})]
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (.streamArn req)))
    (is (= 20 (.limit req)))
    (is (= "01" (.exclusiveStartShardId req)))))

(deftest get-shard-iterator-request-creation
  (let [req ^GetShardIteratorRequest (get-shard-iterator-request
                                      "arn:aws:dynamodb:ddblocal:0:table/etc"
                                      "shardId000"
                                      :after-sequence-number
                                      {:seq-num "000001"})]
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (.streamArn req)))
    (is (= "shardId000" (.shardId req)))
    (is (= ShardIteratorType/AFTER_SEQUENCE_NUMBER (.shardIteratorType req)))
    (is (= "000001" (.sequenceNumber req)))))

(deftest get-records-request-creation
  (let [req ^GetRecordsRequest (get-records-request
                                "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc"
                                {:limit 50})]
    (is (= "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc" (.shardIterator req)))
    (is (= 50 (.limit req)))))

(deftest decribe-ttl-request-creation
  (let [req ^DescribeTimeToLiveRequest (describe-ttl-request {:table-name :my-desc-ttl-table})]
    (is "my-desc-ttl-table" (.tableName req))))

(deftest update-ttl-request-creation
  (let [req ^UpdateTimeToLiveRequest (update-ttl-request
                                      {:table-name :my-update-ttl-table
                                       :enabled? false})]
    (is "my-update-ttl-table" (.tableName req))
    (let [ttl-spec (.timeToLiveSpecification req)]
      (is (not (.enabled ttl-spec)))
      (is "ttl" (.attributeName ttl-spec))))

  (let [req ^UpdateTimeToLiveRequest (update-ttl-request
                                      {:table-name :my-update-ttl-table
                                       :enabled? true
                                       :key-name :ttl})]
    (is "my-update-ttl-table" (.tableName req))
    (let [ttl-spec (.timeToLiveSpecification req)]
      (is (.enabled ttl-spec))
      (is "ttl" (.attributeName ttl-spec)))))
