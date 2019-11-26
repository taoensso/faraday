(ns taoensso.faraday.tests.requests
  (:require
   [taoensso.faraday :as far]
   [clojure.test :refer :all]
   [taoensso.faraday.utils :as utils])

  (:import
   [com.amazonaws.services.dynamodbv2.model
    AttributeAction
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
         (.getTableName ^DescribeTableRequest
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
    (is (= "create-table-name" (.getTableName req)))

    (is (= (ProvisionedThroughput. 5 10)
           (.getProvisionedThroughput req)))

    (is (= #{(KeySchemaElement. "hash-keydef" KeyType/HASH)
             (KeySchemaElement. "range-keydef" KeyType/RANGE)}
           (into #{} (.getKeySchema req))))

    (let [[^LocalSecondaryIndex lsindex & rest] (.getLocalSecondaryIndexes req)]
      (is nil? rest)
      (is (= "local-secondary" (.getIndexName lsindex)))
      (is (= (doto (Projection.)
               (.setProjectionType ProjectionType/INCLUDE)
               (.setNonKeyAttributes ["ls-projection"]))
             (.getProjection lsindex)))
      (is (= #{(KeySchemaElement. "hash-keydef" KeyType/HASH)
               (KeySchemaElement. "ls-range-keydef" KeyType/RANGE)}
             (into #{} (.getKeySchema lsindex)))))

    (let [[^GlobalSecondaryIndex gsindex & rest] (.getGlobalSecondaryIndexes req)]
      (is nil? rest)
      (is (= "global-secondary" (.getIndexName gsindex)))
      (is (= (doto (Projection.)
               (.setProjectionType ProjectionType/KEYS_ONLY))
             (.getProjection gsindex)))
      (is (= #{(KeySchemaElement. "gs-range-keydef" KeyType/RANGE)
               (KeySchemaElement. "gs-hash-keydef" KeyType/HASH)}
             (into #{} (.getKeySchema gsindex))))
      (is (= (ProvisionedThroughput. 10 2)
             (.getProvisionedThroughput gsindex))))

    (let [stream-spec (.getStreamSpecification req)]
      (is true? (.getStreamEnabled stream-spec))
      (is (= (.toString StreamViewType/NEW_IMAGE) (.getStreamViewType stream-spec)))))

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

    (is nil? (.getProvisionedThroughput req))

    (is (= (utils/enum :pay-per-request) (.getBillingMode req)))
    (let [[^GlobalSecondaryIndex gsindex & rest] (.getGlobalSecondaryIndexes req)]
      (is nil? (.getProvisionedThroughput gsindex))))

  (let [req ^CreateTableRequest
            (create-table-request
             :create-table-name [:hash-keydef :n]
             {:range-keydef [:range-keydef :n]
              :gsindexes [{:name "global-secondary"
                           :hash-keydef [:gs-hash-keydef :n]
                           :range-keydef [:gs-range-keydef :n]
                           :projection :keys-only}]})]

    (is nil? (.getProvisionedThroughput req))
    (is (= (utils/enum :provisioned) (.getBillingMode req)))))

(deftest update-tables-request-creation
  (is (= "update-table"
         (.getTableName
          ^UpdateTableRequest
          (update-table-request :update-table {} {:throughput {:read 1 :write 1}}))))

  (is (= (ProvisionedThroughput. 15 7)
         (.getProvisionedThroughput
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
    (is (= "update-table" (.getTableName req)))

    (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.getGlobalSecondaryIndexUpdates req)
          create-action (.getCreate gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (.getIndexName create-action)))
      (is (= (doto (Projection.)
               (.setProjectionType ProjectionType/KEYS_ONLY))
             (.getProjection create-action)))
      (is (= #{(KeySchemaElement. "gs-range-keydef" KeyType/RANGE)
               (KeySchemaElement. "gs-hash-keydef" KeyType/HASH)}
             (into #{} (.getKeySchema create-action))))
      (is (= (ProvisionedThroughput. 10 2)
             (.getProvisionedThroughput create-action)))))

  (let [req ^UpdateTableRequest
            (update-table-request
             :update-table
             {}
             {:gsindexes {:name "global-secondary"
                          :operation :update
                          :throughput {:read 4 :write 2}}})]
    (is (= "update-table" (.getTableName req)))

    (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.getGlobalSecondaryIndexUpdates req)
          update-action (.getUpdate gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (.getIndexName update-action)))
      (is (= (ProvisionedThroughput. 4 2)
             (.getProvisionedThroughput update-action)))))

  (let [req ^UpdateTableRequest
            (update-table-request
             :update-table
             {}
             {:gsindexes {:name "global-secondary"
                          :operation :delete}})]
    (is (= "update-table" (.getTableName req)))

    (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.getGlobalSecondaryIndexUpdates req)
          action (.getDelete gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (.getIndexName action)))))

  (let [req ^UpdateTableRequest (update-table-request
                                 :update-table
                                 {}
                                 {:stream-spec {:enabled? false}})
        stream-spec (.getStreamSpecification req)]
    (is false? (.getStreamEnabled stream-spec))
    (is nil? (.getStreamViewType stream-spec)))

  (testing "updating billing mode"
    (is (= (utils/enum :pay-per-request)
           (.getBillingMode
            ^UpdateTableRequest
            (update-table-request :update-table {} {:billing-mode :pay-per-request}))))

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
      (is (= "update-table" (.getTableName req))))))

(deftest get-item-request-creation
  (is (= "get-item"
         (.getTableName
          ^GetItemRequest
          (get-item-request :get-item {:x 1}))))

  (is (not (.getConsistentRead
            ^GetItemRequest
            (get-item-request :get-item {:x 1} {:consistent? false}))))

  (is
   true?
   (.getConsistentRead
    ^GetItemRequest
    (get-item-request :get-item {:x 1} {:consistent? true})))

  (let [req ^GetItemRequest (get-item-request
                             :get-item-table-name
                             {:hash "y" :range 2}
                             {:attrs [:j1 :j2]})]
    (is (= {"hash" (AttributeValue. "y")
            "range" (doto (AttributeValue.)
                      (.setN "2"))}
           (.getKey req)))

    (is (= #{"j1" "j2"}
           (into #{} (.getAttributesToGet req))))))
(deftest put-item-request-creation
  (is (= "put-item-table-name"
         (.getTableName
          ^PutItemRequest
          (put-item-request :put-item-table-name {:x 1}))))

  (let [req ^PutItemRequest (put-item-request
                             :put-item-table-name
                             {:c1 "hey" :c2 1}
                             {:return :updated-new
                              :expected {:e1 "expected value"
                                         :e2 false}})]
    (is (= (str ReturnValue/UPDATED_NEW) (.getReturnValues req)))
    (is (= {"c1" (AttributeValue. "hey")
            "c2" (doto (AttributeValue.)
                   (.setN "1"))}
           (.getItem req)))
    (is (= {"e1" (doto (ExpectedAttributeValue.)
                   (.setValue (AttributeValue. "expected value")))
            "e2" (doto (ExpectedAttributeValue.)
                   (.setValue (doto (AttributeValue.)
                                (.setBOOL false))))}
           (.getExpected req)))))

(deftest update-item-request-creation
  (let [req ^UpdateItemRequest (update-item-request
                                :update-item
                                {:x 1}
                                {:update-map {:y [:put 2]
                                              :z [:add "xyz"]
                                              :a [:delete]}
                                 :expected {:e1 "expected!"}
                                 :return :updated-old})]

    (is (= "update-item" (.getTableName req)))
    (is (= {"x" (doto (AttributeValue.)
                  (.setN "1"))}
           (.getKey req)))
    (is (= {"y" (AttributeValueUpdate.
                 (doto (AttributeValue.)
                   (.setN "2"))
                 (str AttributeAction/PUT))
            "z" (AttributeValueUpdate.
                 (AttributeValue. "xyz")
                 AttributeAction/ADD)
            "a" (AttributeValueUpdate. nil AttributeAction/DELETE)}
           (.getAttributeUpdates req)))
    (is (= (str ReturnValue/UPDATED_OLD) (.getReturnValues req)))
    (is (= {"e1" (doto (ExpectedAttributeValue.)
                   (.setValue (AttributeValue. "expected!")))}
           (.getExpected req))))

  (let [req ^UpdateItemRequest (update-item-request
                                :update-item
                                {:x 1}
                                {:update-expr "SET #p = :price REMOVE details.tags[2]"
                                 :expr-attr-vals {":price" 0.89}
                                 :expr-attr-names {"#p" "price"}
                                 :expected {:e1 "expected!"}
                                 :return :updated-old})]

    (is (= "update-item" (.getTableName req)))
    (is (= {"x" (doto (AttributeValue.)
                  (.setN "1"))}
           (.getKey req)))
    (is (= "SET #p = :price REMOVE details.tags[2]" (.getUpdateExpression req)))
    (is (= {":price" (doto (AttributeValue.)
                       (.setN "0.89"))} (.getExpressionAttributeValues req)))
    (is (= {"#p" "price"} (.getExpressionAttributeNames req)))
    (is (= (str ReturnValue/UPDATED_OLD) (.getReturnValues req)))
    (is (= {"e1" (doto (ExpectedAttributeValue.)
                   (.setValue (AttributeValue. "expected!")))}
           (.getExpected req)))))

(deftest delete-item-request-creation
  (let [req ^DeleteItemRequest (delete-item-request
                                :delete-item
                                {:k1 "val" :r1 -3}
                                {:return :all-new
                                 :cond-expr "another = :a AND #n = :name"
                                 :expr-attr-vals {":a" 1 ":name" "joe"}
                                 :expr-attr-names {"#n" "name"}
                                 :expected {:e1 1}})]

    (is (= "delete-item" (.getTableName req)))
    (is (= {"k1" (AttributeValue. "val")
            "r1" (doto (AttributeValue.)
                   (.setN "-3"))}
           (.getKey req)))
    (is (= {"e1" (doto (ExpectedAttributeValue.)
                   (.setValue (doto (AttributeValue.)
                                (.setN "1"))))}
           (.getExpected req)))
    (is (= "another = :a AND #n = :name" (.getConditionExpression req)))
    (is (= 2 (count (.getExpressionAttributeValues req))))
    (is (= {"#n" "name"} (.getExpressionAttributeNames req)))
    (is (= (str ReturnValue/ALL_NEW) (.getReturnValues req)))))

(deftest batch-get-item-request-creation
  (let [req
        ^BatchGetItemRequest
        (batch-get-item-request
         false
         (batch-request-items
          {:t1 {:prim-kvs {:t1-k1 -10}
                :attrs [:some-other-guy]}
           :t2 {:prim-kvs {:t2-k1 ["x" "y" "z"]}}}))]

    (is (= {"t1" (doto (KeysAndAttributes.)
                   (.setKeys [{"t1-k1" (doto (AttributeValue.)
                                         (.setN "-10"))}])
                   (.setAttributesToGet ["some-other-guy"]))
            "t2" (doto (KeysAndAttributes.)
                   (.setKeys [{"t2-k1" (AttributeValue. "x")}
                              {"t2-k1" (AttributeValue. "y")}
                              {"t2-k1" (AttributeValue. "z")}]))}
           (.getRequestItems req)))))

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
    (is (= {"t1" [(WriteRequest.
                   (PutRequest. {"k" (AttributeValue. "x")}))
                  (WriteRequest.
                   (PutRequest. {"k" (AttributeValue. "y")}))]
            "t2" [(WriteRequest.
                   (DeleteRequest. {"k" (doto (AttributeValue.)
                                          (.setN "0"))}))]}
           (.getRequestItems req)))))
(deftest query-request-creation
  (let [req ^QueryRequest (query-request
                           :query
                           {:name [:eq "Steve"]
                            :age [:between [10 30]]}
                           {:return :all-projected-attributes
                            :index :lsindex
                            :order :desc
                            :limit 2})]
    (is (= "query" (.getTableName req)))
    (is (= {"name" (doto (Condition.)
                     (.setComparisonOperator ComparisonOperator/EQ)
                     (.setAttributeValueList [(AttributeValue. "Steve")]))
            "age" (doto (Condition.)
                    (.setComparisonOperator ComparisonOperator/BETWEEN)
                    (.setAttributeValueList [(doto (AttributeValue.)
                                               (.setN "10"))
                                             (doto (AttributeValue.)
                                               (.setN "30"))]))}
           (.getKeyConditions req)))
    (is (= (str Select/ALL_PROJECTED_ATTRIBUTES) (.getSelect req)))
    (is (= "lsindex" (.getIndexName req)))
    (is false? (.getScanIndexForward req))
    (is (= 2 (.getLimit req)))))

(deftest scan-request-creation
  (let [req ^ScanRequest (scan-request
                          :scan
                          {:attr-conds {:age [:in [24 27]]}
                           :index :age-index
                           :proj-expr "age, #t"
                           :expr-attr-names {"#t" "year"}
                           :return :count
                           :limit 10})]
    (is (= "scan" (.getTableName req)))
    (is (= 10 (.getLimit req)))
    (is (= {"age" (doto (Condition.)
                    (.setComparisonOperator ComparisonOperator/IN)
                    (.setAttributeValueList [(doto (AttributeValue.)
                                               (.setN "24"))
                                             (doto (AttributeValue.)
                                               (.setN "27"))]))}
           (.getScanFilter req)))
    (is (= (str Select/COUNT) (.getSelect req)))
    (is (= "age-index" (.getIndexName req)))
    (is (= "age, #t" (.getProjectionExpression req)))
    (is (= {"#t" "year"} (.getExpressionAttributeNames req))))

  (let [req ^ScanRequest (scan-request
                          :scan
                          {:filter-expr "age < 25"
                           :index "age-index"
                           :limit 5
                           :consistent? true})]
    (is (= "scan" (.getTableName req)))
    (is (= 5 (.getLimit req)))
    (is (= "age < 25" (.getFilterExpression req)))
    (is (= "age-index" (.getIndexName req)))
    (is (= (.getConsistentRead req)))))

(deftest list-streams-request-creation
  (let [req ^ListStreamsRequest (list-stream-request
                                 {:table-name "stream-table-name"
                                  :limit 42
                                  :start-arn "arn:aws:dynamodb:ddblocal:0:table/etc"})]
    (is (= "stream-table-name" (.getTableName req)))
    (is (= 42 (.getLimit req)))
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (.getExclusiveStartStreamArn req)))))

(deftest describe-streams-request-creation
  (let [req ^DescribeStreamRequest (describe-stream-request
                                    "arn:aws:dynamodb:ddblocal:0:table/etc"
                                    {:limit 20
                                     :start-shard-id "01"})]
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (.getStreamArn req)))
    (is (= 20 (.getLimit req)))
    (is (= "01" (.getExclusiveStartShardId req)))))

(deftest get-shard-iterator-request-creation
  (let [req ^GetShardIteratorRequest (get-shard-iterator-request
                                      "arn:aws:dynamodb:ddblocal:0:table/etc"
                                      "shardId000"
                                      :after-sequence-number
                                      {:seq-num "000001"})]
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (.getStreamArn req)))
    (is (= "shardId000" (.getShardId req)))
    (is (= ShardIteratorType/AFTER_SEQUENCE_NUMBER (ShardIteratorType/fromValue (.getShardIteratorType req))))
    (is (= "000001" (.getSequenceNumber req)))))

(deftest get-records-request-creation
  (let [req ^GetRecordsRequest (get-records-request
                                "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc"
                                {:limit 50})]
    (is (= "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc" (.getShardIterator req)))
    (is (= 50 (.getLimit req)))))

(deftest decribe-ttl-request-creation
  (let [req ^DescribeTimeToLiveRequest (describe-ttl-request {:table-name :my-desc-ttl-table})]
    (is "my-desc-ttl-table" (.getTableName req))))

(deftest update-ttl-request-creation
  (let [req ^UpdateTimeToLiveRequest (update-ttl-request
                                      {:table-name :my-update-ttl-table
                                       :enabled? false})]
    (is "my-update-ttl-table" (.getTableName req))
    (let [ttl-spec (.getTimeToLiveSpecification req)]
      (is (not (.getEnabled ttl-spec)))
      (is "ttl" (.getAttributeName ttl-spec))))

  (let [req ^UpdateTimeToLiveRequest (update-ttl-request
                                      {:table-name :my-update-ttl-table
                                       :enabled? true
                                       :key-name :ttl})]
    (is "my-update-ttl-table" (.getTableName req))
    (let [ttl-spec (.getTimeToLiveSpecification req)]
      (is (.getEnabled ttl-spec))
      (is "ttl" (.getAttributeName ttl-spec)))))
