(ns taoensso.faraday.tests.requests
  (:require
   [expectations     :as test :refer :all]
   [taoensso.encore  :as encore]
   [taoensso.faraday :as far]
   [taoensso.nippy   :as nippy])

  (:import
   [com.amazonaws.services.dynamodbv2.model
    AttributeAction
    AttributeDefinition
    AttributeValue
    AttributeValueUpdate
    BatchGetItemRequest
    BatchGetItemResult
    BatchWriteItemRequest
    BatchWriteItemResult
    ComparisonOperator
    Condition
    ConditionalCheckFailedException
    ConsumedCapacity
    CreateGlobalSecondaryIndexAction
    CreateTableRequest
    CreateTableResult
    DeleteGlobalSecondaryIndexAction
    DeleteItemRequest
    DeleteItemResult
    DeleteRequest
    DeleteTableRequest
    DeleteTableResult
    DescribeStreamRequest
    DescribeTableRequest
    DescribeTableResult
    ExpectedAttributeValue
    GetItemRequest
    GetItemResult
    GetRecordsRequest
    GetShardIteratorRequest
    GlobalSecondaryIndex
    GlobalSecondaryIndexDescription
    GlobalSecondaryIndexUpdate
    InternalServerErrorException
    ItemCollectionMetrics
    ItemCollectionSizeLimitExceededException
    KeySchemaElement
    KeyType
    KeysAndAttributes
    LimitExceededException
    ListStreamsRequest
    ListTablesRequest
    ListTablesResult
    LocalSecondaryIndex
    LocalSecondaryIndexDescription
    Projection
    ProjectionType
    ProvisionedThroughput
    ProvisionedThroughputDescription
    ProvisionedThroughputExceededException
    PutItemRequest
    PutItemResult
    PutRequest
    QueryRequest
    QueryResult
    ResourceInUseException
    ResourceNotFoundException
    ReturnValue
    ScanRequest
    ScanResult
    Select
    ShardIteratorType
    StreamViewType
    TableDescription
    UpdateGlobalSecondaryIndexAction
    UpdateItemRequest
    UpdateItemResult
    UpdateTableRequest
    UpdateTableResult
    WriteRequest]))

(comment
  (remove-ns       'taoensso.faraday.tests.requests)
  (test/run-tests '[taoensso.faraday.tests.requests]))

;;;; Private var aliases

(def describe-table-request     #'taoensso.faraday/describe-table-request)
(def create-table-request       #'taoensso.faraday/create-table-request)
(def update-table-request       #'taoensso.faraday/update-table-request)
(def get-item-request           #'taoensso.faraday/get-item-request)
(def put-item-request           #'taoensso.faraday/put-item-request)
(def update-item-request        #'taoensso.faraday/update-item-request)
(def delete-item-request        #'taoensso.faraday/delete-item-request)
(def batch-get-item-request     #'taoensso.faraday/batch-get-item-request)
(def batch-request-items        #'taoensso.faraday/batch-request-items)
(def batch-write-item-request   #'taoensso.faraday/batch-write-item-request)
(def attr-multi-vs              #'taoensso.faraday/attr-multi-vs)
(def query-request              #'taoensso.faraday/query-request)
(def write-request              #'taoensso.faraday/write-request)
(def scan-request               #'taoensso.faraday/scan-request)
(def list-stream-request        #'taoensso.faraday/list-streams-request)
(def describe-stream-request    #'taoensso.faraday/describe-stream-request)
(def get-shard-iterator-request #'taoensso.faraday/get-shard-iterator-request)
(def get-records-request        #'taoensso.faraday/get-records-request)

;;;;

(expect "describe-table-name"
  (.getTableName ^DescribeTableRequest
    (describe-table-request :describe-table-name)))

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
     (.getProvisionedThroughput gsindex)))

  (let [stream-spec (.getStreamSpecification req)]
    (expect true (.getStreamEnabled stream-spec))
    (expect (.toString StreamViewType/NEW_IMAGE) (.getStreamViewType stream-spec))))

(expect
 "update-table"
 (.getTableName
  ^UpdateTableRequest
  (update-table-request :update-table {:throughput {:read 1 :write 1}})))

(expect
 (ProvisionedThroughput. 15 7)
 (.getProvisionedThroughput
  ^UpdateTableRequest
  (update-table-request :update-table {:throughput {:read 15 :write 7}})))

(let [req ^UpdateTableRequest
          (update-table-request
            :update-table
            {:gsindexes {:name         "global-secondary"
                         :operation    :create
                         :hash-keydef  [:gs-hash-keydef :n]
                         :range-keydef [:gs-range-keydef :n]
                         :projection   :keys-only
                         :throughput   {:read 10 :write 2}}})]
  (expect "update-table" (.getTableName req))

  (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.getGlobalSecondaryIndexUpdates req)
        create-action (.getCreate gsindex)]
    (expect nil? rest)
    (expect "global-secondary" (.getIndexName create-action))
    (expect
      (doto (Projection.)
        (.setProjectionType ProjectionType/KEYS_ONLY))
      (.getProjection create-action))
    (expect
      #{(KeySchemaElement. "gs-range-keydef" KeyType/RANGE)
        (KeySchemaElement. "gs-hash-keydef" KeyType/HASH)}
      (into #{} (.getKeySchema create-action)))
    (expect
      (ProvisionedThroughput. 10 2)
      (.getProvisionedThroughput create-action))))

(let [req ^UpdateTableRequest
          (update-table-request
            :update-table
            {:gsindexes {:name         "global-secondary"
                         :operation    :update
                         :throughput   {:read 4 :write 2}}})]
  (expect "update-table" (.getTableName req))

  (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.getGlobalSecondaryIndexUpdates req)
        update-action (.getUpdate gsindex)]
    (expect nil? rest)
    (expect "global-secondary" (.getIndexName update-action))
    (expect
      (ProvisionedThroughput. 4 2)
      (.getProvisionedThroughput update-action))))

(let [req ^UpdateTableRequest
          (update-table-request
            :update-table
            {:gsindexes {:name      "global-secondary"
                         :operation :delete}})]
  (expect "update-table" (.getTableName req))

  (let [[^GlobalSecondaryIndexUpdate gsindex & rest] (.getGlobalSecondaryIndexUpdates req)
        action (.getDelete gsindex)]
    (expect nil? rest)
    (expect "global-secondary" (.getIndexName action))
    ))

(let [req ^UpdateTableRequest (update-table-request
                                :update-table
                                {:stream-spec {:enabled? false}})
      stream-spec (.getStreamSpecification req)]
  (expect false (.getStreamEnabled stream-spec))
  (expect nil? (.getStreamViewType stream-spec)))

(expect
 "get-item"
 (.getTableName
  ^GetItemRequest
  (get-item-request :get-item {:x 1})))

(expect
 not
 (.getConsistentRead
  ^GetItemRequest
  (get-item-request :get-item {:x 1} {:consistent? false})))

(expect
 true?
 (.getConsistentRead
  ^GetItemRequest
  (get-item-request :get-item {:x 1} {:consistent? true})))

(let [req ^GetItemRequest (get-item-request
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
  (put-item-request :put-item-table-name {:x 1})))

(let [req
      ^PutItemRequest (put-item-request
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
           "e2" (doto (ExpectedAttributeValue.)
                  (.setValue (doto (AttributeValue.)
                               (.setBOOL false))))}
          (.getExpected req)))

(let [req
      ^UpdateItemRequest (update-item-request
                          :update-item
                          {:x 1}
                          {:update-map {:y [:put 2]
                                        :z [:add "xyz"]
                                        :a [:delete]}
                           :expected   {:e1 "expected!"}
                           :return     :updated-old})]

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
      ^UpdateItemRequest (update-item-request
                           :update-item
                           {:x 1}
                           {:update-expr     "SET #p = :price REMOVE details.tags[2]"
                            :expr-attr-vals  {":price" 0.89}
                            :expr-attr-names {"#p" "price"}
                            :expected        {:e1 "expected!"}
                            :return          :updated-old})]

  (expect "update-item" (.getTableName req))
  (expect {"x" (doto (AttributeValue.)
                 (.setN "1"))}
          (.getKey req))
  (expect "SET #p = :price REMOVE details.tags[2]" (.getUpdateExpression req))
  (expect {":price" (doto (AttributeValue.)
                      (.setN "0.89"))} (.getExpressionAttributeValues req))
  (expect {"#p" "price"} (.getExpressionAttributeNames req))
  (expect (str ReturnValue/UPDATED_OLD) (.getReturnValues req))
  (expect {"e1" (doto (ExpectedAttributeValue.)
                  (.setValue (AttributeValue. "expected!")))}
          (.getExpected req)))

(let [req
      ^DeleteItemRequest (delete-item-request
                          :delete-item
                          {:k1 "val" :r1 -3}
                          {:return :all-new
                           :cond-expr "another = :a AND #n = :name"
                           :expr-attr-vals {":a" 1 ":name" "joe"}
                           :expr-attr-names {"#n" "name"}
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
  (expect "another = :a AND #n = :name" (.getConditionExpression req))
  (expect 2 (count (.getExpressionAttributeValues req)))
  (expect {"#n" "name"} (.getExpressionAttributeNames req))
  (expect (str ReturnValue/ALL_NEW) (.getReturnValues req)))

(let [req
      ^BatchGetItemRequest
      (batch-get-item-request
        false
        (batch-request-items
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
      (batch-write-item-request
       false
       {"t1" (map
              #(write-request :put %)
              (attr-multi-vs {:k ["x" "y"]}))
        "t2" (map
              #(write-request :delete %)
              (attr-multi-vs {:k [0]}))})]
  (expect
   {"t1" [(WriteRequest.
           (PutRequest. {"k" (AttributeValue. "x")}))
          (WriteRequest.
           (PutRequest. {"k" (AttributeValue. "y")}))]
    "t2" [(WriteRequest.
           (DeleteRequest. {"k" (doto (AttributeValue.)
                                  (.setN "0"))}))]}
   (.getRequestItems req)))

(let [req ^QueryRequest (query-request
                         :query
                         {:name [:eq "Steve"]
                          :age [:between [10 30]]}
                         {:return :all-projected-attributes
                          :index "lsindex"
                          :order :desc
                          :limit 2})]
  (expect "query" (.getTableName req))
  (expect
   {"name" (doto (Condition.)
             (.setComparisonOperator ComparisonOperator/EQ)
             (.setAttributeValueList [(AttributeValue. "Steve")]))
    "age" (doto (Condition.)
            (.setComparisonOperator ComparisonOperator/BETWEEN)
            (.setAttributeValueList [(doto (AttributeValue.)
                                       (.setN "10"))
                                     (doto (AttributeValue.)
                                       (.setN "30"))]))}
   (.getKeyConditions req))
  (expect (str Select/ALL_PROJECTED_ATTRIBUTES) (.getSelect req))
  (expect "lsindex" (.getIndexName req))
  (expect false? (.getScanIndexForward req))
  (expect 2 (.getLimit req)))

(let [req ^ScanRequest (scan-request
                         :scan
                         {:attr-conds      {:age [:in [24 27]]}
                          :index           "age-index"
                          :proj-expr       "age, #t"
                          :expr-attr-names {"#t" "year"}
                          :return          :count
                          :limit           10})]
  (expect "scan" (.getTableName req))
  (expect 10 (.getLimit req))
  (expect
    {"age" (doto (Condition.)
             (.setComparisonOperator ComparisonOperator/IN)
             (.setAttributeValueList [(doto (AttributeValue.)
                                        (.setN "24"))
                                      (doto (AttributeValue.)
                                        (.setN "27"))]))}
    (.getScanFilter req))
  (expect (str Select/COUNT) (.getSelect req))
  (expect "age-index" (.getIndexName req))
  (expect "age, #t" (.getProjectionExpression req))
  (expect {"#t" "year"} (.getExpressionAttributeNames req))
  )

(let [req ^ScanRequest (scan-request
                         :scan
                         {:filter-expr "age < 25"
                          :index       "age-index"
                          :limit       5
                          :consistent? true})]
  (expect "scan" (.getTableName req))
  (expect 5 (.getLimit req))
  (expect "age < 25" (.getFilterExpression req))
  (expect "age-index" (.getIndexName req))
  (expect (.getConsistentRead req))
  )

(let [req ^ListStreamsRequest (list-stream-request
                                {:table-name "stream-table-name"
                                 :limit 42
                                 :start-arn "arn:aws:dynamodb:ddblocal:0:table/etc"})]
  (expect "stream-table-name" (.getTableName req))
  (expect 42 (.getLimit req))
  (expect "arn:aws:dynamodb:ddblocal:0:table/etc" (.getExclusiveStartStreamArn req)))

(let [req ^DescribeStreamRequest (describe-stream-request
                                   "arn:aws:dynamodb:ddblocal:0:table/etc"
                                   {:limit 20
                                    :start-shard-id "01"})]
  (expect "arn:aws:dynamodb:ddblocal:0:table/etc" (.getStreamArn req))
  (expect 20 (.getLimit req))
  (expect "01" (.getExclusiveStartShardId req)))

(let [req ^GetShardIteratorRequest (get-shard-iterator-request
                                     "arn:aws:dynamodb:ddblocal:0:table/etc"
                                     "shardId000"
                                     :after-sequence-number
                                     {:seq-num "000001"})]
  (expect "arn:aws:dynamodb:ddblocal:0:table/etc" (.getStreamArn req))
  (expect "shardId000" (.getShardId req))
  (expect ShardIteratorType/AFTER_SEQUENCE_NUMBER (ShardIteratorType/fromValue (.getShardIteratorType req)))
  (expect "000001" (.getSequenceNumber req)))

(let [req ^GetRecordsRequest (get-records-request
                               "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc"
                               {:limit 50})]
  (expect "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc" (.getShardIterator req))
  (expect 50 (.getLimit req)))
