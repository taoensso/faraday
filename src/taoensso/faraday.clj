(ns taoensso.faraday
  "Clojure DynamoDB client. A fork of Rotary by James Reeves,
  Ref. https://github.com/weavejester/rotary"
  {:author "Peter Taoussanis"}
  (:require [clojure.string  :as str]
            [taoensso.timbre :as timbre]
            [taoensso.nippy  :as nippy])
  (:import  [com.amazonaws.services.dynamodb.model
             AttributeValue
             BatchGetItemRequest
             BatchGetItemResult
             BatchResponse
             BatchWriteItemRequest
             BatchWriteItemResult
             BatchWriteResponse
             Condition
             CreateTableRequest
             UpdateTableRequest
             DescribeTableRequest
             DescribeTableResult
             DeleteTableRequest
             DeleteItemRequest
             DeleteRequest
             GetItemRequest
             GetItemResult
             Key
             KeySchema
             KeySchemaElement
             KeysAndAttributes
             ProvisionedThroughput
             ProvisionedThroughputDescription
             PutItemRequest
             PutRequest
             ResourceNotFoundException
             ScanRequest
             QueryRequest
             WriteRequest]
            com.amazonaws.auth.BasicAWSCredentials
            com.amazonaws.services.dynamodb.AmazonDynamoDBClient))

;;;; TODO Rotary PRs
;; * Added proxy support (cesarpinera)
;; * Feature/can update dynamo items (mrgordon)
;; * Support conditional PUT (BestFriendChris)
;; * More flexible key support and additional options from SDK (mrgordon)
;; * Added support for binary values (mantree)
;; * Add support for making conditional put requests (edpaget)
;; * Upgrade to use dynamodb v2 api (edpaget)
;; * Local secondary indexes (edpaget)
;; * Non-PR forks.

;;;; TODO
;; * Missing deps.
;; * Code walk-through.
;; * Update SDK dep.
;; * Update to v2 API.
;; * Go through Rotary PRs.
;; * Bin support + serialization.
;; * Docs.
;; * Tests!

(defn- db-client*
  "Get a AmazonDynamoDBClient instance for the supplied credentials."
  [cred]
  (let [aws-creds (BasicAWSCredentials. (:access-key cred) (:secret-key cred))
        client (AmazonDynamoDBClient. aws-creds)]
    (when-let [endpoint (:endpoint cred)]
      (.setEndpoint client endpoint))
    client))

(def db-client (memoize db-client*))

(defprotocol AsMap (as-map [x]))

(defn- to-long [x] (Long. x))

(defn- get-value
  "Get the value of an AttributeValue object."
  [attr-value]
  (or (.getS attr-value)
      (-?>> (.getN attr-value)  to-long)
      (-?>> (.getNS attr-value) (map to-long) (into #{}))
      (-?>> (.getSS attr-value) (into #{}))))

(defn- key-schema-element
  "Create a KeySchemaElement object."
  [{key-name :name, key-type :type}]
  (doto (KeySchemaElement.)
    (.setAttributeName (str key-name))
    (.setAttributeType (str/upper-case (name key-type)))))

(defn- key-schema
  "Create a KeySchema object."
  [hash-key & [range-key]]
  (let [schema (KeySchema. (key-schema-element hash-key))]
    (when range-key
      (.setRangeKeyElement schema (key-schema-element range-key)))
    schema))

(defn- provisioned-throughput
  "Created a ProvisionedThroughput object."
  [{read-units :read, write-units :write}]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn create-table
  "Create a table in DynamoDB with the given map of properties. The properties
  available are:
    :name       - the name of the table (required)
    :hash-key   - a map that defines the hash key name and type (required)
    :range-key  - a map that defines the range key name and type (optional)
    :throughput - a map that defines the read and write throughput (required)

  The hash-key and range-key definitions are maps with the following keys:
    :name - the name of the key
    :type - the type of the key (:s, :n, :ss, :ns)

  Where :s is a string type, :n is a number type, and :ss and :ns are sets of
  strings and number respectively.

  The throughput is a map with two keys:
    :read  - the provisioned number of reads per second
    :write - the provisioned number of writes per second"
  [cred {:keys [name hash-key range-key throughput]}]
  (.createTable
   (db-client cred)
   (doto (CreateTableRequest.)
     (.setTableName (str name))
     (.setKeySchema (key-schema hash-key range-key))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

(defn update-table
  "Update a table in DynamoDB with the given name. Only the throughput may be
  updated. The throughput values can be increased by no more than a factor of
  two over the current values (e.g. if your read throughput was 20, you could
  only set it from 1 to 40). See create-table."
  [cred {:keys [name throughput]}]
  (.updateTable
   (db-client cred)
   (doto (UpdateTableRequest.)
     (.setTableName (str name))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

(extend-protocol AsMap
  KeySchemaElement
  (as-map [element]
    {:name (.getAttributeName element)
     :type (-> (.getAttributeType element)
               (str/lower-case)
               (keyword))})
  KeySchema
  (as-map [schema]
    (merge
     (if-let [e (.getHashKeyElement schema)]  {:hash-key  (as-map e)} {})
     (if-let [e (.getRangeKeyElement schema)] {:range-key (as-map e)} {})))
  ProvisionedThroughputDescription
  (as-map [throughput]
    {:read  (.getReadCapacityUnits throughput)
     :write (.getWriteCapacityUnits throughput)
     :last-decrease (.getLastDecreaseDateTime throughput)
     :last-increase (.getLastIncreaseDateTime throughput)})
  DescribeTableResult
  (as-map [result]
    (let [table (.getTable result)]
      {:name          (.getTableName table)
       :creation-date (.getCreationDateTime table)
       :item-count    (.getItemCount table)
       :key-schema    (as-map (.getKeySchema table))
       :throughput    (as-map (.getProvisionedThroughput table))
       :status        (-> (.getTableStatus table)
                          (str/lower-case)
                          (keyword))}))
  BatchWriteResponse
  (as-map [result]
    {:consumed-capacity-units (.getConsumedCapacityUnits result)})
  BatchWriteItemResult
  (as-map [result]    
    {:responses         (fmap as-map (into {} (.getResponses result)))
     :unprocessed-items (.getUnprocessedItems result)})
  BatchResponse
  (as-map [result]
    {:consumed-capacity-units (.getConsumedCapacityUnits result)
     :items (-?>> (.getItems result) 
                  (into []) 
                  (fmap #(fmap get-value (into {} %))))})
  KeysAndAttributes
  (as-map [result]
    (merge
      (if-let [a (.getAttributesToGet result)] {:attrs (into [] a)} {})
      (if-let [c (.getConsistentRead result)]  {:consistent c} nil)
      (if-let [k (.getKeys result)]            {:keys (fmap as-map (into [] k))} {})))
  BatchGetItemResult
  (as-map [result]
    {:responses        (fmap as-map (into {} (.getResponses result)))
     :unprocessed-keys (fmap as-map (into {} (.getUnprocessedKeys result)))}))

(defn describe-table
  "Returns a map describing the table in DynamoDB with the given name, or nil
  if the table does not exist."
  [cred name]
  (try
    (as-map
     (.describeTable
      (db-client cred)
      (doto (DescribeTableRequest.)
        (.setTableName name))))
    (catch ResourceNotFoundException _
      nil)))

(defn ensure-table
  "Creates the table if it does not already exist."
  [cred {name :name :as properties}]
  (if-not (describe-table cred name)
    (create-table cred properties)))

(defn delete-table
  "Delete a table in DynamoDB with the given name."
  [cred name]
  (.deleteTable
   (db-client cred)
   (DeleteTableRequest. name)))

(defn list-tables
  "Return a list of tables in DynamoDB."
  [cred]
  (-> (db-client cred)
      .listTables
      .getTableNames
      seq))

(defn- set-of [f s]
  (and (set? s) (every? f s)))

(defn- to-attr-value
  "Convert a value into an AttributeValue object."
  [value]
  (cond
   (string? value)        (doto (AttributeValue.) (.setS value))
   (number? value)        (doto (AttributeValue.) (.setN (str value)))
   (set-of string? value) (doto (AttributeValue.) (.setSS value))
   (set-of number? value) (doto (AttributeValue.) (.setNS (map str value)))
   (set? value)    (throw (Exception. "Set must be all numbers or all strings"))
   :else           (throw (Exception. (str "Unknown value type: " (type value))))))

(defn- item-map
  "Turn a item in DynamoDB into a Clojure map."
  [item]
  (if item
    (fmap get-value (into {} item))))

(extend-protocol AsMap
  GetItemResult
  (as-map [result]
    (item-map (.getItem result))))

(defn put-item
  "Add an item (a Clojure map) to a DynamoDB table."
  [cred table item]
  (.putItem
   (db-client cred)
   (doto (PutItemRequest.)
     (.setTableName table)
     (.setItem
      (into {}
            (for [[k v] item]
              [(name k) (to-attr-value v)]))))))

(defn- item-key
  "Create a Key object from a value."
  [{:keys [hash-key range-key]}]
  (let [key (Key.)]
    (when hash-key
      (.setHashKeyElement key (to-attr-value hash-key)))
    (when range-key
      (.setRangeKeyElement key (to-attr-value range-key)))
    key))

(defn get-item
  "Retrieve an item from a DynamoDB table by its hash key."
  [cred table hash-key]
  (as-map
   (.getItem
    (db-client cred)
    (doto (GetItemRequest.)
      (.setTableName table)
      (.setKey (item-key {:hash-key hash-key}))))))

(defn delete-item
  "Delete an item from a DynamoDB table by its hash key."
  [cred table hash-key]
  (.deleteItem
   (db-client cred)
   (DeleteItemRequest. table (item-key {:hash-key hash-key}))))

(extend-protocol AsMap
  Key
  (as-map [k]
    {:hash-key  (get-value (.getHashKeyElement k))
     :range-key (get-value (.getRangeKeyElement k))})
  nil
  (as-map [_] nil))

(defn- batch-item-keys [request-or-keys]
  (for [key (:keys request-or-keys request-or-keys)]
    (if (map? key)
      (item-key key)
      (item-key {:hash-key key}))))

(defn- keys-and-attrs [{:keys [attrs consistent] :as request}]
  (let [kaa (KeysAndAttributes.)]
    (.setKeys kaa (batch-item-keys request))
    (when attrs
      (.setAttributesToGet kaa attrs))
    (when consistent
      (.setConsistentRead kaa consistent))
    kaa))

(defn- batch-request-items [requests]
  (into {}
    (for [[k v] requests]
      [(name k) (keys-and-attrs v)])))

(defn batch-get-item
  "Retrieve a batch of items in a single request. DynamoDB limits
   apply - 100 items and 1MB total size limit. Requested items
   which were elided by Amazon are available in the returned map
   key :unprocessed-keys. 

   Examples:
   (batch-get-item cred
     {:users [\"alice\" \"bob\"]
      :posts {:keys [1 2 3]
              :attrs [\"timestamp\" \"subject\"]
              :consistent true}})"
  [cred requests]
  (as-map
    (.batchGetItem
      (db-client cred)
      (doto (BatchGetItemRequest.)
        (.setRequestItems (batch-request-items requests))))))

(defn- delete-request [item]
  (doto (DeleteRequest.)
    (.setKey (item-key item))))

(defn- put-request [item]  
  (doto (PutRequest.)
    (.setItem
      (into {}
        (for [[id field] item]
          {(name id) (to-attr-value field)})))))

(defn- write-request [verb item]
  (let [wr (WriteRequest.)]
    (case verb
      :delete (.setDeleteRequest wr (delete-request item))
      :put    (.setPutRequest wr (put-request item)))
    wr))

(defn batch-write-item
  "Execute a batch of Puts and/or Deletes in a single request.
   DynamoDB limits apply - 25 items max. No transaction
   guarantees are provided, nor conditional puts.
   
   Example:
   (batch-write-item cred
     [:put :users {:user-id 1 :username \"sally\"}]
     [:put :users {:user-id 2 :username \"jane\"}]
     [:delete :users {:hash-key 3}])"
  [cred & requests]
  (as-map
    (.batchWriteItem
      (db-client cred)
      (doto (BatchWriteItemRequest.)
        (.setRequestItems
         (fmap
          (fn [reqs]
            (reduce
             #(conj %1 (write-request (first %2) (last %2)))
             []
             (partition 3 (flatten reqs))))
          (group-by #(name (second %)) requests)))))))

(defn- result-map [results]
  {:items    (map item-map (.getItems results))
   :count    (.getCount results)
   :last-key (as-map (.getLastEvaluatedKey results))})

(defn- scan-request
  "Create a ScanRequest object."
  [table {:keys [limit count after]}]
  (let [sr (ScanRequest. table)]
    (when limit
      (.setLimit sr (int limit)))
    (when count
      (.setCount sr count))
    (when after
      (.setExclusiveStartKey sr (item-key after)))
    sr))

(defn scan
  "Return the items in a DynamoDB table. Takes the following options:
    :limit - the maximum number of items to return
    :after - only return results after this key

  The items are returned as a map with the following keys:
    :items    - the list of items returned
    :count    - the count of items matching the query
    :last-key - the last evaluated key (useful for paging) "
  [cred table & [options]]
  (result-map
   (.scan
    (db-client cred)
    (scan-request table options))))

(defn- set-range-condition
  "Add the range key condition to a QueryRequest object"
  [query-request operator & [range-key range-end]]
  (let [attribute-list (->> [range-key range-end] (remove nil?) (map to-attr-value))]
    (.setRangeKeyCondition query-request
                           (doto (Condition.)
                             (.withComparisonOperator operator)
                             (.withAttributeValueList attribute-list)))))

(defn- normalize-operator
  "Maps Clojure operators to DynamoDB operators"
  [operator]
  (let [operator-map {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"}
        op (->> operator name str/upper-case)]
    (operator-map (keyword op) op)))

(defn- query-request
  "Create a QueryRequest object."
  [table hash-key range-clause {:keys [order limit after count consistent attrs]}]
  (let [qr (QueryRequest. table (to-attr-value hash-key))
        [operator range-key range-end] range-clause]
    (when operator
      (set-range-condition qr (normalize-operator operator) range-key range-end))
    (when attrs
      (.setAttributesToGet qr (map name attrs)))
    (when order
      (.setScanIndexForward qr (not= order :desc)))
    (when limit
      (.setLimit qr (int limit)))
    (when count
      (.setCount qr count))
    (when consistent
      (.setConsistentRead qr consistent))
    (when after
      (.setExclusiveStartKey qr (item-key after)))
    qr))

(defn- extract-range [[range options]]
  (if (and (map? range) (nil? options))
    [nil range]
    [range options]))

(defn query
  "Return the items in a DynamoDB table matching the supplied hash key.
  Can specify a range clause if the table has a range-key ie. `(>= 234)
  Takes the following options:
    :order - may be :asc or :desc (defaults to :asc)
    :attrs - limit the values returned to the following attribute names
    :limit - the maximum number of items to return
    :after - only return results after this key
    :consistent - return a consistent read if logical true

  The items are returned as a map with the following keys:
    :items    - the list of items returned
    :count    - the count of items matching the query
    :last-key - the last evaluated key (useful for paging)"
  [cred table hash-key & range-and-options]
  (let [[range options] (extract-range range-and-options)]
    (result-map
     (.query
      (db-client cred)
      (query-request table hash-key range options)))))
