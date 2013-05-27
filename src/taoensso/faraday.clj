(ns taoensso.faraday
  "Clojure DynamoDB client. A fork of Rotary by James Reeves,
  Ref. https://github.com/weavejester/rotary"
  {:author "Peter Taoussanis"}
  (:require [clojure.string         :as str]
            [taoensso.timbre        :as timbre]
            [taoensso.nippy         :as nippy]
            [taoensso.faraday.utils :as utils])
  (:import  [com.amazonaws.services.dynamodbv2.model
             AttributeValue
             AttributeValueUpdate
             AttributeDefinition
             BatchGetItemRequest
             BatchGetItemResult
             BatchWriteItemRequest
             BatchWriteItemResult
             Condition
             CreateTableRequest
             UpdateTableRequest
             DescribeTableRequest
             DescribeTableResult
             DeleteTableRequest
             DeleteItemRequest
             DeleteItemResult
             DeleteRequest
             ExpectedAttributeValue
             GetItemRequest
             GetItemResult
             KeySchemaElement
             KeysAndAttributes
             LocalSecondaryIndex
             Projection
             ProvisionedThroughput
             ProvisionedThroughputDescription
             PutItemRequest
             PutItemResult
             PutRequest
             QueryRequest
             QueryResult
             ResourceNotFoundException
             ScanRequest
             ScanResult
             UpdateItemRequest
             UpdateTableRequest
             WriteRequest]
            com.amazonaws.ClientConfiguration
            com.amazonaws.auth.BasicAWSCredentials
            com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
            java.nio.ByteBuffer))

;;;; TODO
;; * Code walk-through.
;; * Go through Rotary PRs.
;; * Go through Rotary non-PR forks.
;; * Nippy support.
;; * Further tests.
;; * Docs!
;; * Benching.
;; * Parallel scans.
;; * Async API.
;; * Auto throughput adjusting.

;;;; Coercion - values

;; TODO see http://goo.gl/hOzhR and http://goo.gl/NSY3Z for types supported by
;; the Java SDK! (incl. Date, Long, BigDecimal, BigInteger, ...).

;; TODO Nippy support (will require special marker bin-wrapping), incl.
;; Serialized (boxed) type (should allow empty strings & ANY type of set)
;; Maybe require special wrapper types for writing bins/serialized

(defn- str->num [^String s] (if (.contains s ".") (Double. s) (Long. s)))
(defn- bb->ba   [^ByteBuffer bb] (.array bb))

(defn- db-val->clj-val "Returns the Clojure value of given AttributeValue object."
  [^AttributeValue x]
  (or (.getS x)
      (some->> (.getN  x) str->num)
      (some->> (.getB  x) bb->ba)
      (some->> (.getSS x) (into #{}))
      (some->> (.getNS x) (map str->num) (into #{}))
      (some->> (.getBS x) (map bb->ba)   (into #{}))))

(def ^:private ^:const ba-class (Class/forName "[B"))
(defn- ba? [x] (instance? ba-class x))
(defn- ba-buffer [^bytes ba] (ByteBuffer/wrap ba))
(defn- simple-num? [x] (or (instance? Long    x)
                           (instance? Double  x)
                           (instance? Integer x)
                           (instance? Float   x)))

(defn- clj-val->db-val "Returns an AttributeValue object for given Clojure value."
  ^AttributeValue [x]
  (cond
   (string? x)
   (if (.isEmpty ^String x)
     (throw (Exception. "Invalid DynamoDB value: \"\""))
     (doto (AttributeValue.) (.setS x)))

   (simple-num? x) (doto (AttributeValue.) (.setN (str x)))
   (ba? x)         (doto (AttributeValue.) (.setB (ba-buffer x)))

   (set? x)
   (if (empty? x)
     (throw (Exception. "Invalid DynamoDB value: empty set"))
     (cond
      (every? string?     x) (doto (AttributeValue.) (.setSS x))
      (every? simple-num? x) (doto (AttributeValue.) (.setNS (map str x)))
      (every? ba?         x) (doto (AttributeValue.) (.setBS (map ba-buffer x)))
      :else (throw (Exception. (str "Invalid DynamoDB value: set of invalid type"
                                    " or more than one type")))))

   :else (throw (Exception. (str "Unknown value type: " (type x) "."
                                 " Wrap with `serialize`?")))))

(comment (map clj-val->db-val ["foo" 10 3.14 (.getBytes "foo")
                               #{"a" "b" "c"} #{1 2 3.14} #{(.getBytes "foo")}]))

;;;; API - object wrappers

(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied IAM credentials."
  (memoize
   (fn [{:keys [access-key secret-key endpoint proxy-host proxy-port] :as creds}]
     (let [aws-creds     (BasicAWSCredentials. access-key secret-key)
           client-config (ClientConfiguration.)]
       (when proxy-host (.setProxyHost client-config proxy-host))
       (when proxy-port (.setProxyPort client-config proxy-port))
       (let [client (AmazonDynamoDBClient. aws-creds client-config)]
         (when endpoint (.setEndpoint client endpoint))
         client)))))

(defn- db-client ^AmazonDynamoDBClient [creds] (db-client* creds))

(defn- key-schema-element "Returns a new KeySchemaElement object."
  [key-name key-type]
  (doto (KeySchemaElement.)
    (.setAttributeName (str key-name))
    (.setKeyType (utils/ucname key-type))))

(defn- key-schema "Returns a new KeySchema object."
  [hash-key & [range-key]]
  (let [schema (key-schema-element (:name hash-key) :hash)]
    (if range-key
      (conj schema (key-schema-element (:name range-key) :range))
      schema)))

(defn- provisioned-throughput "Returns a new ProvisionedThroughput object."
  [{read-units :read write-units :write}]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits  (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn- attribute-definitions "Returns a vector of new AttributeDefinition objects."
  [defs]
  (mapv
   (fn [{key-name :name key-type :type :as def}]
     (doto (AttributeDefinition.)
       (.setAttributeName key-name)
       (.setAttributeType (utils/ucname key-type))))
   defs))

(defn- projection "Returns a new Projection object."
  [projection & [included-attrs]]
  (let [pr (Projection.)]
    (.setProjectionType pr (utils/ucname projection))
    (when included-attrs (.setNonKeyAttributes pr included-attrs))
    pr))

(defn- local-indexes "Returns a vector of new LocalSecondaryIndexes objects."
  [hash-key indexes]
  (mapv
   (fn [{:keys [name range-key projection included-attrs]
        :or   {projection :all}
        :as   index}]
     (doto (LocalSecondaryIndex.)
       (.setIndexName  name)
       (.setKeySchema  (key-schema hash-key range-key))
       (.setProjection (projection projection included-attrs))))
   indexes))

(defn- expected-values
  "Returns a map of attribute/ExpectedAttributeValue pairs from a map of
  attribute/condition pairs: {\"my-attr\" \"expected-value\"}, etc.

  All conditions must be met for operation to succeed."
  [expected]
  (utils/fmap
   #(case %
      (true  ::exists)     (ExpectedAttributeValue. true) ;; TODO Valid?
      (false ::not-exists) (ExpectedAttributeValue. false)
      (ExpectedAttributeValue. (clj-val->db-val %)))
   expected))

;;;; Coercion - objects

(defprotocol AsMap (as-map [x]))

(defn- db-map->clj-map [x] (when x (utils/fmap db-val->clj-val (into {} x))))

(defmacro ^:private am-item-result [result get-form]
  `(when-let [get-form# ~get-form]
     (with-meta (db-map->clj-map get-form#)
       {:consumed-capacity (.getConsumedCapacity ~result)})))

(defmacro ^:private am-query|scan-result [result]
  `(let [result# ~result]
     {:items (map db-map->clj-map (.getItems result#))
      :count (.getCount result#)
      :consumed-capacity (.getConsumedCapacity result#)
      :last-key (as-map (.getLastEvaluatedKey result#))}))

(extend-protocol AsMap
  nil                 (as-map [_] nil)
  java.util.ArrayList (as-map [a] (into [] (map as-map a)))
  java.util.HashMap   (as-map [m] (utils/fmap as-map (into {} m)))
  AttributeValue      (as-map [v] (db-val->clj-val v))
  KeySchemaElement    (as-map [e] {:name (.getAttributeName e)})
  GetItemResult       (as-map [r] (am-item-result r (.getItem r)))
  PutItemResult       (as-map [r] (am-item-result r (.getAttributes r)))
  DeleteItemResult    (as-map [r] (am-item-result r (.getAttributes r)))
  QueryResult         (as-map [r] (am-query|scan-result r))
  ScanResult          (as-map [r] (am-query|scan-result r))
  KeysAndAttributes
  (as-map [result]
    (merge
     (when-let [a (.getAttributesToGet result)] {:attrs (into [] a)})
     (when-let [c (.getConsistentRead  result)] {:consistent c})
     (when-let [k (.getKeys            result)] {:keys (utils/fmap as-map (into [] k))})))

  ;; TODO CreateTableResult, ListTablesResult, UpdateItemResult, UpdateTableResult

  BatchGetItemResult
  (as-map [result]
    {:responses         (utils/fmap as-map (into {} (.getResponses result)))
     :unprocessed-keys  (utils/fmap as-map (into {} (.getUnprocessedKeys result)))
     :consumed-capacity (.getConsumedCapacity result)})
  BatchWriteItemResult
  (as-map [result] {:unprocessed-items (into {} (.getUnprocessedItems result))
                    :consumed-capacity (.getConsumedCapacity result)})

  DescribeTableResult
  (as-map [result]
    (let [table (.getTable result)]
      {:name          (.getTableName table)
       :creation-date (.getCreationDateTime table)
       :item-count    (.getItemCount table)
       :size-bytes    (.getTableSizeBytes table)
       :key-schema    (as-map (.getKeySchema table))
       :throughput    (as-map (.getProvisionedThroughput table))
       ;; :indexes    (as-map (.getLocalSecondaryIndexes table)) ; TODO
       :status        (-> (.getTableStatus table) (str/lower-case) (keyword))}))

  ;; LocalSecondaryIndexDescription ; TODO

  ProvisionedThroughputDescription
  (as-map [throughput] {:read          (.getReadCapacityUnits    throughput)
                        :write         (.getWriteCapacityUnits   throughput)
                        :last-decrease (.getLastDecreaseDateTime throughput)
                        :last-increase (.getLastIncreaseDateTime throughput)}))

;;;; API - tables

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
    :write - the provisioned number of writes per second

  The indexes vector is a vector of maps with keys:
    :name           - the name of the Local Secondary Index (required)
    :range-key      - a map that defines the range key name and type (required)
    :projection     - keyword that defines the projection may be:
      :all, :keys_only, :include (optional - default is :keys-only)
    :included-attrs - a vector of attribute names when :projection is :include
                      (optional)"
  [creds {:keys [name hash-key range-key throughput indexes]}]
  (.createTable
   (db-client creds)
   (let [defined-attrs (->> (conj [] hash-key range-key)
                            (concat (map #(:range-key %) indexes))
                            (filter identity))]
     (doto (CreateTableRequest.)
       (.setTableName (str name))
       (.setKeySchema (key-schema hash-key range-key))
       (.setAttributeDefinitions (attribute-definitions defined-attrs))
       (.setProvisionedThroughput (provisioned-throughput throughput))
       (.setLocalSecondaryIndexes (local-indexes hash-key indexes))))))

(defn update-table
  "Update a table in DynamoDB with the given name. Only the throughput may be
  updated. The throughput values can be increased by no more than a factor of
  two over the current values (e.g. if your read throughput was 20, you could
  only set it from 1 to 40). See create-table."
  [creds {:keys [name throughput]}]
  (.updateTable
   (db-client creds)
   (doto (UpdateTableRequest.)
     (.setTableName (str name))
     (.setProvisionedThroughput (provisioned-throughput throughput)))))

(defn describe-table
  "Returns a map describing the table in DynamoDB with the given name, or nil
  if the table doesn't exist."
  [creds name]
  (try (as-map
        (.describeTable
         (db-client creds)
         (doto (DescribeTableRequest.)
           (.setTableName name))))
       (catch ResourceNotFoundException _)))

(defn ensure-table
  "Creates the table if it does not already exist."
  [creds {name :name :as properties}]
  (if-not (describe-table creds name)
    (create-table creds properties)))

(defn delete-table
  "Delete a table in DynamoDB with the given name."
  [creds name]
  (.deleteTable
   (db-client creds)
   (DeleteTableRequest. name)))

(defn list-tables
  "Return a list of tables in DynamoDB."
  [creds]
  (-> (db-client creds)
      .listTables
      .getTableNames
      seq))

;;;; TODO Code walk-through below

;;;; API - items

(defn- set-expected-value! ; TODO PR
  "Makes a Put request conditional by setting its expected value"
  [^PutItemRequest req expected]
  (when expected
    (.setExpected req
      (utils/fmap
       #(if (instance? Boolean %)
          (ExpectedAttributeValue. ^Boolean %)
          (ExpectedAttributeValue. (clj-val->db-val %)))
       expected))))

(defn put-item
  ;;"Add an item (a Clojure map) to a DynamoDB table."
  "Add an item (a Clojure map) to a DynamoDB table. Optionally accepts
  a map after :expected that declares expected values of the item in DynamoDB
  or the Boolean false if they are not expected to be present. If the condition
  is not met, it fails with a ConditionalCheckFailedException.

  Example: (would not add item if it already exists))
    (put-item credentials \"table\"
      {\"id\" 1 \"attr\" \"foo\"}
      :expected {\"id\" false})"
  [creds table item & {:keys [expected]}] ; TODO PR
  ;; [creds table item]
  (.putItem
   (db-client creds)
   (doto (PutItemRequest.)
     (.setTableName table)
     (.setItem
      (into {}
            (for [[k v] item]
              [(name k) (clj-val->db-val v)])))
     (set-expected-value! expected))))

;; (defn- item-key
;;   "Create a Key object from a value."
;;   [{:keys [hash-key range-key]}]
;;   (let [key (Key.)]
;;     (when hash-key
;;       (.setHashKeyElement key (clj-val->db-val hash-key)))
;;     (when range-key
;;       (.setRangeKeyElement key (clj-val->db-val range-key)))
;;     key))

(defn- item-key "Returns a new Key map." ; TODO Key?
  [key] ; TODO key?
  (into {} (map (fn [[k v]] {(name k) (clj-val->db-val v)}) key)))

(defn get-item
  "Retrieve an item from a DynamoDB table by its hash key."
  ;; TODO hash-key should be specified as a map of {:attr-name value}. Huh??
  [creds table hash-key]
  (as-map
   (.getItem
    (db-client creds)
    (doto (GetItemRequest.)
      (.setTableName table)
      (.setKey (item-key hash-key))))))

(defn delete-item
  "Delete an item from a DynamoDB table by its hash key."
  [creds table hash-key]
  (.deleteItem
   (db-client creds)
   (DeleteItemRequest. table (item-key hash-key))))

;;;; API - batch ops

(defn- batch-item-keys [request-or-keys]
  (for [key (:keys request-or-keys request-or-keys)]
    (item-key {(:key-name request-or-keys) key})))

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

  Example:
  (batch-get-item cred
    {:users {:key-name \"names\"
             :keys [\"alice\" \"bob\"]}
     :posts {:key-name \"id\"
             :keys [1 2 3]
             :attrs [\"timestamp\" \"subject\"]
             :consistent true}})"
  [creds requests]
  (as-map
    (.batchGetItem
      (db-client creds)
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
          {(name id) (clj-val->db-val field)})))))

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
   (batch-write-item creds
     [:put :users {:user-id 1 :username \"sally\"}]
     [:put :users {:user-id 2 :username \"jane\"}]
     [:delete :users {:hash-key 3}])"
  [creds & requests]
  (as-map
    (.batchWriteItem
      (db-client creds)
      (doto (BatchWriteItemRequest.)
        (.setRequestItems
         (utils/fmap
          (fn [reqs]
            (reduce
             #(conj %1 (write-request (first %2) (last %2)))
             []
             (partition 3 (flatten reqs))))
          (group-by #(name (second %)) requests)))))))

;;;; API - queries & scans

(defn- scan-request
  "Create a ScanRequest object."
  [table {:keys [limit count after]}]
  (let [sr (ScanRequest. table)]
    (when limit (.setLimit sr (int limit)))
    (when after (.setExclusiveStartKey sr (item-key after)))
    sr))

(defn scan
  "Return the items in a DynamoDB table. Takes the following options:
    :limit - the maximum number of items to return
    :after - only return results after this key

  The items are returned as a map with the following keys:
    :items    - the list of items returned
    :count    - the count of items matching the query
    :last-key - the last evaluated key (useful for paging) "
  [creds table & [options]]
  (as-map
   (.scan
    (db-client creds)
    (scan-request table options))))

(defn- set-hash-condition
  "Create a map of specifying the hash-key condition for query"
  [hash-key]
  (utils/fmap #(doto (Condition.)
                 (.setComparisonOperator "EQ")
                 (.setAttributeValueList [(clj-val->db-val %)]))
              hash-key))

(defn- set-range-condition
  "Add the range key condition to a QueryRequest object"
  [range-key operator & [range-value range-end]]
  (let [attribute-list (->> [range-value range-end] (filter identity) (map clj-val->db-val))]
    {range-key (doto (Condition.)
                 (.setComparisonOperator ^String operator)
                 (.setAttributeValueList attribute-list))}))

(defn- normalize-operator
  "Maps Clojure operators to DynamoDB operators"
  [operator]
  (let [operator-map {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"}
        op (->> operator utils/ucname)]
    (operator-map (keyword op) op)))

(defn- query-request
  "Create a QueryRequest object."
  [table hash-key range-clause {:keys [order limit after count consistent attrs index]}]
  (let [qr (QueryRequest.)
        hash-clause (set-hash-condition hash-key)
        [range-key operator range-value range-end] range-clause
        query-conditions (if-not operator
                           hash-clause
                           (merge hash-clause (set-range-condition range-key
                                                                   (normalize-operator operator)
                                                                   range-value
                                                                   range-end)))]
    (.setTableName qr table)
    (.setKeyConditions qr query-conditions)
    (when attrs      (.setAttributesToGet qr (map name attrs)))
    (when order      (.setScanIndexForward qr (not= order :desc)))
    (when limit      (.setLimit qr (int limit)))
    (when consistent (.setConsistentRead qr consistent))
    (when after      (.setExclusiveStartKey qr (item-key after)))
    (when index      (.setIndexName qr index))
    qr))

(defn- extract-range [[range options]]
  (if (and (map? range) (not options))
    [nil range]
    [range options]))

(defn query
  "Return the items in a DynamoDB table matching the supplied hash key,
  defined in the form {\"hash-attr\" hash-value}.
  Can specify a range clause if the table has a range-key ie. `(\"range-attr\" >= 234)
  Takes the following options:
    :order - may be :asc or :desc (defaults to :asc)
    :attrs - limit the values returned to the following attribute names
    :limit - the maximum number of items to return
    :after - only return results after this key
    :consistent - return a consistent read if logical true
    :index - the secondary index to query

  The items are returned as a map with the following keys:
    :items - the list of items returned
    :count - the count of items matching the query
    :last-key - the last evaluated key (useful for paging)"
  [creds table hash-key & range-and-options]
  (let [[range options] (extract-range range-and-options)]
    (as-map
     (.query
      (db-client creds)
      (query-request table hash-key range options)))))
