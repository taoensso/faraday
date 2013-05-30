(ns taoensso.faraday
  "Clojure DynamoDB client. Adapted from Rotary by James Reeves.
  Ref. https://github.com/weavejester/rotary (Rotary),
       http://goo.gl/22QGA (DynamoDBv2 API)

  Definitions (with '=>' as 'implies'):
    * item        => {<attribute> <value>}
    * key         => hash OR range key => special attribute
    * primary key => hash key WITH optional range key
    * attribute   ≠> key (i.e. does not imply)"

  {:author "Peter Taoussanis"}
  (:require [clojure.string         :as str]
            [taoensso.timbre        :as timbre]
            [taoensso.nippy         :as nippy]
            [taoensso.faraday.utils :as utils])
  (:import  [com.amazonaws.services.dynamodbv2.model
             AttributeDefinition
             AttributeValue
             AttributeValueUpdate
             BatchGetItemRequest
             BatchGetItemResult
             BatchWriteItemRequest
             BatchWriteItemResult
             Condition
             ConsumedCapacity
             CreateTableRequest
             CreateTableResult
             DeleteItemRequest
             DeleteItemResult
             DeleteRequest
             DeleteTableRequest
             DeleteTableResult
             DescribeTableRequest
             DescribeTableResult
             ExpectedAttributeValue
             GetItemRequest
             GetItemResult
             ItemCollectionMetrics
             KeysAndAttributes
             KeySchemaElement
             ListTablesRequest
             ListTablesResult
             LocalSecondaryIndex
             LocalSecondaryIndexDescription
             Projection
             ProvisionedThroughput
             ProvisionedThroughputDescription
             PutItemRequest
             PutItemResult
             PutRequest
             QueryRequest
             QueryResult
             ScanRequest
             ScanResult
             TableDescription
             UpdateItemRequest
             UpdateItemResult
             UpdateTableRequest
             UpdateTableResult
             WriteRequest
             ConditionalCheckFailedException
             InternalServerErrorException
             ItemCollectionSizeLimitExceededException
             LimitExceededException
             ProvisionedThroughputExceededException
             ResourceInUseException
             ResourceNotFoundException]
            com.amazonaws.ClientConfiguration
            com.amazonaws.auth.BasicAWSCredentials
            com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
            java.nio.ByteBuffer))

;;;; TODO
;; * Finish up code walk-through.
;; * Finish up outstanding API: as-map types, update-item, scan+query stuff.
;; * Go through Rotary PRs, non-PR forks.
;; * README docs, benchmarks, further tests.
;; * Long-term: async API, auto throughput adjusting, ...?

;;;; API - exceptions

(def ^:const ex "DynamoDB API exceptions. Use #=(ex _) for `try` blocks, etc."
  {:conditional-check-failed            ConditionalCheckFailedException
   :internal-server-error               InternalServerErrorException
   :item-collection-size-limit-exceeded ItemCollectionSizeLimitExceededException
   :limit-exceeded                      LimitExceededException
   :provisioned-throughput-exceeded     ProvisionedThroughputExceededException
   :resource-in-use                     ResourceInUseException
   :resource-not-found                  ResourceNotFoundException})

;;;; Coercion - values

;; TODO Consider alternative approaches to freezing that would _not_ serialize
;; unwrapped binary data; this is too presumptuous.

(deftype Frozen [value])
(defn freeze [x] (Frozen. x))

(def ^:private ^:const ba-class (Class/forName "[B"))
(defn- freeze?     [x] (or (instance? Frozen x) (instance? ba-class x)))
(defn- freeze*     [x] (ByteBuffer/wrap (nippy/freeze-to-bytes
                                         (if (instance? Frozen x)
                                           (.value ^Frozen x) x))))
(defn- thaw        [^ByteBuffer bb] (nippy/thaw-from-bytes (.array bb)))
(defn- str->num    [^String s] (if (.contains s ".") (Double. s) (Long. s)))
(defn- simple-num? [x] (or (instance? Long    x)
                           (instance? Double  x)
                           (instance? Integer x)
                           (instance? Float   x)))

(defn- db-val->clj-val "Returns the Clojure value of given AttributeValue object."
  [^AttributeValue x]
  (or (.getS x)
      (some->> (.getN  x) str->num)
      (some->> (.getB  x) thaw)
      (some->> (.getSS x) (into #{}))
      (some->> (.getNS x) (map str->num) (into #{}))
      (some->> (.getBS x) (map thaw)     (into #{}))))

(defn- clj-val->db-val "Returns an AttributeValue object for given Clojure value."
  ^AttributeValue [x]
  (cond
   (string? x)
   (if (.isEmpty ^String x)
     (throw (Exception. "Invalid DynamoDB value: \"\""))
     (doto (AttributeValue.) (.setS x)))

   (simple-num? x) (doto (AttributeValue.) (.setN (str x)))
   (freeze?     x) (doto (AttributeValue.) (.setB (freeze* x)))

   (set? x)
   (if (empty? x)
     (throw (Exception. "Invalid DynamoDB value: empty set"))
     (cond
      (every? string?     x) (doto (AttributeValue.) (.setSS x))
      (every? simple-num? x) (doto (AttributeValue.) (.setNS (map str x)))
      (every? freeze?     x) (doto (AttributeValue.) (.setBS (map freeze* x)))
      :else (throw (Exception. (str "Invalid DynamoDB value: set of invalid type"
                                    " or more than one type")))))

   :else (throw (Exception. (str "Unknown DynamoDB value type: " (type x) "."
                                 " See `freeze` for serialization.")))))

(comment
  (map clj-val->db-val [  "a"    1 3.14    (.getBytes "a")    (freeze :a)
                        #{"a"} #{1 3.14} #{(.getBytes "a")} #{(freeze :a)}]))

(def db-item->clj-item (partial utils/keyword-map db-val->clj-val))
(def clj-item->db-item (partial utils/name-map    clj-val->db-val))

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
    (.setAttributeName (name key-name))
    (.setKeyType (utils/enum key-type))))

(defn- key-schema
  "Returns a [{<hash-key> KeySchemaElement}], or
             [{<hash-key> KeySchemaElement} {<range-key> KeySchemaElement}]
   vector for use as a table/index primary key."
  [hash-key & [range-key]]
  (let [schema [(key-schema-element (:name hash-key) :hash)]]
    (if range-key
      (conj schema (key-schema-element (:name range-key) :range))
      schema)))

(defn- provisioned-throughput "Returns a new ProvisionedThroughput object."
  [{read-units :read write-units :write}]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits  (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn- attribute-definitions
  "[{:name _ :type _} ...] -> [AttributeDefinition ...]"
  [defs]
  (mapv
   (fn [{key-name :name key-type :type :as def}]
     (doto (AttributeDefinition.)
       (.setAttributeName (name key-name))
       (.setAttributeType (utils/enum key-type))))
   defs))

(defn- local-indexes
  "[{:name _ :range-key _ :projection _} ...] indexes -> [LocalSecondaryIndex ...]"
  [hash-key indexes]
  (mapv
   (fn [{index-name :name
        :keys [range-key projection]
        :or   {projection :all}
        :as   index}]
     (doto (LocalSecondaryIndex.)
       (.setIndexName  (name index-name))
       (.setKeySchema  (key-schema hash-key range-key))
       (.setProjection
        (let [pr    (Projection.)
              ptype (if (coll? projection) :include projection)]
          (.setProjectionType pr (utils/enum ptype))
          (when (= ptype :include) (.setNonKeyAttributes pr (mapv name projection)))
          pr))))
   indexes))

(defn- expected-values
  "{<attr> <cond> ...} -> {<attr> ExpectedAttributeValue ...}"
  [expected]
  (when (seq expected)
    (utils/name-map
     #(case % (true  ::exists)     (ExpectedAttributeValue. true)
              (false ::not-exists) (ExpectedAttributeValue. false)
              (ExpectedAttributeValue. (clj-val->db-val %)))
     expected)))

(defn- keys-and-attrs "Returns a new KeysAndAttributes object."
  [{:keys [key vals attrs consistent?] :as request}]
  (doto (KeysAndAttributes.)
    (.setKeys (for [v vals] {(name key) (clj-val->db-val v)}))
    (.setAttributesToGet (when attrs (mapv name attrs)))
    (.setConsistentRead  consistent?)))

(defn- batch-request-items
  "{<table> <request> ...} -> {<table> KeysAndAttributes> ...}"
  [requests]
  (into {} (for [[table request] requests]
             [(name table) (keys-and-attrs request)])))

;;;; Coercion - objects

(defprotocol AsMap (as-map [x]))

(defmacro ^:private am-item-result [result get-form]
  `(when-let [get-form# ~get-form]
     (with-meta (db-item->clj-item get-form#)
       {:consumed-capacity (.getConsumedCapacity ~result)})))

(defmacro ^:private am-query|scan-result [result]
  `(let [result# ~result]
     {:items (mapv db-item->clj-item (.getItems result#))
      :count (.getCount result#)
      :consumed-capacity (.getConsumedCapacity result#)
      :last-key (as-map (.getLastEvaluatedKey result#))}))

(extend-protocol AsMap
  nil                 (as-map [_] nil)
  java.util.ArrayList (as-map [a] (mapv as-map a))
  java.util.HashMap   (as-map [m] (utils/keyword-map as-map m))

  AttributeValue      (as-map [v] (db-val->clj-val v))
  KeySchemaElement    (as-map [e] {:name (keyword (.getAttributeName e))
                                   :type (utils/un-enum (.getKeyType e))})
  KeysAndAttributes
  (as-map [x]
    (merge
     (when-let [a (.getAttributesToGet x)] {:attrs (mapv keyword a)})
     (when-let [c (.getConsistentRead  x)] {:consistent? c})
     (when-let [k (.getKeys            x)] {:keys (mapv db-item->clj-item k)})))

  GetItemResult       (as-map [r] (am-item-result r (.getItem r)))
  PutItemResult       (as-map [r] (am-item-result r (.getAttributes r)))
  UpdateItemResult    (as-map [r] (am-item-result r (.getAttributes r)))
  DeleteItemResult    (as-map [r] (am-item-result r (.getAttributes r)))

  QueryResult         (as-map [r] (am-query|scan-result r))
  ScanResult          (as-map [r] (am-query|scan-result r))

  CreateTableResult   (as-map [r] r) ; TODO
  UpdateTableResult   (as-map [r] r) ; TODO
  DeleteTableResult   (as-map [r] r) ; TODO

  BatchGetItemResult
  (as-map [r]
    {:responses         (utils/keyword-map as-map (.getResponses       r))
     :unprocessed-keys  (utils/keyword-map as-map (.getUnprocessedKeys r))
     :consumed-capacity (.getConsumedCapacity r)})
  BatchWriteItemResult
  (as-map [r]
    {:unprocessed-items (utils/keyword-map as-map (.getUnprocessedItems r))
     :consumed-capacity (.getConsumedCapacity r)})

  DescribeTableResult
  (as-map [r] (let [t (.getTable r)]
                {:name          (.getTableName t)
                 :creation-date (.getCreationDateTime t)
                 :item-count    (.getItemCount t)
                 :size          (.getTableSizeBytes t)
                 :key-schema    (as-map (.getKeySchema t))
                 :throughput    (as-map (.getProvisionedThroughput t))
                 :indexes       (as-map (.getLocalSecondaryIndexes t))
                 :status        (utils/un-enum (.getTableStatus t))}))

  LocalSecondaryIndexDescription
  (as-map [d] {:name       (.getIndexName d)
               :size       (.getIndexSizeBytes d)
               :item-count (.getItemCount d)
               :key-schema (as-map (.getKeySchema d))
               :projection (as-map (.getProjection d))})
  ProvisionedThroughputDescription
  (as-map [d] {:read                (.getReadCapacityUnits d)
               :write               (.getWriteCapacityUnits d)
               :last-decrease       (.getLastDecreaseDateTime d)
               :last-increase       (.getLastIncreaseDateTime d)
               :num-decreases-today (.getNumberOfDecreasesToday d)}))

;;;; API - tables

(defn list-tables "Returns a vector of table names."
  [creds] (->> (db-client creds) (.listTables) (.getTableNames) (mapv keyword)))

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [creds table]
  (try (as-map (.describeTable (db-client creds)
                (doto (DescribeTableRequest.) (.setTableName (name table)))))
       (catch ResourceNotFoundException _ nil)))

(defn create-table
  "Creates a table with the given map of options:
    :name       - (required) table name.
    :throughput - (required) {:read <units> :write <units>}.
    :hash-key   - (required) {:name _ :type <#{:s :n :ss :ns :b :bs}>}.
    :range-key  - (optional) {:name _ :type <#{:s :n :ss :ns :b :bs}>}.
    :indexes    - (optional) [{:name _ :range-key _
                               :projection #{:all :keys-only [<attr> ...]}}]"
  [creds {table-name :name
          :keys [throughput hash-key range-key indexes]
          :or   {throughput {:read 1 :write 1}}}]
  (as-map
   (.createTable (db-client creds)
     (let [attr-defs (->> (conj [] hash-key range-key)
                          (concat (map :range-key indexes))
                          (filter identity))]
       (doto (CreateTableRequest.)
         (.setTableName (name table-name))
         (.setKeySchema (key-schema hash-key range-key))
         (.setAttributeDefinitions  (attribute-definitions attr-defs))
         (.setProvisionedThroughput (provisioned-throughput throughput))
         (.setLocalSecondaryIndexes (local-indexes hash-key indexes)))))))

(defn ensure-table "Creates a table iff it doesn't already exist."
  [creds {table-name :name :as opts}]
  (when-not (describe-table creds table-name) (create-table creds opts)))

(defn update-table
  "Updates a table. Ref. http://goo.gl/Bj9TC for important throughput
  upgrade/downgrade limits."
  [creds {:keys [table throughput]}]
  (as-map
   (.updateTable (db-client creds)
     (let [utr (UpdateTableRequest.)]
       (when table      (.setTableName utr (name table)))
       (when throughput (.setProvisionedThroughput utr (provisioned-throughput
                                                        throughput)))))))

(defn delete-table "Deletes a table."
  [creds table-name]
  (as-map (.deleteTable (db-client creds) (DeleteTableRequest. (name table-name)))))

;;;; API - items

(defn get-item
  "Retrieves an item from a table by its primary key,
  {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}"
  [creds table prim-key & [{:keys [consistent? attrs-to-get]}]]
  (as-map
   (.getItem (db-client creds)
    (doto (GetItemRequest.)
      (.setTableName      (name table))
      (.setKey            (clj-item->db-item prim-key))
      (.setConsistentRead  consistent?)
      (.setAttributesToGet attrs-to-get)))))

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return   - e/o #{:none :all-old :updated-old :all-new :updated-new}
    :expected - a map of attribute/condition pairs, all of which must be met for
                the operation to succeed. e.g.:
                  {<attr> \"expected-value\" ...}
                  {<attr> true  ...} ; Attribute must exist
                  {<attr> false ...} ; Attribute must not exist"
  [creds table item & [{:keys [return expected]
                        :or   {return :none}}]]
  (as-map
   (.putItem (db-client creds)
     (doto (PutItemRequest.)
       (.setTableName    (name table))
       (.setItem         (clj-item->db-item item))
       (.setExpected     (expected-values expected))
       (.setReturnValues (utils/enum return))))))

(defn update-item
  "Updates an item in a table by its primary key.
  See `put-item` for option docs."
  [creds table prim-key update-map & [{:keys [return expected]
                                       :or   {return :none}}]]
  (as-map
   (.updateItem (db-client creds)
     (doto (UpdateItemRequest.)
       (.setTableName        (name table))
       (.setKey              (clj-item->db-item prim-key))
       (.setAttributeUpdates nil) ; TODO
       (.setExpected         (expected-values expected))
       (.setReturnValues     (utils/enum return))))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [creds table prim-key & [{:keys [return expected]
                            :or   {return :none}}]]
  (as-map
   (.deleteItem (db-client creds)
     (doto (DeleteItemRequest.)
       (.setTableName    (name table))
       (.setKey          (clj-item->db-item prim-key))
       (.setExpected     (expected-values expected))
       (.setReturnValues (utils/enum return))))))

;;;; API - batch ops

(defn batch-get-item
  "Retrieves a batch of items in a single request.
  Limits apply, Ref. http://goo.gl/Bj9TC.

  (batch-get-item creds
    {:users {:key  :names
             :vals [\"alice\" \"bob\"]}
     :posts {:key   :id
             :vals  [1 2 3]
             :attrs [:timestamp :subject]
             :consistent? true}})"
  [creds requests]
  (as-map
    (.batchGetItem (db-client creds)
      (doto (BatchGetItemRequest.)
        (.setRequestItems (batch-request-items requests))))))

(defn- put-request    [i] (doto (PutRequest.)    (.setItem (clj-item->db-item i))))
(defn- delete-request [i] (doto (DeleteRequest.) (.setKey  (clj-item->db-item i))))
(defn- write-request  [action item]
  (case action
    :delete (doto (WriteRequest.) (.setDeleteRequest (delete-request item)))
    :put    (doto (WriteRequest.) (.setPutRequest    (put-request    item)))))

(defn batch-write-item
  "Executes a batch of Puts and/or Deletes in a single request.
   Limits apply, Ref. http://goo.gl/Bj9TC. No transaction guarantees are
   provided, nor conditional puts.

   (batch-write-item creds
     [:put    :users {:user-id 1 :username \"sally\"}]
     [:put    :users {:user-id 2 :username \"jane\"}]
     [:delete :users {:user-id 3}])"
  [creds & requests]
  (as-map
    (.batchWriteItem (db-client creds)
      (doto (BatchWriteItemRequest.)
        (.setRequestItems
         (utils/name-map
          (fn [reqs]
            (reduce (fn [v [action _ item]] (conj v (write-request action item)))
                    [] (partition 3 (flatten reqs))))
          (group-by (fn [[_ table _]] table) requests)))))))

;;;; API - queries & scans

;;;; TODO Continue code walk-through below

(defn- scan-request
  "Create a ScanRequest object."
  [table {:keys [limit count after]}]
  (let [sr (ScanRequest. table)]
    (when limit (.setLimit sr (int limit)))
    (when after (.setExclusiveStartKey sr (clj-item->db-item after)))
    sr))

(defn scan
  "Return the items in a table. Takes the following options:
    :limit - the maximum number of items to return
    :after - only return results after this key

  The items are returned as a map with the following keys:
    :items    - the list of items returned
    :count    - the count of items matching the query
    :last-key - the last evaluated key (useful for paging) "
  [creds table & [options]]
  (as-map
   (.scan (db-client creds)
    (scan-request table options))))

(defn- set-hash-condition
  "Create a map of specifying the hash-key condition for query"
  [hash-key]
  (utils/name-map ; TODO Correct? Used to be fmap
   #(doto (Condition.)
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
        op (->> operator utils/enum)]
    (operator-map (keyword op) op)))

(defn- query-request
  "Create a QueryRequest object."
  [table hash-key range-clause {:keys [order limit after count consistent? attrs index]}]
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
    (when consistent? (.setConsistentRead qr consistent?)) ; TODO
    (when after      (.setExclusiveStartKey qr (clj-item->db-item after)))
    (when index      (.setIndexName qr index))
    qr))

(defn- extract-range [[range options]]
  (if (and (map? range) (not options))
    [nil range]
    [range options]))

(defn query
  "Return the items in a table matching the supplied hash key,
  defined in the form {:hash-attr hash-value}.
  Can specify a range clause if the table has a range-key ie. `(:range-attr >= 234)
  Takes the following options:
    :order - may be :asc or :desc (defaults to :asc)
    :attrs - limit the values returned to the following attribute names
    :limit - the maximum number of items to return
    :after - only return results after this key
    :consistent? - return a consistent read iff logical true
    :index - the secondary index to query

  The items are returned as a map with the following keys:
    :items - the list of items returned
    :count - the count of items matching the query
    :last-key - the last evaluated key (useful for paging)"
  [creds table hash-key & range-and-options]
  (let [[range options] (extract-range range-and-options)]
    (as-map
     (.query (db-client creds)
             (query-request table hash-key range options)))))

;;;; README

(comment
  (require '[taoensso.faraday :as far])

  (def my-creds {:access-key ""
                 :secret-key ""})

  (far/list-tables my-creds))