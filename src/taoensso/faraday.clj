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
;; * Finish up outstanding API: as-map types, Rotary PRs, non-PR forks.
;; * Benchmarks, further tests.
;; * Long-term: async API, auto throughput adjusting, ...?

;;;; Connections

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

;;;; Exceptions

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

;;;; Coercion - objects

(def db-item->clj-item (partial utils/keyword-map db-val->clj-val))
(def clj-item->db-item (partial utils/name-map    clj-val->db-val))

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
      :last-prim-kv (as-map (.getLastEvaluatedKey result#))}))

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
  ScanResult          (as-map [r] (assoc (am-query|scan-result r)
                                    :scanned-count (.getScannedCount r)))

  CreateTableResult   (as-map [r] r) ; TODO
  UpdateTableResult   (as-map [r] r) ; TODO
  DeleteTableResult   (as-map [r] r) ; TODO

  BatchGetItemResult
  (as-map [r]
    ;; TODO Better way of grouping responses & unprocessed-keys? Might not be
    ;; possible unless we force the inclusion of the prim-kv in :attrs.
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

(defn- attribute-defs "[{:name _ :type _} ...] defs -> [AttributeDefinition ...]"
  [hash-key range-key indexes]
  (let [defs (->> (conj [] hash-key range-key)
                  (concat (map :range-key indexes))
                  (filterv identity))]
    (mapv
     (fn [{key-name :name key-type :type :as def}]
       (doto (AttributeDefinition.)
         (.setAttributeName (name key-name))
         (.setAttributeType (utils/enum key-type))))
     defs)))

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

(defn create-table
  "Creates a table with options:
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
     (doto (CreateTableRequest.)
       (.setTableName (name table-name))
       (.setKeySchema (key-schema hash-key range-key))
       (.setProvisionedThroughput (provisioned-throughput throughput))
       (.setAttributeDefinitions  (attribute-defs hash-key range-key indexes))
       (.setLocalSecondaryIndexes (local-indexes hash-key indexes))))))

(defn ensure-table "Creates a table iff it doesn't already exist."
  [creds {table-name :name :as opts}]
  (when-not (describe-table creds table-name) (create-table creds opts)))

(defn update-table
  "Updates a table. Ref. http://goo.gl/Bj9TC for important throughput
  upgrade/downgrade limits."
  [creds {table-name :name :keys [throughput]}]
  (as-map
   (.updateTable (db-client creds)
     (doto (UpdateTableRequest.)
       (.setTableName             (when table-name (name table-name)))
       (.setProvisionedThroughput (when throughput (provisioned-throughput
                                                    throughput)))))))

(defn delete-table "Deletes a table, go figure."
  [creds table-name]
  (as-map (.deleteTable (db-client creds) (DeleteTableRequest. (name table-name)))))

;;;; API - items

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs     - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs       - attrs to return, [<attr> ...].
    :consistent? - use strongly (rather than eventually) consistent reads?"
  [creds table prim-kvs & [{:keys [attrs consistent?]}]]
  (as-map
   (.getItem (db-client creds)
    (doto (GetItemRequest.)
      (.setTableName      (name table))
      (.setKey            (clj-item->db-item prim-kvs))
      (.setConsistentRead  consistent?)
      (.setAttributesToGet attrs)))))

(defn- expected-values
  "{<attr> <cond> ...} -> {<attr> ExpectedAttributeValue ...}"
  [expected-map]
  (when (seq expected-map)
    (utils/name-map
     #(if (= false %)
        (ExpectedAttributeValue. false)
        (ExpectedAttributeValue. (clj-val->db-val %)))
     expected-map)))

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return   - e/o #{:none :all-old :updated-old :all-new :updated-new}.
    :expected - a map of item attribute/condition pairs, all of which must be
                met for the operation to succeed. e.g.:
                  {<attr> <expected-value> ...}
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

(defn- attribute-updates
  "{<attr> [<action> <value>] ...} -> {<attr> AttributeValueUpdate ...}"
  [update-map]
  (when (seq update-map)
    (utils/name-map
     (fn [[action val]] (AttributeValueUpdate. (when val (clj-val->db-val val))
                                              (utils/enum action)))
     update-map)))

(defn update-item
  "Updates an item in a table by its primary key with options:
    prim-kvs   - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    update-map - {<attr> [<#{:put :add :delete}> <optional value>]}.
    :return    - e/o #{:none :all-old :updated-old :all-new :updated-new}.
    :expected  - a map of item attribute/condition pairs, all of which must be
                 met for the operation to succeed. e.g.:
                   {<attr> <expected-value> ...}
                   {<attr> false ...} ; Attribute must not exist"
  [creds table prim-kvs update-map & [{:keys [return expected]
                                       :or   {return :none}}]]
  (as-map
   (.updateItem (db-client creds)
     (doto (UpdateItemRequest.)
       (.setTableName        (name table))
       (.setKey              (clj-item->db-item prim-kvs))
       (.setExpected         (expected-values expected))
       (.setReturnValues     (utils/enum return))
       (.setAttributeUpdates (attribute-updates update-map))))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [creds table prim-kvs & [{:keys [return expected]
                            :or   {return :none}}]]
  (as-map
   (.deleteItem (db-client creds)
     (doto (DeleteItemRequest.)
       (.setTableName    (name table))
       (.setKey          (clj-item->db-item prim-kvs))
       (.setExpected     (expected-values expected))
       (.setReturnValues (utils/enum return))))))

;;;; API - batch ops

(defn- batch-request-items
  "{<table> <request> ...} -> {<table> KeysAndAttributes> ...}"
  [requests]
  (utils/name-map
   (fn [{:keys [prim-kvs attrs consistent?]}]
     (doto (KeysAndAttributes.)
       (.setKeys
        (->> (for [[k v-or-vs] prim-kvs] ; {<k> <v-or-vs> ...} -> [{k v} ...]
               (if (coll? v-or-vs) (for [v v-or-vs] [k v]) (list [k v-or-vs])))
             (reduce into [])
             (mapv (fn [[k v]] {(name k) (clj-val->db-val v)}))))
       (.setAttributesToGet (when attrs (mapv name attrs)))
       (.setConsistentRead  consistent?)))
   requests))

(comment (batch-request-items {:my-table {:prim-kvs {:my-hash "1"} :attrs []}}))

(defn batch-get-item
  "Retrieves a batch of items in a single request.
  Limits apply, Ref. http://goo.gl/Bj9TC.

  (batch-get-item creds
    {:users {:prim-kvs {:name \"alice\"}}
     :posts {:prim-kvs {:id [1 2 3]} ; Matches multiple key values
             :attrs    [:timestamp :subject]
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
    :put    (doto (WriteRequest.) (.setPutRequest    (put-request    item)))
    :delete (doto (WriteRequest.) (.setDeleteRequest (delete-request item)))))

(defn batch-write-item
  "Executes a batch of Puts and/or Deletes in a single request.
   Limits apply, Ref. http://goo.gl/Bj9TC. No transaction guarantees are
   provided, nor conditional puts.

   Request execution order is undefined. TODO Alternatives?

   (batch-write-item creds
     {:users {:put    [{:user-id 1 :username \"sally\"}
                       {:user-id 2 :username \"jane\"}]
              :delete [{:user-id 3}]}})"
  [creds requests]
  (as-map
    (.batchWriteItem (db-client creds)
      (doto (BatchWriteItemRequest.)
        (.setRequestItems
         (utils/name-map
          ;; {<table> <table-reqs> ...} -> {<table> [WriteRequest ...] ...}
          (fn [table-request]
            (reduce into [] (for [action (keys table-request)
                                  :let [items (table-request action)]]
                              (mapv (partial write-request action) items))))
          requests))))))

;;;; API - queries & scans

(defn- ^String enum-op [operator]
  (-> operator {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"} (or operator)
      utils/enum))

(defn- query|scan-conditions
  "{<attr> [operator <values>] ...} -> {<attr> Condition ...}"
  [conditions]
  (when (seq conditions)
    (utils/name-map
     (fn [[operator values]]
       (doto (Condition.)
         (.setComparisonOperator (enum-op operator))
         (.setAttributeValueList (mapv clj-val->db-val values))))
     conditions)))

(defn query
  "Retries items from a table (indexed) with options:
    prim-key-conds - {<key-attr> [comparison-operator <values>] ...}.
    :last-prim-kv  - primary key-val from which to eval, useful for paging.
    :return        - e/o #{:all-attributes :all-projected-attributes :count
                           [<attr> ...]}.
    :index         - name of a secondary index to query.
    :order         - index scaning order e/o #{:asc :desc}.
    :limit         - max num >=1 of items to eval (≠ num of matching items).
    :consistent?   - use strongly (rather than eventually) consistent reads?

  For unindexed item retrievel see `scan`."
  [creds table prim-key-conds
   & [{:keys [last-prim-kv return index order limit consistent?]
       :or   {order :asc}}]]
  (as-map
   (.query (db-client creds)
     (doto (QueryRequest.)
       (.setTableName         (name table))
       (.setKeyConditions     (query|scan-conditions prim-key-conds))
       (.setExclusiveStartKey (when last-prim-kv (clj-item->db-item last-prim-kv)))
       (.setAttributesToGet   (when     (coll? return) return))
       (.setSelect            (when-not (coll? return) (utils/enum return)))
       (.setIndexName         index)
       (.setScanIndexForward  (case order :asc true :desc false))
       (.setLimit             (when limit (long limit)))
       (.setConsistentRead    consistent?)))))

(defn scan
  "Retrieves items from a table (unindexed) with options:
    :attr-conds     - {<attr> [comparison-operator <values>] ...}.
    :limit          - max num >=1 of items to eval (≠ num of matching items).
    :last-prim-kv   - primary key-val from which to eval, useful for paging.
    :return         - e/o #{:all-attributes :all-projected-attributes :count
                            [<attr> ...]}.
    :total-segments - total number of parallel scan segments.
    :segment        - calling worker's segment number (>=0, <=total-segments).

  See also `scan-parallel` for automatic parallelization & segment control."
  [creds table
   & [{:keys [attr-conds last-prim-kv return limit total-segments segment]
       :or   {return :all-attributes}}]]
  (as-map
   (.scan (db-client creds)
     (doto (ScanRequest.)
       (.setTableName         (name table))
       (.setScanFilter        (query|scan-conditions attr-conds))
       (.setExclusiveStartKey (when last-prim-kv (clj-item->db-item last-prim-kv)))
       (.setAttributesToGet   (when     (coll? return) return))
       (.setSelect            (when-not (coll? return) (utils/enum return)))
       (.setLimit             (when limit (long limit)))
       (.setTotalSegments     (when total-segments (long total-segments)))
       (.setSegment           (when segment        (long segment)))))))

(defn scan-parallel
  "Like `scan` but starts a number of worker threads and automatically handles
  parallel scan options (:total-segments and :segment). Returns a vector of
  `scan` results.

  Ref. http://goo.gl/KLwnn (official parallel scan documentation)."
  [creds table total-segments & [opts]]
  (let [opts (assoc opts :total-segments total-segments)]
    (->> (mapv (fn [seg] (future (scan creds table (assoc opts :segment seg))))
               (range total-segments))
         (mapv deref))))

;;;; README

(comment
  (require '[taoensso.faraday :as far])

  (def my-creds {:access-key ""
                 :secret-key ""})

  (far/list-tables my-creds)

  (far/create-table my-creds
    {:name :my-table
     :throughput {:read 1 :write 1}
     :hash-key   {:name :id
                  :type :n}})

  (far/put-item my-creds
    :my-table
    {:id 0 :name "Steve" :age 22 :data (far/freeze {:vector    [1 2 3]
                                                    :set      #{1 2 3}
                                                    :rational (/ 22 7)})})

  (far/get-item my-creds :my-table {:id 0}))