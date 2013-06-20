(ns taoensso.faraday
  "Clojure DynamoDB client. Adapted from Rotary by James Reeves.
  Ref. https://github.com/weavejester/rotary (Rotary),
       http://goo.gl/22QGA (DynamoDBv2 API)

  Definitions (with '=>' as 'implies'):
    * item        => {<attribute> <value>}.
    * key         => hash OR range key => special attribute.
    * primary key => hash key WITH optional range key.
    * attribute   ≠> key (i.e. does not imply)."

  {:author "Peter Taoussanis"}
  (:require [clojure.string         :as str]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.faraday.utils :as utils :refer (coll?* doto-maybe)])
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

;;;; Connections

(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied IAM credentials."
  (memoize
   (fn [{:keys [access-key secret-key endpoint proxy-host proxy-port
               conn-timeout max-conns max-error-retry socket-timeout] :as creds}]
     (assert (and access-key secret-key) "Please provide valid IWS credentials!")
     (let [aws-creds     (BasicAWSCredentials. access-key secret-key)
           client-config (doto-maybe (ClientConfiguration.) g
                           proxy-port      (.setProxyHost         g)
                           conn-timeout    (.setConnectionTimeout g)
                           max-conns       (.setMaxConnections    g)
                           max-error-retry (.setMaxErrorRetry     g)
                           socket-timeout  (.setSocketTimeout     g))]
       (doto-maybe (AmazonDynamoDBClient. aws-creds client-config) g
         endpoint (.setEndpoint g))))))

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

(def ^:private nt-freeze (comp #(ByteBuffer/wrap %) nippy-tools/freeze))
(def ^:private nt-thaw   (comp nippy-tools/thaw #(.array ^ByteBuffer %)))

(utils/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(utils/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type to be subject to automatic de/serialization with
  Nippy.")

(defn- freeze?     [x] (or (nippy-tools/wrapped-for-freezing? x)
                           (utils/bytes? x)))
(defn- str->num    [^String s] (if (.contains s ".") (Double. s) (Long. s)))
(defn- simple-num? [x] (or (instance? Long    x)
                           (instance? Double  x)
                           (instance? Integer x)
                           (instance? Float   x)))

(defn- db-val->clj-val "Returns the Clojure value of given AttributeValue object."
  [^AttributeValue x]
  (or (.getS x)
      (some->> (.getN  x) str->num)
      (some->> (.getB  x) nt-thaw)
      (some->> (.getSS x) (into #{}))
      (some->> (.getNS x) (mapv str->num) (into #{}))
      (some->> (.getBS x) (mapv nt-thaw) (into #{}))))

(defn- clj-val->db-val "Returns an AttributeValue object for given Clojure value."
  ^AttributeValue [x]
  (cond
   (string? x)
   (if (.isEmpty ^String x)
     (throw (Exception. "Invalid DynamoDB value: \"\""))
     (doto (AttributeValue.) (.setS x)))

   (simple-num? x) (doto (AttributeValue.) (.setN (str x)))
   (freeze?     x) (doto (AttributeValue.) (.setB (nt-freeze x)))

   (set? x)
   (if (empty? x)
     (throw (Exception. "Invalid DynamoDB value: empty set"))
     (cond
      (every? string?     x) (doto (AttributeValue.) (.setSS x))
      (every? simple-num? x) (doto (AttributeValue.) (.setNS (mapv str x)))
      (every? freeze?     x) (doto (AttributeValue.) (.setBS (mapv nt-freeze x)))
      :else (throw (Exception. (str "Invalid DynamoDB value: set of invalid type"
                                    " or more than one type")))))

   (instance? AttributeValue x) x
   :else (throw (Exception. (str "Unknown DynamoDB value type: " (type x) "."
                                 " See `freeze` for serialization.")))))

(comment
  (mapv clj-val->db-val [  "a"    1 3.14    (.getBytes "a")    (freeze :a)
                         #{"a"} #{1 3.14} #{(.getBytes "a")} #{(freeze :a)}]))

;;;; Coercion - objects

(def db-item->clj-item (partial utils/keyword-map db-val->clj-val))
(def clj-item->db-item (partial utils/name-map    clj-val->db-val))

(defprotocol AsMap (as-map [x]))

(defmacro ^:private am-item-result [result get-form]
  `(when-let [get-form# ~get-form]
     (with-meta (db-item->clj-item get-form#)
       {:consumed-capacity (.getConsumedCapacity ~result)})))

(defmacro ^:private am-query|scan-result [result & [meta]]
  `(let [result# ~result]
     (with-meta (mapv db-item->clj-item (.getItems result#))
       (merge {:count (.getCount result#)
               :consumed-capacity (.getConsumedCapacity result#)
               :last-prim-kvs (as-map (.getLastEvaluatedKey result#))}
              ~meta))))

(extend-protocol AsMap
  nil                 (as-map [_] nil)
  java.util.ArrayList (as-map [a] (mapv as-map a))
  java.util.HashMap   (as-map [m] (utils/keyword-map as-map m))

  AttributeValue      (as-map [v] (db-val->clj-val v))
  AttributeDefinition (as-map [d] {:name (keyword       (.getAttributeName d))
                                   :type (utils/un-enum (.getAttributeType d))})
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
  ScanResult          (as-map [r] (am-query|scan-result r
                                    {:scanned-count (.getScannedCount r)}))

  CreateTableResult   (as-map [r] r) ; TODO
  UpdateTableResult   (as-map [r] r) ; TODO
  DeleteTableResult   (as-map [r] r) ; TODO

  BatchGetItemResult
  (as-map [r]
    (with-meta (utils/keyword-map as-map (.getResponses r))
      {:unprocessed-keys  (utils/keyword-map as-map (.getUnprocessedKeys r))
       :consumed-capacity (.getConsumedCapacity r)}))
  BatchWriteItemResult
  (as-map [r]
    {:unprocessed-items (utils/keyword-map as-map (.getUnprocessedItems r))
     :consumed-capacity (.getConsumedCapacity r)})

  DescribeTableResult
  (as-map [r] (let [t (.getTable r)]
                {:name          (keyword (.getTableName t))
                 :creation-date (.getCreationDateTime t)
                 :item-count    (.getItemCount t)
                 :size          (.getTableSizeBytes t)
                 :throughput    (as-map (.getProvisionedThroughput t))
                 :indexes       (as-map (.getLocalSecondaryIndexes t))
                 :status        (utils/un-enum (.getTableStatus t))
                 :prim-keys
                 (let [schema (as-map (.getKeySchema t))
                       defs   (as-map (.getAttributeDefinitions t))]
                   (merge-with merge
                     (reduce-kv (fn [m k v] (assoc m (:name v) {:key-type  (:type v)}))
                                {} schema)
                     (reduce-kv (fn [m k v] (assoc m (:name v) {:data-type (:type v)}))
                                {} defs)))}))

  LocalSecondaryIndexDescription
  (as-map [d] {:name       (keyword (.getIndexName d))
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

(defn block-while-table-status
  "BLOCKS to poll for a change to table's status. On status change, polling will
  terminate and the table's new description will be returned."
  [creds table-name status & [{:keys [poll-ms timeout-ms timeout-val]
                               :or   {poll-ms 5000}}]]
  (assert (#{:creating :updating :deleting :active} (utils/un-enum status))
          (throw (Exception. (str "Invalid table status: " status))))
  (let [polling-future
        (future
          (loop []
            (let [current-descr (describe-table creds table-name)]
              (if-not (= (:status current-descr) (utils/un-enum status))
                current-descr
                (do (Thread/sleep poll-ms)
                    (recur))))))]
    (if-not timeout-ms
      (deref polling-future)
      (let [deref-result (deref polling-future timeout-ms ::timeout)]
        (if-not (= deref-result ::timeout)
          deref-result
          (do (future-cancel polling-future)
              timeout-val))))))

(comment
  (create-table mc {:name "delete-me5"
                    :throughput  {:read 1 :write 1}
                    :hash-keydef {:name :id :type :s}})
  (def descr (describe-table mc "delete-me5"))
  (block-while-table-status mc "delete-me5" :creating) ; ~53000ms
  )

(defn- key-schema-element "Returns a new KeySchemaElement object."
  [key-name key-type]
  (doto (KeySchemaElement.)
    (.setAttributeName (name key-name))
    (.setKeyType (utils/enum key-type))))

(defn- key-schema
  "Returns a [{<hash-key> KeySchemaElement}], or
             [{<hash-key> KeySchemaElement} {<range-key> KeySchemaElement}]
   vector for use as a table/index primary key."
  [hash-keydef & [range-keydef]]
  (cond-> [(key-schema-element (:name hash-keydef) :hash)]
          range-keydef (conj (key-schema-element (:name range-keydef) :range))))

(defn- provisioned-throughput "Returns a new ProvisionedThroughput object."
  [{read-units :read write-units :write :as throughput}]
  (assert (and read-units write-units)
          (str "Malformed throughput: " throughput))
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits  (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn- attribute-defs "[{:name _ :type _} ...] defs -> [AttributeDefinition ...]"
  [hash-keydef range-keydef indexes]
  (let [defs (->> (conj [] hash-keydef range-keydef)
                  (concat (mapv :range-keydef indexes))
                  (filterv identity))]
    (mapv
     (fn [{key-name :name key-type :type :as def}]
       (assert (and key-name key-type) (str "Malformed def: " def))
       (doto (AttributeDefinition.)
         (.setAttributeName (name key-name))
         (.setAttributeType (utils/enum key-type))))
     defs)))

(defn- local-indexes
  "[{:name _ :range-keydef _ :projection _} ...] indexes -> [LocalSecondaryIndex ...]"
  [hash-keydef indexes]
  (mapv
   (fn [{index-name :name
        :keys [range-keydef projection]
        :or   {projection :all}
        :as   index}]
     (assert (and index-name range-keydef projection)
             (str "Malformed index: " index))
     (doto (LocalSecondaryIndex.)
       (.setIndexName  (name index-name))
       (.setKeySchema  (key-schema hash-keydef range-keydef))
       (.setProjection
        (let [pr    (Projection.)
              ptype (if (coll?* projection) :include projection)]
          (.setProjectionType pr (utils/enum ptype))
          (when (= ptype :include) (.setNonKeyAttributes pr (mapv name projection)))
          pr))))
   indexes))

(defn create-table
  "Creates a table with options:
    :name         - (required) table name.
    :throughput   - (required) {:read <units> :write <units>}.
    :hash-keydef  - (required) {:name _ :type <#{:s :n :ss :ns :b :bs}>}.
    :range-keydef - (optional) {:name _ :type <#{:s :n :ss :ns :b :bs}>}.
    :indexes      - (optional) [{:name _ :range-keydef _
                                 :projection #{:all :keys-only [<attr> ...]}}].
    :block?       - (optional) blocks for table to actually be active."
  [creds {table-name :name
          :keys [throughput hash-keydef range-keydef indexes block?]
          :or   {throughput {:read 1 :write 1}}}]

  (let [request-result
        (as-map
         (.createTable (db-client creds)
           (doto (CreateTableRequest.)
             (.setTableName (name table-name))
             (.setKeySchema (key-schema hash-keydef range-keydef))
             (.setProvisionedThroughput (provisioned-throughput throughput))
             (.setAttributeDefinitions  (attribute-defs hash-keydef range-keydef indexes))
             (.setLocalSecondaryIndexes (local-indexes hash-keydef indexes)))))]

    (if-not block?
      request-result
      (do (block-while-table-status creds table-name :creating)
          request-result))))

(comment
  (time (create-table mc {:name "delete-me7"
                          :throughput  {:read 1 :write 1}
                          :hash-keydef {:name :id :type :s}
                          :block?      true})))

(defn ensure-table "Creates a table iff it doesn't already exist."
  [creds {table-name :name :as opts}]
  (when-not (describe-table creds table-name) (create-table creds opts)))

(defn update-table
  "Updates a table. Ref. http://goo.gl/Bj9TC for important throughput
  upgrade/downgrade limits."
  [creds {table-name :name :keys [throughput]}]
  (as-map
   (.updateTable (db-client creds)
     (doto-maybe (UpdateTableRequest.) g
       table-name (.setTableName (name g))
       throughput (.setProvisionedThroughput (provisioned-throughput g))))))

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
    (doto-maybe (GetItemRequest.) g
      :always     (.setTableName       (name table))
      :always     (.setKey             (clj-item->db-item prim-kvs))
      consistent? (.setConsistentRead  g)
      attrs       (.setAttributesToGet (mapv name g))))))

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
     (doto-maybe (PutItemRequest.) g
       :always  (.setTableName    (name table))
       :always  (.setItem         (clj-item->db-item item))
       expected (.setExpected     (expected-values g))
       return   (.setReturnValues (utils/enum g))))))

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
    :expected  - {<attr> <#{<expected-value> false}> ...}."
  [creds table prim-kvs update-map & [{:keys [return expected]
                                       :or   {return :none}}]]
  (as-map
   (.updateItem (db-client creds)
     (doto-maybe (UpdateItemRequest.) g
       :always  (.setTableName        (name table))
       :always  (.setKey              (clj-item->db-item prim-kvs))
       :always  (.setAttributeUpdates (attribute-updates update-map))
       expected (.setExpected         (expected-values g))
       return   (.setReturnValues     (utils/enum g))))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [creds table prim-kvs & [{:keys [return expected]
                            :or   {return :none}}]]
  (as-map
   (.deleteItem (db-client creds)
     (doto-maybe (DeleteItemRequest.) g
       :always  (.setTableName    (name table))
       :always  (.setKey          (clj-item->db-item prim-kvs))
       expected (.setExpected     (expected-values g))
       return   (.setReturnValues (utils/enum g))))))

;;;; API - batch ops

(defn- attr-multi-vs
  "[{<attr> <v-or-vs*> ...} ...]* -> [{<attr> <v> ...} ...] (* => optional vec)"
  [attr-multi-vs-map]
  (let [ensure-coll (fn [x] (if (coll?* x) x [x]))]
    (reduce (fn [r attr-multi-vs]
              (let [attrs (keys attr-multi-vs)
                    vs    (mapv ensure-coll (vals attr-multi-vs))]
                (when (> (count (filter next vs)) 1)
                  (-> (Exception. "Can range over only a single attr's values")
                      (throw)))
                (into r (mapv (comp clj-item->db-item (partial zipmap attrs))
                              (apply utils/cartesian-product vs)))))
            [] (ensure-coll attr-multi-vs-map))))

(comment (attr-multi-vs {:a "a1" :b ["b1" "b2" "b3"] :c ["c1" "c2"]}) ; ex
         (attr-multi-vs {:a "a1" :b ["b1" "b2" "b3"] :c ["c1"]}))

(defn- expand-requests
  "Desugars and partitions GET/WRITE batch requests:
  {<table> <sugared-request> ...} -> [{<table> <desugared-request> ...} ...]

  Relevant DynamoDB limits, Ref. http://goo.gl/wA06O
    * Max item size: 64KB (incl. key size).
    * Batch GET: <= 100 items, <= 1MB total.
    * Batch PUT: <= 25 items,  <= 1MB total.

  So as a -very- simple way of ensuring we don't exceed limits, we'll partition
  requests into groups of (/ 1024 62.0) ~ 16 items per request. An ex will be
  thrown by AWS client if any particular item exceeds max size."
  [expanding-ks requests]
  (let [;; Desugar expanding-ks into individual reqs for partitioning
        flat-single-reqs ; [{<table> {#{:prim-kvs :put :delete} <attr-vs> ...}} ...]
        (reduce into []
          (for [table (keys requests)
                exp-k expanding-ks
                :let [unexp-part (apply dissoc (get requests table) expanding-ks)
                      sugared-attr-vs (get-in requests [table exp-k])]
                :when sugared-attr-vs]
            (mapv (fn [attr-vs] {table (assoc unexp-part exp-k [attr-vs])})
                  (attr-multi-vs sugared-attr-vs))))]

    (mapv ; [{<table> {#{:prim-kvs :put :delete} [<attr-vs> ...] ...} ...]
     (fn [flat-req-partition]
       (reduce (fn [m single-req]
                 (utils/deep-merge-with
                  (fn [r l] (if (and (not= r l) (vector l)) (into l r) r))
                  m single-req))
               {} flat-req-partition))
     (partition-all 16 flat-single-reqs))))

(comment (expand-requests [:prim-kvs]
          {:t1 {:prim-kvs [{:hash  ["a"] :range (range 2)}] :attrs []}
           :t2 {:prim-kvs [{:hash  ["a"] :range (range 2)}]
                :consistent? true}})
         (expand-requests [:put :delete]
           {:t1 {:put    {:hash "a"}
                 :delete [{:hash "b"}]}})
         (expand-requests [:put :delete]
           {:t1 {:put    {:hash "a" :attr "foo"}
                 :delete [{:hash "b"}]}}))

(defn- batch-request-items
  "{<table> <request> ...} -> {<table> KeysAndAttributes> ...}"
  [requests]
  (utils/name-map
   (fn [{:keys [prim-kvs attrs consistent?]}]
     (doto-maybe (KeysAndAttributes.) g
       attrs       (.setAttributesToGet (mapv name g))
       consistent? (.setConsistentRead  g)
       :always     (.setKeys (mapv clj-item->db-item prim-kvs))))
   requests))

(comment (->> {:t1 {:prim-kvs [{:hash "a" :range 0}] :attrs []}}
              (batch-request-items)) ; 1 request
         (->> {:t1 {:prim-kvs [{:hash "a" :range (range 200)}] :attrs []}}
              (expand-requests [:prim-kvs])
              (mapv batch-request-items)) ; Multiple partitioned requests
         )

(defn batch-get-item
  "Retrieves a batch of items in a single request.
  Limits apply, Ref. http://goo.gl/Bj9TC.

  (batch-get-item creds
    {:users   {:prim-kvs {:name \"alice\"}}
     :posts   {:prim-kvs {:id [1 2 3]}
               :attrs    [:timestamp :subject]
               :consistent? true}
     :friends {:prim-kvs [{:catagory \"favorites\" :id [1 2 3]}
                          {:catagory \"recent\"    :id [7 8 9]}]}})"
  [creds requests]
  (as-map
   (.batchGetItem (db-client creds)
     (doto (BatchGetItemRequest.) ; {table-str KeysAndAttributes}
       (.setRequestItems

        ;; TODO Run & merge multiple partitions...
        (first (map batch-request-items (expand-requests [:prim-kvs] requests)))
        ;;(batch-request-items requests)
        )))))

(comment (batch-write-item creds
           {:faraday.tests.main {:put [{:id 0 :name "foo"}
                                       {:id 1 :name "bar"}]}})
         (batch-get-item creds
           {:faraday.tests.main {:prim-kvs {:id [0 1]}}}))

(defn- write-request [action item]
  (case action
    :put    (doto (WriteRequest.) (.setPutRequest    (doto (PutRequest.)    (.setItem item))))
    :delete (doto (WriteRequest.) (.setDeleteRequest (doto (DeleteRequest.) (.setKey  item))))))

(defn batch-write-item
  "Executes a batch of Puts and/or Deletes in a single request.
   Limits apply, Ref. http://goo.gl/Bj9TC. No transaction guarantees are
   provided, nor conditional puts. Request execution order is undefined.

   (batch-write-item creds
     {:users {:put    [{:user-id 1 :username \"sally\"}
                       {:user-id 2 :username \"jane\"}]
              :delete [{:user-id [3 4 5]}]}})"
  [creds requests]
  (as-map
    (.batchWriteItem (db-client creds)
      (doto (BatchWriteItemRequest.)
        (.setRequestItems
         (utils/name-map
          ;; {<table> <table-reqs> ...} -> {<table> [WriteRequest ...] ...}
          (fn [table-request]
            (reduce into []
             (for [action (keys table-request)
                   :let [items (attr-multi-vs (table-request action))]]
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
     (fn [[operator values :as condition]]
       (assert (coll?* values) (str "Malformed condition: " condition))
       (doto (Condition.)
         (.setComparisonOperator (enum-op operator))
         (.setAttributeValueList (mapv clj-val->db-val values))))
     conditions)))

(defn query
  "Retries items from a table (indexed) with options:
    prim-key-conds - {<key-attr> [<comparison-operator> <values>] ...}.
    :last-prim-kvs - primary key-val from which to eval, useful for paging.
    :return        - e/o #{:all-attributes :all-projected-attributes :count
                           [<attr> ...]}.
    :index         - name of a secondary index to query.
    :order         - index scaning order e/o #{:asc :desc}.
    :limit         - max num >=1 of items to eval (≠ num of matching items).
    :consistent?   - use strongly (rather than eventually) consistent reads?

  comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between}.

  For unindexed item retrievel see `scan`."
  [creds table prim-key-conds
   & [{:keys [last-prim-kvs return index order limit consistent?]
       :or   {order :asc}}]]
  (println (query|scan-conditions prim-key-conds))
  (as-map
   (.query (db-client creds)
     (doto-maybe (QueryRequest.) g
       :always (.setTableName        (name table))
       :always (.setKeyConditions    (query|scan-conditions prim-key-conds))
       :always (.setScanIndexForward (case order :asc true :desc false))
       last-prim-kvs   (.setExclusiveStartKey (clj-item->db-item g))
       limit           (.setLimit    (long g))
       index           (.setIndexName      g)
       consistent?     (.setConsistentRead g)
       (coll?* return) (.setAttributesToGet (mapv name return))
       (and return (not (coll?* return))) (.setSelect (utils/enum return))))))

(defn scan
  "Retrieves items from a table (unindexed) with options:
    :attr-conds     - {<attr> [<comparison-operator> <values>] ...}.
    :limit          - max num >=1 of items to eval (≠ num of matching items).
    :last-prim-kvs  - primary key-val from which to eval, useful for paging.
    :return         - e/o #{:all-attributes :all-projected-attributes :count
                            [<attr> ...]}.
    :total-segments - total number of parallel scan segments.
    :segment        - calling worker's segment number (>=0, <=total-segments).

  comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between :ne
                             :not-null :null :contains :not-contains :in}.

  For automatic parallelization & segment control see `scan-parallel`.
  For indexed item retrievel see `query`."
  [creds table
   & [{:keys [attr-conds last-prim-kvs return limit total-segments segment]}]]
  (as-map
   (.scan (db-client creds)
     (doto-maybe (ScanRequest.) g
       :always (.setTableName (name table))
       attr-conds      (.setScanFilter        (query|scan-conditions g))
       last-prim-kvs   (.setExclusiveStartKey (clj-item->db-item g))
       limit           (.setLimit             (long g))
       total-segments  (.setTotalSegments     (long g))
       segment         (.setSegment           (long g))
       (coll?* return) (.setAttributesToGet (mapv name return))
       (and return (not (coll?* return))) (.setSelect (utils/enum return))))))

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

;;;; Misc. helpers

;; TODO Automatic throughput adjustment tools

(defn items-by-attrs
  "Groups one or more items by one or more attributes, returning a map of form
  {<attr-val> <item> ...} or {{<attr> <val> ...} <item>}.

  Good for collecting batch or query/scan items results into useable maps,
  indexed by their 'primary key' (which, remember, may consist of 1 OR 2 attrs)."
  [attr-or-attrs item-or-items]
  (letfn [(item-by-attrs [a-or-as item] ;
            (if-not (utils/coll?* a-or-as)
              (let [a  a-or-as] {(get item a) (dissoc item a)})
              (let [as a-or-as] {(select-keys item as) (apply dissoc item as)})))]

    (if-not (utils/coll?* item-or-items)
      (let [item  item-or-items] (item-by-attrs attr-or-attrs item))
      (let [items item-or-items]
        (into {} (mapv (partial item-by-attrs attr-or-attrs) items))))))

(comment
  (items-by-attrs :a      {:a :A :b :B :c :C})
  (items-by-attrs [:a]    {:a :A :b :B :c :C})
  (items-by-attrs [:a :b] {:a :A :b :B :c :C})
  (items-by-attrs :a      [{:a :A1 :b :B1 :c :C2}
                           {:a :A2 :b :B2 :c :C2}])
  (items-by-attrs [:a]    [{:a :A1 :b :B1 :c :C2}
                           {:a :A2 :b :B2 :c :C2}])
  (items-by-attrs [:a :b] [{:a :A1 :b :B1 :c :C2}
                           {:a :A2 :b :B2 :c :C2}]))

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