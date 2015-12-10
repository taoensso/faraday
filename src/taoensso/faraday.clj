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
            [taoensso.encore        :as encore :refer (doto-cond)]
            [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.faraday.utils :as utils :refer (coll?*)])
  (:import  [clojure.lang BigInt]
            [com.amazonaws.services.dynamodbv2.model
             AttributeDefinition
             AttributeValue
             AttributeValueUpdate
             BatchGetItemRequest
             BatchGetItemResult
             BatchWriteItemRequest
             BatchWriteItemResult
             Condition
             ConsumedCapacity
             ComparisonOperator
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
             GlobalSecondaryIndex
             GlobalSecondaryIndexDescription
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
            com.amazonaws.auth.AWSCredentials
            com.amazonaws.auth.AWSCredentialsProvider
            com.amazonaws.auth.BasicAWSCredentials
            com.amazonaws.auth.DefaultAWSCredentialsProviderChain
            com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
            com.amazonaws.services.dynamodbv2.AmazonDynamoDB
            java.nio.ByteBuffer))

(if (vector? taoensso.encore/encore-version)
  (encore/assert-min-encore-version [2 11 0])
  (encore/assert-min-encore-version  2.11))

;;;; Connections

(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied client opts:
    (db-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                 :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}),
    (db-client* {:creds my-AWSCredentials-instance}),
    etc."
  (memoize
   (fn [{:keys [provider creds access-key secret-key endpoint proxy-host proxy-port
               proxy-username proxy-password
               conn-timeout max-conns max-error-retry socket-timeout keep-alive?]
        :as client-opts}]
     (if (empty? client-opts) (AmazonDynamoDBClient.) ; Default client
       (let [creds (or creds (:credentials client-opts)) ; Deprecated opt
             _ (assert (or (nil? creds)    (instance? AWSCredentials         creds)))
             _ (assert (or (nil? provider) (instance? AWSCredentialsProvider provider)))
             ^AWSCredentials aws-creds
             (when-not provider
               (cond
                 creds      creds ; Given explicit AWSCredentials
                 access-key (BasicAWSCredentials. access-key secret-key)))
             ^AWSCredentialsProvider provider
             (or provider (when-not aws-creds (DefaultAWSCredentialsProviderChain.)))
             client-config
             (doto-cond [g (ClientConfiguration.)]
               proxy-host      (.setProxyHost         g)
               proxy-port      (.setProxyPort         g)
               proxy-username  (.setProxyUsername     g)
               proxy-password  (.setProxyPassword     g)
               conn-timeout    (.setConnectionTimeout g)
               max-conns       (.setMaxConnections    g)
               max-error-retry (.setMaxErrorRetry     g)
               socket-timeout  (.setSocketTimeout     g)
               keep-alive?     (.setUseTcpKeepAlive   g))]
         (doto-cond [g (if provider
                         (AmazonDynamoDBClient. provider  client-config)
                         (AmazonDynamoDBClient. aws-creds client-config))]
           endpoint (.setEndpoint g)))))))

(defn- db-client ^AmazonDynamoDB [client-opts] (db-client* client-opts))

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

(def ^:private nt-freeze  (comp #(ByteBuffer/wrap %) nippy-tools/freeze))
;; (def ^:private nt-thaw (comp nippy-tools/thaw #(.array ^ByteBuffer %)))

(defn- nt-thaw [bb]
  (let [ba          (.array ^ByteBuffer bb)
        serialized? (#'nippy/try-parse-header ba)]
    (if-not serialized?
      ba ; No Nippy header => assume non-serialized binary data (e.g. other client)
      (try ; Header match _may_ have been a fluke (though v. unlikely)
        (nippy-tools/thaw ba)
        (catch Exception e
          ba)))))

(encore/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(encore/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type to be subject to automatic de/serialization with
  Nippy.")

(defn- freeze?  [x] (or (nippy-tools/wrapped-for-freezing? x)
                        (encore/bytes? x)))
(defn- stringy? [x] (or (string? x) (keyword? x)))

(defn- assert-precision [x]
  (let [^BigDecimal dec (if (string? x) (BigDecimal. ^String x) (bigdec x))]
    (assert (<= (.precision dec) 38)
      (str "DynamoDB numbers have <= 38 digits of precision. See `freeze` for "
           "arbitrary-precision binary serialization."))
    true))

(comment
  (assert-precision "0.00000000000000000000000000000000000001")
  (assert-precision "0.12345678901234567890123456789012345678")
  (assert-precision "12345678901234567890123456789012345678")
  (assert-precision 10))

(defn- ddb-num? [x]
  "Is `x` a number type natively storable by DynamoDB? Note that DDB stores _all_
  numbers as exact-value strings with <= 38 digits of precision. For greater
  precision, use `freeze`.
  Ref. http://goo.gl/jzzsIW"
  (or (instance? Long    x)
      (instance? Double  x)
      (instance? Integer x)
      (instance? Float   x)
      ;;; High-precision types:
      (and (instance? BigInt     x) (assert-precision x))
      (and (instance? BigDecimal x) (assert-precision x))
      (and (instance? BigInteger x) (assert-precision x))))

(defn- num->ddb-num
  "Coerce any special Clojure types that'd trip up the DDB Java client."
  [x]
  (cond (instance? BigInt x) (biginteger x)
        :else x))

(defn- ddb-num-str->num [^String s]
  ;;; In both cases we'll err on the side of caution, assuming the most
  ;;; accurate possible type
  (if (.contains s ".")
    (BigDecimal. s)
    (bigint (BigInteger. s))))

(defn- db-val->clj-val "Returns the Clojure value of given AttributeValue object."
  [^AttributeValue x]
  (let [[x type]
        (or
          (some-> (.getS    x) (vector :s))
          (some-> (.getN    x) (vector :n))
          (some-> (.getNULL x) (vector :null))
          (some-> (.getBOOL x) (vector :bool))
          (some-> (.getSS   x) (vector :ss))
          (some-> (.getNS   x) (vector :ns))
          (some-> (.getBS   x) (vector :bs))
          (some-> (.getB    x) (vector :b))
          (some-> (.getM    x) (vector :m))
          (some-> (.getL    x) (vector :l)))]

    (case type
      :s    x
      :n    (ddb-num-str->num x)
      :null nil
      :bool (boolean  x)
      :ss   (into #{} x)
      :ns   (into #{} (mapv ddb-num-str->num x))
      :bs   (into #{} (mapv nt-thaw          x))
      :b    (nt-thaw  x)

      :l (mapv db-val->clj-val x)
      :m (zipmap (mapv keyword         (.keySet ^java.util.HashMap x))
                 (mapv db-val->clj-val (.values ^java.util.HashMap x))))))

(defn- clj-val->db-val "Returns an AttributeValue object for given Clojure value."
  ^AttributeValue [x]
  (cond
   (stringy? x)
   (let [^String s (encore/fq-name x)]
     (if (.isEmpty s)
       (throw (Exception. "Invalid DynamoDB value: \"\""))
       (doto (AttributeValue.) (.setS s))))

   (nil? x)              (doto (AttributeValue.) (.setNULL true))
   (instance? Boolean x) (doto (AttributeValue.) (.setBOOL x))
   (ddb-num? x)          (doto (AttributeValue.) (.setN (str x)))
   (freeze?  x)          (doto (AttributeValue.) (.setB (nt-freeze x)))

   (vector?  x) (doto (AttributeValue.) (.setL (mapv clj-val->db-val x)))
   (map?     x) (doto (AttributeValue.) (.setM (encore/map-kvs
                                                 (fn [k _] (name k))
                                                 (fn [_ v] (clj-val->db-val v))
                                                 x)))
   (set? x)
   (if (empty? x)
     (throw (Exception. "Invalid DynamoDB value: empty set"))
     (cond
      (every? stringy? x) (doto (AttributeValue.) (.setSS (mapv encore/fq-name x)))
      (every? ddb-num? x) (doto (AttributeValue.) (.setNS (mapv str  x)))
      (every? freeze?  x) (doto (AttributeValue.) (.setBS (mapv nt-freeze x)))
      :else (throw (Exception. (str "Invalid DynamoDB value: set of invalid type"
                                    " or more than one type")))))

   (instance? AttributeValue x) x
   :else (throw (Exception. (str "Unknown DynamoDB value type: " (type x) "."
                                 " See `freeze` for serialization.")))))

(comment
  (mapv clj-val->db-val [  "a"    1 3.14    (.getBytes "a")    (freeze :a)
                         #{"a"} #{1 3.14} #{(.getBytes "a")} #{(freeze :a)}]))

(defn- enum-op ^String [operator]
  (-> operator {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"} (or operator)
      utils/enum))

;;;; Coercion - objects

(def db-item->clj-item (partial utils/keyword-map db-val->clj-val))
(def clj-item->db-item (partial utils/name-map    clj-val->db-val))

(defn- cc-units [^ConsumedCapacity cc] (some-> cc (.getCapacityUnits)))
(defn- batch-cc-units [ccs]
  (reduce
    (fn [m ^ConsumedCapacity cc]
      (assoc m (keyword (.getTableName cc)) (cc-units cc)))
    {}
    ccs))

(defprotocol AsMap (as-map [x]))

(defmacro ^:private am-item-result [result get-form]
  `(when-let [get-form# ~get-form]
     (with-meta (db-item->clj-item get-form#)
       {:cc-units (cc-units (.getConsumedCapacity ~result))})))

(defmacro ^:private am-query|scan-result [result & [meta]]
  `(let [result# ~result]
     (merge {:items (mapv db-item->clj-item (.getItems result#))
             :count (.getCount result#)
             :cc-units (cc-units (.getConsumedCapacity result#))
             :last-prim-kvs (as-map (.getLastEvaluatedKey result#))}
            ~meta)))

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

  BatchGetItemResult
  (as-map [r]
    {:items       (utils/keyword-map as-map (.getResponses r))
     :unprocessed (.getUnprocessedKeys r)
     :cc-units    (batch-cc-units (.getConsumedCapacity r))})

  BatchWriteItemResult
  (as-map [r]
    {:unprocessed (.getUnprocessedItems r)
     :cc-units    (batch-cc-units (.getConsumedCapacity r))})

  TableDescription
  (as-map [d]
    {:name          (keyword (.getTableName d))
     :creation-date (.getCreationDateTime d)
     :item-count    (.getItemCount d)
     :size          (.getTableSizeBytes d)
     :throughput    (as-map (.getProvisionedThroughput  d))
     :indexes       (as-map (.getLocalSecondaryIndexes  d)) ; DEPRECATED
     :lsindexes     (as-map (.getLocalSecondaryIndexes  d))
     :gsindexes     (as-map (.getGlobalSecondaryIndexes d))
     :status        (utils/un-enum (.getTableStatus d))
     :prim-keys
     (let [schema (as-map (.getKeySchema d))
           defs   (as-map (.getAttributeDefinitions d))]
       (merge-with merge
         (reduce-kv (fn [m k v] (assoc m (:name v) {:key-type  (:type v)}))
                    {} schema)
         (reduce-kv (fn [m k v] (assoc m (:name v) {:data-type (:type v)}))
                    {} defs)))})

  DescribeTableResult (as-map [r] (as-map (.getTable r)))
  CreateTableResult   (as-map [r] (as-map (.getTableDescription r)))
  UpdateTableResult   (as-map [r] (as-map (.getTableDescription r)))
  DeleteTableResult   (as-map [r] (as-map (.getTableDescription r)))

  Projection
  (as-map [p] {:projection-type    (.getProjectionType p)
               :non-key-attributes (.getNonKeyAttributes p)})

  LocalSecondaryIndexDescription
  (as-map [d] {:name       (keyword (.getIndexName d))
               :size       (.getIndexSizeBytes d)
               :item-count (.getItemCount d)
               :key-schema (as-map (.getKeySchema d))
               :projection (as-map (.getProjection d))})

  GlobalSecondaryIndexDescription
  (as-map [d] {:name       (keyword (.getIndexName d))
               :size       (.getIndexSizeBytes d)
               :item-count (.getItemCount d)
               :key-schema (as-map (.getKeySchema d))
               :projection (as-map (.getProjection d))
               :throughput (as-map (.getProvisionedThroughput d))})

  ProvisionedThroughputDescription
  (as-map [d] {:read                (.getReadCapacityUnits d)
               :write               (.getWriteCapacityUnits d)
               :last-decrease       (.getLastDecreaseDateTime d)
               :last-increase       (.getLastIncreaseDateTime d)
               :num-decreases-today (.getNumberOfDecreasesToday d)}))

;;;; API - tables

(defn list-tables "Returns a lazy sequence of table names."
  [client-opts]
  (letfn [(step [^String offset]
            (lazy-seq
             (let [client   (db-client client-opts)
                   result   (if (nil? offset)
                              (.listTables client)
                              (.listTables client offset))
                   last-key (.getLastEvaluatedTableName result)
                   chunk    (map keyword (.getTableNames result))]
               (if last-key
                 (concat chunk (step (name last-key)))
                 chunk))))]
    (step nil)))

(defn describe-table-request "Implementation detail."
  ^DescribeTableRequest [table]
  (doto (DescribeTableRequest.) (.setTableName (name table))))

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [client-opts table]
  (try (as-map (.describeTable (db-client client-opts)
                 (describe-table-request table)))
       (catch ResourceNotFoundException _ nil)))

(defn table-status-watch
  "Creates a future to poll for a change to table's status, and returns a
  promise to which the table's new description will be delivered. Deref this
  promise to block until table status changes."
  [client-opts table status & [{:keys [poll-ms]
                                :or   {poll-ms 5000}}]]
  (assert (#{:creating :updating :deleting :active} (utils/un-enum status))
          (str "Invalid table status: " status))
  (let [p (promise)]
    (future
      (loop []
        (let [current-descr (describe-table client-opts table)]
          (if-not (= (:status current-descr) (utils/un-enum status))
            (deliver p current-descr)
            (do (Thread/sleep poll-ms)
                (recur))))))
    p))

(comment (create-table mc "delete-me5" [:id :s])
         @(table-status-watch mc "delete-me5" :creating) ; ~53000ms
         (def descr (describe-table mc "delete-me5")))

(defn- key-schema-element "Returns a new KeySchemaElement object."
  [key-name key-type]
  (doto (KeySchemaElement.)
    (.setAttributeName (name key-name))
    (.setKeyType (utils/enum key-type))))

(defn- key-schema
  "Returns a [{<hash-key> KeySchemaElement}], or
             [{<hash-key> KeySchemaElement} {<range-key> KeySchemaElement}]
   vector for use as a table/index primary key."
  [[hname _ :as hash-keydef] & [[rname _ :as range-keydef]]]
  (cond-> [(key-schema-element hname :hash)]
          range-keydef (conj (key-schema-element rname :range))))

(defn- provisioned-throughput "Returns a new ProvisionedThroughput object."
  [{read-units :read write-units :write :as throughput}]
  (assert (and read-units write-units)
          (str "Malformed throughput: " throughput))
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits  (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn- keydefs "[<name> <type>] defs -> [AttributeDefinition ...]"
  [hash-keydef range-keydef lsindexes gsindexes]
  (let [defs (->> (conj [] hash-keydef range-keydef)
                  (concat (mapv :range-keydef lsindexes))
                  (concat (mapv :hash-keydef  gsindexes))
                  (concat (mapv :range-keydef gsindexes))
                  (filterv identity)
                  (distinct))]
    (mapv
     (fn [[key-name key-type :as def]]
       (assert (and key-name key-type) (str "Malformed keydef: " def))
       (assert (#{:s :n :ss :ns :b :bs} key-type)
               (str "Invalid keydef type: " key-type))
       (doto (AttributeDefinition.)
         (.setAttributeName (name key-name))
         (.setAttributeType (utils/enum key-type))))
     defs)))

(defn local-2nd-indexes
  "Implementation detail.
  [{:name _ :range-keydef _ :projection _} ...] indexes -> [LocalSecondaryIndex ...]"
  [hash-keydef indexes]
  (when indexes
    (mapv
      (fn [{index-name :name
           :keys      [range-keydef projection]
           :or        {projection :all}
           :as        index}]
        (assert (and index-name range-keydef projection)
                (str "Malformed local secondary index (LSI): " index))
        (doto (LocalSecondaryIndex.)
          (.setIndexName (name index-name))
          (.setKeySchema (key-schema hash-keydef range-keydef))
          (.setProjection
            (let [pr (Projection.)
                  ptype (if (coll?* projection) :include projection)]
              (.setProjectionType pr (utils/enum ptype))
              (when (= ptype :include)
                (.setNonKeyAttributes pr (mapv name projection)))
              pr))))
      indexes)))

(defn global-2nd-indexes
  "Implementation detail.
  [{:name _ :hash-keydef _ :range-keydef _ :projection _ :throughput _} ...]
  indexes -> [GlobalSecondaryIndex ...]"
  [indexes]
  (when indexes
    (mapv
      (fn [{index-name :name
           :keys      [hash-keydef range-keydef throughput projection]
           :or        {projection :all}
           :as        index}]
        (assert (and index-name hash-keydef projection throughput)
                (str "Malformed global secondary index (GSI): " index))
        (doto (GlobalSecondaryIndex.)
          (.setIndexName (name index-name))
          (.setKeySchema (key-schema hash-keydef range-keydef))
          (.setProjection
            (let [pr (Projection.)
                  ptype (if (coll?* projection) :include projection)]
              (.setProjectionType pr (utils/enum ptype))
              (when (= ptype :include)
                (.setNonKeyAttributes pr (mapv name projection)))
              pr))
          (.setProvisionedThroughput (provisioned-throughput throughput))))
      indexes)))

(defn create-table-request "Implementation detail."
  [table-name hash-keydef
   & [{:keys [range-keydef throughput lsindexes gsindexes block?]
       :or   {throughput {:read 1 :write 1}} :as opts}]]
  (let [lsindexes (or lsindexes (:indexes opts))]
    (doto-cond [_ (CreateTableRequest.)]
      :always (.setTableName (name table-name))
      :always (.setKeySchema (key-schema hash-keydef range-keydef))
      :always (.setProvisionedThroughput (provisioned-throughput throughput))
      :always (.setAttributeDefinitions
               (keydefs hash-keydef range-keydef lsindexes gsindexes))
      lsindexes (.setLocalSecondaryIndexes
                 (local-2nd-indexes hash-keydef lsindexes))
      gsindexes (.setGlobalSecondaryIndexes
                 (global-2nd-indexes gsindexes)))))

(defn create-table
  "Creates a table with options:
    hash-keydef   - [<name> <#{:s :n :ss :ns :b :bs}>].
    :range-keydef - [<name> <#{:s :n :ss :ns :b :bs}>].
    :throughput   - {:read <units> :write <units>}.
    :lsindexes    - [{:name _ :range-keydef _
                      :projection #{:all :keys-only [<attr> ...]}}].
    :gsindexes    - [{:name _ :hash-keydef _ :range-keydef _
                      :projection #{:all :keys-only [<attr> ...]}
                      :throughput _}].
    :block?       - Block for table to actually be active?"
  [client-opts table-name hash-keydef
   & [{:keys [range-keydef throughput lsindexes gsindexes block?]
       :as opts}]]
  (let [lsindexes (or lsindexes (:indexes opts)) ; DEPRECATED
        result
        (as-map
         (.createTable (db-client client-opts)
           (create-table-request table-name hash-keydef opts)))]
    (if-not block? result @(table-status-watch client-opts table-name :creating))))

(comment (time (create-table mc "delete-me7" [:id :s] {:block? true})))

(defn ensure-table "Creates a table if it doesn't already exist."
  [client-opts table-name hash-keydef & [opts]]
  (when-not (describe-table client-opts table-name)
    (create-table client-opts table-name hash-keydef opts)))

(defn throughput-steps
  "Implementation detail.
  Dec by any amount, inc by <= 2x current amount, Ref. http://goo.gl/Bj9TC.
  x - start, x' - current, x* - goal."
  [[r w] [r* w*]]
  (let [step (fn [x* x'] (if (< x* x') x* (min x* (* 2 x'))))]
    (loop [steps [] [r' w'] [r w]]
      (if (= [r' w'] [r* w*])
        steps
        (let [[r' w' :as this-step] [(step r* r') (step w* w')]]
          (recur (conj steps this-step) this-step))))))

(comment (throughput-steps [1 1] [1 1])
         (throughput-steps [1 1] [12 12])
         (throughput-steps [3 3] [27 27])
         (throughput-steps [17 8] [3 22])
         (throughput-steps [1 1] [300 300]))

(defn update-table-request "Implementation detail."
  [table throughput]
  (doto (UpdateTableRequest.)
    (.setTableName (name table))
    (.setProvisionedThroughput (provisioned-throughput throughput))))

(defn update-table
  "Updates a table. Allows automatic multi-step adjustments to conform to
  update limits, Ref. http://goo.gl/Bj9TC.

  Returns a promise to which the final resulting table description will be
  delivered. Deref this promise to block until update (all steps) complete."
  [client-opts table throughput & [{:keys [span-reqs]
                                    :or   {span-reqs {:max 5}}}]]
  (let [throughput* throughput
        {read* :read write* :write} throughput*
        {:keys [status throughput]} (describe-table client-opts table)
        {:keys [read write num-decreases-today]} throughput
        read*  (or read*  read)
        write* (or write* write)
        {max-reqs :max} span-reqs
        decreasing? (or (< read* read) (< write* write))
        steps  (throughput-steps [read write] [read* write*])
        nsteps (count steps)]
    (cond (not= status :active)
          (throw (Exception. (str "Invalid table status: " status)))
          (and decreasing? (>= num-decreases-today 4)) ; API limit
          (throw (Exception. (str "Max 4 decreases per 24hr period")))
          (> nsteps max-reqs)
          (throw (Exception. (str "`max-reqs` too low, needs reqs: " nsteps)))
          :else
          (letfn [(run1 [[r' w']]
                    (as-map
                     (.updateTable (db-client client-opts)
                       (update-table-request table {:read r' :write w'})))
                    ;; Returns _new_ descr when ready:
                    @(table-status-watch client-opts table :updating))]

            (let [p (promise)]
              (future (deliver p (peek (mapv run1 steps))))
              p)))))

(comment
  (def dt (describe-table client-opts :faraday.tests.main))
  (let [p (update-table client-opts :faraday.tests.main {:read 1 :write 1})]
    @p))

(defn delete-table-request "Implementation detail."
  ^DeleteTableRequest [table]
  (DeleteTableRequest. (name table)))

(defn delete-table "Deletes a table, go figure."
  [client-opts table]
  (as-map (.deleteTable (db-client client-opts) (delete-table-request table))))

;;;; API - items

(defn get-item-request "Implementation detail."
  [table prim-kvs & [{:keys [attrs consistent? return-cc? projection expr-attr-names]}]]
  (doto-cond [g (GetItemRequest.)]
    :always         (.setTableName       (name table))
    :always         (.setKey             (clj-item->db-item prim-kvs))
    consistent?     (.setConsistentRead  g)
    attrs           (.setAttributesToGet (mapv name g))
    projection      (.setProjectionExpression g)
    expr-attr-names (.setExpressionAttributeNames expr-attr-names)
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))))

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs         - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs           - Attrs to return, [<attr> ...].
    :projection      - Projection expression as a string
    :expr-attr-names - Map of strings for ExpressionAttributeNames
    :consistent?     - Use strongly (rather than eventually) consistent reads?"
  [client-opts table prim-kvs & [opts]]
  (as-map
   (.getItem (db-client client-opts)
    (get-item-request table prim-kvs opts))))

(defn- expected-values
  "{<attr> <cond> ...} -> {<attr> ExpectedAttributeValue ...}"
  [expected-map]
  (when (seq expected-map)
    (utils/name-map
     #(case %
        :exists (doto (ExpectedAttributeValue.)
                  (.withComparisonOperator ComparisonOperator/NOT_NULL))
        :not-exists (ExpectedAttributeValue. false)
        (if (vector? %)
          (doto (ExpectedAttributeValue.)
            (.withComparisonOperator (enum-op (first %)))
            (.setAttributeValueList (mapv clj-val->db-val (rest %))))
          (ExpectedAttributeValue. (clj-val->db-val %))))
     expected-map)))

(defn- clj->db-expr-vals-map [m] (encore/map-vals clj-val->db-val m))

(def ^:private deprecation-warning-expected_
  (delay
    (println "Faraday WARNING: `:expected` option is deprecated in favor of `:cond-expr`")))

(def ^:private deprecation-warning-update-map_
  (delay
    (println "Faraday WARNING: `update-map` is deprecated in favor of `:cond-expr`")))

(defn put-item-request
  [table item &
   [{:keys [return expected return-cc? cond-expr expr-attr-names expr-attr-vals]
     :or   {return :none}}]]
  (doto-cond [g (PutItemRequest.)]
    :always         (.setTableName    (name table))
    :always         (.setItem         (clj-item->db-item item))
    expected        (.setExpected     (expected-values g))
    cond-expr       (.setConditionExpression cond-expr)
    expr-attr-names (.withExpressionAttributeNames expr-attr-names)
    expr-attr-vals  (.withExpressionAttributeValues
                      (clj->db-expr-vals-map expr-attr-vals))
    return          (.setReturnValues (utils/enum g))
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))))

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return          - e/o #{:none :all-old}
    :cond-expr       - \"attribute_exists(attr_name) AND|OR ...\"
    :expr-attr-names - {\"#attr_name\" \"name\"}
    :expr-attr-vals  - {\":attr_value\" \"value\"}
    :expected        - DEPRECATED in favor of `:cond-expr`,
      {<attr> <#{:exists :not-exists [<comparison-operator> <value>] <value>}> ...}
      With comparison-operator e/o #{:eq :le :lt :ge :gt :begins-with :between}."

  [client-opts table item &
   [{:keys [return expected return-cc? cond-expr]
     :as opts}]]

  (assert (not (and expected cond-expr))
    "Only one of :expected or :cond-expr should be provided")

  (when expected @deprecation-warning-expected_)

  (as-map
   (.putItem (db-client client-opts)
     (put-item-request table item opts))))

(defn- attribute-updates
  "{<attr> [<action> <value>] ...} -> {<attr> AttributeValueUpdate ...}"
  [update-map]
  (when (seq update-map)
    (utils/name-map
     (fn [[action val]]
       (AttributeValueUpdate. (when
                                  (or (not= action :delete)
                                      (set? val))
                                (clj-val->db-val val))
                              (utils/enum action)))
     update-map)))

(defn update-item-request
  [table prim-kvs update-map &
   [{:keys [return expected return-cc?
            cond-expr update-expr expr-attr-names expr-attr-vals]
     :or   {return :none}}]]

  (doto-cond [g (UpdateItemRequest.)]
    :always         (.setTableName        (name table))
    :always         (.setKey (clj-item->db-item prim-kvs))
    update-map      (.setAttributeUpdates (attribute-updates update-map))
    update-expr     (.setUpdateExpression update-expr)
    expr-attr-names (.withExpressionAttributeNames expr-attr-names)
    expr-attr-vals  (.withExpressionAttributeValues
                      (clj->db-expr-vals-map expr-attr-vals))
    expected        (.setExpected         (expected-values g))
    cond-expr       (.setConditionExpression cond-expr)
    return          (.setReturnValues     (utils/enum g))
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))))

(defn update-item
  "Updates an item in a table by its primary key with options:
    prim-kvs         - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}
    update-map       - DEPRECATED in favor of `:update-expr`,
                       {<attr> [<#{:put :add :delete}> <optional value>]}
    :cond-expr       - \"attribute_exists(attr_name) AND|OR ...\"
    :update-expr     - \"SET #attr_name = :attr_value\"
    :expr-attr-names - {\"#attr_name\" \"name\"}
    :expr-attr-vals  - {\":attr_value\" \"value\"}
    :return          - e/o #{:none :all-old :updated-old :all-new :updated-new}
    :expected        - DEPRECATED in favor of `:cond-expr`,
      {<attr> <#{:exists :not-exists [<comparison-operator> <value>] <value>}> ...}
      With comparison-operator e/o #{:eq :le :lt :ge :gt :begins-with :between}."

  ([client-opts table prim-kvs update-map &
    [{:keys [return expected return-cc? cond-expr update-expr]
      :as opts}]]

   (assert (not (and expected cond-expr))
     "Only one of :expected or :cond-expr should be provided")

   (assert (not (and update-expr (seq update-map)))
     "Only one of 'update-map' or :update-expr should be provided")

   (when expected         @deprecation-warning-expected_)
   (when (seq update-map) @deprecation-warning-update-map_)

   (as-map
     (.updateItem (db-client client-opts)
       (update-item-request table prim-kvs update-map opts)))))

(defn delete-item-request "Implementation detail."
  [table prim-kvs & [{:keys [return expected return-cc?]
                      :or   {return :none}}]]
  (doto-cond [g (DeleteItemRequest.)]
    :always  (.setTableName    (name table))
    :always  (.setKey          (clj-item->db-item prim-kvs))
    expected (.setExpected     (expected-values g))
    return   (.setReturnValues (utils/enum g))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [client-opts table prim-kvs & [{:keys [return expected return-cc?]
                                  :as opts}]]
  (as-map
   (.deleteItem (db-client client-opts)
     (delete-item-request table prim-kvs opts))))

;;;; API - batch ops

(def ^:dynamic *attr-multi-vs?* "Temporary hack/workaround for [#63]" true)

(defn attr-multi-vs
  "Implementation detail.
  [{<attr> <v-or-vs*> ...} ...]* -> [{<attr> <v> ...} ...] (* => optional vec)"
  [attr-multi-vs-map]
  (if-not *attr-multi-vs?*
    (clj-item->db-item attr-multi-vs)
    (let [;; ensure-coll (fn [x] (if (coll?* x) x [x]))
          ensure-sequential (fn [x] (if (sequential? x) x [x]))]
      (reduce (fn [r attr-multi-vs]
                (let [attrs (keys attr-multi-vs)
                      vs    (mapv ensure-sequential (vals attr-multi-vs))]
                  (when (> (count (filter next vs)) 1)
                    (-> (Exception. "Can range over only a single attr's values")
                        (throw)))
                  (into r (mapv (comp clj-item->db-item (partial zipmap attrs))
                                (apply utils/cartesian-product vs)))))
        [] (ensure-sequential attr-multi-vs-map)))))

(comment
  (attr-multi-vs {:a "a1" :b ["b1" "b2" "b3"] :c ["c1" "c2"]}) ; ex
  (attr-multi-vs {:a "a1" :b ["b1" "b2" "b3"] :c ["c1"]})       ; Range over b's
  (attr-multi-vs {:a "a1" :b ["b1" "b2" "b3"] :c #{"c1" "c2"}}) ; ''
  )

(defn batch-request-items
  "Implementation detail.
  {<table> <request> ...} -> {<table> KeysAndAttributes> ...}"
  [requests]
  (utils/name-map
   (fn [{:keys [prim-kvs attrs consistent?]}]
     (doto-cond [g (KeysAndAttributes.)]
       attrs       (.setAttributesToGet (mapv name g))
       consistent? (.setConsistentRead  g)
       :always     (.setKeys (attr-multi-vs prim-kvs))))
   requests))

(comment (batch-request-items {:my-table {:prim-kvs [{:my-hash  ["a" "b"]
                                                      :my-range ["0" "1"]}]
                                          :attrs []}}))

(defn- merge-more
  "Enables auto paging for batch batch-get/write and query/scan requests.
  Particularly useful for throughput limitations."
  [more-f {max-reqs :max :keys [throttle-ms]} last-result]
  (loop [{:keys [unprocessed last-prim-kvs] :as last-result} last-result idx 1]
    (let [more (or unprocessed last-prim-kvs)]
      (if (or (empty? more) (nil? max-reqs) (>= idx max-reqs))
        (if-let [items (:items last-result)]
          (with-meta items (dissoc last-result :items))
          last-result)
        (let [merge-results (fn [l r] (cond (number? l) (+    l r)
                                           (vector? l) (into l r)
                                           :else               r))]
          (when throttle-ms (Thread/sleep throttle-ms))
          (recur (merge-with merge-results last-result (more-f more))
                 (inc idx)))))))

(defn batch-get-item-request "Implementation detail."
  ^BatchGetItemRequest [return-cc? raw-req]
  (doto-cond [g (BatchGetItemRequest.)]
    :always    (.setRequestItems raw-req)
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn batch-get-item
  "Retrieves a batch of items in a single request.
  Limits apply, Ref. http://goo.gl/Bj9TC.

  (batch-get-item client-opts
    {:users   {:prim-kvs {:name \"alice\"}}
     :posts   {:prim-kvs {:id [1 2 3]}
               :attrs    [:timestamp :subject]
               :consistent? true}
     :friends {:prim-kvs [{:catagory \"favorites\" :id [1 2 3]}
                          {:catagory \"recent\"    :id [7 8 9]}]}})

  :span-reqs - {:max _ :throttle-ms _} allows a number of requests to
  automatically be stitched together (to exceed throughput limits, for example)."
  [client-opts requests & [{:keys [return-cc? span-reqs attr-multi-vs?] :as opts
                            :or   {span-reqs {:max 5}
                                   attr-multi-vs? true}}]]
  (binding [*attr-multi-vs?* attr-multi-vs?]
    (letfn [(run1 [raw-req]
              (as-map
                (.batchGetItem (db-client client-opts)
                  (batch-get-item-request return-cc? raw-req))))]
      (merge-more run1 span-reqs (run1 (batch-request-items requests))))))

(comment
  (def bigval (.getBytes (apply str (range 14000))))
  (defn- ids [from to] (for [n (range from to)] {:id n :name bigval}))
  (batch-write-item client-opts {:faraday.tests.main {:put (ids 0 10)}})
  (batch-get-item   client-opts {:faraday.tests.main {:prim-kvs {:id (range 20)}}})
  (scan client-opts :faraday.tests.main))

(defn write-request [action item] "Implementation detail."
  (case action
    :put    (doto (WriteRequest.) (.setPutRequest    (doto (PutRequest.)    (.setItem item))))
    :delete (doto (WriteRequest.) (.setDeleteRequest (doto (DeleteRequest.) (.setKey  item))))))

(defn batch-write-item-request "Implementation detail."
  ^BatchWriteItemRequest [return-cc? raw-req]
  (doto-cond [g (BatchWriteItemRequest.)]
    :always    (.setRequestItems raw-req)
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn batch-write-item
  "Executes a batch of Puts and/or Deletes in a single request.
   Limits apply, Ref. http://goo.gl/Bj9TC. No transaction guarantees are
   provided, nor conditional puts. Request execution order is undefined.

   (batch-write-item client-opts
     {:users {:put    [{:user-id 1 :username \"sally\"}
                       {:user-id 2 :username \"jane\"}]
              :delete [{:user-id [3 4 5]}]}})

  :span-reqs - {:max _ :throttle-ms _} allows a number of requests to
  automatically be stitched together (to exceed throughput limits, for example)."
  [client-opts requests & [{:keys [return-cc? span-reqs attr-multi-vs?] :as opts
                            :or   {span-reqs {:max 5}
                                   attr-multi-vs? true}}]]
  (binding [*attr-multi-vs?* attr-multi-vs?]
    (letfn [(run1 [raw-req]
              (as-map
                (.batchWriteItem (db-client client-opts)
                  (batch-write-item-request return-cc? raw-req))))]
      (merge-more run1 span-reqs
        (run1
          (utils/name-map
            ;; {<table> <table-reqs> ...} -> {<table> [WriteRequest ...] ...}
            (fn [table-request]
              (reduce into []
                (for [action (keys table-request)
                      :let [items (attr-multi-vs (table-request action))]]
                  (mapv (partial write-request action) items))))
            requests))))))

;;;; API - queries & scans

(defn- query|scan-conditions
  "{<attr> [operator <val-or-vals>] ...} -> {<attr> Condition ...}"
  [conditions]
  (when (seq conditions)
    (utils/name-map
     (fn [[operator val-or-vals & more :as condition]]
       (assert (not (seq more)) (str "Malformed condition: " condition))
       (let [vals (if (coll?* val-or-vals) val-or-vals [val-or-vals])]
         (doto (Condition.)
           (.setComparisonOperator (enum-op operator))
           (.setAttributeValueList (mapv clj-val->db-val vals)))))
     conditions)))

(defn query-request "Implementation detail."
  [table prim-key-conds
   & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
              projection filter expr-attr-vals expr-attr-names return-cc?] :as opts
       :or {order :asc}}]]
  (doto-cond [g (QueryRequest.)]
    :always (.setTableName        (name table))
    :always (.setKeyConditions    (query|scan-conditions prim-key-conds))
    :always (.setScanIndexForward (case order :asc true :desc false))
    last-prim-kvs   (.setExclusiveStartKey
                     (clj-item->db-item last-prim-kvs))
    query-filter    (.setQueryFilter (query|scan-conditions query-filter))
    projection      (.setProjectionExpression projection)
    filter          (.setFilterExpression g)
    expr-attr-names (.setExpressionAttributeNames expr-attr-names)
    expr-attr-vals  (.setExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
    limit           (.setLimit     (int g))
    index           (.setIndexName      g)
    consistent?     (.setConsistentRead g)
    (coll?* return) (.setAttributesToGet (mapv name return))
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))
    (and return (not (coll?* return)))
    (.setSelect (utils/enum return))))

(defn query
  "Retrieves items from a table (indexed) with options:
    prim-key-conds   - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :last-prim-kvs   - Primary key-val from which to eval, useful for paging.
    :query-filter    - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :projection      - Projection expression string
    :filter          - Filter expression string
    :expr-attr-names - Expression attribute names, as a map of {\"#attr_name\" \"name\"}
    :expr-attr-vals  - Expression attribute values, as a map {\":attr_value\" \"value\"}
    :span-reqs       - {:max _ :throttle-ms _} controls automatic multi-request
                       stitching.
    :return          - e/o #{:all-attributes :all-projected-attributes :count
                             [<attr> ...]}.
    :index           - Name of a local or global secondary index to query.
    :order           - Index scaning order e/o #{:asc :desc}.
    :limit           - Max num >=1 of items to eval (≠ num of matching items).
                       Useful to prevent harmful sudden bursts of read activity.
    :consistent?     - Use strongly (rather than eventually) consistent reads?

  (create-table client-opts :my-table [:name :s]
    {:range-keydef [:age :n] :block? true})

  (do (put-item client-opts :my-table {:name \"Steve\" :age 24})
      (put-item client-opts :my-table {:name \"Susan\" :age 27}))
  (query client-opts :my-table {:name [:eq \"Steve\"]
                                :age  [:between [10 30]]})
  => [{:age 24, :name \"Steve\"}]

  comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between}.

  For unindexed item retrievel see `scan`.

  Ref. http://goo.gl/XfGKW for query+scan best practices."
  [client-opts table prim-key-conds
   & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
              return-cc?] :as opts
       :or   {span-reqs {:max 5}}}]]
  (letfn [(run1 [last-prim-kvs]
            (as-map
             (.query
              (db-client client-opts)
              (query-request table prim-key-conds
                             (assoc opts :last-prim-kvs last-prim-kvs)))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs))))

(defn scan-request
  [table
   & [{:keys [attr-conds last-prim-kvs span-reqs return limit total-segments
              segment return-cc?] :as opts}]]
  (doto-cond [g (ScanRequest.)]
    :always (.setTableName (name table))
    attr-conds      (.setScanFilter        (query|scan-conditions g))
    last-prim-kvs   (.setExclusiveStartKey
                     (clj-item->db-item last-prim-kvs))
    limit           (.setLimit             (int g))
    total-segments  (.setTotalSegments     (int g))
    segment         (.setSegment           (int g))
    (coll?* return) (.setAttributesToGet (mapv name return))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))
    (and return (not (coll?* return)))
    (.setSelect (utils/enum return))))

(defn scan
  "Retrieves items from a table (unindexed) with options:
    :attr-conds     - {<attr> [<comparison-operator> <val-or-vals>] ...}.
    :limit          - Max num >=1 of items to eval (≠ num of matching items).
                      Useful to prevent harmful sudden bursts of read activity.
    :last-prim-kvs  - Primary key-val from which to eval, useful for paging.
    :span-reqs      - {:max _ :throttle-ms _} controls automatic multi-request
                      stitching.
    :return         - e/o #{:all-attributes :all-projected-attributes :count
                            [<attr> ...]}.
    :total-segments - Total number of parallel scan segments.
    :segment        - Calling worker's segment number (>=0, <=total-segments).

  comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between :ne
                             :not-null :null :contains :not-contains :in}.

  (create-table client-opts :my-table [:name :s]
    {:range-keydef [:age :n] :block? true})

  (do (put-item client-opts :my-table {:name \"Steve\" :age 24})
      (put-item client-opts :my-table {:name \"Susan\" :age 27}))
  (scan client-opts :my-table {:attr-conds {:age [:in [24 27]]}})
  => [{:age 24, :name \"Steve\"} {:age 27, :name \"Susan\"}]

  For automatic parallelization & segment control see `scan-parallel`.
  For indexed item retrievel see `query`.

  Ref. http://goo.gl/XfGKW for query+scan best practices."
  [client-opts table
   & [{:keys [attr-conds last-prim-kvs span-reqs return limit total-segments
              segment return-cc?] :as opts
       :or   {span-reqs {:max 5}}}]]
  (letfn [(run1 [last-prim-kvs]
            (as-map
             (.scan
              (db-client client-opts)
              (scan-request table (assoc opts :last-prim-kvs last-prim-kvs)))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs))))

(defn scan-parallel
  "Like `scan` but starts a number of worker threads and automatically handles
  parallel scan options (:total-segments and :segment). Returns a vector of
  `scan` results.

  Ref. http://goo.gl/KLwnn (official parallel scan documentation)."

  ;; TODO GitHub #17:
  ;; http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
  ;; In a parallel scan, a Scan request that includes ExclusiveStartKey must
  ;; specify the same segment whose previous Scan returned the corresponding
  ;; value of LastEvaluatedKey.

  [client-opts table total-segments & [opts]]
  (let [opts (assoc opts :total-segments total-segments)]
    (->> (mapv (fn [seg] (future (scan client-opts table (assoc opts :segment seg))))
               (range total-segments))
         (mapv deref))))

;;;; Misc utils, etc.

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

(def remove-empty-attr-vals
  "Alpha, subject to change.
  Util to help remove (or coerce to nil) empty val types prohibited by DDB,
  Ref. http://goo.gl/Xg85pO. See also `freeze` as an alternative."
  (let [->?seq (fn [c] (when (seq c) c))]
    (fn f1 [x]
      (cond
        (map? x)
        (->?seq
          (reduce-kv
            (fn [acc k v] (let [v* (f1 v)] (if (nil? v*) acc (assoc acc k v* ))))
            {} x))

        (coll? x)
        (->?seq
          (reduce
            (fn rf [acc in] (let [v* (f1 in)] (if (nil? v*) acc (conj acc v*))))
            (if (sequential? x) [] (empty x)) x))

        (string?       x) (when-not (.isEmpty ^String x)       x)
        (encore/bytes? x) (when-not (zero? (alength ^bytes x)) x)
        :else x))))

(comment
  (remove-empty-attr-vals ; => {:b [{:a "b"}], :f false}
    {:b [{:a "b" :c [[]] :d #{}}, {}] :a nil :empt-str "" :e #{""} :f false}))

(comment ; README

(require '[taoensso.faraday :as far])

(def client-opts
  {:access-key "<AWS_DYNAMODB_ACCESS_KEY>"
   :secret-key "<AWS_DYNAMODB_SECRET_KEY>"} ; Your IAM keys here
  )

(far/list-tables client-opts)
;; => [] ; No tables yet :-(

(far/create-table client-opts :my-table
  [:id :n]  ; Primary key named "id", (:n => number type)
  {:throughput {:read 1 :write 1} ; Read & write capacity (units/sec)
   :block? true ; Block thread during table creation
   })

;; Wait a minute for the table to be created... got a sandwich handy?

(far/list-tables client-opts)
;; => [:my-table] ; There's our new table!

(far/put-item client-opts
  :my-table
  {:id 0 ; Remember that this is our primary (indexed) key
   :name "Steve" :age 22 :data (far/freeze {:vector    [1 2 3]
                                            :set      #{1 2 3}
                                            :rational (/ 22 7)
                                            ;; ... Any Clojure data goodness
                                            })})

(far/get-item client-opts :my-table {:id 0})
;; => {:id 0 :name "Steve" :age 22 :data {:vector [1 2 3] ...}}

)
