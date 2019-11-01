(ns taoensso.faraday
  "Clojure DynamoDB client. Originally adapted from Rotary by James Reeves.
  Ref. https://github.com/weavejester/rotary (Rotary),
       http://goo.gl/22QGA (DynamoDBv2 API)

  Definitions:
    * Attribute   - [<name> <val>] pair
    * Item        - Collection of attributes
    * Table       - Collection of items
    * Primary key - Partition (hash) key or
                    Partition (hash) AND sort (range) key"
  {:author "Peter Taoussanis & contributors"}
  (:require [clojure.string         :as str]
            [taoensso.encore        :as enc :refer (doto-cond)]
            [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.faraday.utils :as utils :refer (coll?*)])
  (:import [clojure.lang BigInt Keyword IPersistentVector]
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
            CreateGlobalSecondaryIndexAction
            CreateTableRequest
            CreateTableResult
            DeleteItemRequest
            DeleteItemResult
            DeleteRequest
            DescribeStreamRequest
            DescribeStreamResult
            DeleteTableRequest
            DeleteTableResult
            DescribeTableRequest
            DescribeTableResult
            ExpectedAttributeValue
            GetItemRequest
            GetItemResult
            GetRecordsRequest
            GetRecordsResult
            GetShardIteratorRequest
            KeysAndAttributes
            KeySchemaElement
            ListStreamsRequest
            ListStreamsResult
            LocalSecondaryIndex
            LocalSecondaryIndexDescription
            GlobalSecondaryIndex
            GlobalSecondaryIndexDescription
            GlobalSecondaryIndexUpdate
            Projection
            ProvisionedThroughput
            ProvisionedThroughputDescription
            PutItemRequest
            PutItemResult
            PutRequest
            QueryRequest
            QueryResult
            Record
            ScanRequest
            ScanResult
            SequenceNumberRange
            Shard
            Stream
            StreamDescription
            StreamRecord
            StreamSpecification
            StreamViewType
            TableDescription
            UpdateItemRequest
            UpdateItemResult
            UpdateTableRequest
            UpdateTableResult
            WriteRequest

            ConditionalCheckFailedException
            DeleteGlobalSecondaryIndexAction
            InternalServerErrorException
            ItemCollectionSizeLimitExceededException
            LimitExceededException
            ProvisionedThroughputExceededException
            ResourceInUseException
            ResourceNotFoundException
            UpdateGlobalSecondaryIndexAction
            BillingModeSummary]

           com.amazonaws.ClientConfiguration
           com.amazonaws.auth.AWSCredentials
           com.amazonaws.auth.AWSCredentialsProvider
           com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           [com.amazonaws.services.dynamodbv2
            AmazonDynamoDB
            AmazonDynamoDBClient
            AmazonDynamoDBStreams
            AmazonDynamoDBStreamsClient]
           java.nio.ByteBuffer
           (taoensso.nippy.tools WrappedForFreezing)
           (java.util Map Set ArrayList HashMap)))

(if (vector? taoensso.encore/encore-version)
  (enc/assert-min-encore-version [2 67 2])
  (enc/assert-min-encore-version  2.67))

;;;; Connections

(defn- client-params
  [{:as   client-opts
    :keys [provider creds access-key secret-key proxy-host proxy-port
           proxy-username proxy-password
           conn-timeout max-conns max-error-retry socket-timeout keep-alive?]}]

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

    [aws-creds provider client-config]))

(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied client opts:
    (db-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                 :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}),
    (db-client* {:creds my-AWSCredentials-instance}),
    etc."
  (memoize
   (fn [{:keys [endpoint] :as client-opts}]
     (if (empty? client-opts)
       (AmazonDynamoDBClient.) ; Default client
       (let [[^AWSCredentials aws-creds
              ^AWSCredentialsProvider provider
              ^ClientConfiguration client-config] (client-params client-opts)]
         (doto-cond [g (if provider
                         (AmazonDynamoDBClient. provider client-config)
                         (AmazonDynamoDBClient. aws-creds client-config))]
           endpoint (.setEndpoint g)))))))

(def ^:private db-streams-client*
  "Returns a new AmazonDynamoDBStreamsClient instance for the given client opts:
    (db-streams-client* {:creds my-AWSCredentials-instance}),
    (db-streams-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                         :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}), etc."
  (memoize
    (fn [{:keys [endpoint] :as client-opts}]
      (if (empty? client-opts)
        (AmazonDynamoDBStreamsClient.) ; Default client
        (let [[^AWSCredentials aws-creds
               ^AWSCredentialsProvider provider
               ^ClientConfiguration client-config] (client-params client-opts)]
          (doto-cond [g (if provider
                          (AmazonDynamoDBStreamsClient. provider client-config)
                          (AmazonDynamoDBStreamsClient. aws-creds client-config))]
            endpoint (.setEndpoint g)))))))

(defn- db-client ^AmazonDynamoDB [client-opts] (db-client* client-opts))
(defn- db-streams-client ^AmazonDynamoDBStreams [client-opts] (db-streams-client* client-opts))

;;;; Exceptions

(def ^:const ex "DynamoDB API exceptions. Use #=(ex _) for `try` blocks, etc."
  {:conditional-check-failed            ConditionalCheckFailedException
   :internal-server-error               InternalServerErrorException
   :item-collection-size-limit-exceeded ItemCollectionSizeLimitExceededException
   :limit-exceeded                      LimitExceededException
   :provisioned-throughput-exceeded     ProvisionedThroughputExceededException
   :resource-in-use                     ResourceInUseException
   :resource-not-found                  ResourceNotFoundException})

;;;; Value coercions

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

(enc/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(enc/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type to be subject to automatic de/serialization with
  Nippy.")

(defn- freeze? [x] (or (nippy-tools/wrapped-for-freezing? x) (enc/bytes? x)))

(defn- assert-precision [x]
  (let [^BigDecimal dec (if (string? x) (BigDecimal. ^String x) (bigdec x))]
    (assert (<= (.precision dec) 38)
      (str "DynamoDB numbers have <= 38 digits of precision. See `freeze` for "
           "arbitrary-precision binary serialization."))
    x))

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

(defn- ddb-num-str->num [^String s]
  ;; In both cases we'll err on the side of caution, assuming the most
  ;; accurate possible type
  (if (.contains s ".")
    (BigDecimal. s)
    (bigint (BigInteger. s))))

(defn- deserialize
  "Returns the Clojure value of given AttributeValue object."
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
      :s x
      :n (ddb-num-str->num x)
      :null nil
      :bool (boolean  x)
      :ss (into #{} x)
      :ns (into #{} (mapv ddb-num-str->num x))
      :bs (into #{} (mapv nt-thaw          x))
      :b (nt-thaw  x)

      :l (mapv deserialize x)
      :m (zipmap (mapv keyword (.keySet ^HashMap x))
                 (mapv deserialize (.values ^HashMap x))))))

(defprotocol ISerializable
  "Extensible protocol for mapping Clojure vals to AttributeValue objects."
  (serialize ^AttributeValue [this]))

(extend-protocol ISerializable
  AttributeValue (serialize [x] x)
  nil        (serialize [_] (doto (AttributeValue.) (.setNULL true)))
  Boolean    (serialize [x] (doto (AttributeValue.) (.setBOOL x)))
  Long       (serialize [x] (doto (AttributeValue.) (.setN (str x))))
  Double     (serialize [x] (doto (AttributeValue.) (.setN (str x))))
  Integer    (serialize [x] (doto (AttributeValue.) (.setN (str x))))
  Float      (serialize [x] (doto (AttributeValue.) (.setN (str x))))
  BigInt     (serialize [x] (doto (AttributeValue.) (.setN (str (assert-precision x)))))
  BigDecimal (serialize [x] (doto (AttributeValue.) (.setN (str (assert-precision x)))))
  BigInteger (serialize [x] (doto (AttributeValue.) (.setN (str (assert-precision x)))))

  WrappedForFreezing
  (serialize [x] (doto (AttributeValue.) (.setB (nt-freeze x))))

  Keyword (serialize [kw] (serialize (enc/as-qname kw)))
  String
  (serialize [s]
    (if (.isEmpty s)
      (throw (Exception. "Invalid DynamoDB value: \"\" (empty string)"))
      (doto (AttributeValue.) (.setS s))))

  IPersistentVector
  (serialize [v] (doto (AttributeValue.) (.setL (mapv serialize v))))

  Map
  (serialize [m]
    (doto (AttributeValue.)
      (.setM
        (persistent!
          (reduce-kv
            (fn [acc k v] (assoc! acc (enc/as-qname k) (serialize v)))
            (transient {})
            m)))))

  Set
  (serialize [s]
    (if (empty? s)
      (throw (Exception. "Invalid DynamoDB value: empty set"))
      (cond
        (enc/revery? enc/stringy? s) (doto (AttributeValue.) (.setSS (mapv enc/as-qname s)))
        (enc/revery? ddb-num?     s) (doto (AttributeValue.) (.setNS (mapv str s)))
        (enc/revery? freeze?      s) (doto (AttributeValue.) (.setBS (mapv nt-freeze s)))
        :else (throw (Exception. "Invalid DynamoDB value: set of invalid type or more than one type"))))))

(extend-type (Class/forName "[B")
  ISerializable
  (serialize [ba] (doto (AttributeValue.) (.setB (nt-freeze ba)))))

(defn- enum-op ^String [operator]
  (-> operator {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"} (or operator)
      utils/enum))

;;;; Object coercions

(def db-item->clj-item (partial utils/keyword-map deserialize))
(def clj-item->db-item (partial utils/name-map      serialize))

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
  ArrayList (as-map [a] (mapv as-map a))
  HashMap   (as-map [m] (utils/keyword-map as-map m))

  AttributeValue      (as-map [v] (deserialize v))
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
    {:name                (keyword (.getTableName d))
     :creation-date       (.getCreationDateTime d)
     :item-count          (.getItemCount d)
     :size                (.getTableSizeBytes d)
     :throughput          (as-map (.getProvisionedThroughput d))
     :billing-mode        (as-map (.getBillingModeSummary d))
     :indexes             (as-map (.getLocalSecondaryIndexes d)) ; DEPRECATED
     :lsindexes           (as-map (.getLocalSecondaryIndexes d))
     :gsindexes           (as-map (.getGlobalSecondaryIndexes d))
     :stream-spec         (as-map (.getStreamSpecification d))
     :latest-stream-label (.getLatestStreamLabel d)
     :latest-stream-arn   (.getLatestStreamArn d)
     :status              (utils/un-enum (.getTableStatus d))
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
  (as-map [p]
    {:projection-type    (.getProjectionType p)
     :non-key-attributes (.getNonKeyAttributes p)})

  LocalSecondaryIndexDescription
  (as-map [d]
    {:name       (keyword (.getIndexName d))
     :size       (.getIndexSizeBytes d)
     :item-count (.getItemCount d)
     :key-schema (as-map (.getKeySchema d))
     :projection (as-map (.getProjection d))})

  GlobalSecondaryIndexDescription
  (as-map [d]
    {:name       (keyword (.getIndexName d))
     :size       (.getIndexSizeBytes d)
     :item-count (.getItemCount d)
     :key-schema (as-map (.getKeySchema d))
     :projection (as-map (.getProjection d))
     :throughput (as-map (.getProvisionedThroughput d))})

  ProvisionedThroughputDescription
  (as-map [d]
    {:read                (.getReadCapacityUnits d)
     :write               (.getWriteCapacityUnits d)
     :last-decrease       (.getLastDecreaseDateTime d)
     :last-increase       (.getLastIncreaseDateTime d)
     :num-decreases-today (.getNumberOfDecreasesToday d)})

  BillingModeSummary
  (as-map [d]
    {:name                (utils/un-enum (.getBillingMode d))
     :last-update         (.getLastUpdateToPayPerRequestDateTime d)})

  StreamSpecification
  (as-map [s]
    {:enabled?  (.getStreamEnabled s)
     :view-type (utils/un-enum (.getStreamViewType s))})

  DescribeStreamResult
  (as-map [r] (as-map (.getStreamDescription r)))

  StreamDescription
  (as-map [d]
    {:stream-arn (.getStreamArn d)
     :stream-label (.getStreamLabel d)
     :stream-status (utils/un-enum (.getStreamStatus d))
     :stream-view-type (utils/un-enum (.getStreamViewType d))
     :creation-request-date-time (.getCreationRequestDateTime d)
     :table-name (.getTableName d)
     :key-schema (as-map (.getKeySchema d))
     :shards (as-map (.getShards d))
     :last-evaluated-shard-id (.getLastEvaluatedShardId d)})

  Shard
  (as-map [d]
    {:shard-id              (.getShardId d)
     :parent-shard-id       (.getParentShardId d)
     :seq-num-range (as-map (.getSequenceNumberRange d))})

  SequenceNumberRange
  (as-map [d]
    {:starting-seq-num (.getStartingSequenceNumber d)
     :ending-seq-num   (.getEndingSequenceNumber d)})

  GetRecordsResult
  (as-map [r]
    {:next-shard-iterator (.getNextShardIterator r)
     :records             (as-map (.getRecords r))})

  Record
  (as-map [r]
    {:event-id      (.getEventID r)
     :event-name    (utils/un-enum (.getEventName r))
     :event-version (.getEventVersion r)
     :event-source  (.getEventSource r)
     :aws-region    (.getAwsRegion r)
     :stream-record (as-map (.getDynamodb r))})

  StreamRecord
  (as-map [r]
    {:keys      (db-item->clj-item (.getKeys r))
     :old-image (db-item->clj-item (.getOldImage r))
     :new-image (db-item->clj-item (.getNewImage r))
     :seq-num   (.getSequenceNumber r)
     :size      (.getSizeBytes r)
     :view-type (utils/un-enum (.getStreamViewType r))})

  ListStreamsResult
  (as-map [r]
    {:last-stream-arn (.getLastEvaluatedStreamArn r)
     :streams (as-map (.getStreams r))})

  Stream
  (as-map [s]
    {:stream-arn (.getStreamArn s)
     :table-name (.getTableName s)}))

;;;; Tables

(defn list-tables "Returns a lazy sequence of table names."
  [client-opts]
  (let [step
        (fn step [^String offset]
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

(defn- describe-table-request "Implementation detail."
  ^DescribeTableRequest [table]
  (doto (DescribeTableRequest.) (.setTableName (name table))))

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [client-opts table]
  (try (as-map (.describeTable (db-client client-opts)
                 (describe-table-request table)))
       (catch ResourceNotFoundException _ nil)))

(defn- table-status-watch
  "Returns a future to poll for a change to table's status from the one
  passed as a parameter."
  [client-opts table status & [{:keys [poll-ms]
                                :or   {poll-ms 1500}}]]
  (assert (#{:creating :updating :deleting :active} (utils/un-enum status))
    (str "Invalid table status: " status))
  (future
    (loop []
      (let [current-descr (describe-table client-opts table)]
        (if-not (= (:status current-descr) (utils/un-enum status))
          current-descr
          (do (Thread/sleep poll-ms)
              (recur)))))))

(defn- index-status-watch
  "Returns a future to poll for index status.
  `index-type` - e/o #{:gsindexes :lsindexes} (currently only supports :gsindexes)"
  [client-opts table index-type index-name & [{:keys [poll-ms]
                                               :or   {poll-ms 1500}}]]
  (future
    (loop []
      (let [current-descr (describe-table client-opts table)
            [index]       (filterv #(= (:name %) (keyword index-name))
                            (index-type current-descr))]
        (cond
          (nil? index) nil

          (or (nil? (:size       index))
              (nil? (:item-count index)))
          (do (Thread/sleep poll-ms)
              (recur))

          :else index)))))

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

(defn- local-2nd-indexes
  "Implementation detail.
  [{:name _ :range-keydef _ :projection _} ...] indexes -> [LocalSecondaryIndex ...]"
  [hash-keydef indexes]
  (when indexes
    (mapv
      (fn [{index-name :name
           :keys [range-keydef projection]
           :or   {projection :all}
           :as   index}]
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

(defn- global-2nd-indexes
  "Implementation detail.
  [{:name _ :hash-keydef _ :range-keydef _ :projection _ :throughput _} ...]
  indexes -> [GlobalSecondaryIndex ...]"
  [indexes billing-mode]
  (when indexes
    (mapv
      (fn [{index-name :name
           :keys [hash-keydef range-keydef throughput projection]
           :or   {projection :all}
           :as   index}]
        (assert (and index-name hash-keydef projection (or
                                                        (and throughput (not (= :pay-per-request billing-mode)))
                                                        (and (= :pay-per-request billing-mode) (not throughput))))
                (str "Malformed global secondary index (GSI): " index))
        (doto-cond [_ (GlobalSecondaryIndex.)]
          :always (.setIndexName (name index-name))
          :always (.setKeySchema (key-schema hash-keydef range-keydef))
          :always (.setProjection
                    (let [pr (Projection.)
                          ptype (if (coll?* projection) :include projection)]
                      (.setProjectionType pr (utils/enum ptype))
                      (when (= ptype :include)
                        (.setNonKeyAttributes pr (mapv name projection)))
                      pr))
          throughput (.setProvisionedThroughput (provisioned-throughput throughput))))
      indexes)))

(defn- stream-specification "Implementation detail."
  [{:keys [enabled? view-type]}]
  (enc/doto-cond [_ (StreamSpecification.)]
    (not (nil? enabled?)) (.setStreamEnabled enabled?)
    view-type (.setStreamViewType (StreamViewType/fromValue (utils/enum view-type)))))

(defn- create-table-request "Implementation detail."
  [table-name hash-keydef
   & [{:keys [range-keydef throughput lsindexes gsindexes stream-spec billing-mode]
       :or   {billing-mode :provisioned} :as opts}]]
  (assert (not (and throughput (= :pay-per-request billing-mode))) "Can't specify :throughput and :pay-per-request billing-mode")
  (let [lsindexes (or lsindexes (:indexes opts))]
    (doto-cond [_ (CreateTableRequest.)]
      :always (.setTableName (name table-name))
      :always (.setKeySchema (key-schema hash-keydef range-keydef))
      throughput (.setProvisionedThroughput (provisioned-throughput (or throughput {:read 1 :write 1})))
      :always (.setBillingMode (utils/enum billing-mode))
      :always (.setAttributeDefinitions
               (keydefs hash-keydef range-keydef lsindexes gsindexes))
      lsindexes (.setLocalSecondaryIndexes
                 (local-2nd-indexes hash-keydef lsindexes))
      gsindexes (.setGlobalSecondaryIndexes
                 (global-2nd-indexes gsindexes billing-mode))
      stream-spec (.setStreamSpecification
                   (stream-specification stream-spec)))))

(defn create-table
  "Creates a table with options:
    hash-keydef   - [<name> <#{:s :n :ss :ns :b :bs}>].
    :range-keydef - [<name> <#{:s :n :ss :ns :b :bs}>].
    :throughput   - {:read <units> :write <units>}.
    :billing-mode - :provisioned | :pay-per-request ; defaults to provisioned
    :block?       - Block for table to actually be active?
    :lsindexes    - [{:name _ :range-keydef _
                      :projection <#{:all :keys-only [<attr> ...]}>}].
    :gsindexes    - [{:name _ :hash-keydef _ :range-keydef _
                      :projection <#{:all :keys-only [<attr> ...]}>
                      :throughput _}].
    :stream-spec  - {:enabled? <default true if spec is present>
                     :view-type <#{:keys-only :new-image :old-image :new-and-old-images}>}"
  [client-opts table-name hash-keydef
   & [{:keys [block?] :as opts}]]
  (let [result
        (as-map
         (.createTable (db-client client-opts)
           (create-table-request table-name hash-keydef opts)))]
    (if-not block?
      result
      @(table-status-watch client-opts table-name :creating))))

(defn ensure-table "Creates a table iff it doesn't already exist."
  [client-opts table-name hash-keydef & [opts]]
  (when-not (describe-table client-opts table-name)
    (create-table client-opts table-name hash-keydef opts)))

;;;; Table updates

(defn- global-2nd-index-updates
  "Implementation detail.
  {:operation _ :name _ :hash-keydef _ :range-keydef _ :projection _ :throughput _}
  index -> GlobalSecondaryIndexUpdate}"
  [{index-name :name
    :keys [hash-keydef range-keydef throughput projection operation]
    :or   {projection :all}
    :as   index}]

  (case operation
    :create
    (do
      (assert (and index-name hash-keydef projection throughput)
              (str "Malformed global secondary index (GSI): " index))
      (doto (GlobalSecondaryIndexUpdate.)
        (.setCreate
          (doto
            (CreateGlobalSecondaryIndexAction.)
            (.setIndexName (name index-name))
            (.setKeySchema (key-schema hash-keydef range-keydef))
            (.setProjection
              (let [pr    (Projection.)
                    ptype (if (coll?* projection) :include projection)]
                (.setProjectionType pr (utils/enum ptype))
                (when (= ptype :include)
                  (.setNonKeyAttributes pr (mapv name projection)))
                pr))
            (.setProvisionedThroughput (provisioned-throughput throughput))))))

    :update
    (doto (GlobalSecondaryIndexUpdate.)
      (.setUpdate
        (doto
          (UpdateGlobalSecondaryIndexAction.)
          (.setIndexName (name index-name))
          (.setProvisionedThroughput (provisioned-throughput throughput)))))

    :delete
    (doto (GlobalSecondaryIndexUpdate.)
      (.setDelete
        (doto
          (DeleteGlobalSecondaryIndexAction.)
          (.setIndexName (name index-name)))))
    nil))

(defn- update-table-request "Implementation detail."
  [table {:keys [throughput gsindexes stream-spec billing-mode] :as params}]
  (assert (not (and throughput
               (= :pay-per-request billing-mode))) "Can't specify :throughput and :pay-per-request billing-mode")
  (let [attr-defs (keydefs nil nil nil [gsindexes])]
    (doto-cond
      [_ (UpdateTableRequest.)]
      :always (.setTableName (name table))
      throughput (.setProvisionedThroughput (provisioned-throughput throughput))
      billing-mode (.setBillingMode (utils/enum billing-mode))
      gsindexes (.setGlobalSecondaryIndexUpdates [(global-2nd-index-updates gsindexes)])
      stream-spec (.setStreamSpecification (stream-specification stream-spec))
      (seq attr-defs) (.setAttributeDefinitions attr-defs))))

(defn- validate-update-opts [table-desc {:keys [throughput billing-mode] :as params}]
  (let [{read* :read write* :write} throughput
        current-throughput (:throughput table-desc)
        {:keys [read write num-decreases-today]} current-throughput
        read*              (or read* read)
        write*             (or write* write)
        decreasing?        (or (< read* read) (< write* write))]
    (cond

      (and throughput
           (= :pay-per-request billing-mode))
      (throw (Exception. "Can't specify :throughput and :pay-per-request billing-mode"))

      (and (not throughput)
           (= :pay-per-request billing-mode))
      params

      ;; Hard API limit
      (and decreasing? (>= num-decreases-today 4))
      (throw (Exception. (str "API Limit - Max 4 decreases per 24hr period")))

      ;; Only send a throughput update req if it'd actually change throughput
      (and (= (:read  throughput) (:read  current-throughput))
           (= (:write throughput) (:write current-throughput)))
      (dissoc params :throughput)

      :else params)))

(defn update-table
  "Returns a future which updates table and returns the post-update table
  description.

  Update opts:
    :throughput   - {:read <units> :write <units>}
    :billing-mode - :provisioned | :pay-per-request   ; defaults to provisioned
    :gsindexes    - {:operation                       ; e/o #{:create :update :delete}
                    :name                             ; Required
                    :throughput                       ; Only for :update / :create
                    :hash-keydef                      ; Only for :create
                    :range-keydef                     ;
                    :projection                       ; e/o #{:all :keys-only [<attr> ...]}}
    :stream-spec  - {:enabled?                        ;
                    :view-type                        ; e/o #{:keys-only :new-image :old-image :old-and-new-images}}

  Only one global secondary index operation can take place at a time.
  In order to change a stream view-type, you need to disable and re-enable the stream."
  [client-opts table update-opts & [{:keys [span-reqs]
                                     :or   {span-reqs {:max 5}}}]]
  (let [table-desc  (describe-table client-opts table)
        status      (:status table-desc)
        update-opts (validate-update-opts table-desc update-opts)]

    (cond
      (not= status :active)
      (throw (Exception. (str "Invalid table status: " status)))

      :else
      (future
        ;; If we are not receiving any actual update requests, or
        ;; they were all cleared out by validation, simply return
        ;; the same table description
        (if (empty? update-opts)
          table-desc
          (do
            (.updateTable
              (db-client client-opts)
              (update-table-request table update-opts))
            ;; Returns _new_ descr when ready:
            @(table-status-watch client-opts table :updating)))))))

(defn- delete-table-request "Implementation detail."
  ^DeleteTableRequest [table]
  (DeleteTableRequest. (name table)))

(defn delete-table "Deletes a table, go figure."
  [client-opts table]
  (as-map (.deleteTable (db-client client-opts) (delete-table-request table))))

;;;; Items

(defn- get-item-request "Implementation detail."
  [table prim-kvs & [{:keys [attrs consistent? return-cc? proj-expr expr-attr-names]}]]
  (doto-cond [g (GetItemRequest.)]
    :always         (.setTableName       (name table))
    :always         (.setKey             (clj-item->db-item prim-kvs))
    consistent?     (.setConsistentRead  g)
    attrs           (.setAttributesToGet (mapv name g))
    proj-expr       (.setProjectionExpression g)
    expr-attr-names (.setExpressionAttributeNames expr-attr-names)
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))))

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs         - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs           - Attrs to return, [<attr> ...].
    :proj-expr       - Projection expression as a string
    :expr-attr-names - Map of strings for ExpressionAttributeNames
    :consistent?     - Use strongly (rather than eventually) consistent reads?"
  [client-opts table prim-kvs & [opts]]
  (as-map
   (.getItem (db-client client-opts)
    (get-item-request table prim-kvs opts))))

(defn- expected-vals
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
            (.setAttributeValueList (mapv serialize (rest %))))
          (ExpectedAttributeValue. (serialize %))))
     expected-map)))

(defn- clj->db-expr-vals-map [m] (enc/map-vals serialize m))

(def ^:private deprecation-warning-expected_
  (delay
    (println "Faraday WARNING: `:expected` option is deprecated in favor of `:cond-expr`")))

(def ^:private deprecation-warning-update-map_
  (delay
    (println "Faraday WARNING: `update-map` is deprecated in favor of `:update-expr`")))

(defn- put-item-request
  [table item &
   [{:keys [return expected return-cc? cond-expr expr-attr-names expr-attr-vals]
     :or   {return :none}}]]
  (doto-cond [g (PutItemRequest.)]
    :always         (.setTableName    (name table))
    :always         (.setItem         (clj-item->db-item item))
    expected        (.setExpected     (expected-vals g))
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
       (AttributeValueUpdate.
         (when (or (not= action :delete) (set? val))
           (serialize val))
         (utils/enum action)))
      update-map)))

(defn- update-item-request
  [table prim-kvs &
   [{:keys [return expected return-cc? update-map
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
    expected        (.setExpected         (expected-vals g))
    cond-expr       (.setConditionExpression cond-expr)
    return          (.setReturnValues     (utils/enum g))
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))))

(defn update-item
  "Updates an item in a table by its primary key with options:
    prim-kvs         - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}
    :update-map      - DEPRECATED in favor of `:update-expr`,
                       {<attr> [<#{:put :add :delete}> <optional value>]}
    :cond-expr       - \"attribute_exists(attr_name) AND|OR ...\"
    :update-expr     - \"SET #attr_name = :attr_value\"
    :expr-attr-names - {\"#attr_name\" \"name\"}
    :expr-attr-vals  - {\":attr_value\" \"value\"}
    :return          - e/o #{:none :all-old :updated-old :all-new :updated-new}
    :expected        - DEPRECATED in favor of `:cond-expr`,
      {<attr> <#{:exists :not-exists [<comparison-operator> <value>] <value>}> ...}
      With comparison-operator e/o #{:eq :le :lt :ge :gt :begins-with :between}."

  ([client-opts table prim-kvs &
    [{:keys [update-map return expected return-cc? cond-expr update-expr]
      :as opts}]]

   (assert (not (and expected cond-expr))
     "Only one of :expected or :cond-expr should be provided")

   (assert (not (and update-expr (seq update-map)))
     "Only one of 'update-map' or :update-expr should be provided")

   (when expected         @deprecation-warning-expected_)
   (when (seq update-map) @deprecation-warning-update-map_)

   (as-map
     (.updateItem (db-client client-opts)
       (update-item-request table prim-kvs opts)))))

(defn- delete-item-request "Implementation detail."
  [table prim-kvs & [{:keys [return expected return-cc? cond-expr expr-attr-vals expr-attr-names]
                      :or   {return :none}}]]
  (doto-cond [g (DeleteItemRequest.)]
    :always         (.setTableName    (name table))
    :always         (.setKey          (clj-item->db-item prim-kvs))
    cond-expr       (.setConditionExpression cond-expr)
    expr-attr-names (.setExpressionAttributeNames expr-attr-names)
    expr-attr-vals  (.setExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
    expected        (.setExpected     (expected-vals g))
    return          (.setReturnValues (utils/enum g))
    return-cc?      (.setReturnConsumedCapacity (utils/enum :total))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [client-opts table prim-kvs & [{:keys [return expected return-cc?]
                                  :as opts}]]
  (as-map
   (.deleteItem (db-client client-opts)
     (delete-item-request table prim-kvs opts))))

;;;; Batch ops

;; TODO Do we want to change this to `false` in a future breaking release?
;; Would require updating `batch-get-item`, `batch-write-item` docstrings and
;; updating consumers of these fns in tests
(def ^:private  attr-multi-vs?-default true)
(def ^:dynamic *attr-multi-vs?*
  "Treat attribute vals as expansions rather than literals?
  nil => use `attr-multi-vs?-default` (currently `true` though this may be
  changed in future to better support DDB's new collection types,
  Ref. https://github.com/ptaoussanis/faraday/issues/63)." nil)

(defmacro with-attr-multi-vs    [& body] `(binding [*attr-multi-vs?*  true] ~@body))
(defmacro without-attr-multi-vs [& body] `(binding [*attr-multi-vs?* false] ~@body))

(defn- attr-multi-vs
  "Implementation detail.
  [{<attr> <v-or-vs*> ...} ...]* -> [{<attr> <v> ...} ...] (* => optional vec)"
  [attr-multi-vs-map]
  (let [expand? *attr-multi-vs?*
        expand? (if (nil? expand?) attr-multi-vs?-default expand?)]

    (let [ensure-sequential (fn [x] (if (sequential? x) x [x]))]
      (if-not expand?
        (mapv clj-item->db-item (ensure-sequential attr-multi-vs-map))
        (reduce
          (fn [r attr-multi-vs]
            (let [attrs (keys attr-multi-vs)
                  vs    (mapv ensure-sequential (vals attr-multi-vs))]
              (when (> (count (filter next vs)) 1)
                (-> (Exception. "Can range over only a single attr's values")
                  (throw)))
              (into r (mapv (comp clj-item->db-item (partial zipmap attrs))
                        (apply utils/cartesian-product vs)))))
          [] (ensure-sequential attr-multi-vs-map))))))

(defn- batch-request-items
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

(defn- batch-get-item-request "Implementation detail."
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
  [client-opts requests
   & [{:keys [return-cc? span-reqs attr-multi-vs?] :as opts
       :or   {span-reqs {:max 5}}}]]
  (binding [*attr-multi-vs?* attr-multi-vs?]
    (let [run1
          (fn [raw-req]
            (as-map
              (.batchGetItem (db-client client-opts)
                (batch-get-item-request return-cc? raw-req))))]
      (merge-more run1 span-reqs (run1 (batch-request-items requests))))))

(defn- write-request [action item] "Implementation detail."
  (case action
    :put    (doto (WriteRequest.) (.setPutRequest    (doto (PutRequest.)    (.setItem item))))
    :delete (doto (WriteRequest.) (.setDeleteRequest (doto (DeleteRequest.) (.setKey  item))))))

(defn- batch-write-item-request "Implementation detail."
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
  [client-opts requests &
   [{:keys [return-cc? span-reqs attr-multi-vs?] :as opts
     :or   {span-reqs {:max 5}}}]]

  (binding [*attr-multi-vs?* attr-multi-vs?]
    (let [run1
          (fn [raw-req]
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
           (.setAttributeValueList (mapv serialize vals)))))
     conditions)))

(defn- query-request "Implementation detail."
  [table prim-key-conds
   & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
              proj-expr filter-expr expr-attr-vals expr-attr-names return-cc?] :as opts
       :or {order :asc}}]]
  (doto-cond [g (QueryRequest.)]
    :always (.setTableName        (name table))
    :always (.setKeyConditions    (query|scan-conditions prim-key-conds))
    :always (.setScanIndexForward (case order :asc true :desc false))
    last-prim-kvs   (.setExclusiveStartKey
                     (clj-item->db-item last-prim-kvs))
    query-filter    (.setQueryFilter (query|scan-conditions query-filter))
    proj-expr       (.setProjectionExpression proj-expr)
    filter-expr     (.setFilterExpression g)
    expr-attr-names (.setExpressionAttributeNames expr-attr-names)
    expr-attr-vals  (.setExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
    limit           (.setLimit     (int g))
    index           (.setIndexName      (name g))
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
    :proj-expr       - Projection expression string
    :filter-expr     - Filter expression string
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
  (let [run1
        (fn [last-prim-kvs]
          (as-map
            (.query
              (db-client client-opts)
              (query-request table prim-key-conds
                (assoc opts :last-prim-kvs last-prim-kvs)))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs))))

(defn- scan-request
  [table
   & [{:keys [attr-conds last-prim-kvs return limit total-segments
              proj-expr expr-attr-names filter-expr expr-attr-vals
              consistent? index segment return-cc?] :as opts}]]
  (doto-cond [g (ScanRequest.)]
    :always (.setTableName (name table))
    attr-conds      (.setScanFilter        (query|scan-conditions g))
    expr-attr-names (.setExpressionAttributeNames expr-attr-names)
    expr-attr-vals  (.setExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
    filter-expr     (.setFilterExpression filter-expr)
    index           (.setIndexName (name index))
    last-prim-kvs   (.setExclusiveStartKey
                     (clj-item->db-item last-prim-kvs))
    limit           (.setLimit             (int g))
    proj-expr       (.setProjectionExpression g)
    total-segments  (.setTotalSegments     (int g))
    segment         (.setSegment           (int g))
    consistent?     (.setConsistentRead consistent?)
    (coll?* return) (.setAttributesToGet (mapv name return))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))
    (and return (not (coll?* return)))
    (.setSelect (utils/enum return))))

(defn scan
  "Retrieves items from a table (unindexed) with options:
    :attr-conds      - {<attr> [<comparison-operator> <val-or-vals>] ...}.
    :expr-attr-names - Expression attribute names, as a map of {\"#attr_name\" \"name\"}
    :expr-attr-vals  - Expression attribute names, as a map of {\":attr\" \"value\"}
    :filter-expr     - Filter expression string
    :index           - Index name to use.
    :proj-expr       - Projection expression as a string
    :limit           - Max num >=1 of items to eval (≠ num of matching items).
                       Useful to prevent harmful sudden bursts of read activity.
    :last-prim-kvs   - Primary key-val from which to eval, useful for paging.
    :span-reqs       - {:max _ :throttle-ms _} controls automatic multi-request
                       stitching.
    :return          - e/o #{:all-attributes :all-projected-attributes :count
                             [<attr> ...]}.
    :total-segments  - Total number of parallel scan segments.
    :segment         - Calling worker's segment number (>=0, <=total-segments).
    :consistent?     - Use strongly (rather than eventually) consistent reads?

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
              filter-expr
              segment return-cc?] :as opts
       :or   {span-reqs {:max 5}}}]]

  (assert (not (and filter-expr (seq attr-conds)))
          "Only one of ':filter-expr' or :attr-conds should be provided")

  (let [run1
        (fn [last-prim-kvs]
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

;;;; DB Streams API
;; Ref. http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/Welcome.html

(defn- list-streams-request
  [{:keys [table-name limit start-arn]}]
  (enc/doto-cond [_ (ListStreamsRequest.)]
    table-name (.setTableName (name table-name))
    limit (.setLimit (int limit))
    start-arn (.setExclusiveStartStreamArn start-arn)))

(defn list-streams
  "Returns a lazy sequence of stream descriptions. Each item is a map of:
   {:stream-arn - Stream identifier string
    :table-name - The table name for the stream}

    Options:
     :start-arn  - The stream identifier to start listing from (exclusive)
     :table-name - List only the streams for <table-name>
     :limit      - Retrieve streams in batches of <limit> at a time"
  [client-opts & [{:keys [start-arn table-name limit] :as opts}]]
  (let [client (db-streams-client client-opts)
        step (fn step [stream-arn]
               (lazy-seq
                 (let [chunk (as-map
                               (.listStreams client
                                             (list-streams-request (assoc opts :start-arn stream-arn))))]
                   (if (:last-stream-arn chunk)
                     (concat (:streams chunk) (step (:last-stream-arn chunk)))
                     (seq (:streams chunk))))))]
    (step start-arn)))

(defn- describe-stream-request
  ^DescribeStreamRequest [stream-arn {:keys [limit start-shard-id]}]
  (enc/doto-cond [_ (DescribeStreamRequest.)]
    stream-arn (.setStreamArn stream-arn)
    limit (.setLimit (int limit))
    start-shard-id (.setExclusiveStartShardId start-shard-id)))

(defn describe-stream
  "Returns a map describing a stream, or nil if the stream doesn't exist."
  [client-opts stream-arn & [{:keys [limit start-shard-id] :as opts}]]
  (try (as-map (.describeStream (db-streams-client client-opts)
                                (describe-stream-request stream-arn opts)))
       (catch ResourceNotFoundException _ nil)))

(defn- get-shard-iterator-request
  [stream-arn shard-id iterator-type {:keys [seq-num]}]
  (enc/doto-cond [_ (GetShardIteratorRequest.)]
    :always (.setStreamArn stream-arn)
    :always (.setShardId shard-id)
    :always (.setShardIteratorType (utils/enum iterator-type))
    seq-num (.setSequenceNumber seq-num)))

(defn shard-iterator
  "Returns the iterator string that can be used in the get-stream-records call
   or nil when the stream or shard doesn't exist."
  [client-opts stream-arn shard-id iterator-type
   & [{:keys [seq-num] :as opts}]]
  (try
    (.. (db-streams-client client-opts)
        (getShardIterator (get-shard-iterator-request stream-arn shard-id iterator-type opts))
        (getShardIterator))
    (catch ResourceNotFoundException _ nil)))

(defn- get-records-request
  [iter-str {:keys [limit]}]
  (enc/doto-cond [_ (GetRecordsRequest.)]
    :always (.setShardIterator iter-str)
    limit (.setLimit (int limit))))

(defn get-stream-records
  [client-opts shard-iterator & [{:keys [limit] :as opts}]]
  (as-map (.getRecords (db-streams-client client-opts)
                       (get-records-request shard-iterator opts))))

;;;; Misc utils, etc.

(defn items-by-attrs
  "Groups one or more items by one or more attributes, returning a map of form
  {<attr-val> <item> ...} or {{<attr> <val> ...} <item>}.

  Good for collecting batch or query/scan items results into useable maps,
  indexed by their 'primary key' (which, remember, may consist of 1 OR 2 attrs)."
  [attr-or-attrs item-or-items]
  (let [item-by-attrs
        (fn [a-or-as item]
          (if-not (utils/coll?* a-or-as)
            (let [a  a-or-as] {(get item a) (dissoc item a)})
            (let [as a-or-as] {(select-keys item as) (apply dissoc item as)})))]
    (if-not (utils/coll?* item-or-items)
      (let [item  item-or-items] (item-by-attrs attr-or-attrs item))
      (let [items item-or-items]
        (into {} (mapv (partial item-by-attrs attr-or-attrs) items))))))

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

        (string?    x) (when-not (.isEmpty ^String x)       x)
        (enc/bytes? x) (when-not (zero? (alength ^bytes x)) x)
        :else x))))

