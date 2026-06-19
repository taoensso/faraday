(ns taoensso.faraday2
  (:require [clojure.string :as str]
            [taoensso.encore :as enc]
            [taoensso.faraday2.utils :as utils :refer [coll?*]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.tools :as nippy-tools])
  (:import [clojure.lang BigInt IPersistentVector Keyword LazySeq]
           [java.net URI]
           [java.time Duration]
           [java.util Collection List Map Set]
           [org.apache.http.client.utils URIBuilder]
           [software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    AwsCredentials
                                                    AwsCredentialsProvider
                                                    DefaultCredentialsProvider
                                                    StaticCredentialsProvider]
           [software.amazon.awssdk.core SdkBytes]
           [software.amazon.awssdk.core.client.config ClientOverrideConfiguration]
           [software.amazon.awssdk.http.apache ApacheHttpClient ProxyConfiguration]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.retries StandardRetryStrategy]
           [software.amazon.awssdk.services.dynamodb DynamoDbClient DynamoDbClientBuilder]
           [software.amazon.awssdk.services.dynamodb.model AttributeDefinition
                                                           AttributeValue
                                                           AttributeValueUpdate
                                                           BatchGetItemRequest
                                                           BatchGetItemResponse
                                                           BatchWriteItemRequest
                                                           BatchWriteItemResponse
                                                           BillingModeSummary
                                                           ComparisonOperator
                                                           Condition
                                                           ConditionCheck
                                                           ConditionalCheckFailedException
                                                           ConsumedCapacity
                                                           CreateGlobalSecondaryIndexAction
                                                           CreateTableRequest
                                                           CreateTableResponse
                                                           Delete
                                                           DeleteGlobalSecondaryIndexAction
                                                           DeleteItemRequest
                                                           DeleteItemResponse
                                                           DeleteRequest
                                                           DeleteTableRequest
                                                           DeleteTableResponse
                                                           DescribeStreamRequest
                                                           DescribeStreamResponse
                                                           DescribeTableRequest
                                                           DescribeTableResponse
                                                           DescribeTimeToLiveRequest
                                                           DescribeTimeToLiveResponse
                                                           ExpectedAttributeValue
                                                           Get
                                                           GetItemRequest
                                                           GetItemResponse
                                                           GetRecordsRequest
                                                           GetRecordsResponse
                                                           GetShardIteratorRequest
                                                           GlobalSecondaryIndex
                                                           GlobalSecondaryIndexDescription
                                                           GlobalSecondaryIndexUpdate
                                                           InternalServerErrorException
                                                           ItemCollectionSizeLimitExceededException
                                                           ItemResponse
                                                           KeySchemaElement
                                                           KeysAndAttributes
                                                           LimitExceededException
                                                           ListStreamsRequest
                                                           ListStreamsResponse
                                                           ListTablesRequest
                                                           LocalSecondaryIndex
                                                           LocalSecondaryIndexDescription
                                                           Projection
                                                           ProvisionedThroughput
                                                           ProvisionedThroughputDescription
                                                           ProvisionedThroughputExceededException
                                                           Put
                                                           PutItemRequest
                                                           PutItemResponse
                                                           PutRequest
                                                           QueryRequest
                                                           QueryResponse
                                                           Record
                                                           ResourceInUseException
                                                           ResourceNotFoundException
                                                           ScanRequest
                                                           ScanResponse
                                                           SequenceNumberRange
                                                           Shard
                                                           Stream
                                                           StreamDescription
                                                           StreamRecord
                                                           StreamSpecification
                                                           StreamViewType
                                                           TableDescription
                                                           TimeToLiveSpecification
                                                           TransactGetItem
                                                           TransactGetItemsRequest
                                                           TransactGetItemsResponse
                                                           TransactWriteItem
                                                           TransactWriteItemsRequest
                                                           TransactWriteItemsResponse
                                                           Update
                                                           UpdateGlobalSecondaryIndexAction
                                                           UpdateItemRequest
                                                           UpdateItemResponse
                                                           UpdateTableRequest
                                                           UpdateTableResponse
                                                           UpdateTimeToLiveRequest
                                                           UpdateTimeToLiveResponse
                                                           WriteRequest]
           [software.amazon.awssdk.services.dynamodb.streams DynamoDbStreamsClient DynamoDbStreamsClientBuilder]
           [taoensso.nippy.tools WrappedForFreezing]))

(enc/assert-min-encore-version [3 34 0])

;;;; Connections

(defn- client-params
  [{:as   client-opts
    :keys [provider creds access-key secret-key proxy-host proxy-port
           proxy-username proxy-password conn-timeout max-conns
           max-error-retry socket-timeout keep-alive? protocol]}]

  (let [creds (or creds (:credentials client-opts)) ; Deprecated opt

        _ (assert (or (nil? creds)    (instance? AwsCredentials         creds)))
        _ (assert (or (nil? provider) (instance? AwsCredentialsProvider provider)))

        ^AwsCredentials aws-creds
        (when-not provider
          (cond
            creds creds ; Given explicit AWSCredentials
            access-key (AwsBasicCredentials/create access-key secret-key)))

        ^AwsCredentialsProvider provider
        (or provider (if aws-creds
                       (StaticCredentialsProvider/create aws-creds)
                       (DefaultCredentialsProvider/create)))

        proxy-config (when (or proxy-host proxy-port proxy-username proxy-password)
                       (let [scheme (some-> protocol name str/lower-case)]
                         (when (and scheme (not (#{"http" "https"} scheme)))
                           (throw (IllegalArgumentException. (str "Invalid proxy protocol " scheme))))
                         (cond-> (ProxyConfiguration/builder)
                                 (or proxy-host proxy-port)
                                 (.endpoint (cond-> (URIBuilder.)
                                                    proxy-host (.setHost proxy-host)
                                                    proxy-port (.setPort proxy-port)
                                                    scheme (.setScheme scheme)
                                                    :always (.build)))

                                 proxy-username (.username proxy-username)
                                 proxy-password (.password proxy-password)
                                 :always (.build))))

        http-client (when (or proxy-config conn-timeout max-conns socket-timeout keep-alive?)
                      (cond-> (ApacheHttpClient/builder)
                              proxy-config        (.proxyConfiguration proxy-config)
                              conn-timeout        (.connectionTimeout (Duration/ofMillis conn-timeout))
                              max-conns           (.maxConnections max-conns)
                              socket-timeout      (.socketTimeout (Duration/ofMillis socket-timeout))
                              (some? keep-alive?) (.tcpKeepAlive keep-alive?)
                              :always (.build)))

        override-config (when max-error-retry
                          (-> (ClientOverrideConfiguration/builder)
                              (.retryStrategy (-> (StandardRetryStrategy/builder)
                                                  (.maxAttempts max-error-retry)
                                                  (.build)))
                              (.build)))]

    [provider http-client override-config]))

(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied client opts:
    (db-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                 :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}),
    (db-client* {:creds my-AWSCredentials-instance}),
    etc."
  (memoize
    (fn [{:keys [client endpoint region] :as client-opts}]
      (cond (empty? client-opts) (DynamoDbClient/create) ; Default client
            client client
            :else (let [[^AwsCredentialsProvider provider
                         ^ApacheHttpClient http-client
                         ^ClientOverrideConfiguration override-config] (client-params client-opts)]
                    (cond-> (DynamoDbClient/builder)
                            provider ^DynamoDbClientBuilder (.credentialsProvider provider)
                            http-client ^DynamoDbClientBuilder (.httpClient http-client)
                            override-config ^DynamoDbClientBuilder (.overrideConfiguration override-config)
                            endpoint ^DynamoDbClientBuilder (.endpointOverride (URI/create endpoint))
                            region ^DynamoDbClientBuilder (.region (Region/of region))
                            :always (.build)))))))

(def ^:private db-streams-client*
  "Returns a new AmazonDynamoDBStreamsClient instance for the given client opts:
    (db-streams-client* {:creds my-AWSCredentials-instance}),
    (db-streams-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                         :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}), etc."
  (memoize
    (fn [{:keys [client endpoint region] :as client-opts}]
      (cond (empty? client-opts) (DynamoDbStreamsClient/create) ; Default client
            client client
            :else (let [[^AwsCredentialsProvider provider
                         ^ApacheHttpClient http-client
                         ^ClientOverrideConfiguration override-config] (client-params client-opts)]
                    (cond-> (DynamoDbStreamsClient/builder)
                            provider ^DynamoDbStreamsClientBuilder (.credentialsProvider provider)
                            http-client ^DynamoDbStreamsClientBuilder (.httpClient http-client)
                            override-config ^DynamoDbStreamsClientBuilder (.overrideConfiguration override-config)
                            endpoint ^DynamoDbStreamsClientBuilder (.endpointOverride (URI/create endpoint))
                            region ^DynamoDbStreamsClientBuilder (.region (Region/of region))
                            :always (.build)))))))

(defn- db-client ^DynamoDbClient [client-opts] (db-client* client-opts))
(defn- db-streams-client ^DynamoDbStreamsClient [client-opts] (db-streams-client* client-opts))

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

(def ^:private nt-freeze (comp #(SdkBytes/fromByteArray %) nippy-tools/freeze))
;; (def ^:private nt-thaw (comp nippy-tools/thaw #(.array ^ByteBuffer %)))

(defn- nt-thaw [bb]
  (let [ba          (.asByteArray ^SdkBytes bb)
        serialized? (#'nippy/try-parse-header ba)]
    (if-not serialized?
      ba ; No Nippy header => assume non-serialized binary data (e.g. other client)
      (try ; Header match _may_ have been a fluke (though v. unlikely)
        (nippy-tools/thaw ba)
        (catch Exception _
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

(defn- ddb-num?
  "Is `x` a number type natively storable by DynamoDB? Note that DDB stores _all_
  numbers as exact-value strings with <= 38 digits of precision. For greater
  precision, use `freeze`.
  Ref. http://goo.gl/jzzsIW"
  [x]
  (or (instance? Long    x)
      (instance? Double  x)
      (instance? Integer x)
      (instance? Float   x)
      ;; High-precision types:
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
          (some-> (.s x)    (vector :s))
          (some-> (.n x)    (vector :n))
          (some-> (.nul x)  (vector :null))
          (some-> (.bool x) (vector :bool))
          (some-> (.b x)    (vector :b))
          (when (.hasSs x)  [(.ss x) :ss])
          (when (.hasNs x)  [(.ns x) :ns])
          (when (.hasBs x)  [(.bs x) :bs])
          (when (.hasM x)   [(.m x) :m])
          (when (.hasL x)   [(.l x) :l]))]

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
      :m (zipmap (mapv keyword (.keySet ^Map x))
                 (mapv deserialize (.values ^Map x))))))

(defprotocol ISerializable
  "Extensible protocol for mapping Clojure vals to AttributeValue objects."
  (serialize ^AttributeValue [this]))

(extend-protocol ISerializable
  AttributeValue (serialize [x] x)
  nil        (serialize [_] (-> (AttributeValue/builder) (.nul true) (.build)))
  Boolean    (serialize [x] (-> (AttributeValue/builder) (.bool x) (.build)))
  Long       (serialize [x] (-> (AttributeValue/builder) (.n (str x)) (.build)))
  Double     (serialize [x] (-> (AttributeValue/builder) (.n (str x)) (.build)))
  Integer    (serialize [x] (-> (AttributeValue/builder) (.n (str x)) (.build)))
  Float      (serialize [x] (-> (AttributeValue/builder) (.n (str x)) (.build)))
  BigInt     (serialize [x] (-> (AttributeValue/builder) (.n (str (assert-precision x))) (.build)))
  BigDecimal (serialize [x] (-> (AttributeValue/builder) (.n (str (assert-precision x))) (.build)))
  BigInteger (serialize [x] (-> (AttributeValue/builder) (.n (str (assert-precision x))) (.build)))

  WrappedForFreezing
  (serialize [x] (-> (AttributeValue/builder) (.b (nt-freeze x)) (.build)))

  Keyword
  (serialize [kw] (serialize (enc/as-qname kw)))

  String
  (serialize [s] (-> (AttributeValue/builder) (.s s) (.build)))

  IPersistentVector
  (serialize [v] (-> (AttributeValue/builder) (.l ^Collection (mapv serialize v)) (.build)))

  Map
  (serialize [m]
    (-> (AttributeValue/builder)
        (.m
          (persistent!
            (reduce-kv
              (fn [acc k v] (assoc! acc (enc/as-qname k) (serialize v)))
              (transient {})
              m)))
        (.build)))

  Set
  (serialize [s]
    (if (empty? s)
      (throw (Exception. "Invalid DynamoDB value: empty set"))
      (cond
        (enc/revery? enc/stringy? s) (-> (AttributeValue/builder) (.ss ^Collection (mapv enc/as-qname s)) (.build))
        (enc/revery? ddb-num?     s) (-> (AttributeValue/builder) (.ns ^Collection (mapv str s)) (.build))
        (enc/revery? freeze?      s) (-> (AttributeValue/builder) (.bs ^Collection (mapv nt-freeze s)) (.build))
        :else (throw (Exception. "Invalid DynamoDB value: set of invalid type or more than one type")))))

  LazySeq
  (serialize [s]
    (if (.isRealized s)
      (-> (AttributeValue/builder) (.l ^Collection (mapv serialize s)) (.build))
      (throw (IllegalArgumentException. "Unrealized lazy sequences are not supported. Realize this sequence before calling Faraday (e.g. doall) or replace the sequence with a non-lazy alternative (e.g. 'mapv' instead of 'map', or use 'into []'). Faraday avoids attempting to realize values that might be infinite, since this could cause strange an unexpected problems that are hard to diagnose.")))))

(extend-type (Class/forName "[B")
  ISerializable
  (serialize [ba] (-> (AttributeValue/builder) (.b (nt-freeze ba)) (.build))))

(defn- enum-op ^String [operator]
  (-> operator {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"} (or operator)
      utils/enum))

;;;; Object coercions

(def db-item->clj-item (partial utils/keyword-map deserialize))
(defn clj-item->db-item [item]
  (utils/name-map serialize item))

(defn- cc-units [^ConsumedCapacity cc] (some-> cc (.capacityUnits)))
(defn- batch-cc-units [ccs]
  (reduce
    (fn [m ^ConsumedCapacity cc]
      (assoc m (keyword (.tableName cc)) (cc-units cc)))
    {}
    ccs))

(defprotocol AsMap (as-map [x]))

(defmacro ^:private am-item-result [result get-form]
  `(when-let [get-form# ~get-form]
     (with-meta (db-item->clj-item get-form#)
                {:cc-units (cc-units (.consumedCapacity ~result))})))

(defmacro ^:private am-query|scan-result [result & [meta]]
  `(let [result# ~result]
     (merge {:items (mapv db-item->clj-item (.items result#))
             :count (.count result#)
             :cc-units (cc-units (.consumedCapacity result#))
             :last-prim-kvs (as-map (when (.hasLastEvaluatedKey result#) (.lastEvaluatedKey result#)))}
            ~meta)))

(extend-protocol AsMap
  nil (as-map [_] nil)
  List (as-map [a] (mapv as-map a))
  Map (as-map [m] (utils/keyword-map as-map m))

  AttributeValue (as-map [v] (deserialize v))
  AttributeDefinition (as-map [d] {:name (keyword       (.attributeName d))
                                   :type (utils/un-enum (.attributeType d))})
  KeySchemaElement (as-map [e] {:name (keyword (.attributeName e))
                                   :type (utils/un-enum (.keyType e))})
  KeysAndAttributes
  (as-map [x]
    (merge
      (when-let [a (.attributesToGet x)] {:attrs (mapv keyword a)})
      (when-let [c (.consistentRead  x)] {:consistent? c})
      (when-let [k (.keys            x)] {:keys (mapv db-item->clj-item k)})))


  GetItemResponse    (as-map [r] (when (.hasItem r) (am-item-result r (.item r))))
  PutItemResponse    (as-map [r] (am-item-result r (.attributes r)))
  UpdateItemResponse (as-map [r] (am-item-result r (.attributes r)))
  DeleteItemResponse (as-map [r] (am-item-result r (.attributes r)))

  QueryResponse (as-map [r] (am-query|scan-result r))
  ScanResponse  (as-map [r] (am-query|scan-result r {:scanned-count (.scannedCount r)}))

  BatchGetItemResponse
  (as-map [r]
    {:items       (utils/keyword-map as-map (.responses r))
     :unprocessed (.unprocessedKeys r)
     :cc-units    (batch-cc-units (.consumedCapacity r))})

  BatchWriteItemResponse
  (as-map [r]
    {:unprocessed (.unprocessedItems r)
     :cc-units    (batch-cc-units (.consumedCapacity r))})

  TransactWriteItemsResponse
  (as-map [r]
    {:cc-units (batch-cc-units (.consumedCapacity r))})

  TransactGetItemsResponse
  (as-map [r]
    {:items (mapv #(utils/keyword-map as-map (.item ^ItemResponse %)) (.responses r))
     :cc-units (batch-cc-units (.consumedCapacity r))})

  TableDescription
  (as-map [d]
    {:name                (keyword (.tableName d))
     :creation-date       (.creationDateTime d)
     :item-count          (.itemCount d)
     :size                (.tableSizeBytes d)
     :throughput          (as-map (.provisionedThroughput d))
     :billing-mode        (as-map (or (.billingModeSummary d)
                                      (-> (BillingModeSummary/builder)
                                          (.billingMode "PROVISIONED")
                                          (.build))))
     :indexes             (as-map (.localSecondaryIndexes d)) ; DEPRECATED
     :lsindexes           (as-map (.localSecondaryIndexes d))
     :gsindexes           (as-map (.globalSecondaryIndexes d))
     :stream-spec         (as-map (.streamSpecification d))
     :latest-stream-label (.latestStreamLabel d)
     :latest-stream-arn   (.latestStreamArn d)
     :status              (utils/un-enum (.tableStatus d))
     :prim-keys
     (let [schema (as-map (.keySchema d))
           defs   (as-map (.attributeDefinitions d))]
       (merge-with merge
                   (reduce-kv (fn [m _k v] (assoc m (:name v) {:key-type  (:type v)}))
                              {} schema)
                   (reduce-kv (fn [m _k v] (assoc m (:name v) {:data-type (:type v)}))
                              {} defs)))})

  DescribeTableResponse (as-map [r] (as-map (.table r)))
  CreateTableResponse (as-map [r] (as-map (.tableDescription r)))
  UpdateTableResponse (as-map [r] (as-map (.tableDescription r)))
  DeleteTableResponse (as-map [r] (as-map (.tableDescription r)))

  Projection
  (as-map [p]
    {:projection-type    (.toString (.projectionType p))
     :non-key-attributes (when (.hasNonKeyAttributes p) (.nonKeyAttributes p))})

  LocalSecondaryIndexDescription
  (as-map [d]
    {:name (keyword (.indexName d))
     :size       (.indexSizeBytes d)
     :item-count (.itemCount d)
     :key-schema (as-map (.keySchema d))
     :projection (as-map (.projection d))})

  GlobalSecondaryIndexDescription
  (as-map [d]
    {:name       (keyword (.indexName d))
     :size       (.indexSizeBytes d)
     :item-count (.itemCount d)
     :key-schema (as-map (.keySchema d))
     :projection (as-map (.projection d))
     :throughput (as-map (.provisionedThroughput d))})

  ProvisionedThroughputDescription
  (as-map [d]
    {:read                (.readCapacityUnits d)
     :write               (.writeCapacityUnits d)
     :last-decrease       (.lastDecreaseDateTime d)
     :last-increase       (.lastIncreaseDateTime d)
     :num-decreases-today (.numberOfDecreasesToday d)})

  BillingModeSummary
  (as-map [d]
    {:name                (utils/un-enum (.billingMode d))
     :last-update         (.lastUpdateToPayPerRequestDateTime d)})

  StreamSpecification
  (as-map [s]
    {:enabled?  (.streamEnabled s)
     :view-type (utils/un-enum (.streamViewType s))})

  DescribeStreamResponse
  (as-map [r] (as-map (.streamDescription r)))

  StreamDescription
  (as-map [d]
    {:stream-arn (.streamArn d)
     :stream-label (.streamLabel d)
     :stream-status (utils/un-enum (.streamStatus d))
     :stream-view-type (utils/un-enum (.streamViewType d))
     :creation-request-date-time (.creationRequestDateTime d)
     :table-name (.tableName d)
     :key-schema (as-map (.keySchema d))
     :shards (as-map (.shards d))
     :last-evaluated-shard-id (.lastEvaluatedShardId d)})

  Shard
  (as-map [d]
    {:shard-id              (.shardId d)
     :parent-shard-id       (.parentShardId d)
     :seq-num-range (as-map (.sequenceNumberRange d))})

  SequenceNumberRange
  (as-map [d]
    {:starting-seq-num (.startingSequenceNumber d)
     :ending-seq-num   (.endingSequenceNumber d)})

  GetRecordsResponse
  (as-map [r]
    {:next-shard-iterator (.nextShardIterator r)
     :records             (as-map (.records r))})

  Record
  (as-map [r]
    {:event-id      (.eventID r)
     :event-name    (utils/un-enum (.eventName r))
     :event-version (.eventVersion r)
     :event-source  (.eventSource r)
     :aws-region    (.awsRegion r)
     :stream-record (as-map (.dynamodb r))})

  StreamRecord
  (as-map [r]
    {:keys      (db-item->clj-item (.keys r))
     :old-image (db-item->clj-item (.oldImage r))
     :new-image (db-item->clj-item (.newImage r))
     :seq-num   (.sequenceNumber r)
     :size      (.sizeBytes r)
     :view-type (utils/un-enum (.streamViewType r))})

  ListStreamsResponse
  (as-map [r]
    {:last-stream-arn (.lastEvaluatedStreamArn r)
     :streams (as-map (.streams r))})

  Stream
  (as-map [s]
    {:stream-arn (.streamArn s)
     :table-name (.tableName s)})

  DescribeTimeToLiveResponse
  (as-map [r]
    (let [ttl-desc (.timeToLiveDescription r)]
      {:attribute-name (.attributeName ttl-desc)
       :status (utils/un-enum (.timeToLiveStatus ttl-desc))}))

  UpdateTimeToLiveResponse
  (as-map [r]
    (let [ttl-spec (.timeToLiveSpecification r)]
      {:enabled? (.enabled ttl-spec)
       :attribute-name (.attributeName ttl-spec)})))

;;;; Tables

(defn list-tables "Returns a lazy sequence of table names."
  [client-opts]
  (let [step
        (fn step [^String offset]
          (lazy-seq
            (let [client ^DynamoDbClient (db-client client-opts)
                  result (if (nil? offset)
                           (.listTables client)
                           (.listTables client (-> (ListTablesRequest/builder)
                                                   (.exclusiveStartTableName offset)
                                                   ^ListTablesRequest (.build))))
                  last-key (.lastEvaluatedTableName result)
                  chunk (map keyword (.tableNames result))]
              (if last-key
                (concat chunk (step (name last-key)))
                chunk))))]
    (step nil)))

(defn- describe-table-request "Implementation detail."
  ^DescribeTableRequest [table]
  (-> (DescribeTableRequest/builder) (.tableName (name table)) (.build)))

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
          (do (Thread/sleep ^long poll-ms)
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
          (do (Thread/sleep ^long poll-ms)
              (recur))

          :else index)))))

(defn- key-schema-element "Returns a new KeySchemaElement object."
  [key-name key-type]
  (-> (KeySchemaElement/builder)
      (.attributeName (name key-name))
      (.keyType (utils/enum key-type))
      (.build)))

(defn- key-schema
  "Returns a [{<hash-key> KeySchemaElement}], or
             [{<hash-key> KeySchemaElement} {<range-key> KeySchemaElement}]
   vector for use as a table/index primary key."
  ^Collection [[hname _ :as _hash-keydef] & [[rname _ :as range-keydef]]]
  (cond-> [(key-schema-element hname :hash)]
          range-keydef (conj (key-schema-element rname :range))))

(defn- provisioned-throughput "Returns a new ProvisionedThroughput object."
  ^ProvisionedThroughput [{read-units :read write-units :write :as throughput}]
  (assert (and read-units write-units)
          (str "Malformed throughput: " throughput))
  (-> (ProvisionedThroughput/builder)
      (.readCapacityUnits (long read-units))
      (.writeCapacityUnits (long write-units))
      (.build)))

(defn- keydefs "[<name> <type>] defs -> [AttributeDefinition ...]"
  ^Collection [hash-keydef range-keydef lsindexes gsindexes]
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
        (-> (AttributeDefinition/builder)
            (.attributeName (name key-name))
            (.attributeType (utils/enum key-type))
            (.build)))
      defs)))

(defn- projection-obj
  ^Projection [projection]
  (let [ptype (if (coll?* projection) :include projection)]
    (-> (Projection/builder)
        (.projectionType (utils/enum ptype))
        (cond-> (= ptype :include) (.nonKeyAttributes ^Collection (mapv name projection)))
        (.build))))

(defn- local-2nd-indexes
  "Implementation detail.
  [{:name _ :range-keydef _ :projection _} ...] indexes -> [LocalSecondaryIndex ...]"
  ^Collection [hash-keydef indexes]
  (when indexes
    (mapv
      (fn [{index-name :name
            :keys [range-keydef projection]
            :or   {projection :all}
            :as   index}]
        (assert (and index-name range-keydef projection)
                (str "Malformed local secondary index (LSI): " index))
        (-> (LocalSecondaryIndex/builder)
              (.indexName (name index-name))
              (.keySchema (key-schema hash-keydef range-keydef))
              (.projection (projection-obj projection))
            (.build)))
      indexes)))

(defn- global-2nd-indexes
  "Implementation detail.
  [{:name _ :hash-keydef _ :range-keydef _ :projection _ :throughput _} ...]
  indexes -> [GlobalSecondaryIndex ...]"
  ^Collection [indexes billing-mode]
  (when indexes
    (mapv
      (fn [{index-name :name
            :keys [hash-keydef range-keydef throughput projection]
            :or   {projection :all}
            :as   index}]
        (assert (and index-name hash-keydef projection (or
                                                         (not (= :pay-per-request billing-mode))
                                                         (and (= :pay-per-request billing-mode) (not throughput))))
                (str "Malformed global secondary index (GSI): " index))
        (cond-> (GlobalSecondaryIndex/builder)
                :always (.indexName (name index-name))
                :always (.keySchema (key-schema hash-keydef range-keydef))
                :always (.projection (projection-obj projection))
                throughput (.provisionedThroughput (provisioned-throughput throughput))
                :always (.build)))
      indexes)))

(defn- stream-specification "Implementation detail."
  ^StreamSpecification [{:keys [enabled? view-type]}]
  (cond-> (StreamSpecification/builder)
          (not (nil? enabled?)) (.streamEnabled enabled?)
          view-type (.streamViewType (StreamViewType/fromValue (utils/enum view-type)))
          :always (.build)))

(defn- create-table-request "Implementation detail."
  ^CreateTableRequest [table-name hash-keydef
                       & [{:keys [range-keydef throughput lsindexes gsindexes stream-spec billing-mode]
                           :or {billing-mode :provisioned} :as opts}]]
  (assert (not (and throughput (= :pay-per-request billing-mode))) "Can't specify :throughput and :pay-per-request billing-mode")
  (let [lsindexes (or lsindexes (:indexes opts))]
    (cond-> (CreateTableRequest/builder)
            :always (.tableName (name table-name))
            :always (.keySchema (key-schema hash-keydef range-keydef))
            throughput (.provisionedThroughput (provisioned-throughput (or throughput {:read 1 :write 1})))
            :always (.billingMode (utils/enum billing-mode))
            :always (.attributeDefinitions
                      (keydefs hash-keydef range-keydef lsindexes gsindexes))
            lsindexes (.localSecondaryIndexes
                        (local-2nd-indexes hash-keydef lsindexes))
            gsindexes (.globalSecondaryIndexes
                        (global-2nd-indexes gsindexes billing-mode))
            stream-spec (.streamSpecification
                          (stream-specification stream-spec))
            :always (.build))))

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
  "Validate new GSI configuration and create a GlobalSecondaryIndexUpdate"
  [{:keys [billing-mode] :as _table-desc}
   {index-name :name
    :keys [hash-keydef range-keydef throughput projection operation]
    :or {projection :all}
    :as index}]
  (case operation
    :create
    (do
      (assert (and index-name hash-keydef projection (or (and throughput
                                                              (not= :pay-per-request (:name billing-mode)))
                                                         (and (not throughput)
                                                              (= :pay-per-request (:name billing-mode)))))
              (str "Malformed global secondary index (GSI): " index))
      (-> (GlobalSecondaryIndexUpdate/builder)
          (.create (-> (CreateGlobalSecondaryIndexAction/builder)
                       (.indexName (name index-name))
                       (.keySchema (key-schema hash-keydef range-keydef))
                       (.projection (projection-obj projection))
                       (cond-> throughput (.provisionedThroughput (provisioned-throughput throughput)))
                       ^CreateGlobalSecondaryIndexAction (.build)))
          (.build)))

    :update
    (-> (GlobalSecondaryIndexUpdate/builder)
        (.update (-> (UpdateGlobalSecondaryIndexAction/builder)
                     (.indexName (name index-name))
                     (.provisionedThroughput (provisioned-throughput throughput))
                     ^UpdateGlobalSecondaryIndexAction (.build)))
        (.build))

    :delete
    (-> (GlobalSecondaryIndexUpdate/builder)
        (.delete (-> (DeleteGlobalSecondaryIndexAction/builder)
                     (.indexName (name index-name))
                     ^DeleteGlobalSecondaryIndexAction (.build)))
        (.build))
    nil))

(defn- update-table-request "Implementation detail."
  ^UpdateTableRequest [table table-desc {:keys [throughput gsindexes stream-spec billing-mode] :as _params}]
  (assert (not (and throughput
                    (= :pay-per-request billing-mode))) "Can't specify :throughput and :pay-per-request billing-mode")
  (let [gsindexes (cond-> gsindexes (map? gsindexes) vector)
        attr-defs (keydefs nil nil nil gsindexes)]
    (cond-> (UpdateTableRequest/builder)
            :always (.tableName (name table))
            throughput (.provisionedThroughput (provisioned-throughput throughput))
            billing-mode (.billingMode (utils/enum billing-mode))
            gsindexes (.globalSecondaryIndexUpdates ^Collection (mapv (partial global-2nd-index-updates table-desc) gsindexes))
            stream-spec (.streamSpecification (stream-specification stream-spec))
            (seq attr-defs) (.attributeDefinitions attr-defs)
            :always (.build))))

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
  [client-opts table update-opts & [{:keys [span-reqs]}]]
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
              (update-table-request table table-desc update-opts))
            ;; Returns _new_ descr when ready:
            @(table-status-watch client-opts table :updating)))))))

(defn- delete-table-request "Implementation detail."
  ^DeleteTableRequest [table]
  (-> (DeleteTableRequest/builder)
      (.tableName (name table))
      (.build)))

(defn delete-table "Deletes a table, go figure."
  [client-opts table]
  (as-map (.deleteTable (db-client client-opts) (delete-table-request table))))

;;;; Items

(defn- get-item-request "Implementation detail."
  ^GetItemRequest [table prim-kvs & [{:keys [attrs consistent? return-cc? proj-expr expr-attr-names]}]]
  (cond-> (GetItemRequest/builder)
          :always (.tableName (name table))
          :always (.key (clj-item->db-item prim-kvs))
          consistent? (.consistentRead consistent?)
          attrs (.attributesToGet ^Collection (mapv name attrs))
          proj-expr (.projectionExpression proj-expr)
          expr-attr-names (.expressionAttributeNames expr-attr-names)
          return-cc? (.returnConsumedCapacity (utils/enum :total))
          :always (.build)))

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
         :exists (-> (ExpectedAttributeValue/builder)
                     (.comparisonOperator ComparisonOperator/NOT_NULL)
                     (.build))
         :not-exists (-> (ExpectedAttributeValue/builder)
                         (.exists false)
                         (.build))
         (if (vector? %)
           (-> (ExpectedAttributeValue/builder)
               (.comparisonOperator (enum-op (first %)))
               (.attributeValueList ^Collection (mapv serialize (rest %)))
               (.build))
           (-> (ExpectedAttributeValue/builder)
               (.value (serialize %))
               (.build))))
      expected-map)))

(defn- clj->db-expr-vals-map [m] (enc/map-vals serialize m))

(def ^:private deprecation-warning-expected_
  (delay
    (println "Faraday WARNING: `:expected` option is deprecated in favor of `:cond-expr`")))

(def ^:private deprecation-warning-update-map_
  (delay
    (println "Faraday WARNING: `update-map` is deprecated in favor of `:update-expr`")))

(defn- put-item-request
  ^PutItemRequest [table item &
                   [{:keys [return expected return-cc? cond-expr expr-attr-names expr-attr-vals]
                     :or {return :none}}]]
  (cond-> (PutItemRequest/builder)
          :always         (.tableName    (name table))
          :always         (.item         (clj-item->db-item item))
          expected        (.expected     (expected-vals expected))
          cond-expr       (.conditionExpression cond-expr)
          expr-attr-names (.expressionAttributeNames expr-attr-names)
          expr-attr-vals  (.expressionAttributeValues
                            (clj->db-expr-vals-map expr-attr-vals))
          return          (.returnValues (utils/enum return))
          return-cc?      (.returnConsumedCapacity (utils/enum :total))
          :always (.build)))

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
        (-> (AttributeValueUpdate/builder)
            (.value (when (or (not= action :delete) (set? val))
                      (serialize val)))
            (.action (utils/enum action))
            (.build)))
      update-map)))

(defn- update-item-request
  ^UpdateItemRequest [table prim-kvs &
                      [{:keys [return expected return-cc? update-map
                               cond-expr update-expr expr-attr-names expr-attr-vals]
                        :or {return :none}}]]
(cond-> (UpdateItemRequest/builder)
        :always         (.tableName        (name table))
        :always         (.key (clj-item->db-item prim-kvs))
        update-map      (.attributeUpdates (attribute-updates update-map))
        update-expr     (.updateExpression update-expr)
        expr-attr-names (.expressionAttributeNames expr-attr-names)
        expr-attr-vals  (.expressionAttributeValues
                          (clj->db-expr-vals-map expr-attr-vals))
        expected        (.expected         (expected-vals expected))
        cond-expr       (.conditionExpression cond-expr)
        return          (.returnValues     (utils/enum return))
        return-cc?      (.returnConsumedCapacity (utils/enum :total))
        :always         (.build)))

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
  ^DeleteItemRequest [table prim-kvs & [{:keys [return expected return-cc? cond-expr expr-attr-vals expr-attr-names]
                                         :or {return :none}}]]
  (cond-> (DeleteItemRequest/builder)
          :always         (.tableName    (name table))
          :always         (.key          (clj-item->db-item prim-kvs))
          cond-expr       (.conditionExpression cond-expr)
          expr-attr-names (.expressionAttributeNames expr-attr-names)
          expr-attr-vals  (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
          expected        (.expected     (expected-vals expected))
          return          (.returnValues (utils/enum return))
          return-cc?      (.returnConsumedCapacity (utils/enum :total))
          :always (.build)))

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
  ^Collection [attr-multi-vs-map]
  (let [expand? *attr-multi-vs?*
        expand? (if (nil? expand?) attr-multi-vs?-default expand?)
        ensure-sequential (fn [x] (if (sequential? x) x [x]))]
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
        [] (ensure-sequential attr-multi-vs-map)))))

(defn- batch-request-items
  "Implementation detail.
  {<table> <request> ...} -> {<table> KeysAndAttributes> ...}"
  [requests]
  (utils/name-map
    (fn [{:keys [prim-kvs attrs consistent?]}]
      (cond-> (KeysAndAttributes/builder)
              attrs       (.attributesToGet ^Collection (mapv name attrs))
              consistent? (.consistentRead consistent?)
              :always     (.keys (attr-multi-vs prim-kvs))
              :always     (.build)))
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
          (when throttle-ms (Thread/sleep ^long throttle-ms))
          (recur (let [next-result (more-f more)]
                   (merge (merge-with merge-results (dissoc last-result :items) (dissoc next-result :items))
                          {:items (enc/nested-merge-with into (:items last-result) (:items next-result))}))
                 (inc idx)))))))

(defn- batch-get-item-request "Implementation detail."
  ^BatchGetItemRequest [return-cc? raw-req]
  (cond-> (BatchGetItemRequest/builder)
          :always    (.requestItems raw-req)
          return-cc? (.returnConsumedCapacity (utils/enum :total))
          :always    (.build)))

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
   & [{:keys [return-cc? span-reqs attr-multi-vs?] :as _opts}]]
  (binding [*attr-multi-vs?* attr-multi-vs?]
    (let [run1
          (fn [raw-req]
            (as-map
              (.batchGetItem (db-client client-opts)
                             (batch-get-item-request return-cc? raw-req))))]
      (merge-more run1 span-reqs (run1 (batch-request-items requests))))))

(defn- write-request
  "Implementation detail."
  ^WriteRequest [action item]
  (case action
    :put    (-> (WriteRequest/builder)
                (.putRequest (-> (PutRequest/builder) (.item item) ^PutRequest (.build)))
                (.build))
    :delete (-> (WriteRequest/builder)
                (.deleteRequest (-> (DeleteRequest/builder) (.key item) ^DeleteRequest (.build)))
                (.build))))

(defn- batch-write-item-request "Implementation detail."
  ^BatchWriteItemRequest [return-cc? raw-req]
  (cond-> (BatchWriteItemRequest/builder)
          :always    (.requestItems raw-req)
          return-cc? (.returnConsumedCapacity (utils/enum :total))
          :always    (.build)))

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
   [{:keys [return-cc? span-reqs attr-multi-vs?] :as _opts}]]

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

(defmulti ^:private transact-write-item first)

(defmethod transact-write-item :cond-check
  [[_ {:keys [table-name prim-kvs cond-expr
              expr-attr-names expr-attr-vals return] :as _request
       :or {return :none}}]]
  (let [condition-check (cond-> (ConditionCheck/builder)
                                :always (.tableName (name table-name))
                                :always (.key (clj-item->db-item prim-kvs))
                                :always (.conditionExpression cond-expr)
                                expr-attr-names (.expressionAttributeNames expr-attr-names)
                                expr-attr-vals (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                                return (.returnValuesOnConditionCheckFailure (utils/enum return))
                                :always (.build))]
   (-> (TransactWriteItem/builder)
       (.conditionCheck ^ConditionCheck condition-check)
       (.build))))

(defmethod transact-write-item :put
  [[_ {:keys [table-name item cond-expr expr-attr-names expr-attr-vals return]
       :or {return :none}}]]
  (let [put (cond-> (Put/builder)
                    :always (.tableName (name table-name))
                    :always (.item (clj-item->db-item item))
                    cond-expr (.conditionExpression cond-expr)
                    expr-attr-names (.expressionAttributeNames expr-attr-names)
                    expr-attr-vals (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                    return (.returnValuesOnConditionCheckFailure (utils/enum return))
                    :always (.build))]
    (-> (TransactWriteItem/builder)
        (.put ^Put put)
        (.build))))

(defmethod transact-write-item :delete
  [[_ {:keys [table-name prim-kvs cond-expr expr-attr-names expr-attr-vals return]
       :or {return :none}}]]
  (let [delete (cond-> (Delete/builder)
                       :always (.tableName (name table-name))
                       :always (.key (clj-item->db-item prim-kvs))
                       cond-expr (.conditionExpression cond-expr)
                       expr-attr-names (.expressionAttributeNames expr-attr-names)
                       expr-attr-vals (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                       return (.returnValuesOnConditionCheckFailure (utils/enum return))
                       :always (.build))]
    (-> (TransactWriteItem/builder)
        (.delete ^Delete delete)
        (.build))))

(defmethod transact-write-item :update
  [[_ {:keys [table-name prim-kvs update-expr
              cond-expr expr-attr-names expr-attr-vals return]
       :or {return :none}}]]
  (let [u (cond-> (Update/builder)
                  :always (.tableName (name table-name))
                  :always (.key (clj-item->db-item prim-kvs))
                  :always (.updateExpression update-expr)
                  cond-expr (.conditionExpression cond-expr)
                  expr-attr-names (.expressionAttributeNames expr-attr-names)
                  expr-attr-vals (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                  return (.returnValuesOnConditionCheckFailure (utils/enum return))
                  :always (.build))]
    (-> (TransactWriteItem/builder)
        (.update ^Update u)
        (.build))))

(defn- transact-write-items-request
  ^TransactWriteItemsRequest [{:keys [client-req-token return-cc? items]}]
  (cond-> (TransactWriteItemsRequest/builder)
          :always          (.transactItems ^Collection (mapv transact-write-item items))
          client-req-token (.clientRequestToken client-req-token)
          return-cc?       (.returnConsumedCapacity (utils/enum :total))
          :always          (.build)))

(defn transact-write-items
  "Executes a transaction comprising puts, updates, deletes and cond-checks; either all succeed or all fail.
   Execution order is the order in the items vector.

   For example:
   (far/transact-write-items client-opts
                             {:items [[:cond-check {:table-name ttable
                                                    :prim-kvs {:id 300}
                                                    :cond-expr \"attribute_exists(#id)\"
                                                    :expr-attr-names {\"#id\" \"id\"}}]
                                      [:put {:table-name ttable
                                             :item i2
                                             :cond-expr \"attribute_not_exists(#id)\"
                                             :expr-attr-names {\"#id\" \"id\"}}]
                                      [:delete {:table-name ttable
                                                :prim-kvs {:id 303}
                                                :cond-expr \"attribute_exists(#id)\"
                                                :expr-attr-names {\"#id\" \"id\"}}]
                                      [:update {:table-name ttable
                                                :prim-kvs {:id 300}
                                                :update-expr \"SET #name = :name\"
                                                :cond-expr \"attribute_exists(#id) AND #name = :oldname\"
                                                :expr-attr-names {\"#id\" \"id\", \"#name\" \"name\"}
                                                :expr-attr-vals {\":oldname\" \"foo\", \":name\" \"foobar\"}}]]})
  returns {:cc-units {<table> <consumed-capacity>}}}"
  [client-opts raw-req]
  (as-map
    (.transactWriteItems (db-client client-opts)
                         (transact-write-items-request raw-req))))

(defn- transact-get-item
  [{:keys [table-name prim-kvs expr-attr-names proj-expr]}]
  (let [g (cond-> (Get/builder)
                  :always         (.tableName (name table-name))
                  :always         (.key (clj-item->db-item prim-kvs))
                  expr-attr-names (.expressionAttributeNames expr-attr-names)
                  proj-expr       (.projectionExpression proj-expr)
                  :always         (.build))]
    (-> (TransactGetItem/builder)
        (.get ^Get g)
        (.build))))

(defn- transact-get-items-request
  ^TransactGetItemsRequest [{:keys [return-cc? items]}]
  (cond-> (TransactGetItemsRequest/builder)
          :always (.transactItems ^Collection (mapv transact-get-item items))
          return-cc? (.returnConsumedCapacity (utils/enum :total))
          :always (.build)))

(defn transact-get-items
  "Transactionally fetches requested items by primary key

  e.g.
  (far/transact-get-items client-opts
                          {:return-cc? true
                           :items [{:table-name ttable
                                    :prim-kvs {:id 305}}
                                   {:table-name ttable
                                   :prim-kvs {:id 306}}]})
  returns {:items [<item>] :cc-units {<table> <consumed-capacity>}}"
  [client-opts raw-req]


  (as-map
    (.transactGetItems (db-client client-opts)
                       (transact-get-items-request raw-req))))


;;;; API - queries & scans

(defn- query|scan-conditions
  "{<attr> [operator <val-or-vals>] ...} -> {<attr> Condition ...}"
  [conditions]
  (when (seq conditions)
    (utils/name-map
      (fn [[operator val-or-vals & more :as condition]]
        (assert (not (seq more)) (str "Malformed condition: " condition))
        (let [vals (if (coll?* val-or-vals) val-or-vals [val-or-vals])]
          (-> (Condition/builder)
              (.comparisonOperator (enum-op operator))
              (.attributeValueList ^Collection (mapv serialize vals))
              (.build))))
      conditions)))

(defn- query-request "Implementation detail."
  ^QueryRequest [table prim-key-conds
                 & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
                            proj-expr filter-expr expr-attr-vals expr-attr-names return-cc?] :as _opts
                     :or {order :asc}}]]
  (cond-> (QueryRequest/builder)
          :always (.tableName        (name table))
          :always (.keyConditions    (query|scan-conditions prim-key-conds))
          :always (.scanIndexForward (case order :asc true :desc false))
          last-prim-kvs   (.exclusiveStartKey
                            (clj-item->db-item last-prim-kvs))
          query-filter    (.queryFilter (query|scan-conditions query-filter))
          proj-expr       (.projectionExpression proj-expr)
          filter-expr     (.filterExpression filter-expr)
          expr-attr-names (.expressionAttributeNames expr-attr-names)
          expr-attr-vals  (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
          limit           (.limit (int limit))
          index           (.indexName (name index))
          consistent?     (.consistentRead consistent?)
          (coll?* return) (.attributesToGet ^Collection (mapv name return))
          return-cc?      (.returnConsumedCapacity (utils/enum :total))
          (and return (not (coll?* return))) (.select (utils/enum return))
          :always (.build)))

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
              return-cc?] :as opts}]]
  (let [run1
        (fn [last-prim-kvs]
          (update-in
            (as-map
              (.query
                (db-client client-opts)
                (query-request table prim-key-conds
                               (assoc opts :last-prim-kvs last-prim-kvs))))
            [:items]
            (fn [items] {table items})))
        result (merge-more run1 span-reqs (run1 last-prim-kvs))]
    (with-meta (get result table) (meta result))))

(defn- scan-request
  ^ScanRequest [table
                & [{:keys [attr-conds last-prim-kvs return limit total-segments
                           proj-expr expr-attr-names filter-expr expr-attr-vals
                           consistent? index segment return-cc?] :as opts}]]
  (cond-> (ScanRequest/builder)
          :always (.tableName (name table))
          attr-conds      (.scanFilter (query|scan-conditions attr-conds))
          expr-attr-names (.expressionAttributeNames expr-attr-names)
          expr-attr-vals  (.expressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
          filter-expr     (.filterExpression filter-expr)
          index           (.indexName (name index))
          last-prim-kvs   (.exclusiveStartKey
                            (clj-item->db-item last-prim-kvs))
          limit           (.limit (int limit))
          proj-expr       (.projectionExpression proj-expr)
          total-segments  (.totalSegments (int total-segments))
          segment         (.segment (int segment))
          consistent?     (.consistentRead consistent?)
          (coll?* return) (.attributesToGet ^Collection (mapv name return))
          return-cc? (.returnConsumedCapacity (utils/enum :total))
          (and return (not (coll?* return))) (.select (utils/enum return))
          :always (.build)))

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
              segment return-cc?] :as opts}]]

  (assert (not (and filter-expr (seq attr-conds)))
          "Only one of ':filter-expr' or :attr-conds should be provided")

  (let [run1
        (fn [last-prim-kvs]
          (update-in
            (as-map
              (.scan
                (db-client client-opts)
                (scan-request table (assoc opts :last-prim-kvs last-prim-kvs))))
            [:items]
            (fn [items] {table items})))
        result (merge-more run1 span-reqs (run1 last-prim-kvs))]
    (with-meta (get result table) (meta result))))

(defn scan-lazy-seq
  "Returns a lazy sequence of items from table. Requests are stitched together to
  produce a continuous sequence, with additional requests occurring only when
  needed.

  See scan for supported options. :span-reqs is ignored."
  [client-opts table {:keys [limit] :as opts}]
  (lazy-seq
    (let [result (scan client-opts table (assoc opts :span-reqs {:max 0}))
          {:keys [last-prim-kvs]} (meta result)
          result-seq (if last-prim-kvs
                       (lazy-cat result
                                 (scan-lazy-seq client-opts table (-> opts
                                                                      (assoc :last-prim-kvs last-prim-kvs)
                                                                      (dissoc :limit))))
                       result)]
      (if limit
        (take limit result-seq)
        result-seq))))

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
  ^ListStreamsRequest [{:keys [table-name limit start-arn]}]
  (cond-> (ListStreamsRequest/builder)
          table-name (.tableName (name table-name))
          limit (.limit (int limit))
          start-arn (.exclusiveStartStreamArn start-arn)
          :always (.build)))

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
  (cond-> (DescribeStreamRequest/builder)
          stream-arn (.streamArn stream-arn)
          limit (.limit (int limit))
          start-shard-id (.exclusiveStartShardId start-shard-id)
          :always (.build)))

(defn describe-stream
  "Returns a map describing a stream, or nil if the stream doesn't exist."
  [client-opts stream-arn & [{:keys [limit start-shard-id] :as opts}]]
  (try (as-map (.describeStream (db-streams-client client-opts)
                                (describe-stream-request stream-arn opts)))
       (catch ResourceNotFoundException _ nil)))

(defn- get-shard-iterator-request
  ^GetShardIteratorRequest [stream-arn shard-id iterator-type {:keys [seq-num]}]
  (cond-> (GetShardIteratorRequest/builder)
          :always (.streamArn stream-arn)
          :always (.shardId shard-id)
          :always (.shardIteratorType (utils/enum iterator-type))
          seq-num (.sequenceNumber seq-num)
          :always (.build)))

(defn shard-iterator
  "Returns the iterator string that can be used in the get-stream-records call
   or nil when the stream or shard doesn't exist."
  [client-opts stream-arn shard-id iterator-type
   & [{:keys [seq-num] :as opts}]]
  (try
    (-> (db-streams-client client-opts)
        (.getShardIterator (get-shard-iterator-request stream-arn shard-id iterator-type opts))
        (.shardIterator))
    (catch ResourceNotFoundException _ nil)))

(defn- get-records-request
  ^GetRecordsRequest [iter-str {:keys [limit]}]
  (cond-> (GetRecordsRequest/builder)
          :always (.shardIterator iter-str)
          limit   (.limit (int limit))
          :always (.build)))

(defn get-stream-records
  [client-opts shard-iterator & [{:keys [limit] :as opts}]]
  (as-map (.getRecords (db-streams-client client-opts)
                       (get-records-request shard-iterator opts))))

;;;; TTL API

(defn- describe-ttl-request
  ^DescribeTimeToLiveRequest [{:keys [table-name]}]
  (-> (DescribeTimeToLiveRequest/builder)
      (.tableName (name table-name))
      (.build)))

(defn describe-ttl
  "Returns a map describing the TTL configuration for a table, or nil if
  the table does not exist."
  [client-opts table-name]
  (try
    (as-map
      (.describeTimeToLive (db-client client-opts)
                           (describe-ttl-request {:table-name table-name})))
    (catch ResourceNotFoundException _ nil)))

(defn- update-ttl-request
  ^UpdateTimeToLiveRequest [{:keys [table-name enabled? key-name]}]
  (let [ttl-spec (cond-> (TimeToLiveSpecification/builder)
                         :always  (.enabled enabled?)
                         key-name (.attributeName (name key-name))
                         :always  (.build))]
    (-> (UpdateTimeToLiveRequest/builder)
        (.tableName (name table-name))
        (.timeToLiveSpecification ^TimeToLiveSpecification ttl-spec)
        (.build))))

(defn update-ttl
  "Updates the TTL configuration for a table."
  [client-opts table-name enabled? key-name]
  (as-map
    (.updateTimeToLive (db-client client-opts)
                       (update-ttl-request {:table-name table-name
                                            :enabled? enabled?
                                            :key-name key-name}))))

(defn ensure-ttl
  "Activates TTL with the given key-name iff the TTL configuration is
  not already set in this way."
  [client-opts table-name key-name]
  (let [ttl-spec (describe-ttl client-opts table-name)]
    (when-not (or (= {:attribute-name (name key-name) :status :enabled} ttl-spec)
                  (= {:attribute-name (name key-name) :status :enabling} ttl-spec))
      (update-ttl client-opts table-name true key-name))))

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
  Ref. http://goo.gl/Xg85pO. Note that empty string and binary attributes are
  now permitted: https://amzn.to/3szZ0E5 See also `freeze` as an alternative."
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

        :else x))))
