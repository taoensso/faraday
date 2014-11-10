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
  (:require [clojure.string            :as str]
            [taoensso.encore           :as encore :refer (doto-cond)]
            [taoensso.nippy            :as nippy]
            [taoensso.nippy.tools      :as nippy-tools]
            [taoensso.faraday.utils    :as utils :refer (coll?*)]
            [taoensso.faraday.requests :as reqs]
            [taoensso.faraday.coercion :refer [db-item->clj-item
                                               db-val->clj-val
                                               clj-item->db-item]])
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
             CreateTableResult
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
             ListTablesResult
             LocalSecondaryIndex
             LocalSecondaryIndexDescription
             GlobalSecondaryIndex
             GlobalSecondaryIndexDescription
             Projection
             ProvisionedThroughput
             ProvisionedThroughputDescription
             PutItemResult
             QueryResult
             ScanResult
             TableDescription
             UpdateItemResult
             UpdateTableResult
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
            java.nio.ByteBuffer))

;; TODO Add support for splitting data > 64KB over > 1 keys? This may be tough
;; to do well relative to the payoff. And I'm guessing there may be an official
;; (Java) lib to offer this capability at some point?

;;;; Connections

(def ^:private db-client*
  "Returns a new AmazonDynamoDBClient instance for the supplied client opts:
    (db-client* {:access-key \"<AWS_DYNAMODB_ACCESS_KEY>\"
                 :secret-key \"<AWS_DYNAMODB_SECRET_KEY>\"}),
    (db-client* {:creds my-AWSCredentials-instance}),
    etc."
  (memoize
   (fn [{:keys [provider creds access-key secret-key endpoint proxy-host proxy-port
               conn-timeout max-conns max-error-retry socket-timeout]
        :as client-opts}]
     (if (empty? client-opts) (AmazonDynamoDBClient.) ; Default client
       (let [creds (or creds (:credentials client-opts)) ; Deprecated opt
             _ (assert (or (nil? creds) (instance? AWSCredentials creds)))
             _ (assert (or (nil? provider) (instance? AWSCredentialsProvider provider)))
             ^AWSCredentials aws-creds
             (when-not provider
               (cond
                creds      creds ; Given explicit AWSCredentials
                access-key (BasicAWSCredentials. access-key secret-key)
                :else      (DefaultAWSCredentialsProviderChain.)))
             client-config
             (doto-cond [g (ClientConfiguration.)]
               proxy-host      (.setProxyHost         g)
               proxy-port      (.setProxyPort         g)
               conn-timeout    (.setConnectionTimeout g)
               max-conns       (.setMaxConnections    g)
               max-error-retry (.setMaxErrorRetry     g)
               socket-timeout  (.setSocketTimeout     g))]
         (doto-cond [g (AmazonDynamoDBClient. (or provider aws-creds) client-config)]
           endpoint (.setEndpoint g)))))))

(defn- db-client ^AmazonDynamoDBClient [client-opts] (db-client* client-opts))

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

(encore/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(encore/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type to be subject to automatic de/serialization with
  Nippy.")

(defn- cc-units [^ConsumedCapacity cc] (some-> cc (.getCapacityUnits)))

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
     :cc-units    (cc-units (.getConsumedCapacity r))})

  BatchWriteItemResult
  (as-map [r]
    {:unprocessed (.getUnprocessedItems r)
     :cc-units    (cc-units (.getConsumedCapacity r))})

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

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [client-opts table]
  (try (as-map (.describeTable (db-client client-opts)
                ^DescribeTableRequest (reqs/describe-table-request table)))
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
       :as   opts}]]
  (let [lsindexes (or lsindexes (:indexes opts)) ; DEPRECATED
        result
        (as-map
         (.createTable
          (db-client client-opts)
          (reqs/create-table-request table-name hash-keydef opts)))]
    (if-not block? result @(table-status-watch client-opts table-name :creating))))

(comment (time (create-table mc "delete-me7" [:id :s] {:block? true})))

(defn ensure-table "Creates a table iff it doesn't already exist."
  [client-opts table-name hash-keydef & [opts]]
  (when-not (describe-table client-opts table-name)
    (create-table client-opts table-name hash-keydef opts)))

(defn- throughput-steps
  "Dec by any amount, inc by <= 2x current amount, Ref. http://goo.gl/Bj9TC.
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
                     (.updateTable
                      (db-client client-opts)
                      (reqs/update-table-request table {:read r' :write w'})))
                    ;; Returns _new_ descr when ready:
                    @(table-status-watch client-opts table :updating))]

            (let [p (promise)]
              (future (deliver p (peek (mapv run1 steps))))
              p)))))

(comment
  (def dt (describe-table client-opts :faraday.tests.main))
  (let [p (update-table client-opts :faraday.tests.main {:read 1 :write 1})]
    @p))

(defn delete-table "Deletes a table, go figure."
  [client-opts table]
  (as-map (.deleteTable (db-client client-opts) ^DeleteTableRequest (reqs/delete-table-request table))))

;;;; API - items

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs     - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs       - Attrs to return, [<attr> ...].
    :consistent? - Use strongly (rather than eventually) consistent reads?"
  [client-opts table prim-kvs & [opts]]
  (as-map
   (.getItem (db-client client-opts)
    (reqs/get-item-request table prim-kvs opts))))

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return   - e/o #{:none :all-old :updated-old :all-new :updated-new}.
    :expected - A map of item attribute/condition pairs, all of which must be
                met for the operation to succeed. e.g.:
                  {<attr> <expected-value> ...}
                  {<attr> false ...} ; Attribute must not exist"
  [client-opts table item & [{:keys [return expected return-cc?]
                              :as   opts}]]
  (as-map
   (.putItem (db-client client-opts)
     (reqs/put-item-request table item opts))))

(defn update-item
  "Updates an item in a table by its primary key with options:
    prim-kvs   - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    update-map - {<attr> [<#{:put :add :delete}> <optional value>]}.
    :return    - e/o #{:none :all-old :updated-old :all-new :updated-new}.
    :expected  - {<attr> <#{<expected-value> false}> ...}."
  [client-opts table prim-kvs update-map & [{:keys [return expected return-cc?]
                                             :as   opts}]]
  (as-map
   (.updateItem (db-client client-opts)
     (reqs/update-item-request table prim-kvs update-map opts))))


(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [client-opts table prim-kvs & [{:keys [return expected return-cc?]
                                  :as   opts}]]
  (as-map
   (.deleteItem (db-client client-opts)
     (reqs/delete-item-request table prim-kvs opts))))

;;;; API - batch ops

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
  [client-opts requests & [{:keys [return-cc? span-reqs] :as opts
                            :or   {span-reqs {:max 5}}}]]
  (letfn [(run1 [raw-req]
            (as-map
             (.batchGetItem (db-client client-opts)
               ^BatchGetItemRequest (reqs/batch-get-item-request return-cc? raw-req))))]
    (merge-more run1 span-reqs (run1 (reqs/batch-request-items requests)))))

(comment
  (def bigval (.getBytes (apply str (range 14000))))
  (defn- ids [from to] (for [n (range from to)] {:id n :name bigval}))
  (batch-write-item client-opts {:faraday.tests.main {:put (ids 0 10)}})
  (batch-get-item   client-opts {:faraday.tests.main {:prim-kvs {:id (range 20)}}})
  (scan client-opts :faraday.tests.main))

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
  [client-opts requests & [{:keys [return-cc? span-reqs] :as opts
                            :or   {span-reqs {:max 5}}}]]
  (letfn [(run1 [raw-req]
            (as-map
             (.batchWriteItem (db-client client-opts)
               ^BatchWriteItemRequest (reqs/batch-write-item-request return-cc? raw-req))))]
    (merge-more run1 span-reqs
      (run1
       (utils/name-map
        ;; {<table> <table-reqs> ...} -> {<table> [WriteRequest ...] ...}
        (fn [table-request]
          (reduce into []
            (for [action (keys table-request)
                  :let [items (reqs/attr-multi-vs (table-request action))]]
              (mapv (partial reqs/write-request action) items))))
        requests)))))

;;;; API - queries & scans

(defn query
  "Retrieves items from a table (indexed) with options:
    prim-key-conds - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :last-prim-kvs - Primary key-val from which to eval, useful for paging.
    :query-filter  - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :span-reqs     - {:max _ :throttle-ms _} controls automatic multi-request
                     stitching.
    :return        - e/o #{:all-attributes :all-projected-attributes :count
                           [<attr> ...]}.
    :index         - Name of a local or global secondary index to query.
    :order         - Index scaning order e/o #{:asc :desc}.
    :limit         - Max num >=1 of items to eval (≠ num of matching items).
                     Useful to prevent harmful sudden bursts of read activity.
    :consistent?   - Use strongly (rather than eventually) consistent reads?

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
              (reqs/query-request table prim-key-conds opts))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs))))

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
             (.scan (db-client client-opts)
                    (reqs/scan-request table opts))))]
    (merge-more run1 span-reqs (run1 last-prim-kvs))))

(defn scan-parallel
  "Like `scan` but starts a number of worker threads and automatically handles
  parallel scan options (:total-segments and :segment). Returns a vector of
  `scan` results.

  Ref. http://goo.gl/KLwnn (official parallel scan documentation)."
  [client-opts table total-segments & [opts]]
  (let [opts (assoc opts :total-segments total-segments)]
    (->> (mapv (fn [seg] (future (scan client-opts table (assoc opts :segment seg))))
               (range total-segments))
         (mapv deref))))

;;;; Misc. helpers

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
