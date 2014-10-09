(ns taoensso.faraday.requests
  (:require  [taoensso.encore           :as encore :refer (doto-cond)]
             [taoensso.faraday.utils    :as utils :refer (coll?*)]
             [taoensso.faraday.coercion :refer [clj-item->db-item
                                                clj-val->db-val]])
  (:import  [clojure.lang BigInt]
            [com.amazonaws.services.dynamodbv2.model
             AttributeValue
             AttributeValueUpdate
             KeySchemaElement
             ProvisionedThroughput
             AttributeDefinition
             LocalSecondaryIndex
             GlobalSecondaryIndex
             Projection
             ExpectedAttributeValue
             Condition
             BatchGetItemRequest
             BatchWriteItemRequest
             CreateTableRequest
             DeleteItemRequest
             DeleteRequest
             DeleteTableRequest
             DescribeTableRequest
             GetItemRequest
             ListTablesRequest
             PutItemRequest
             PutRequest
             QueryRequest
             ScanRequest
             UpdateItemRequest
             UpdateTableRequest
             WriteRequest]))

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

(defn- local-2nd-indexes
  "[{:name _ :range-keydef _ :projection _} ...] indexes -> [LocalSecondaryIndex ...]"
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

(defn- provisioned-throughput "Returns a new ProvisionedThroughput object."
  [{read-units :read write-units :write :as throughput}]
  (assert (and read-units write-units)
          (str "Malformed throughput: " throughput))
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits  (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn- global-2nd-indexes
  "[{:name _ :hash-keydef _ :range-keydef _ :projection _ :throughput _} ...]
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

(defn describe-table-request
  [table]
  (doto (DescribeTableRequest.) (.setTableName (name table))))

(defn create-table-request
  [client-opts table-name hash-keydef
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

(defn update-table-request
  [table throughput]
  (doto (UpdateTableRequest.)
    (.setTableName (name table))
    (.setProvisionedThroughput (provisioned-throughput throughput))))

(defn delete-table-request
  [table]
  (DeleteTableRequest. (name table)))

(defn get-item-request
  [table prim-kvs & [{:keys [attrs consistent? return-cc?]}]]
  (doto-cond [g (GetItemRequest.)]
    :always     (.setTableName       (name table))
    :always     (.setKey             (clj-item->db-item prim-kvs))
    consistent? (.setConsistentRead  g)
    attrs       (.setAttributesToGet (mapv name g))
    return-cc?  (.setReturnConsumedCapacity (utils/enum :total))))

(defn- expected-values
  "{<attr> <cond> ...} -> {<attr> ExpectedAttributeValue ...}"
  [expected-map]
  (when (seq expected-map)
    (utils/name-map
     #(if (= false %)
        (ExpectedAttributeValue. false)
        (ExpectedAttributeValue. (clj-val->db-val %)))
     expected-map)))

(defn put-item-request
  [table item & [{:keys [return expected return-cc?]
                  :or   {return :none}}]]
  (doto-cond [g (PutItemRequest.)]
    :always  (.setTableName    (name table))
    :always  (.setItem         (clj-item->db-item item))
    expected (.setExpected     (expected-values g))
    return   (.setReturnValues (utils/enum g))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn- attribute-updates
  "{<attr> [<action> <value>] ...} -> {<attr> AttributeValueUpdate ...}"
  [update-map]
  (when (seq update-map)
    (utils/name-map
     (fn [[action val]] (AttributeValueUpdate. (when val (clj-val->db-val val))
                                              (utils/enum action)))
     update-map)))

(defn update-item-request
  [table prim-kvs update-map & [{:keys [return expected return-cc?]
                                 :or   {return :none}}]]
  (doto-cond [g (UpdateItemRequest.)]
    :always  (.setTableName        (name table))
    :always  (.setKey              (clj-item->db-item prim-kvs))
    :always  (.setAttributeUpdates (attribute-updates update-map))
    expected (.setExpected         (expected-values g))
    return   (.setReturnValues     (utils/enum g))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn delete-item-request
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [table prim-kvs & [{:keys [return expected return-cc?]
                      :or   {return :none}}]]
  (doto-cond [g (DeleteItemRequest.)]
    :always  (.setTableName    (name table))
    :always  (.setKey          (clj-item->db-item prim-kvs))
    expected (.setExpected     (expected-values g))
    return   (.setReturnValues (utils/enum g))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn batch-get-item-request
  [return-cc? raw-req]
  (doto-cond [g (BatchGetItemRequest.)]
    :always    (.setRequestItems raw-req)
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn write-request [action item]
  (case action
    :put    (doto (WriteRequest.) (.setPutRequest    (doto (PutRequest.)    (.setItem item))))
    :delete (doto (WriteRequest.) (.setDeleteRequest (doto (DeleteRequest.) (.setKey  item))))))

(defn batch-write-item-request
  [return-cc? raw-req]
  (doto-cond [g (BatchWriteItemRequest.)]
    :always    (.setRequestItems raw-req)
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn- enum-op ^String [operator]
  (-> operator {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"} (or operator)
      utils/enum))

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

(defn query-request
  [table prim-key-conds
   & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
              return-cc?] :as opts
       :or {order :asc}}]]
  (doto-cond [g (QueryRequest.)]
    :always (.setTableName        (name table))
    :always (.setKeyConditions    (query|scan-conditions prim-key-conds))
    :always (.setScanIndexForward (case order :asc true :desc false))
    last-prim-kvs   (.setExclusiveStartKey
                     (clj-item->db-item last-prim-kvs))
    query-filter    (.setQueryFilter (query|scan-conditions query-filter))
    limit           (.setLimit     (int g))
    index           (.setIndexName      g)
    consistent?     (.setConsistentRead g)
    (coll?* return) (.setAttributesToGet (mapv name return))
    return-cc? (.setReturnConsumedCapacity (utils/enum :total))
    (and return (not (coll?* return)))
    (.setSelect (utils/enum return))))

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


