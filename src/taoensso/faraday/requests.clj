(ns taoensso.faraday.requests)

(defn describe-table-request
  [client-opts table]
  (doto (DescribeTableRequest.) (.setTableName (name table))))

(defn create-table-request
  [client-opts table-name hash-keydef
   & [{:keys [range-keydef throughput lsindexes gsindexes block?] :as opts}]]
  (let [lsindexes (or lsindexes (:indexes opts))]
    (doto-cond
     [_ (CreateTableRequest.)]
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
  (doto-cond
   [g (GetItemRequest.)]
   :always     (.setTableName       (name table))
   :always     (.setKey             (clj-item->db-item prim-kvs))
   consistent? (.setConsistentRead  g)
   attrs       (.setAttributesToGet (mapv name g))
   return-cc?  (.setReturnConsumedCapacity (utils/enum :total))))

(defn put-item-request
  [table item & [{:keys [return expected return-cc?]
                  :or   {return :none}}]]
  (doto-cond
   [g (PutItemRequest.)]
   :always  (.setTableName    (name table))
   :always  (.setItem         (clj-item->db-item item))
   expected (.setExpected     (expected-values g))
   return   (.setReturnValues (utils/enum g))
   return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn update-item-request
  [client-opts table prim-kvs update-map & [{:keys [return expected return-cc?]}]]
  (doto-cond
   [g (UpdateItemRequest.)]
   :always  (.setTableName        (name table))
   :always  (.setKey              (clj-item->db-item prim-kvs))
   :always  (.setAttributeUpdates (attribute-updates update-map))
   expected (.setExpected         (expected-values g))
   return   (.setReturnValues     (utils/enum g))
   return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn delete-item-request
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [table prim-kvs & [{:keys [return expected return-cc?]}]]
  (doto-cond
   [g (DeleteItemRequest.)]
   :always  (.setTableName    (name table))
   :always  (.setKey          (clj-item->db-item prim-kvs))
   expected (.setExpected     (expected-values g))
   return   (.setReturnValues (utils/enum g))
   return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn- attr-multi-vs
  "[{<attr> <v-or-vs*> ...} ...]* -> [{<attr> <v> ...} ...] (* => optional vec)"
  [attr-multi-vs-map]
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
            [] (ensure-sequential attr-multi-vs-map))))

(defn batch-get-item-request
  [return-cc? raw-req]
  (doto-cond
   [g (BatchGetItemRequest.)]
   :always    (.setRequestItems raw-req)
   return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

(defn- write-request [action item]
  (case action
    :put    (doto (WriteRequest.) (.setPutRequest    (doto (PutRequest.)    (.setItem item))))
    :delete (doto (WriteRequest.) (.setDeleteRequest (doto (DeleteRequest.) (.setKey  item))))))

(defn batch-write-item-request
  [return-cc? raw-req]
  (doto-cond
   [g (BatchWriteItemRequest.)]
   :always    (.setRequestItems raw-req)
   return-cc? (.setReturnConsumedCapacity (utils/enum :total))))

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
              return-cc?] :as opts}]]
  (doto-cond
   [g (QueryRequest.)]
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
  (doto-cond
   [g (ScanRequest.)]
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


