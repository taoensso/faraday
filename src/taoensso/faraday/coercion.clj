(ns taoensso.faraday.coercion
  (:require [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.encore        :as encore]
            [taoensso.faraday.utils :as utils :refer (coll?*)])
  (:import [clojure.lang BigInt]
           [com.amazonaws.services.dynamodbv2.model
            AttributeDefinition
            AttributeValue
            AttributeValueUpdate]
           java.nio.ByteBuffer))

(def ^:private nt-freeze  (comp #(ByteBuffer/wrap %) nippy-tools/freeze))
;; (def ^:private nt-thaw (comp nippy-tools/thaw #(.array ^ByteBuffer %)))

(defn- freeze?  [x] (or (nippy-tools/wrapped-for-freezing? x)
                        (encore/bytes? x)))

(defn- stringy? [x] (or (string? x) (keyword? x)))

(defn nt-thaw [bb]
  (let [ba          (.array ^ByteBuffer bb)
        serialized? (#'nippy/try-parse-header ba)]
    (if-not serialized?
      ba ; No Nippy header => assume non-serialized binary data (e.g. other client)
      (try ; Header match _may_ have been a fluke (though v. unlikely)
        (nippy-tools/thaw ba)
        (catch Exception e
          ba)))))

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

(defn ddb-num? [x]
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

(defn num->ddb-num
  "Coerce any special Clojure types that'd trip up the DDB Java client."
  [x]
  (cond (instance? BigInt x) (biginteger x)
        :else x))

(defn ddb-num-str->num [^String s]
  ;;; In both cases we'll err on the side of caution, assuming the most
  ;;; accurate possible type
  (if (.contains s ".")
    (BigDecimal. s)
    (bigint (BigInteger. s))))

(defn db-val->clj-val "Returns the Clojure value of given AttributeValue object."
  [^AttributeValue x]
  (or (.getS x)
      (some->> (.getN  x) ddb-num-str->num)
      (some->> (.getSS x) (into #{}))
      (some->> (.getNS x) (mapv ddb-num-str->num) (into #{}))
      (some->> (.getBS x) (mapv nt-thaw)          (into #{}))
      (some->> (.getB  x) nt-thaw) ; Last, may be falsey
      ))

(defn clj-val->db-val "Returns an AttributeValue object for given Clojure value."
  ^AttributeValue [x]
  (cond
   (stringy? x)
   (let [^String s (encore/fq-name x)]
     (if (.isEmpty s)
       (throw (Exception. "Invalid DynamoDB value: \"\""))
       (doto (AttributeValue.) (.setS s))))

   (ddb-num? x) (doto (AttributeValue.) (.setN (str x)))
   (freeze?  x) (doto (AttributeValue.) (.setB (nt-freeze x)))

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

;;;; Coercion - objects

(def db-item->clj-item (partial utils/keyword-map db-val->clj-val))
(def clj-item->db-item (partial utils/name-map    clj-val->db-val))
