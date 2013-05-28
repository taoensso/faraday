(ns taoensso.faraday.utils
  {:author "Peter Taoussanis"}
  (:require [clojure.string      :as str]
            [clojure.tools.macro :as macro]))

(defprotocol Functor "Generic functor interface, Ref. http://goo.gl/1DThA"
  (fmap* [coll f]))

(extend-protocol Functor
  clojure.lang.IPersistentList   (fmap* [c f] (map f c))
  clojure.lang.IPersistentVector (fmap* [v f] (mapv f v))
  clojure.lang.IPersistentSet    (fmap* [s f] (into #{} (map f s)))
  clojure.lang.IPersistentMap    (fmap* [m f] (reduce-kv
                                               (fn [m k v] (assoc m k (f v)))
                                               {} m)))

(defn fmap [f coll]
  "Applies function f to each item in the collection and returns a collection
  of the same type."
  (fmap* coll f))

(comment (map (partial fmap inc) ['(1 2 3) [1 2 3] {:1 1 :2 2 :3 3} #{1 2 3}]))

(defmacro time-ns "Returns number of nanoseconds it takes to execute body."
  [& body] `(let [t0# (System/nanoTime)] ~@body (- (System/nanoTime) t0#)))

(defmacro bench
  "Repeatedly executes form and returns time taken to complete execution."
  [num-laps form & {:keys [warmup-laps num-threads as-ns?]}]
  `(try (when ~warmup-laps (dotimes [_# ~warmup-laps] ~form))
        (let [nanosecs#
              (if-not ~num-threads
                (time-ns (dotimes [_# ~num-laps] ~form))
                (let [laps-per-thread# (int (/ ~num-laps ~num-threads))]
                  (time-ns
                   (->> (fn [] (future (dotimes [_# laps-per-thread#] ~form)))
                        (repeatedly ~num-threads)
                        doall
                        (map deref)
                        dorun))))]
          (if ~as-ns? nanosecs# (Math/round (/ nanosecs# 1000000.0))))
        (catch Exception e# (str "DNF: " (.getMessage e#)))))

(defn map-kvs [kf vf m]
  (let [m (if (instance? java.util.HashMap m) (into {} m) m)]
    (persistent! (reduce-kv (fn [m k v] (assoc! m (kf k) (if vf (vf v) v)))
                            (transient {}) m))))

(defn keyword-map ([m] (keyword-map nil m)) ([vf m] (map-kvs keyword vf m)))
(defn name-map    ([m] (name-map    nil m)) ([vf m] (map-kvs name    vf m)))

(def  enum*   (memoize (fn [x] (-> x (name) (str/upper-case) (str/replace "-" "_")))))
(def  un-enum (memoize (fn [e] (-> e (str/lower-case) (str/replace "_" "-")
                                  (keyword)))))
(defn enum ^String [x] (enum* x))