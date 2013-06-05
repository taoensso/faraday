(ns taoensso.faraday.utils
  {:author "Peter Taoussanis"}
  (:require [clojure.string      :as str]
            [clojure.tools.macro :as macro]))

(defn map-kvs [kf vf m]
  (let [m (if (instance? java.util.HashMap m) (into {} m) m)]
    (persistent! (reduce-kv (fn [m k v] (assoc! m (if kf (kf k) k)
                                                 (if vf (vf v) v)))
                            (transient {}) m))))

(defn keyword-map ([m] (keyword-map nil m)) ([vf m] (map-kvs keyword vf m)))
(defn name-map    ([m] (name-map    nil m)) ([vf m] (map-kvs name    vf m)))

(def  enum*   (memoize (fn [x] (-> x (name) (str/upper-case) (str/replace "-" "_")))))
(def  un-enum (memoize (fn [e] (-> e (str/lower-case) (str/replace "_" "-")
                                  (keyword)))))
(defn enum ^String [x] (enum* x))

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

(defn nnil?  [x] (not (nil? x)))
(defn coll?* [x] (and (coll? x) (not (map? x ))))

(defmacro doto-maybe "Diabolical cross between `doto`, `cond->` and `as->`."
  [x name & clauses]
  (assert (even? (count clauses)))
  (let [g (gensym)
        pstep (fn [[test-expr step]] `(when-let [~name ~test-expr]
                                       (-> ~g ~step)))]
    `(let [~g ~x]
       ~@(map pstep (partition 2 clauses))
       ~g)))

(defn cartesian-product ; Stolen from clojure.math.combinatorics
  "Returns all the combinations of one element from each sequence."
  [& seqs]
  (let [v-original-seqs (vec seqs)
        step
        (fn step [v-seqs]
          (let [increment
                (fn [v-seqs]
                  (loop [i (dec (count v-seqs)), v-seqs v-seqs]
                    (if (= i -1) nil
                        (if-let [rst (next (v-seqs i))]
                          (assoc v-seqs i rst)
                          (recur (dec i) (assoc v-seqs i (v-original-seqs i)))))))]
            (when v-seqs
              (cons (map first v-seqs)
                    (lazy-seq (step (increment v-seqs)))))))]
    (when (every? seq seqs)
      (lazy-seq (step v-original-seqs)))))

(comment (cartesian-product [:a :b] [1 2] [:A :B])
         (cartesian-product [:a :b]))

(def ^:const bytes-class (Class/forName "[B"))
(defn bytes? [x] (instance? bytes-class x))