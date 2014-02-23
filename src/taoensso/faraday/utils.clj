(ns taoensso.faraday.utils
  {:author "Peter Taoussanis"}
  (:require [clojure.string  :as str]
            [taoensso.encore :as encore]))

(defn map-kvs [kf vf m]
  (let [m (if (instance? java.util.HashMap m) (into {} m) m)]
    (persistent! (reduce-kv (fn [m k v] (assoc! m (if kf (kf k) k)
                                                 (if vf (vf v) v)))
                            (transient {}) m))))

(defn keyword-map ([m] (keyword-map nil m)) ([vf m] (map-kvs keyword vf m)))
(defn name-map    ([m] (name-map    nil m)) ([vf m] (map-kvs name    vf m)))

(def  enum*   (memoize (fn [x] (-> x (name) (str/upper-case) (str/replace "-" "_")))))
(def  un-enum (memoize (fn [e] (if (keyword? e) e
                                  (-> e (str/lower-case) (str/replace "_" "-")
                                      (keyword))))))
(defn enum ^String [x] (enum* x))

(defn coll?* [x] (and (coll? x) (not (map? x ))))

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
