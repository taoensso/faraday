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
