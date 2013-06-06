(defproject com.taoensso/faraday "0.5.2"
  :description "Clojure DynamoDB client"
  :url "https://github.com/ptaoussanis/faraday"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure        "1.5.1"]
                 [org.clojure/tools.macro    "0.1.1"]
                 [com.amazonaws/aws-java-sdk "1.4.4.1"]
                 [com.taoensso/nippy         "1.2.1"]
                 [com.taoensso/timbre        "2.0.1"]]
  :profiles {:1.5   {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :dev   {:dependencies []}
             :test  {:dependencies []}
             :bench {:dependencies []}}
  :aliases {"test-all" ["with-profile" "test,1.5" "test"]}
  :plugins [[codox "0.6.4"]]
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
