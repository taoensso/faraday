(defproject com.taoensso/faraday "0.6.0"
  :description "Clojure DynamoDB client"
  :url "https://github.com/ptaoussanis/faraday"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure        "1.5.1"]
                 [org.clojure/tools.macro    "0.1.1"]
                 [com.amazonaws/aws-java-sdk "1.4.4.1"]
                 [expectations               "1.4.43"]
                 [com.taoensso/nippy         "1.3.0-alpha3"]
                 [com.taoensso/timbre        "2.1.2"]]
  :profiles {:1.5   {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :dev   {:dependencies [[com.taoensso/carmine "1.12.0"]]}
             :test  {:dependencies [[com.taoensso/carmine "1.12.0"]]}
             :bench {:dependencies []}}
  :aliases {"test-all"  ["with-profile" "test,1.5" "expectations"]
            "start-dev" ["with-profile" "dev,test" "repl" ":headless"]}
  :plugins [[lein-expectations "0.0.7"]
            [lein-autoexpect   "0.2.5"]
            [codox             "0.6.4"]]
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
