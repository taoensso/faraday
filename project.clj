(defproject com.taoensso/faraday "1.10.0-alpha1"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Clojure DynamoDB client"
  :url "https://github.com/ptaoussanis/faraday"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars {*warn-on-reflection* true
                *assert* true}

  :dependencies
  [[org.clojure/clojure "1.5.1"]
   [com.taoensso/encore "2.116.0"]
   [com.taoensso/nippy  "2.14.0"]
   [joda-time           "2.10.5"]
   [com.amazonaws/aws-java-sdk-dynamodb "1.11.664"
    :exclusions [joda-time]]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure "1.8.0"]]}
   :1.9  {:dependencies [[org.clojure/clojure "1.9.0"]]}
   :1.10 {:dependencies [[org.clojure/clojure "1.10.1"]]}
   :test {:dependencies [[expectations           "2.1.3"]
                         [org.clojure/test.check "0.10.0"]]
          :plugins [[lein-expectations "0.0.8"]
                    [lein-autoexpect   "1.9.0"]]}
   :dev
   [:1.9 :test :server-jvm
    {:plugins [[lein-ancient "0.6.14"]
               [lein-codox   "0.9.1"]
               [clj-dynamodb-local "0.1.2"]]}]}

  :dynamodb-local {:port 6798
                   :in-memory? true}

  :test-paths ["test" "src"]

  :codox
  {:language :clojure
   :source-uri "https://github.com/ptaoussanis/faraday/blob/master/{filepath}#L{line}"}

  :aliases
  {"test-all"   ["with-profile" "+1.10:+1.9:+1.8:+1.7:+1.6:+1.5" "dynamodb-local" "expectations"]
   "test-auto"  ["with-profile" "+test" "dynamodb-local" "autoexpect"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+dev" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})
