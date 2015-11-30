(defproject com.taoensso/faraday "1.8.0"
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
   [com.taoensso/encore "2.26.1"]
   [com.taoensso/nippy  "2.10.0"]
   [joda-time           "2.9.1"] ; For exclusion, see Github #27
   [com.amazonaws/aws-java-sdk-dynamodb "1.9.39" ; TODO 1.10.x
    :exclusions [joda-time]]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.5  {:dependencies [[org.clojure/clojure    "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure    "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure    "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure    "1.8.0-RC2"]]}
   :test {:dependencies [[expectations           "2.1.4"]
                         [org.clojure/test.check "0.9.0"]]
          :plugins [[lein-expectations "0.0.8"]
                    [lein-autoexpect   "1.7.0"]]}
   :dev
   [:1.7 :test
    {:plugins [[lein-ancient "0.6.4"]
               [codox        "0.8.11"]]}]}

  :test-paths ["test" "src"]
  :aliases
  {"test-all"   ["with-profile" "+1.5:+1.6:+1.7:+1.8" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+server-jvm" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})
