(defproject com.taoensso/faraday "1.6.0-SNAPSHOT"
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
  [[org.clojure/clojure        "1.5.1"]
   [com.taoensso/encore        "1.19.1"]
   [com.taoensso/nippy         "2.7.1"]
   [com.amazonaws/aws-java-sdk "1.9.14" :exclusions [joda-time]]
   [joda-time                  "2.7"] ; For exclusion, see Github #27
   ]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.7  {:dependencies [[org.clojure/clojure    "1.7.0-alpha3"]]}
   :1.6  {:dependencies [[org.clojure/clojure    "1.6.0"]]}
   :test {:dependencies [[expectations           "2.0.13"]
                         [org.clojure/test.check "0.6.2"]]
          :plugins [[lein-expectations "0.0.8"]
                    [lein-autoexpect   "1.4.2"]]}

   :dev
   [:1.7 :test
    {:plugins [[lein-ancient "0.5.4"]
               [codox        "0.6.7"]]}]}

  :test-paths ["test" "src"]
  :aliases
  {"test-all"   ["with-profile" "default:+1.6:+1.7" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+server-jvm" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})
