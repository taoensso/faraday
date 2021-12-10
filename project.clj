(defproject com.taoensso/faraday "1.11.4-SNAPSHOT"
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
   [com.taoensso/encore "2.127.0"]
   [com.taoensso/nippy  "2.15.3"]
   [joda-time           "2.10.10"]
   [commons-logging     "1.2"]
   [com.amazonaws/aws-java-sdk-dynamodb "1.11.1034"
    :exclusions [joda-time commons-logging]]]

  :profiles
  {:server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure "1.8.0"]]}
   :1.9  {:dependencies [[org.clojure/clojure "1.9.0"]]}
   :1.10 {:dependencies [[org.clojure/clojure "1.10.3"]]}
   :dev
   [:1.10 :server-jvm
    {:dependencies [[org.testcontainers/testcontainers "1.15.1"]
                    [org.slf4j/slf4j-simple "1.7.30"]]
     :plugins [[lein-ancient "0.6.14"]
               [lein-codox   "0.10.6"]]}]}

  :test-paths ["test" "src"]

  :codox
  {:language :clojure
   :source-uri "https://github.com/ptaoussanis/faraday/blob/master/{filepath}#L{line}"}

  :aliases
  {"test-all" ["with-profile" "+1.10:+1.9:+1.8:+1.7:+1.6:+1.5" "test"]}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v"]
                  ["with-profile" "-dev" "deploy" "clojars"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]

  :repositories {"releases" {:url "https://clojars.org/repo"
                             :creds :gpg}})
