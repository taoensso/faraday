(defproject com.taoensso/faraday "1.12.1-SNAPSHOT"
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
  [[org.clojure/clojure "1.7.0"]
   [com.taoensso/encore "3.34.0" :exclusions [org.clojure/tools.reader]]
   [com.taoensso/nippy  "3.2.0"]
   [joda-time           "2.12.2"]
   [commons-logging     "1.2"]
   [com.amazonaws/aws-java-sdk-dynamodb "1.12.410"
    :exclusions [joda-time commons-logging]]]

  :profiles
  {:server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.7  {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure "1.8.0"]]}
   :1.9  {:dependencies [[org.clojure/clojure "1.9.0"]]}
   :1.10 {:dependencies [[org.clojure/clojure "1.10.3"]]}
   :dev
   [:1.10 :server-jvm
    {:dependencies [[org.testcontainers/testcontainers "1.17.6" :exclusions [com.fasterxml.jackson.core/jackson-annotations]]
                    [org.slf4j/slf4j-simple "1.7.36"]]
     :plugins [[lein-ancient "0.7.0"]
               [lein-codox   "0.10.8"]]}]}
  :test-paths ["test" "src"]

  :codox
  {:language :clojure
   :source-uri "https://github.com/ptaoussanis/faraday/blob/master/{filepath}#L{line}"}

  :aliases
  {"test-all" ["with-profile" "+1.10:+1.9:+1.8:+1.7" "test"]}

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
