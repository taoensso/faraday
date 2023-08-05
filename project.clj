(defproject com.taoensso/faraday "1.12.1-SNAPSHOT"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Amazon DynamoDB client for Clojure"
  :url "https://github.com/taoensso/faraday"

  :license
  {:name "Eclipse Public License - v 1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :dependencies
  [[com.taoensso/encore "3.63.0"]
   [com.taoensso/nippy  "3.2.0"]
   [joda-time           "2.12.5"]
   [commons-logging     "1.2"]
   [com.amazonaws/aws-java-sdk-dynamodb "1.12.523"
    :exclusions [joda-time commons-logging]]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.3"]]}
   :c1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}

   :test
   {:jvm-opts ["-Dtaoensso.elide-deprecated=true"]
    :global-vars
    {*warn-on-reflection* true
     *assert*             true
     *unchecked-math*     false #_:warn-on-boxed}}

   :graal-tests
   {:dependencies [[org.clojure/clojure "1.11.1"]
                   [com.github.clj-easy/graal-build-time "0.1.4"]]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"}

   :dev
   [:c1.11 :test
    {:jvm-opts ["-server"]
     :dependencies
     [[org.testcontainers/testcontainers "1.18.3"
       :exclusions [com.fasterxml.jackson.core/jackson-annotations]]
      [org.slf4j/slf4j-simple "1.7.36"]]

     :plugins
     [[lein-pprint  "1.3.2"]
      [lein-ancient "0.7.0"]
      [com.taoensso.forks/lein-codox "0.10.10"]]

     :codox
     {:language #{:clojure #_:clojurescript}
      :base-language :clojure}}]}

  :test-paths ["test" #_"src"]

  :aliases
  {"start-dev"     ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib"    ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"     ["with-profile" "+c1.11:+c1.10:+c1.9" "test"]
   ;; "test-cljs" ["with-profile" "+test" "cljsbuild"   "test"]
   "test-all"     ["do" ["clean"] ["test-clj"] #_["test-cljs"]]}

  :release-tasks
  [["vcs" "assert-committed"]
   ["change" "version" "leiningen.release/bump-version" "release"]
   ["vcs" "commit"]
   ["vcs" "tag" "v"]
   ["with-profile" "-dev" "deploy" "clojars"]
   ["change" "version" "leiningen.release/bump-version"]
   ["vcs" "commit"]
   ["vcs" "push"]]

  :repositories {"releases" {:url "https://clojars.org/repo"
                             :creds :gpg}})
