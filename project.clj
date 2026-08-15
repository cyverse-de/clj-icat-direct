(defproject org.cyverse/clj-icat-direct "2.9.8"
  :description "A Clojure library for accessing the iRODS ICAT database directly."
  :url "https://github.com/cyverse-de/clj-icat-direct"
  :license {:name "BSD Standard License"
            :url "https://cyverse.org/license"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[jonase/eastwood "1.4.3"]
            [lein-ancient "1.0.0"]
            [test2junit "1.4.4"]]
  :profiles {:repl {:dependencies [[cheshire "6.2.0"]]
                    :source-paths ["repl"]}}
  ;; Fail the build on a new dependency conflict rather than printing a
  ;; warning nobody reads.
  :pedantic? :abort
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [com.github.seancorfield/honeysql "2.7.1437"]
                 [korma "0.4.3"]
                 [org.postgresql/postgresql "42.7.13"]]
  :eastwood {:exclude-linters [:def-in-def :unlimited-use]})
