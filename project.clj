(defproject org.cyverse/clj-icat-direct "2.9.4-SNAPSHOT"
  :description "A Clojure library for accessing the iRODS ICAT database directly."
  :url "https://github.com/cyverse-de/clj-icat-direct"
  :license {:name "BSD Standard License"
            :url "http://www.iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[jonase/eastwood "0.3.11"]
            [test2junit "1.2.2"]]
  :profiles {:repl {:dependencies [[cheshire "5.10.0"]]
                    :source-paths ["repl"]}}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [honeysql "1.0.461"]
                 [korma "0.4.3"]
                 [org.postgresql/postgresql "42.2.19"]]
  :eastwood {:exclude-linters [:def-in-def :unlimited-use]})
