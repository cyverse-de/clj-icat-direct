(defproject org.cyverse/clj-icat-direct "2.8.11-SNAPSHOT"
  :description "A Clojure library for accessing the iRODS ICAT database directly."
  :url "https://github.com/cyverse-de/clj-icat-direct"
  :license {:name "BSD Standard License"
            :url "http://www.iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[test2junit "1.2.2"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [korma "0.4.3"]
                 [org.postgresql/postgresql "9.4.1212"]])
