(ns clj-icat-direct.repl-utils
  (:require [cheshire.core :as json]
            [clj-icat-direct.icat :as icat]
            [clojure.java.io :as io]))

(def rods-conf-dir (io/file (System/getProperty "user.home") ".irods"))
(def prod-icat-config (io/file rods-conf-dir ".prod-db.json"))
(def qa-icat-config (io/file rods-conf-dir ".qa-db.json"))

(defn- load-config [path]
  (json/decode-stream (io/reader path) true))

(defn- init [path]
  (let [{:keys [host port user password]} (load-config path)]
    (icat/setup-icat (icat/icat-db-spec host user password :port port))))

(defn init-prod [] (init prod-icat-config))
(defn init-qa [] (init qa-icat-config))
