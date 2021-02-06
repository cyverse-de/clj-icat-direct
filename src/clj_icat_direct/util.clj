(ns clj-icat-direct.util
  (:require [korma.db :as db]))

(defn sql-array
  "Returns a SQL ARRAY(...) object,
   typically for use with a large (>32k) list of items that need to be passed to a SQL function.

   array-type:  the SQL name of the type of the `array-items` (e.g. 'varchar' or 'uuid').
   array-items: the elements that populate the returned SQL ARRAY object."
  [array-type array-items]
  (db/transaction
   (.createArrayOf (:connection db/*current-conn*) array-type (into-array array-items))))
