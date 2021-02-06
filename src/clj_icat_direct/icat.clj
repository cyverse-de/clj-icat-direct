(ns clj-icat-direct.icat
  (:use [clojure.java.io :only [file]])
  (:require [korma.db :as db]
            [korma.core :as k]
            [clojure.string :as string]
            [clj-icat-direct.queries :as q])
  (:import [clojure.lang ISeq Keyword]))

(defn icat-db-spec
  "Creates a Korma db spec for the ICAT."
  [hostname user pass & {:keys [port db]
                         :or {port 5432
                              db "ICAT"}}]
  (db/postgres {:host     hostname
                :port     port
                :db       db
                :user     user
                :password pass}))

(defn setup-icat
  "Defines the icat database. Pass in the return value of icat-db-spec."
  [icat-db-spec]
  (db/defdb icat icat-db-spec))

(defn- run-simple-query
  "Runs one of the defined queries against the ICAT. It's considered a simple query if it doesn't
   require string formatting."
  [query-kw & args]
  (if-not (contains? q/queries query-kw)
    (throw (Exception. (str "query " query-kw " is not defined."))))

  (k/exec-raw icat [(get q/queries query-kw) args] :results))

(defn- run-query-string
  "Runs the passed in query string. Doesn't check to see if it's defined in
   clj-icat-direct.queries first."
  [query & args]
  (k/exec-raw icat [query args] :results))

(defmacro with-icat-transaction
  [& body]
  `(if (map? icat)
    (db/with-db icat
      (db/transaction
        (do ~@body)))
    (do ~@body)))

(defn- run-stuff
  [& queries]
  (let [side-effects (drop-last 1 queries)
        final-query-and-args (last queries)]
    (doseq [q side-effects]
      ; uses exec-raw directly here to exclude :results so this can run DDL statements
      (k/exec-raw icat q))
    (apply run-query-string final-query-and-args)))

(defn- run-transaction
  "Runs the set of passed-in query strings+args in a transaction, returning
  what the last one returns. This is intended to be used to create temporary
  tables to circumvent the fact postgresql is bad at doing estimates on CTEs."
  [& queries]
  (with-icat-transaction
    (apply run-stuff queries)))

(defn user
  "Get a representation of a user with their ID, username, type name, zone, 'info', 'comment', and create/modify timestamps."
  [username zone]
  (->> (q/mk-user username zone)
       (apply run-query-string)
       first))

(defn user-group-ids
  "Get user group IDs, including the user's ID"
  [user zone]
  (->> (q/mk-groups user zone true)
       (apply run-query-string)
       (map :group_user_id)))

(defn path-for-uuid
  [uuid]
  (->> (q/mk-path-for-uuid uuid)
       (apply run-query-string)
       (first)
       :path))

(defn paths-for-uuids
  [uuids]
  (->> (q/mk-paths-for-uuids uuids)
       (apply run-query-string)
       (map (juxt :uuid :path))
       (into {})))

(defn number-of-files-in-folder
  "Returns the number of files in a folder that the user has access to."
  [user zone folder-path]
  (-> (run-simple-query :count-files-in-folder user zone folder-path) first :count))

(defn number-of-folders-in-folder
  "Returns the number of folders in the specified folder that the user has access to."
  [user zone folder-path]
  (-> (run-simple-query :count-folders-in-folder user zone folder-path) first :count))


(defn ^Integer number-of-items-in-folder
  "Returns the total number of files and folders in the specified folder that the user has access
   to and where the files have the given info types.

   Parameters:
     user        - the username of the user
     zone        - the user's authentication zone
     folder-path - the absolute path to the folder being inspected
     entity-type - the type of entities to return (:any|:file|:folder), :any means both files and
                   folders
     info-types  - the info-types of the files to count, if empty, all files are counted

   Returns:
     It returns the total number of folders combined with the total number of files with the given
     info types."
  [^String user ^String zone ^String folder-path ^Keyword entity-type & [info-types]]
  (let [type-cond (q/mk-file-type-cond info-types)
        queries   (case entity-type
                    :any    (q/mk-count-items-in-folder user zone folder-path type-cond)
                    :file   (q/mk-count-files-in-folder user zone folder-path type-cond)
                    :folder (q/mk-count-folders-in-folder user zone folder-path)
                            (throw (Exception. (str "invalid entity type " entity-type))))]
    (-> (apply run-transaction queries) first :total)))


(defn number-of-all-items-under-folder
  "Returns the total number of files and folders in the specified folder and all
   sub-folders that the user has access to."
  [user zone folder-path]
  (-> (run-simple-query :count-all-items-under-folder user zone folder-path folder-path)
      (first)
      (:total)))


(defn ^Integer number-of-bad-items-in-folder
  "Returns the total number of files and folders in the specified folder that the user has access to
   and where the files have the given info types, but should be marked as having a bad name in the
   client.

   Parameters:
     user        - the username of the authorized user
     zone        - the user's authentication zone
     folder-path - The absolute path to the folder of interest
     entity-type - the type of entities to return (:any|:file|:folder), :any means both files and
                   folders
     info-types  - the info-types of the files to count, if empty, all files are considered
     bad-chars   - If a name contains one or more of these characters, the item will be marked as
                   bad
     bad-names   - This is a sequence of names that are bad
     bad-paths   - This is an array of paths to items that will be marked as badr.

   Returns:
     It returns the total."
  [& {:keys [user zone folder-path entity-type info-types bad-chars bad-names bad-paths]}]
  (let [info-type-cond  (q/mk-file-type-cond info-types)
        bad-file-cond   (q/mk-bad-file-cond folder-path bad-chars bad-names bad-paths)
        bad-folder-cond (q/mk-bad-folder-cond folder-path bad-chars bad-names bad-paths)
        query-ctor      (case entity-type
                          :any    q/mk-count-bad-items-in-folder
                          :file   q/mk-count-bad-files-in-folder
                          :folder q/mk-count-bad-folders-in-folder
                                  (throw (Exception. (str "invalid entity type " entity-type))))
        query           (query-ctor
                          :user            user
                          :zone            zone
                          :parent-path     folder-path
                          :info-type-cond  info-type-cond
                          :bad-file-cond   bad-file-cond
                          :bad-folder-cond bad-folder-cond)]
    (-> (run-query-string query) first :total)))


(defn folder-permissions-for-user
  "Returns the highest permission value for the specified user on the folder."
  [user folder-path]
  (let [sorter (partial sort-by :access_type_id)]
    (-> (run-simple-query :folder-permissions-for-user user folder-path)
      sorter last :access_type_id)))

(defn file-permissions-for-user
  "Returns the highest permission value for the specified user on the file."
  [user file-path]
  (let [sorter   (partial sort-by :access_type_id)
        dirname  #(.getParent (file %))
        basename #(.getName (file %))]
    (-> (run-simple-query :file-permissions-for-user user (dirname file-path) (basename file-path))
      sorter last :access_type_id)))

(defn- add-permission
  [user {:keys [full_path type] :as item-map} ]
  (let [perm-func (if (= type "dataobject") file-permissions-for-user folder-permissions-for-user)]
    (assoc item-map :access_type_id (perm-func user full_path))))

(defn list-files-under-folder
  "Lists all of the files in the current folder and all descendants, without regard to file or folder permissions. Use
   of this function should be restricted to administrative endpoints."
  [folder-path]
  (run-simple-query :list-files-under-folder folder-path folder-path))

(defn list-folders-in-folder
  "Returns a listing of the folders contained in the specified folder that the user has access to."
  [user zone folder-path]
  (map (partial add-permission user)
       (run-simple-query :list-folders-in-folder user zone folder-path)))


(defn ^ISeq folder-path-listing
  "Returns a complete folder listing for everything visible to a given user.

   Parameters:
     user        - the name of the user
     zone        - the authentication zone of the user
     folder-path - the absolute path to the folder

   Returns:
     It returns a sequence of paths."
  [^String user ^String zone ^String folder-path]
  (map :full_path (run-simple-query :folder-listing folder-path folder-path user zone)))


(defn- fmt-info-type
  [record]
  (if (and (= "dataobject" (:type record))
           (empty? (:info_type record)))
    (assoc record :info_type "unknown")
    record))


(defn- resolve-sort-column
  [col-key]
  (if-let [col (get q/sort-columns col-key)]
    col
    (throw (Exception. (str "invalid sort column " col-key)))))


(defn- resolve-sort-direction
  [direction-key]
  (if-let [direction (get q/sort-directions direction-key)]
    direction
    (throw (Exception. (str "invalid sort direction" direction-key)))))

(defn- get-item*
  [dirname basename group-ids-query]
  (fmt-info-type (first (apply run-query-string (q/mk-get-item dirname basename group-ids-query)))))

(defn get-item
  ([dirname basename group-ids]
   (if (seq group-ids)
     (let [group-ids-query (apply str "VALUES " (string/join ", " (map #(str "(" % ")") group-ids)))]
       (get-item* dirname basename group-ids-query))
     "")) ;; return an empty string when no group IDs are passed in
  ([dirname basename user zone]
   (let [group-ids-query (str "SELECT group_user_id FROM (" (q/mk-groups user zone) ") g")]
     (get-item* dirname basename group-ids-query))))

(defn- build-groups-table-query [user-group-ids]
  (when user-group-ids
    (apply str
           "SELECT column1 AS group_user_id FROM (VALUES "
           (string/join ", " (map #(str "(" % ")") user-group-ids))
           ") v")))

(defn ^ISeq paged-folder-listing
  "Returns a page from a folder listing.

   Parameters:
     user           - the name of the user determining access privileges
     zone           - the authentication zone of the user
     folder-path    - the folder to list the contents of
     entity-type    - the type of entities to return (:any|:file|:folder), :any means both files and
                      folders
     sort-column    - the column to sort by
                      (:type|:modify-ts|:create-ts|:data-size|:base-name|:full-path)
     sort-direction - the sorting direction (:asc|:desc)
     limit          - the maximum number of results to return
     offset         - the number of results to skip after sorting and before returning results
     file-types     - the info types of interest

   Returns:
     It returns a page of results.

   Throws:
     It throws an exception if a validation fails."
  [& {:keys [user zone folder-path entity-type sort-column sort-direction limit offset info-types transaction?
             user-group-ids]
      :or {transaction? true}}]
  (let [groups-table-query (build-groups-table-query user-group-ids)
        query-ctor (case entity-type
                     :any    q/mk-paged-folder
                     :file   q/mk-paged-files-in-folder
                     :folder q/mk-paged-folders-in-folder
                             (throw (Exception. (str "invalid entity type " entity-type))))
        queries (query-ctor
                :user           user
                :zone           zone
                :groups-table-query groups-table-query
                :parent-path    folder-path
                :info-type-cond (q/mk-file-type-cond info-types)
                :sort-column    (resolve-sort-column sort-column)
                :sort-direction (resolve-sort-direction sort-direction)
                :limit limit
                :offset offset)]
    (map fmt-info-type (apply (if transaction? run-transaction run-stuff) queries))))

(defn prefixed-files-without-attr
  [uuid-prefix attr]
  (let [queries (q/mk-filtered-filenames-without-attr uuid-prefix attr)]
    (map :path (apply run-transaction queries))))

(defn select-files-with-uuids
  "Given a set of UUIDs, it returns a list of UUID-path pairs for each UUID that corresponds to a
   file."
  [uuids]
  ; This can't be run as a simple query.  I suspect the UUID db type is causing trouble
  (let [query (format (:select-files-with-uuids q/queries) (q/prepare-text-set uuids))]
    (run-query-string query)))

(defn select-folders-with-uuids
  "Given a set of UUIDs, it returns a list of UUID-path pairs for each UUID that corresponds to a
   folder."
  [uuids]
  ; This can't be run as a simple query. I suspect the UUID db type is causing trouble
  (let [query (format (:select-folders-with-uuids q/queries) (q/prepare-text-set uuids))]
    (run-query-string query)))

(defn ^ISeq paged-uuid-listing
  "Returns a page of filesystem entries corresponding to a list a set of UUIDs.

   Parameters:
     user        - the name of the user determining access privileges
     zone        - the authentication zone of the user
     sort-column - the column to sort by (type|modify-ts|create-ts|data-size|base-name|full-path)
     sort-order  - the sorting direction (asc|desc)
     limit       - the maximum number of results to return
     offset      - the number of results to skip after sorting and before returning results
     uuids       - the list of UUIDS to look up.
     file-types  - the info types of interest

   Returns:
     The result set"
  [^String  user
   ^String  zone
   ^Keyword sort-column
   ^Keyword sort-order
   ^Long    limit
   ^Long    offset
   ^ISeq    uuids
   ^ISeq    file-types]
  (if (empty? uuids)
    []
    (let [uuid-set (q/prepare-text-set uuids)
          ft-cond  (q/mk-file-type-cond file-types)
          sc       (resolve-sort-column sort-column)
          so       (resolve-sort-direction sort-order)
          query    (format (:paged-uuid-listing q/queries) uuid-set ft-cond sc so)]
      (map fmt-info-type (run-query-string query user zone limit offset)))))


(defn ^Long number-of-uuids-in-folder
  "Returns the number of entities that have provided UUIDs, are visible to the given user and are a
   folder or have one of the given file types.

   Parameters:
     user       - the name of the user determining visibility
     zone       - the authentication zone of the user
     uuids       - the list of UUIDS to look up.
     file-types  - the info types of interest

   Returns:
     The result set"
  [^String user ^String zone ^ISeq uuids ^ISeq file-types]
  (if (empty? uuids)
    0
    (let [uuid-set (q/prepare-text-set uuids)
          ft-cond  (q/mk-file-type-cond file-types)
          query    (q/mk-count-uuids-of-file-type user zone uuid-set ft-cond)]
      (:total (first (run-query-string query))))))
