(ns clj-icat-direct.queries
  (:require [clj-icat-direct.util :refer [sql-array]]
            [clojure.java.io :refer [file]]
            [clojure.string :as string]
            [honeysql.core :as sql]
            [honeysql.helpers :as h])
  (:import  [clojure.lang IPersistentMap ISeq]))


(defn- bad-chars->sql-char-class
  [bad-chars]
  (let [bad-chars (string/replace bad-chars #"'|\[|\]|\\" {"\\" "\\\\"
                                                           "'"  "\\'"
                                                           "["  "\\["
                                                           "]"  "\\]"})]
    (str "[" bad-chars "]")))


(defn- file-bad-chars-cond
  [_ bad-chars]
  (str "d.data_name ~ E'" (bad-chars->sql-char-class bad-chars) "'"))


(defn- folder-bad-chars-cond
  [parent-path bad-chars]
  (str "c.coll_name ~ E'" parent-path "/.*" (bad-chars->sql-char-class bad-chars) "'"))


(defn- file-name-cond
  [_ name]
  (str "d.data_name = '" name "'"))


(defn- file-path-cond
  [parent-path path]
  (str "'" parent-path "' || '/' || d.data_name = '" path "'"))


(defn- folder-name-cond
  [parent-path name]
  (str "c.coll_name = '" parent-path "/" name "'"))


(defn- folder-path-cond
  [_ path]
  (str "c.coll_name = '" path "'"))


(defn- mk-temp-table
  [table-name query]
  (str "CREATE TEMPORARY TABLE " table-name " ON COMMIT DROP AS " query))

(defn- analyze
  [table-name]
  (str "ANALYZE " table-name))

(defn- mk-bad-cond
  [mk-bad-chars-cond mk-bad-name-cond mk-bad-path-cond parent-path bad-chars bad-names bad-paths]
  (let [conds (concat (when-not (empty? bad-chars) [(mk-bad-chars-cond parent-path bad-chars)])
                      (map #(mk-bad-name-cond parent-path %) bad-names)
                      (map #(mk-bad-path-cond parent-path %) bad-paths))
        conds (remove nil? conds)]
    (if (empty? conds)
      "FALSE"
      (string/join " OR " (map #(str "(" % ")") conds)))))


(defn ^String mk-bad-file-cond
  "Returns a WHERE condition for files in the count-bad-items-in-folder query.

   Parameters:
     parent-path - the absolute path to the folder containing the files
     bad-chars   - the characters a name cannot contain
     bad-names   - the bad names
     bad-paths   - the bad paths

   Returns:
     It returns the condition."
  [^String parent-path ^String bad-chars ^ISeq bad-names ^ISeq bad-paths]
  (mk-bad-cond file-bad-chars-cond file-name-cond file-path-cond
               parent-path bad-chars bad-names bad-paths))


(defn ^String mk-bad-folder-cond
  "Returns a WHERE condition for folders in the count-bad-items-in-folder query.

   Parameters:
     parent-path - the absolute path to the folder containing the folders
     bad-chars   - the characters a name cannot contain
     bad-names   - the bad names
     bad-paths   - the bad paths

   Returns:
     It returns the condition."
  [^String parent-path ^String bad-chars ^ISeq bad-names ^ISeq bad-paths]
  (mk-bad-cond folder-bad-chars-cond folder-name-cond folder-path-cond
                  parent-path bad-chars bad-names bad-paths))


(defn prepare-text-set
  "Given a set, it prepares the elements for injection into an SQL query. It returns a string
   containing the quoted values separated by commas."
  [values]
  (string/join ", " (map #(str \' % \') values)))


(defn ^String mk-file-type-cond
  "This function constructs condition for search for files with a given file type.

   Parameters:
     file-types - A list of file types used to filter on. A nil or empty list disables filtering.

   Returns:
     It returns a condition for filtering data objects by file type."
  [^ISeq file-types]
  (let [file-types (mapv string/lower-case file-types)
        fmt-ft     (prepare-text-set file-types)]
    (cond
      (empty? file-types)
      "TRUE"

      (some #(= "unknown" %) file-types)
      (str "f.meta_attr_value IS NULL OR f.meta_attr_value IN ('', " fmt-ft ")")

      :else
      (str "f.meta_attr_value IN (" fmt-ft ")"))))


(def ^IPersistentMap sort-columns
  "This is a mapping of API column keywords to fields in the response document."
  {:type      "type"
   :modify-ts "modify_ts"
   :create-ts "create_ts"
   :data-size "data_size"
   :base-name "base_name"
   :full-path "full_path"})


(def ^IPersistentMap sort-directions
  "This ia a mapping of API sort direction keywords to database sort direction strings."
  {:asc "ASC" :desc "DESC"})

(defn- mk-metaids-by-attr
  [attr]
  (str "SELECT meta_id FROM r_meta_main WHERE meta_attr_name = '" attr "'"))

(defn- mk-metaids-by-value-prefix
  [attr prefix]
  (str (mk-metaids-by-attr attr) " AND meta_attr_value LIKE '" prefix "%'"))

(defn- mk-objids-for-metaids
  [cte]
  (str "SELECT object_id FROM r_objt_metamap WHERE meta_id IN (SELECT meta_id FROM " cte ")"))

(defn- mk-filenames-without-attr
  [objs-cte attr-cte]
  (str "SELECT coll_name || '/' || data_name AS path
          FROM r_data_main JOIN r_coll_main USING (coll_id)
         WHERE data_id IN (SELECT object_id FROM " objs-cte " k
                            WHERE NOT EXISTS (SELECT 1 FROM r_objt_metamap
                                               WHERE r_objt_metamap.object_id = k.object_id
                                                 AND meta_id IN (SELECT meta_id FROM " attr-cte ")))"))

(defn ^ISeq mk-filtered-filenames-without-attr
  [uuid-prefix attr]
  [[(mk-temp-table "attr_metaids" (mk-metaids-by-attr attr))]
   [(mk-temp-table "uuid_metaids" (mk-metaids-by-value-prefix "ipc_UUID" uuid-prefix))]
   [(mk-temp-table "uuid_objids" (mk-objids-for-metaids "uuid_metaids"))]
   [(analyze "attr_metaids")]
   [(analyze "uuid_objids")]
   [(mk-filenames-without-attr "uuid_objids" "attr_metaids")]])

(defn- mk-unique-objs-in-coll
  [coll-path]
  (str "SELECT *
          FROM r_data_main AS d1
          WHERE coll_id = (SELECT coll_id FROM r_coll_main WHERE coll_name = '" coll-path "')
            AND d1.data_repl_num = (SELECT MIN(d2.data_repl_num)
                                      FROM r_data_main AS d2
                                      WHERE d2.data_id = d1.data_id)"))


(defn- mk-obj-avus
  [obj-ids-query]
  (str "SELECT object_id, meta_attr_value, meta_attr_name
          FROM r_objt_metamap AS o JOIN r_meta_main AS m ON o.meta_id = m.meta_id
          WHERE o.object_id = ANY(ARRAY(" obj-ids-query "))"))

(defn- mk-coll-avus
  [parent-path]
  (mk-obj-avus (str "SELECT coll_id FROM r_coll_main WHERE parent_coll_name = '" parent-path "'")))

(defn mk-combined-avus
  [objs-table parent-path]
  (mk-obj-avus (str "SELECT data_id FROM objs UNION SELECT coll_id FROM r_coll_main WHERE parent_coll_name = '"
                    parent-path "'")))

(defn- mk-file-types
  [avus-cte]
  (str "SELECT * FROM " avus-cte " WHERE meta_attr_name = 'ipc-filetype'"))

(defn ^ISeq mk-get-item
  [dirname basename group-ids-query]
  [(str "WITH "
     "objs AS ("
       "SELECT c.coll_id AS object_id, 'collection' AS type, c.coll_name AS full_path, regexp_replace(c.coll_name, '.*/', '') AS base_name, 0 as data_size, c.create_ts, c.modify_ts, NULL AS data_checksum FROM r_coll_main c WHERE coll_name = ? || '/' || ? "
       "UNION "
       "SELECT d.data_id as object_id, 'dataobject' as type, c.coll_name || '/' || d.data_name AS full_path, d.data_name AS base_name, d.data_size, d.create_ts, d.modify_ts, d.data_checksum FROM r_data_main d JOIN r_coll_main c on d.coll_id = c.coll_id WHERE coll_name = ? AND data_name = ?"
     "), "
     "meta AS ("
       "SELECT object_id, max(CASE WHEN meta_attr_name = 'ipc_UUID' THEN meta_attr_value ELSE NULL END) AS uuid, max(CASE WHEN meta_attr_name = 'ipc-filetype' THEN meta_attr_value ELSE NULL END) as info_type FROM objs LEFT JOIN r_objt_metamap mm USING (object_id) LEFT JOIN r_meta_main m USING (meta_id) GROUP BY object_id"
     ") "
     "SELECT objs.object_id, objs.type, meta.uuid, objs.full_path, objs.base_name, meta.info_type, objs.data_size, objs.create_ts, objs.modify_ts, max(a.access_type_id) AS access_type_id, objs.data_checksum FROM objs JOIN meta USING (object_id) JOIN r_objt_access a USING (object_id) WHERE a.user_id IN ("
       group-ids-query
     ") "
     "GROUP BY object_id, type, uuid, full_path, base_name, info_type, data_size, objs.create_ts, objs.modify_ts, objs.data_checksum") dirname basename dirname basename])

(defn- mk-files-in-folder
  [parent-path group-ids-query info-type-cond objs-cte avus-cte include-count?]
  (str "SELECT 'dataobject'                      AS type,
               m.meta_attr_value                 AS uuid,
               '" parent-path "/' || d.data_name AS full_path,
               d.data_name                       AS base_name,
               f.meta_attr_value                 AS info_type,
               d.data_size                       AS data_size,
               d.create_ts                       AS create_ts,
               d.modify_ts                       AS modify_ts,
               MAX(a.access_type_id)             AS access_type_id,
               d.data_checksum                   AS data_checksum"
       (if include-count? ",
               COUNT(*) OVER () AS total_count" "")
          " FROM " objs-cte " AS d
            JOIN " avus-cte " AS m ON d.data_id = m.object_id
            JOIN r_objt_access AS a ON d.data_id = a.object_id
            LEFT JOIN (" (mk-file-types avus-cte) ") AS f
              ON d.data_id = f.object_id
          WHERE a.user_id IN (" group-ids-query ")
            AND m.meta_attr_name = 'ipc_UUID'
            AND (" info-type-cond ")
          GROUP BY type, uuid, full_path, base_name, info_type, data_size, d.create_ts,
                   d.modify_ts, data_checksum"))


(defn- mk-folders-in-folder
  [parent-path group-ids-query include-count? avus-cte]
  (str "SELECT 'collection'                           AS type,
               ca.meta_attr_value                     AS uuid,
               c.coll_name                            AS full_path,
               REGEXP_REPLACE(c.coll_name, '.*/', '') AS base_name,
               NULL                                   AS info_type,
               0                                      AS data_size,
               c.create_ts                            AS create_ts,
               c.modify_ts                            AS modify_ts,
               MAX(a.access_type_id)                  AS access_type_id,
               NULL                                   AS data_checksum"
       (if include-count? ",
               COUNT(*) OVER () AS total_count" "")
         " FROM r_coll_main AS c
            JOIN " avus-cte " AS ca ON ca.object_id = c.coll_id
            JOIN r_objt_access AS a ON c.coll_id = a.object_id
          WHERE c.parent_coll_name = '" parent-path "'
            AND c.coll_type != 'linkPoint'
            AND ca.meta_attr_name = 'ipc_UUID'
            AND a.user_id IN (" group-ids-query ")
          GROUP BY type, uuid, full_path, base_name, info_type, data_size, c.create_ts,
                   c.modify_ts, data_checksum"))


(defn mk-user
  [user zone]
  [(str "SELECT user_id, user_name, user_type_name, zone_name, user_info, r_comment, create_ts, modify_ts FROM r_user_main WHERE user_name = ? AND zone_name = ?") user zone])

(defn mk-groups
  [user zone & placeholder-vec?]
  (let [base "SELECT *
               FROM r_user_group
               WHERE user_id IN (SELECT user_id
                                   FROM r_user_main
                                   WHERE user_name = %s AND zone_name = %s)"]
    (if placeholder-vec?
      [(format base "?" "?") user zone]
      (format base (str "'" user "'") (str "'" zone "'")))))

(defn mk-paths-for-uuids
  [uuids]
  (sql/format
   {:with      [[:objid (-> (h/select :object_id :meta_attr_value)
                            (h/from :r_objt_metamap)
                            (h/join :r_meta_main [:using :meta_id])
                            (h/where [:= :meta_attr_name "ipc_UUID"]
                                     [:= :meta_attr_value (sql/call :any (sql-array "varchar" uuids))]))]]
    :union-all [(-> (h/select [:meta_attr_value :uuid]
                              [:coll_name :path])
                    (h/from :r_coll_main)
                    (h/join :objid [:= :r_coll_main.coll_id :objid.object_id]))
                (-> (h/select [:meta_attr_value :uuid]
                              [(sql/raw "coll_name || '/' || data_name") :path])
                    (h/modifiers :distinct)
                    (h/from :r_data_main)
                    (h/join :r_coll_main [:using :coll_id]
                            :objid [:= :r_data_main.data_id :objid.object_id]))]}))

(defn mk-path-for-uuid
  [uuid]
  (mk-paths-for-uuids [uuid]))

(defn- object-lookup-cte
  "Creates a Common Table Expression (CTE) that can be used to find the collection or data object with
  a given path. The fields returned by the CTE are parameterized so that this function can be used in
  multiple contexts. By default, only the object ID (that is, the collection ID if the path happens
  to correspond to a collection or the data object ID if the path happens to correspond to a data
  object) is returned.

  In order to use custom fields, it's necessary to know a little bit about how the CTE is constructed.
  The CTE is a union of two queries. The first query scans `r_coll_main` for collections with the given
  path. The second query scans `r_data_main` for data objects with the basename of the given path in
  the `data_name` column that happen to be in a collection whose name matches the dirname of the path.
  The query looking for a matching collection does not do any joins, so it doesn't use any table aliases.
  The query looking for a matching data object has to do a join between `r_coll_main` and `r_data_main`,
  so it uses `c` to refer to the object's parent directory and `d` for the object itself. For example,
  this call will include the username and zone of the object's owner in the result set:

    (object-lookup-cte path
                       :collection-fields [[:coll_id :object_id]
                                           [:coll_owner_name :owner_name]
                                           [:coll_owner_zone :owner_zone]]
                       :data-object-fields [[:d.data_id :object_id]
                                            [:d.data_owner_name :owner_name]
                                            [:d.data_owner_zone :owner_zone]])

  In most cases, the column names are unique and the table aliases can be omitted completely:

    (object-lookup-cte path
                       :collection-fields [[:coll_id :object_id]
                                           [:coll_owner_name :owner_name]
                                           [:coll_owner_zone :owner_zone]]
                       :data-object-fields [[:data_id :object_id]
                                            [:data_owner_name :owner_name]
                                            [:data_owner_zone :owner_zone]])

  Please see `mk-perms-for-item` to see how this function should be used within the context of a call
  to `sql/format`."
  [path & {:keys [collection-fields data-object-fields]
           :or   {collection-fields [[:coll_id :object_id]]
                  data-object-fields [[:d.data_id :object_id]]}}]
  (let [[dirname basename] ((juxt #(.getParent %) #(.getName %)) (file path))]
    {:union [(-> (apply h/select collection-fields)
                 (h/from :r_coll_main)
                 (h/where [:= :coll_name path]))
             (-> (apply h/select data-object-fields)
                 (h/from [:r_data_main :d])
                 (h/join [:r_coll_main :c] [:using :coll_id])
                 (h/where [:= :c.coll_name dirname]
                          [:= :d.data_name basename]))]}))

(defn mk-perms-for-item
  [path]
  (sql/format
   (-> {:with [[:object-lookup (object-lookup-cte path)]]}
       (h/select :p.object_id [:u.user_name :user] :p.access_type_id)
       (h/from [:r_objt_access :p])
       (h/join [:r_user_main :u] [:using :user_id]
               [:object-lookup :o] [:using :object_id]))))


(defn- mk-count-colls-in-coll
  [parent-path group-ids-query & {:keys [cond] :or {cond "TRUE"}}]
  (str "SELECT COUNT(DISTINCT c.coll_id) AS total
          FROM r_coll_main c JOIN r_objt_access AS a ON c.coll_id = a.object_id
          WHERE c.parent_coll_name = '" parent-path "'
            AND c.coll_type != 'linkPoint'
            AND a.user_id IN (" group-ids-query ")
            AND (" cond ")"))


(defn- mk-count-objs-of-type
  [objs-cte avus-cte group-query info-type-cond & {:keys [cond] :or {cond "TRUE"}}]
  (str "SELECT COUNT(DISTINCT d.data_id) AS total
          FROM " objs-cte " AS d
            JOIN r_objt_access AS a ON a.object_id = d.data_id
            LEFT JOIN (" (mk-file-types avus-cte) ") AS f ON f.object_id = d.data_id
          WHERE a.user_id IN (" group-query ")
            AND (" info-type-cond ")
            AND (" cond ")"))


(defn ^String mk-count-bad-files-in-folder
  "This function constructs a query for counting all of the files that are direct members of a given
   folder, accessible to a given user, satisfy a given info type condition, and satisfy a given
   condition indicating the file has an invalid name.

   Parameters:
     user           - username of the user that can access the files
     zone           - iRODS authentication zone of the user
     parent-path    - the folder being inspected
     info-type-cond - a WHERE condition indicating the info types of the files to be counted
     bad-file-cond  - a WHERE condition indicating that a file has an invalid name

  Returns:
    It returns the properly formatted SELECT query."
  [& {:keys [user zone parent-path info-type-cond bad-file-cond]}]
  (let [group-query "SELECT group_user_id FROM groups"
        count-query (mk-count-objs-of-type "objs" "file_avus" group-query info-type-cond
                      :cond bad-file-cond)]
    (str "WITH groups    AS (" (mk-groups user zone) "),
               objs      AS (" (mk-unique-objs-in-coll parent-path) "),
               file_avus AS (" (mk-obj-avus "SELECT data_id FROM objs") ")
         " count-query)))


(defn ^String mk-count-bad-folders-in-folder
  "This function constructs a query for counting all of the folders that are direct members of a
   given folder, accessible to a given user, and satisfy a given condition indicating the folder has
   an invalid name.

   Parameters:
     user            - username of the user that can access the folders
     zone            - iRODS authentication zone of the user
     parent-path     - the folder being inspected
     bad-folder-cond - a WHERE condition indicating that a folder has an invalid name

  Returns:
    It returns the properly formatted SELECT query."
  [& {:keys [user zone parent-path bad-folder-cond]}]
  (let [group-query "SELECT group_user_id FROM groups"]
    (str "WITH groups AS (" (mk-groups user zone) ")
         " (mk-count-colls-in-coll parent-path group-query :cond bad-folder-cond))))


(defn ^String mk-count-bad-items-in-folder
  "This function constructs a query for counting all of the files and folders that are direct
   members of a given folder, accessible to a given user, satisfy a given info type condition, and
   satisfy a given condition indicating the file or folder has an invalid name.

   Parameters:
     user            - username of the user that can access the files and folders
     zone            - iRODS authentication zone of the user
     parent-path     - the folder being inspected
     info-type-cond  - a WHERE condition indicating the info types of the files to be counted
     bad-file-cond   - a WHERE condition indicating that a file has an invalid name
     bad-folder-cond - A WHERE condition indicating that a folder has an invalid name

  Returns:
    It returns the properly formatted SELECT query."
  [& {:keys [user zone parent-path info-type-cond bad-file-cond bad-folder-cond]}]
  (let [group-query  "SELECT group_user_id FROM groups"
        folder-query (mk-count-colls-in-coll parent-path group-query :cond bad-folder-cond)
        file-query   (mk-count-objs-of-type "objs" "file_avus" group-query info-type-cond
                       :cond bad-file-cond)]
    (str "WITH groups    AS (" (mk-groups user zone) "),
               objs      AS (" (mk-unique-objs-in-coll parent-path) "),
               file_avus AS (" (mk-obj-avus "SELECT data_id FROM objs") ")
          SELECT ((" folder-query ") + (" file-query ")) AS total")))


(defn ^ISeq mk-count-files-in-folder
  "This function constructs a query for counting all of the files that are direct members of a given
   folder, accessible to a given user, and satisfy a given info type condition.

   Parameters:
     user           - username of the user that can access the files
     zone           - iRODS authentication zone of the user
     parent-path    - the folder being inspected
     info-type-cond - a WHERE condition indicating the info types of the files to be counted

  Returns:
    It returns the properly formatted SELECT query."
  [^String user ^String zone ^String parent-path ^String info-type-cond]
  (let [group-query "SELECT group_user_id FROM groups"]
    [[(mk-temp-table "groups" (mk-groups user zone))]
     [(analyze "groups")]
     [(mk-temp-table "objs" (mk-unique-objs-in-coll parent-path))]
     [(analyze "objs")]
     [(mk-temp-table "file_avus" (mk-obj-avus "SELECT data_id FROM objs"))]
     [(analyze "file_avus")]
     [(str (mk-count-objs-of-type "objs" "file_avus" group-query info-type-cond))]]))


(defn ^ISeq mk-count-folders-in-folder
  "This function constructs a query for counting all of the folders that are direct members of a
   given folder and accessible to a given user.

   Parameters:
     user        - username of the user that can access the files
     zone        - iRODS authentication zone of the user
     parent-path - the folder being inspected

  Returns:
    It returns the properly formatted SELECT query."
  [^String user ^String zone ^String parent-path]
  (let [group-query "SELECT group_user_id FROM groups"]
    [[(str "WITH groups AS (" (mk-groups user zone) ")
           " (mk-count-colls-in-coll parent-path group-query))]]))


(defn ^ISeq mk-count-items-in-folder
  "This function constructs a set of queries for counting all of the files and folders that are direct
   members of a given folder, accessible to a given user, and satisfy a given info type condition.

   Parameters:
     user           - username of the user that can access the files and folders
     zone           - iRODS authentication zone of the user
     parent-path    - the folder being inspected
     info-type-cond - a WHERE condition indicating the info types of the files to be counted

  Returns:
    It returns the properly formatted SELECT query."
  [^String user ^String zone ^String parent-path ^String info-type-cond]
  (let [group-query   "SELECT group_user_id FROM groups"
        folders-query (mk-count-colls-in-coll parent-path group-query)
        files-query   (mk-count-objs-of-type "objs" "file_avus" group-query info-type-cond)]
    [[(mk-temp-table "groups" (mk-groups user zone))]
     [(analyze "groups")]
     [(mk-temp-table "objs" (mk-unique-objs-in-coll parent-path))]
     [(analyze "objs")]
     [(mk-temp-table "file_avus" (mk-obj-avus "SELECT data_id FROM objs"))]
     [(analyze "file_avus")]
     [(str "SELECT ((" folders-query ") + (" files-query ")) AS total")]]))


(defn ^ISeq mk-paged-files-in-folder
  "This function constructs a parameterized set of queries for returning a sorted page of files that are
   direct members of a given folder, accessible to a given user, and satisfy a given info type
   condition.

   Parameters:
     user           - username of the user that can access the files
     zone           - iRODS authentication zone of the user
     parent-path    - the folder being inspected
     info-type-cond - a WHERE condition indicating the info types of the files to be counted
     sort-column    - the result field to sort on:
                      (type|modify_ts|create_ts|data_size|base_name|full_path)
     sort-direction - the direction of the sort
     limit & offset - limit and offset to pass to the query

  Returns:
    It returns the properly formatted parameterized SELECT query. The query is parameterized over
    the page size and offset, respectively. The query will return a result set sorted by the
    provided sort column. Each row in the result set will have the following columns.

      type           - 'dataobject'
      uuid           - the file's UUID
      full_path      - the absolute path to the file in iRODS
      base_name      - the name of the file
      info_type      - the info type of the file
      data_size      - the size in bytes of the file
      create_ts      - the iRODS timestamp string for when the file was created
      modify_ts      - the iRODS timestamp string for when the file was last modified
      access_type_id - the ICAT DB Id indicating the user's level of access to the file"
  [& {:keys [user zone parent-path info-type-cond sort-column sort-direction limit offset groups-table-query]}]
  (let [group-query "SELECT group_user_id FROM groups"]
    [[(mk-temp-table "objs" (mk-unique-objs-in-coll parent-path))]
     [(analyze "objs")]
     [(mk-temp-table "file_avus" (mk-obj-avus "SELECT data_id FROM objs"))]
     [(analyze "file_avus")]
     [(str "WITH groups AS (" (or groups-table-query (mk-groups user zone)) ") "
           (mk-files-in-folder parent-path group-query info-type-cond "objs" "file_avus" true) "
           ORDER BY " sort-column " " sort-direction "
           LIMIT ?
           OFFSET ?") limit offset]]))


(defn ^ISeq mk-paged-folders-in-folder
  "This function constructs a parameterized set of queries for returning a sorted page of folders that are
   direct members of a given folder and accessible to a given user.

   Parameters:
     user           - username of the user that can access the folders
     zone           - iRODS authentication zone of the user
     parent-path    - the folder being inspected
     sort-column    - the result field to sort on:
                      (type|modify_ts|create_ts|data_size|base_name|full_path)
     sort-direction - the direction of the sort
     limit & offset - limit and offset to pass to the query

  Returns:
    It returns the properly formatted parameterized SELECT query. The query is parameterized over
    the page size and offset, respectively. The query will return a result set sorted by the
    provided sort column. Each row in the result set will have the following columns.

      type           - 'collection'
      uuid           - the folder's UUID
      full_path      - the absolute path to the folder in iRODS
      base_name      - the name of the folder
      info_type      - 'NULL'
      data_size      - '0'
      create_ts      - the iRODS timestamp string for when the folder was created
      modify_ts      - the iRODS timestamp string for when the folder was last modified
      access_type_id - the ICAT DB Id indicating the user's level of access to the folder
      data_checksum  - nil for collections, the MD5 checksum of the file for data objects"
  [& {:keys [user zone parent-path sort-column sort-direction limit offset groups-table-query]}]
  [[(mk-temp-table "coll_avus" (mk-coll-avus parent-path))]
   [(analyze "coll_avus")]
   [(str "WITH groups AS (" (or groups-table-query (mk-groups user zone)) ")
       " (mk-folders-in-folder parent-path "SELECT group_user_id FROM groups" true "coll_avus") "
        ORDER BY " sort-column " " sort-direction "
        LIMIT ?
        OFFSET ?") limit offset]])

(defn ^ISeq mk-paged-folder
  "This function constructs a set of parameterized queries for returning a sorted page of files and folders
   that are direct members of a given folder, accessible to a given user, and satisfy a given info
   type condition.

   Parameters:
     user           - username of the user that can access the files and folders
     zone           - iRODS authentication zone of the user
     parent-path    - the folder being inspected
     info-type-cond - a WHERE condition indicating the info types of the files to be counted
     sort-column    - the result field to sort on:
                      (type|modify_ts|create_ts|data_size|base_name|full_path)
     sort-direction - the direction of the sort
     limit & offset - limit and offset to pass to the query

  Returns:
    It returns the properly formatted parameterized SELECT query. The query is parameterized over
    the page size and offset, respectively. The query will return a result set sorted primary by
    entity type, and secondarily by the provided sort column. Each row in the result set will have
    the following columns.

      type           - (collection|dataobject) collection indicates folder and dataobject indicates
                       file
      uuid           - the file's or folder's UUID
      full_path      - the absolute path to the file or folder in iRODS
      base_name      - the name of the file or folder
      info_type      - the info type of the file or NULL for a folder
      data_size      - the size in bytes of the file or 0 for a folder
      create_ts      - the iRODS timestamp string for when the file or folder was created
      modify_ts      - the iRODS timestamp string for when the file or folder was last modified
      access_type_id - the ICAT DB Id indicating the user's level of access to the file or folder
      data_checksum  - nil for collections, the MD5 checksum of the file for data objects"
  [& {:keys [user zone parent-path info-type-cond sort-column sort-direction limit offset groups-table-query]}]
  (let [group-query   "SELECT group_user_id FROM groups"
        folders-query (mk-folders-in-folder parent-path group-query false "avus")
        files-query   (mk-files-in-folder parent-path group-query info-type-cond "objs"
                                          "avus" false)]
    [[(mk-temp-table "objs" (mk-unique-objs-in-coll parent-path))]
     [(analyze "objs")]
     [(mk-temp-table "avus" (mk-combined-avus "objs" parent-path))]
     [(analyze "avus")]
     [(str "WITH groups AS (" (or groups-table-query (mk-groups user zone)) ") "
           "SELECT *, COUNT(*) OVER () AS total_count
            FROM (" folders-query " UNION " files-query ") AS t
            ORDER BY type ASC, " sort-column " " sort-direction "
            LIMIT ?
            OFFSET ?") limit offset]]))


(defn ^String mk-count-uuids-of-file-type
  "This function constructs a query for returning the number of given uuids that are either folders
   or are files satisfying the given info type WHERE condition.

   Parameter:
     user           - username of the user that can access the files and folders
     zone           - iRODS authentication zone of the user
     uuids          - a comma-separated list of uuids to count
     info-type-cond - a WHERE condition indicating the info types of the files to be counted

  Returns:
    It returns the a single row containing a single 'total' column containing the total."
  [^String user ^String zone ^String uuids ^String info-type-cond]
  (str "WITH groups     AS (" (mk-groups user zone) "),
             uuids      AS (SELECT m.meta_attr_value AS uuid, o.object_id
                              FROM r_meta_main AS m
                                JOIN r_objt_metamap AS o ON m.meta_id = o.meta_id
                              WHERE m.meta_attr_name = 'ipc_UUID'
                                AND m.meta_attr_value IN (" uuids ")
                                AND o.object_id IN (SELECT object_id
                                                      FROM r_objt_access
                                                      WHERE user_id in (SELECT group_user_id
                                                                          FROM groups))),
             file_types AS (SELECT *
                              FROM r_objt_metamap AS o
                                JOIN r_meta_main AS m ON m.meta_id = o.meta_id
                              WHERE o.object_id = ANY(ARRAY(SELECT object_id FROM uuids))
                                AND m.meta_attr_name = 'ipc-filetype')
        SELECT ((SELECT COUNT(*)
                   FROM uuids
                   WHERE object_id IN (SELECT coll_id
                                         FROM r_coll_main
                                         WHERE coll_type != 'linkPoint'))
                +
                (SELECT COUNT(*)
                   FROM uuids
                   WHERE object_id IN (SELECT d.data_id
                                         FROM r_data_main As d
                                           LEFT JOIN file_types AS f on d.data_id = f.object_id
                                         WHERE (" info-type-cond ")))) AS total"))


(def queries
  {:count-all-items-under-folder
   "WITH user_groups AS ( SELECT g.group_user_id
                            FROM r_user_main u
                            JOIN r_user_group g ON g.user_id = u.user_id
                           WHERE u.user_name = ?
                             AND u.zone_name = ? ),

         parent      AS ( SELECT coll_id, coll_name from r_coll_main
                           WHERE coll_name = ?
                              OR coll_name LIKE ? || '/%' ),

         data_objs   AS ( SELECT data_id
                            FROM r_data_main
                           WHERE coll_id = ANY(ARRAY( SELECT coll_id FROM parent )) )

    SELECT ((SELECT count(DISTINCT d.data_id) FROM r_objt_access a
               JOIN data_objs d ON a.object_id = d.data_id
              WHERE a.user_id IN ( SELECT group_user_id FROM user_groups )
                AND a.object_id IN ( SELECT data_id from data_objs ))
            +
            (SELECT count(DISTINCT c.coll_id) FROM r_coll_main c
               JOIN r_objt_access a ON c.coll_id = a.object_id
              WHERE a.user_id IN ( SELECT group_user_id FROM user_groups )
                AND c.parent_coll_name = ANY(ARRAY( SELECT coll_name FROM parent ))
                AND c.coll_type != 'linkPoint')) AS total"

   :list-files-under-folder
   "SELECT DISTINCT c.coll_name || '/' || d.data_name AS path,
           m.meta_attr_value AS uuid
      FROM r_coll_main c
      JOIN r_data_main d ON c.coll_id = d.coll_id
      JOIN r_objt_metamap mm ON d.data_id = mm.object_id
      JOIN r_meta_main m ON mm.meta_id = m.meta_id
     WHERE (c.coll_name = ? OR c.coll_name LIKE ? || '/%')
       AND m.meta_attr_name = 'ipc_UUID'
  ORDER BY path"

   :list-folders-in-folder
   "WITH user_groups AS ( SELECT g.group_user_id FROM r_user_main u
                            JOIN r_user_group g ON g.user_id = u.user_id
                           WHERE u.user_name = ?
                             AND u.zone_name = ? )

    SELECT DISTINCT
           c.parent_coll_name                     as dir_name,
           c.coll_name                            as full_path,
           regexp_replace(c.coll_name, '.*/', '') as base_name,
           c.create_ts                            as create_ts,
           c.modify_ts                            as modify_ts,
           'collection'                           as type,
           0                                      as data_size,
           m.meta_attr_value                      as uuid,
           MAX(a.access_type_id)                  as access_type_id
      FROM r_coll_main c
      JOIN r_objt_access a ON c.coll_id = a.object_id
      JOIN r_objt_metamap mm ON mm.object_id = c.coll_id
      JOIN r_meta_main m ON m.meta_id = mm.meta_id
     WHERE a.user_id IN ( SELECT group_user_id FROM user_groups )
       AND c.coll_type != 'linkPoint'
       AND c.parent_coll_name = ?
       AND m.meta_attr_name = 'ipc_UUID'
  GROUP BY dir_name, full_path, base_name, c.create_ts, c.modify_ts, type, data_size, uuid
  ORDER BY base_name ASC"

   :count-files-in-folder
   "WITH user_groups AS ( SELECT g.group_user_id
                            FROM r_user_main u
                            JOIN r_user_group g ON g.user_id = u.user_id
                           WHERE u.user_name = ?
                             AND u.zone_name = ? ),

         parent      AS ( SELECT coll_id from r_coll_main
                           WHERE coll_name = ? ),

         data_objs   AS ( SELECT data_id
                            FROM r_data_main
                           WHERE coll_id = ANY(ARRAY( SELECT coll_id FROM parent )) )

      SELECT count(DISTINCT d.data_id) FROM r_objt_access a
        JOIN data_objs d ON a.object_id = d.data_id
       WHERE a.user_id IN ( SELECT group_user_id FROM user_groups )
         AND a.object_id IN ( SELECT data_id from data_objs )"

   :count-folders-in-folder
   "WITH user_groups AS ( SELECT g.group_user_id
                            FROM r_user_main u
                            JOIN r_user_group g ON g.user_id = u.user_id
                           WHERE u.user_name = ?
                             AND u.zone_name = ? )

    SELECT count(DISTINCT c.coll_id) FROM r_coll_main c
      JOIN r_objt_access a ON c.coll_id = a.object_id
     WHERE a.user_id IN ( SELECT group_user_id FROM user_groups )
       AND c.coll_type != 'linkPoint'
       AND c.parent_coll_name = ?"

   :file-permissions
   "SELECT DISTINCT o.access_type_id, u.user_name
      FROM r_user_main u,
           r_data_main d,
           r_coll_main c,
           r_tokn_main t,
           r_objt_access o
     WHERE c.coll_name = ?
       AND d.data_name = ?
       AND c.coll_id = d.coll_id
       AND o.object_id = d.data_id
       AND t.token_namespace = 'access_type'
       AND u.user_id = o.user_id
       AND o.access_type_id = t.token_id
     LIMIT ?
    OFFSET ?"

   :folder-permissions
   "SELECT DISTINCT a.access_type_id, u.user_name
     FROM r_coll_main c
     JOIN r_objt_access a ON c.coll_id = a.object_id
     JOIN r_user_main u ON a.user_id = u.user_id
    WHERE c.parent_coll_name = ?
      AND c.coll_name = ?
    LIMIT ?
   OFFSET ?"

   :folder-permissions-for-user
   "WITH user_lookup AS ( SELECT u.user_id as user_id FROM r_user_main u WHERE u.user_name = ?)
    SELECT DISTINCT a.access_type_id
      FROM r_coll_main c
      JOIN r_objt_access a ON c.coll_id = a.object_id
      JOIN r_user_main u ON a.user_id = u.user_id
     WHERE c.coll_name = ?
       AND u.user_id IN ( SELECT g.group_user_id
                           FROM  r_user_group g,
                                 user_lookup
                           WHERE g.user_id = user_lookup.user_id )"

   :file-permissions-for-user
   "WITH user_lookup AS ( SELECT u.user_id as user_id FROM r_user_main u WHERE u.user_name = ? ),
              parent AS ( SELECT c.coll_id as coll_id, c.coll_name as coll_name FROM r_coll_main c WHERE c.coll_name = ? )
    SELECT DISTINCT a.access_type_id
      FROM r_data_main d
      JOIN r_coll_main c ON c.coll_id = d.coll_id
      JOIN r_objt_access a ON d.data_id = a.object_id
      JOIN r_user_main u ON a.user_id = u.user_id,
           user_lookup,
           parent
     WHERE u.user_id IN ( SELECT g.group_user_id
                           FROM  r_user_group g,
                                 user_lookup
                           WHERE g.user_id = user_lookup.user_id )
       AND c.coll_id = parent.coll_id
       AND d.data_name = ?"

   :folder-listing
   "WITH objs AS (SELECT coll_id AS object_id, coll_name AS full_path
                  FROM r_coll_main
                  WHERE parent_coll_name = ?
                    AND coll_type != 'linkPoint'
                  UNION
                  SELECT d.data_id, c.coll_name || '/' || d.data_name
                  FROM r_data_main d
                           JOIN r_coll_main c ON d.coll_id = c.coll_id
                  WHERE c.coll_name = ?),
         user_access AS (SELECT DISTINCT object_id
                         FROM r_objt_access
                         WHERE user_id IN (SELECT g.group_user_id
                                           FROM r_user_main u
                                                    JOIN r_user_group g ON g.user_id = u.user_id
                                           WHERE u.user_name = ? AND u.zone_name = ?)
                           AND object_id IN (SELECT object_id FROM objs))
    SELECT full_path
    FROM objs
    WHERE object_id IN (SELECT object_id FROM user_access)"

   :select-files-with-uuids
   "SELECT DISTINCT m.meta_attr_value                   uuid,
                    (c.coll_name || '/' || d.data_name) path,
                    1000 * CAST(d.create_ts AS BIGINT)  \"date-created\",
                    1000 * CAST(d.modify_ts AS BIGINT)  \"date-modified\",
                    d.data_size                         \"file-size\"
      FROM r_meta_main m
        JOIN r_objt_metamap o ON m.meta_id = o.meta_id
        JOIN r_data_main d ON o.object_id = d.data_id
        JOIN r_coll_main c ON d.coll_id = c.coll_id
      WHERE m.meta_attr_name = 'ipc_UUID' AND m.meta_attr_value IN (%s)"

   :select-folders-with-uuids
   "SELECT m.meta_attr_value                  uuid,
           c.coll_name                        path,
           1000 * CAST(c.create_ts AS BIGINT) \"date-created\",
           1000 * CAST(c.modify_ts AS BIGINT) \"date-modified\"
      FROM r_meta_main m
        JOIN r_objt_metamap o ON m.meta_id = o.meta_id
        JOIN r_coll_main c ON o.object_id = c.coll_id
      WHERE m.meta_attr_name = 'ipc_UUID' AND m.meta_attr_value IN (%s)"

   :paged-uuid-listing
   "WITH groups AS (SELECT group_user_id
                      FROM r_user_group
                      WHERE user_id IN (SELECT user_id
                                          FROM r_user_main
                                          WHERE user_name = ? AND zone_name = ?)),
         uuids AS (SELECT m.meta_attr_value uuid,
                          o.object_id
                     FROM r_meta_main m JOIN r_objt_metamap o ON m.meta_id = o.meta_id
                     WHERE m.meta_attr_name = 'ipc_UUID'
                       AND m.meta_attr_value IN (%s)
                       AND o.object_id IN (SELECT object_id
                                             FROM r_objt_access
                                             WHERE user_id in (SELECT group_user_id FROM groups))),
         file_types AS (SELECT om.object_id, mm.meta_attr_value
                          FROM r_objt_metamap AS om
                            JOIN r_meta_main AS mm ON mm.meta_id = om.meta_id
                          WHERE om.object_id = ANY(ARRAY(SELECT object_id FROM uuids))
                            AND mm.meta_attr_name = 'ipc-filetype')
    SELECT p.type,
           p.uuid,
           p.full_path,
           p.base_name,
           p.info_type,
           p.data_size,
           p.create_ts,
           p.modify_ts,
           MAX(p.access_type_id) AS access_type_id,
           p.data_checksum
      FROM (SELECT 'collection'                           AS type,
                   u.uuid                                 AS uuid,
                   c.coll_name                            AS full_path,
                   regexp_replace(c.coll_name, '.*/', '') AS base_name,
                   NULL                                   AS info_type,
                   0                                      AS data_size,
                   c.create_ts                            AS create_ts,
                   c.modify_ts                            AS modify_ts,
                   a.access_type_id                       AS access_type_id,
                   NULL                                   AS data_checksum
              FROM uuids u
                JOIN r_coll_main c ON u.object_id = c.coll_id
                JOIN r_objt_access AS a ON c.coll_id = a.object_id
              WHERE c.coll_type != 'linkPoint' AND a.user_id IN (SELECT group_user_id FROM groups)
            UNION
            SELECT 'dataobject'                         AS type,
                   u.uuid                               AS uuid,
                   (c.coll_name || '/' || d1.data_name) AS full_path,
                   d1.data_name                         AS base_name,
                   f.meta_attr_value                    AS info_type,
                   d1.data_size                         AS data_size,
                   d1.create_ts                         AS create_ts,
                   d1.modify_ts                         AS modify_ts,
                   a.access_type_id                     AS access_type_id,
                   d1.data_checksum                     AS data_checksum
              FROM uuids u
                JOIN r_data_main AS d1 ON u.object_id = d1.data_id
                JOIN r_coll_main c ON d1.coll_id = c.coll_id
                JOIN r_objt_access AS a ON d1.data_id = a.object_id
                LEFT JOIN file_types AS f ON d1.data_id = f.object_id
              WHERE d1.data_repl_num = (SELECT MIN(d2.data_repl_num)
                                          FROM r_data_main AS d2
                                          WHERE d2.data_id = d1.data_id)
                AND a.user_id IN (SELECT group_user_id FROM groups)
                AND (%s)) AS p
      GROUP BY p.type, p.uuid, p.full_path, p.base_name, p.info_type, p.data_size, p.create_ts,
               p.modify_ts, p.data_checksum
      ORDER BY p.type ASC, %s %s
      LIMIT ?
      OFFSET ?"})
