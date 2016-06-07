;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
;; Copyright (c) 2013-2016, Kenneth Leung. All rights reserved.

(ns ^{:doc ""
      :author "kenl" }

  czlab.dbio.sql

  (:require
    [czlab.xlib.meta :refer [bytesClass charsClass]]
    [czlab.xlib.io :refer [readChars readBytes]]
    [czlab.xlib.str
     :refer [sname
             ucase
             lcase
             hgl?
             addDelim!
             strim]]
    [czlab.xlib.logging :as log]
    [czlab.xlib.core
     :refer [flattenNil
             trap!
             nowJTstamp
             nnz]]
    [czlab.xlib.dates :refer [gmtCal]])

  (:use [czlab.dbio.core])

  (:import
    [java.util
     Calendar
     TimeZone
     GregorianCalendar]
    [czlab.dbio
     Schema
     SQLr
     DBIOError
     OptLockError]
    [java.math
     BigDecimal
     BigInteger]
    [java.io
     Reader
     InputStream]
    [czlab.dbio DBAPI]
    [czlab.xlib XData]
    [java.sql
     ResultSet
     Types
     SQLException
     Date
     Timestamp
     Blob
     Clob
     Statement
     Connection
     PreparedStatement
     DatabaseMetaData
     ResultSetMetaData]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fmtUpdateWhere

  ""

  ^String
  [^Connection conn model]

  (str (fmtSQLIdStr conn (dbColname :rowid model)) "=?"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- lockError?

  ""

  [^String opcode cnt ^String table rowID]

  (when (== cnt 0)
    (trap! OptLockError opcode table rowID)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlFilterClause

  "[sql-filter-string, values]"

  [^Connection conn model filters]

  (let
    [flds (:fields (meta model))
     wc (reduce
          #(let [k (first %2)
                 fld (flds k)
                 c (if (nil? fld)
                     (sname k)
                     (:column fld))]
             (addDelim!
               %1
               " AND "
               (str (fmtSQLIdStr conn c)
                    (if (nil? (last %2))
                      " IS NULL "
                      " =? "))))
          (StringBuilder.)
          filters)]
    [(str wc) (flattenNil (vals filters))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readCol

  "Read column value, handling blobs"

  ^Object
  [sqlType pos ^ResultSet rset]

  (let [obj (.getObject rset (int pos))
        ^InputStream
        inp (condp instance? obj
              Blob (.getBinaryStream ^Blob obj)
              InputStream obj
              nil)
        ^Reader
        rdr (condp instance? obj
              Clob (.getCharacterStream ^Clob obj)
              Reader obj
              nil) ]
    (cond
      (some? rdr) (with-open [r rdr] (readChars r))
      (some? inp) (with-open [p inp] (readBytes p))
      :else obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readOneCol

  ""

  [sqlType pos ^ResultSet rset]

  (condp == (int sqlType)
    Types/TIMESTAMP (.getTimestamp rset (int pos) (gmtCal))
    Types/DATE (.getDate rset (int pos) (gmtCal))
    (readCol sqlType pos rset)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- modelInjtor

  "Row is a transient object"

  [model row cn ct cv]

  (let [fdef (-> (:columns (meta model))
                 (get (ucase cn)))]
    (if (nil? fdef)
      row
      (assoc! row (:id fdef) cv))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stdInjtor

  "Generic resultset, no model defined
   Row is a transient object"

  [row cn ct cv]

  (assoc! row (keyword cn) cv))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- row2Obj

  "Convert a jdbc row into object"

  [finj ^ResultSet rs ^ResultSetMetaData rsmeta]

  (with-local-vars [row (transient {})]
    (doseq [pos (range 1 (inc (.getColumnCount rsmeta)))
            :let [cn (.getColumnName rsmeta (int pos))
                  ct (.getColumnType rsmeta (int pos))
                  cv (readOneCol ct (int pos) rs)]]
      (var-set row (finj @row cn ct cv)))
    (persistent! @row)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- insert?

  ""

  [^String sql]

  (.startsWith (lcase (strim sql)) "insert"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- setBindVar

  ""

  [^PreparedStatement ps pos p]

  (condp instance? p
    String (.setString ps pos p)
    Long (.setLong ps pos p)
    Integer (.setInt ps pos p)
    Short (.setShort ps pos p)
    BigDecimal (.setBigDecimal ps pos p)
    BigInteger (.setBigDecimal ps
                               pos (BigDecimal. ^BigInteger p))
    InputStream (.setBinaryStream ps pos p)
    Reader (.setCharacterStream ps pos p)
    Blob (.setBlob ps ^long pos ^Blob p)
    Clob (.setClob ps ^long pos ^Clob p)
    (charsClass) (.setString ps pos (String. ^chars p))
    (bytesClass) (.setBytes ps pos ^bytes p)
    XData (.setBinaryStream ps pos (.stream ^XData p))
    Boolean (.setInt ps pos (if p 1 0))
    Double (.setDouble ps pos p)
    Float (.setFloat ps pos p)
    Timestamp (.setTimestamp ps pos p (gmtCal))
    Date (.setDate ps pos p (gmtCal))
    Calendar (.setTimestamp ps pos
                            (Timestamp. (.getTimeInMillis ^Calendar p))
                            (gmtCal))
    (throwDBError (str "Unsupported param type: " (type p)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mssqlTweakSqlstr

  ""

  [^String sqlstr token cmd]

  (loop [stop false
         sql sqlstr]
    (if stop
      sql
      (let [pos (.indexOf (lcase sql)
                          (sname token))
            rc (if (< pos 0)
                 []
                 [(.substring sql 0 pos)
                  (.substring sql pos)])]
        (if (empty? rc)
          (recur true sql)
          (recur false (str (first rc)
                            " WITH ("
                            cmd
                            ") " (last rc)) ))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- jiggleSQL

  ""

  ^String
  [^DBAPI db ^String sqlstr]

  (let [sql (strim sqlstr)
        lcs (lcase sql)
        v (.vendor db)]
    (if (= SQLServer (:id v))
      (cond
        (.startsWith lcs "select")
        (mssqlTweakSqlstr sql :where "NOLOCK")
        (.startsWith lcs "delete")
        (mssqlTweakSqlstr sql :where "ROWLOCK")
        (.startsWith lcs "update")
        (mssqlTweakSqlstr sql :set "ROWLOCK")
        :else sql)
      sql)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- buildStmt

  ""

  ^PreparedStatement
  [db ^Connection conn sqlstr params]

  (let [sql (jiggleSQL db sqlstr)
        ps (if (insert? sql)
             (.prepareStatement conn
                                sql
                                Statement/RETURN_GENERATED_KEYS)
             (.prepareStatement conn sql))]
    (log/debug "Building SQLStmt: %s" sql)
    (doseq [n (range 0 (count params)) ]
      (setBindVar ps (inc n) (nth params n)))
    ps))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- handleGKeys

  ""

  [^ResultSet rs cnt options]

  (let [rc (if (== cnt 1)
             (.getObject rs 1)
             (.getLong rs (str (:pkey options)))) ]
    {:1 rc}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExecWithOutput

  ""

  [db conn sql pms options]

  (with-open [stmt (buildStmt db conn sql pms) ]
    (when (> (.executeUpdate stmt) 0)
      (with-open [rs (.getGeneratedKeys stmt) ]
        (let [cnt (if (nil? rs)
                    0
                    (-> (.getMetaData rs)
                        (.getColumnCount))) ]
          (if (and (> cnt 0)
                   (.next rs))
            (handleGKeys rs cnt options)
            {}
            ))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlSelect+

  ""

  [db conn sql pms func post]

  (with-open
    [stmt (buildStmt db conn sql pms)
     rs (.executeQuery stmt) ]
    (let [rsmeta (.getMetaData rs) ]
      (loop [sum (transient [])
             ok (.next rs) ]
        (if-not ok
          (persistent! sum)
          (recur (->> (post (func rs rsmeta))
                      (conj! sum))
                 (.next rs)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlSelect

  ""

  [db conn sql pms]

  (sqlSelect+ db conn sql pms
              (partial row2Obj stdInjtor) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExec

  ""

  [db conn sql pms]

  (with-open [stmt (buildStmt db conn sql pms) ]
    (.executeUpdate stmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- insertFlds

  "Format sql string for insert"

  [^Connection conn obj flds]

  (with-local-vars [ps (transient []) ]
    (let [sb2 (StringBuilder.)
          sb1 (StringBuilder.)]
      (doseq [[k v] obj
              :let [fdef (flds k)]]
        (when (and (some? fdef)
                   (not (:auto fdef))
                   (not (:system fdef)))
          (addDelim! sb1 "," (fmtSQLIdStr conn (dbColname fdef)))
          (addDelim! sb2 "," (if (nil? v) "NULL" "?"))
          (when (some? v)
            (var-set ps (conj! @ps v)))))
      [(str sb1) (str sb2) (persistent! @ps)] )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- updateFlds

  "Format sql string for update"

  [^Connection conn obj flds]

  (with-local-vars [ps (transient []) ]
    (let [sb1 (StringBuilder.)]
      (doseq [[k v]  obj
              :let [fdef (flds k)]]
        (when (and (some? fdef)
                   (:updatable fdef)
                   (not (:auto fdef))
                   (not (:system fdef)))
          (doto sb1
            (addDelim! "," (fmtSQLIdStr conn (dbColname fdef)))
            (.append (if (nil? v) "=NULL" "=?")))
          (when (some? v)
            (var-set ps (conj! @ps v)))))
      [(str sb1) (persistent! @ps)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- postFmtModelRow

  ""

  [model obj]

  (with-meta obj {:model model}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExecWithOutput

  ""

  [db metas conn sql pms options]

  (sqlExecWithOutput db conn sql pms options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExec

  ""

  [db metas conn sql pms]

  (sqlExec db conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery+

  ""

  [db metas conn sql pms model]

  (if-let [mcz (metas model)]
    (sqlSelect+ db conn sql pms
                (partial row2Obj
                         (partial modelInjtor mcz))
                #(postFmtModelRow model %))
    (throwDBError (str "Unknown model " model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery

  ""

  [db metas conn sql pms]

  (sqlSelect db conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doCount

  ""

  [db metas conn model]

  (let
    [rc (doQuery db
                 metas
                 conn
                 (str "SELECT COUNT(*) FROM "
                      (fmtSQLIdStr conn (dbTablename model metas)))
                 [])]
    (if (empty? rc)
      0
      (last (first (seq (first rc)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doPurge

  ""

  [db metas conn model]

  (let [sql (str "DELETE FROM "
                 (fmtSQLIdStr conn (dbTablename model metas))) ]
    (sqlExec db conn sql [])
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doDelete

  ""

  [db schema conn obj]

  (if-let [mcz (:model (meta obj))]
    (doExec db
            schema
            conn
            (str "DELETE FROM "
                 (fmtSQLIdStr conn (dbTablename mcz))
                 " WHERE "
                 (fmtUpdateWhere conn mcz))
            [(:rowid obj)])
    (throwDBError (str "Unknown model for " obj))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doInsert

  ""

  [db schema conn obj]

  (if-let [mcz (:model (meta obj))]
    (let [[s1 s2 pms]
          (insertFlds conn obj (:fields (meta mcz)))]
      (when (hgl? s1)
        (let [out (doExecWithOutput
                    db
                    schema
                    conn
                    (str "INSERT INTO "
                         (fmtSQLIdStr conn (dbTablename mcz))
                         "(" s1 ") VALUES (" s2 ")")
                    pms
                    {:pkey (dbColname :rowid mcz)})]
            (if (empty? out)
              (throwDBError (str "Insert requires row-id to be returned."))
              (log/debug "Exec-with-out %s" out))
            (let [wm {:rowid (:1 out) } ]
              (when-not (number? (:rowid wm))
                (throwDBError (str "RowID data-type must be a Long.")))
              (vary-meta obj mergeMeta wm)))))
      (throwDBError (str "Unknown model for " obj))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doUpdate

  ""

  [db schema conn obj]

  (if-let [mcz (:model (meta obj))]
    (let [[sb1 pms]
          (updateFlds conn
                      obj
                      (:fields (meta mcz)))]
      (if (hgl? sb1)
        (doExec db
                metas
                conn
                (str "UPDATE "
                     (fmtSQLIdStr conn (dbTablename mcz))
                     " SET "
                     sb1
                     " WHERE "
                     (fmtUpdateWhere conn mcz))
                (conj pms (:rowid info)))
        0))
    (throwDBError (str "Unknown model for " obj))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExtraSQL

  ""

  ^String
  [^String sql extra]

  sql)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn reifySQLr

  "Create a SQLr"

  ^SQLr
  [^DBAPI db getc runc]

  {:pre [(fn? getc) (fn? runc)]}

  (let [metaz (.getMetas db)]
    (reify

      SQLr

      (findSome [this model filters]
        (.findSome this model filters {} ))

      (findAll [this model extra]
        (.findSome this model {} extra))

      (findAll [this model]
        (.findAll this model {}))

      (findOne [this model filters]
        (when-some [rset (.findSome this model filters {})]
          (when-not (empty? rset) (first rset))))

      (findSome [_ model filters extraSQL]
        (let
          [func #(let
                   [mcz (metaz model)
                    s (str "SELECT * FROM "
                           (gtable mcz))
                    [wc pms]
                    (sqlFilterClause mcz filters) ]
                   (if (hgl? wc)
                     (doQuery+ db metaz %1
                               (doExtraSQL (str s " WHERE " wc)
                                           extraSQL)
                               pms model)
                     (doQuery+ db metaz %1
                               (doExtraSQL s extraSQL) [] model)))]
          (runc (getc db) func)))

      (escId [_ s] (ese "" s ""))

      (metas [_] metaz)

      (update [_ obj]
        (->> #(doUpdate db metaz %1 obj)
             (runc (getc db) )))

      (delete [_ obj]
        (->> #(doDelete db metaz %1 obj)
             (runc (getc db) )))

      (insert [_ obj]
        (->> #(doInsert db metaz %1 obj)
             (runc (getc db) )))

      (select [_ model sql params]
        (->> #(doQuery+ db metaz %1 sql params model)
             (runc (getc db) )))

      (select [_ sql params]
        (->> #(doQuery db metaz %1 sql params)
             (runc (getc db) )))

      (execWithOutput [_ sql pms]
        (->> #(doExecWithOutput db metaz %1
                                sql pms {:pkey COL_ROWID})
             (runc (getc db) )))

      (exec [_ sql pms]
        (->> #(doExec db metaz %1 sql pms)
             (runc (getc db) )))

      (countAll [_ model]
        (->> #(doCount db metaz %1 model)
             (runc (getc db) )))

      (purge [_ model]
        (->> #(doPurge db metaz %1 model)
             (runc (getc db) ))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


