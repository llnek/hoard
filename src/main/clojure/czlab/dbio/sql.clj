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

(ns ^{:doc "Low level SQL JDBC functions."
      :author "Kenneth Leung" }

  czlab.dbio.sql

  (:require
    [czlab.xlib.meta :refer [bytesClass charsClass]]
    [czlab.xlib.io :refer [readChars readBytes]]
    [czlab.xlib.str
     :refer [sname
             ucase
             lcase
             strbf
             hgl?
             addDelim!
             strim]]
    [czlab.xlib.logging :as log]
    [clojure.string :as cs]
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
     DBIOError ]
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
  [vendor model]

  (let [pk (:pkey model)]
    (str (fmtSQLId vendor (dbColname pk model)) "=?")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlFilterClause

  "returns [sql-filter-string, values]"
  [vendor model filters]

  (let [flds (:fields model)
        wc (reduce
             #(let [[k v] %2
                    fd (flds k)
                    c (if (nil? fd)
                        (sname k)
                        (:column fd))]
                (addDelim!
                  %1
                  " and "
                  (str (fmtSQLId vendor c)
                       (if (nil? v)
                         " is null " " =? "))))
             (strbf) filters)]
    [(str wc) (flattenNil (vals filters))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readCol

  "Read column value, handling blobs"
  ^Object
  [pos ^ResultSet rset]

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
              nil)]
    (cond
      (some? rdr) (with-open [r rdr] (readChars r))
      (some? inp) (with-open [p inp] (readBytes p))
      :else obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readOneCol

  "Read column"
  [sqlType pos ^ResultSet rset]

  (condp == (int sqlType)
    Types/TIMESTAMP (.getTimestamp rset (int pos) (gmtCal))
    Types/DATE (.getDate rset (int pos) (gmtCal))
    (readCol pos rset)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- modelInjtor

  "Row is a transient object"
  [model row cn ct cv]

  ;;if column is not defined in the model, ignore it
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

  (persistent!
    (reduce
      #(let [ct (.getColumnType rsmeta (int %2))]
         (finj %1
               (.getColumnName rsmeta (int %2))
               ct
               (readOneCol ct (int %2) rs)))
      (transient {})
      (range 1 (inc (.getColumnCount rsmeta))))))

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
    (dberr "Unsupported param-type: %s" (type p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mssqlTweakSqlstr

  ""
  [^String sqlstr token cmd]

  (loop [target (sname token)
         start 0
         stop false
         sql sqlstr]
    (if stop
      sql
      (let [pos (.indexOf (lcase sql) target start)
            rc (if (< pos 0)
                 nil
                 [(.substring sql 0 pos)
                  " with ("
                  cmd
                  ") "
                  (.substring sql pos)])]
        (if (empty? rc)
          (recur target 0 true sql)
          (recur target
                 (+ pos (.length target))
                 false
                 (cs/join "" rc)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- jiggleSQL

  ""
  ^String
  [vendor ^String sqlstr]

  (let [sql (strim sqlstr)
        lcs (lcase sql)]
    (if (= SQLServer (:id vendor))
      (cond
        (.startsWith lcs "select")
        (mssqlTweakSqlstr sql :where "nolock")
        (.startsWith lcs "delete")
        (mssqlTweakSqlstr sql :where "rowlock")
        (.startsWith lcs "update")
        (mssqlTweakSqlstr sql :set "rowlock")
        :else sql)
      sql)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fmtStmt

  ""
  ^PreparedStatement
  [vendor ^Connection conn sqlstr params]

  (let [sql (jiggleSQL vendor sqlstr)
        ps (if (insert? sql)
             (.prepareStatement conn
                                sql
                                Statement/RETURN_GENERATED_KEYS)
             (.prepareStatement conn sql))]
    (log/debug "SQLStmt: %s" sql)
    (doseq [n (range 0 (count params))]
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
  [vendor conn sql pms options]

  (with-open [s (fmtStmt vendor conn sql pms)]
    (when (> (.executeUpdate s) 0)
      (with-open [rs (.getGeneratedKeys s)]
        (let [cnt (if (nil? rs)
                    0
                    (-> (.getMetaData rs)
                        (.getColumnCount)))]
          (if (and (> cnt 0)
                   (.next rs))
            (handleGKeys rs cnt options)
            {}
            ))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlSelect+

  ""
  [vendor conn sql pms func post]

  (with-open
    [s (fmtStmt vendor conn sql pms)
     rs (.executeQuery s)]
    (let [rsmeta (.getMetaData rs)]
      (loop [sum (transient [])
             ok (.next rs)]
        (if-not ok
          (persistent! sum)
          (recur (->> (post (func rs rsmeta))
                      (conj! sum))
                 (.next rs)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlSelect

  ""
  [vendor conn sql pms]

  (sqlSelect+ vendor
              conn
              sql
              pms
              (partial row2Obj stdInjtor) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExec

  ""
  [vendor conn sql pms]

  (with-open [s (fmtStmt vendor conn sql pms)]
    (.executeUpdate s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- insertFlds

  "Format sql string for insert"
  [vendor obj flds]

  (let
    [sb2 (strbf)
     sb1 (strbf)
     ps
     (reduce
       #(let [[k v] %2
              fd (get flds k)]
          (if (and (some? fd)
                   (not (:auto fd))
                   (not (:system fd)))
            (do
              (addDelim! sb1 "," (fmtSQLId vendor (dbColname fd)))
              (addDelim! sb2 "," (if (nil? v) "null" "?"))
              (if (some? v) (conj! %1 v) %1))
            %1))
       (transient [])
       obj)]
    [(str sb1) (str sb2) (persistent! ps)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- updateFlds

  "Format sql string for update"
  [vendor obj flds]

  (let
    [sb1 (strbf)
     ps
     (reduce
       #(let [[k v] %2
              fd (get flds k)]
          (if (and (some? fd)
                   (:updatable fd)
                   (not (:auto fd))
                   (not (:system fd)))
            (do
              (addDelim! sb1 "," (fmtSQLId vendor (dbColname fd)))
              (.append sb1 (if (nil? v) "=null" "=?"))
              (if (some? v) (conj! %1 v) %1))
            %1))
       (transient [])
       obj)]
    [(str sb1) (persistent! ps)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- postFmtModelRow

  ""
  [model obj]

  (with-meta obj {:model model}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExec+

  ""
  [vendor conn sql pms options]

  (sqlExecWithOutput vendor conn sql pms options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExec

  ""
  [vendor conn sql pms]

  (sqlExec vendor conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery+

  ""
  [vendor conn sql pms model]

  (sqlSelect+ vendor
              conn
              sql
              pms
              (partial row2Obj
                       (partial modelInjtor model))
              #(postFmtModelRow model %)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery

  ""
  [vendor conn sql pms]

  (sqlSelect vendor conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doCount

  ""
  [vendor conn model]

  (let
    [rc (doQuery vendor
                 conn
                 (str "select count(*) from "
                      (fmtSQLId vendor (dbTablename model) ))
                 [])]
    (if (empty? rc)
      0
      (last (first (seq (first rc)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doPurge

  ""
  [vendor conn model]

  (let [sql (str "delete from "
                 (fmtSQLId vendor (dbTablename model) ))]
    (sqlExec vendor conn sql [])
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doDelete

  ""
  [vendor conn obj]

  (if-some [mcz (gmodel obj)]
    (doExec vendor
            conn
            (str "delete from "
                 (->> (dbTablename mcz)
                      (fmtSQLId vendor ))
                 " where "
                 (fmtUpdateWhere vendor mcz))
            [(goid obj)])
    (dberr "Unknown model for: %s" obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doInsert

  ""
  [vendor conn obj]

  (if-some [mcz (gmodel obj)]
    (let [pke (:pkey mcz)
          [s1 s2 pms]
          (insertFlds vendor obj (:fields mcz))]
      (when (hgl? s1)
        (let [out (doExec+
                    vendor
                    conn
                    (str "insert into "
                         (->> (dbTablename mcz)
                              (fmtSQLId vendor ))
                         " ("
                         s1
                         ") values (" s2 ")")
                    pms
                    {:pkey (dbColname pke mcz)})]
          (if (empty? out)
            (dberr "rowid must be returned")
            (log/debug "Exec-with-out %s" out))
          (let [n (:1 out)]
            (when-not (number? n)
              (dberr "rowid must be a Long"))
            (merge obj {pke  n})))))
    (dberr "Unknown model for: %s" obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doUpdate

  ""
  [vendor conn obj]

  (if-some [mcz (gmodel obj)]
    (let [[sb1 pms]
          (updateFlds vendor
                      obj
                      (:fields mcz))]
      (if (hgl? sb1)
        (doExec vendor
                conn
                (str "update "
                     (->> (dbTablename mcz)
                          (fmtSQLId vendor ))
                     " set "
                     sb1
                     " where "
                     (fmtUpdateWhere vendor mcz))
                (conj pms (goid obj)))
        0))
    (dberr "Unknown model for: %s" obj)))

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
  {:tag SQLr :no-doc true}
  [^DBAPI db runc]
  {:pre [(fn? runc)]}

  (let [schema (.getMetas db)
        vendor (.vendor db)]
    (reify

      SQLr

      (findSome [me typeid filters]
        (.findSome me typeid filters {} ))

      (findAll [me typeid extra]
        (.findSome me typeid {} extra))

      (findAll [me typeid]
        (.findAll me typeid {}))

      (findOne [me typeid filters]
        (let [rs (.findSome me typeid filters)]
          (when-not (empty? rs) (first rs))))

      (findSome [_ typeid filters extraSQL]
        (if-some [mcz (.get schema typeid)]
          (runc
            #(let [s (str "select * from "
                          (fmtSQLId vendor (:table mcz)))
                   [wc pms]
                   (sqlFilterClause vendor mcz filters)]
               (doQuery+
                 vendor
                 %1
                 (doExtraSQL
                   (if (hgl? wc)
                     (str s " where " wc) s)
                   extraSQL)
                 pms mcz)))
          (dberr "Unknown model: %s" typeid)))

      (fmtId [_ s] (fmtSQLId vendor s))

      (metas [_] schema)

      (update [_ obj]
        (runc #(doUpdate vendor %1 obj)))

      (delete [_ obj]
        (runc #(doDelete vendor %1 obj)))

      (insert [_ obj]
        (runc #(doInsert vendor %1 obj)))

      (select [_ typeid sql params]
        (if-some [m (.get schema typeid)]
          (runc #(doQuery+ vendor
                           %1
                           sql
                           params
                           m))
          (dberr "Unknown model: %s" typeid)))

      (select [_ sql params]
        (runc #(doQuery vendor %1 sql params)))

      (execWithOutput [_ sql pms]
        (runc #(doExec+ vendor
                        %1
                        sql
                        pms
                        {:pkey COL_ROWID})))

      (exec [_ sql pms]
        (runc #(doExec vendor %1 sql pms)))

      (countAll [_ typeid]
        (if-some [m (.get schema typeid)]
          (runc #(doCount vendor %1 m))
          (dberr "Unknown model: %s" typeid)))

      (purge [_ typeid]
        (if-some [m (.get schema typeid)]
          (runc #(doPurge vendor %1 m))
          (dberr "Unknown model: %s" typeid))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


