;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Low level SQL JDBC functions."
      :author "Kenneth Leung"}

  czlab.horde.sql

  (:require [czlab.basal.io :refer [readChars readBytes]]
            [czlab.basal.meta
             :refer
             [instBytes?
              instChars?
              bytesClass
              charsClass]]
            [czlab.basal.logging :as log]
            [clojure.string :as cs]
            [czlab.basal.dates :refer [gmt<>]])

  (:use [czlab.horde.core]
        [czlab.basal.core]
        [czlab.basal.str])

  (:import [java.util Calendar TimeZone GregorianCalendar]
           [java.math BigDecimal BigInteger]
           [java.io Reader InputStream]
           [czlab.jasal XData]
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
  "Filter on primary key"
  [vendor model]
  (str (fmtSqlId vendor (dbcol (:pkey model) model)) "=?"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlFilterClause
  "[sql-filter-string, values]"
  [vendor model filters]
  (let
    [flds (:fields model)
     wc (sreduce<>
          #(let [[k v] %2
                 fd (flds k)
                 c (if (nil? fd)
                     (sname k)
                     (:column fd))]
             (addDelim!
               %1
               " and "
               (str (fmtSqlId vendor c)
                    (if (nil? v)
                      " is null " " =? ")))) filters)]
    ;;returns the where clause and parameters
    [wc (flatnil (vals filters))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readCol
  "Read column value, handling blobs"
  [pos ^ResultSet rset]
  (let
    [obj (.getObject rset (int pos))
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
  (let [c (gmt<>)
        pos (int pos)]
    (condp == (int sqlType)
      Types/TIMESTAMP (.getTimestamp rset pos c)
      Types/DATE (.getDate rset pos c)
      (readCol pos rset))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- modelInjtor
  "Row is a transient object"
  [model row cn ct cv]
  ;;if column is not defined in the model, ignore it
  (if-some
    [fdef (-> (:columns (meta model))
              (get (ucase cn)))]
    (assoc! row (:id fdef) cv)
    row))

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
  (preduce<map>
    #(let
       [pos (int %2)
        ct (.getColumnType rsmeta pos)]
       (finj %1
             (.getColumnName rsmeta pos)
             ct
             (readOneCol ct pos rs)))
    (range 1 (inc (.getColumnCount rsmeta)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private insert?
  "" [sql] `(.startsWith (lcase (strim ~sql)) "insert"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declare setBindVar)
(defn- onXData
  "" [^PreparedStatement ps pos ^XData x]

  (let [c (.content x)]
    (->>
      (if (or (string? c)
              (instBytes? c)
              (instChars? c)) c (.stream x))
      (setBindVar ps pos ))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- setBindVar
  "" [^PreparedStatement ps pos p]

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
    XData (onXData ps pos p)
    Boolean (.setInt ps pos (if p 1 0))
    Double (.setDouble ps pos p)
    Float (.setFloat ps pos p)
    Timestamp (.setTimestamp ps pos p (gmt<>))
    Date (.setDate ps pos p (gmt<>))
    Calendar (.setTimestamp ps pos
                            (Timestamp. (.getTimeInMillis ^Calendar p))
                            (gmt<>))
    (dberr! "Unsupported param-type: %s" (type p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mssqlTweakSqlstr
  "" [^String sqlstr token cmd]

  (loop [target (sname token)
         start 0
         stop false
         sql sqlstr]
    (if stop
      sql
      (let [pos (.indexOf (lcase sql)
                          target start)
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
  "" ^String [vendor ^String sqlstr]

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
  "" ^PreparedStatement
  [vendor ^Connection conn sqlstr params]

  (let [sql (jiggleSQL vendor sqlstr)
        ps (if (insert? sql)
             (.prepareStatement
               conn
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
  "" [^ResultSet rs cnt args]
  {:1 (if (== cnt 1)
        (.getObject rs 1)
        (.getLong rs (str (:pkey args))))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExecWithOutput
  "" [vendor conn sql pms args]

  (with-open
    [s (fmtStmt vendor conn sql pms)]
    (if (> (.executeUpdate s) 0)
      (with-open
        [rs (.getGeneratedKeys s)]
        (let [cnt (if (nil? rs)
                    0
                    (-> (.getMetaData rs)
                        (.getColumnCount)))]
          (if (and (> cnt 0)
                   (.next rs))
            (handleGKeys rs cnt args) {}))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqls+
  "" [vendor conn sql pms func post]

  (with-open
    [s (fmtStmt vendor conn sql pms)
     rs (.executeQuery s)]
    (let [m (.getMetaData rs)]
      (loop [sum (transient [])
             ok (.next rs)]
        (if-not ok
          (pcoll! sum)
          (recur (->> (post (func rs m))
                      (conj! sum)) (.next rs)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlSelect
  "" [vendor conn sql pms]
  (sqls+ vendor
         conn
         sql pms (partial row2Obj stdInjtor) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExec
  "" [vendor conn sql pms]
  (with-open [s (fmtStmt vendor
                         conn sql pms)] (.executeUpdate s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- insertFlds
  "Format sql for insert" [vendor obj flds]

  (let [sb2 (strbf<>)
        sb1 (strbf<>)
        ps
        (preduce<vec>
          #(let [[k v] %2
                 fd (get flds k)]
             (if (and fd
                      (not (:auto? fd))
                      (not (:system? fd)))
               (do
                 (addDelim! sb1
                            "," (fmtSqlId vendor (dbcol fd)))
                 (addDelim! sb2
                            "," (if (nil? v) "null" "?"))
                 (if v (conj! %1 v) %1))
               %1)) obj)]
    [(str sb1) (str sb2) ps]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- updateFlds
  "Format sql for update" [vendor obj flds]

  (let [sb1 (strbf<>)
        ps
        (preduce<vec>
          #(let [[k v] %2
                 fd (get flds k)]
             (if (and fd
                      (:updatable? fd)
                      (not (:auto? fd))
                      (not (:system? fd)))
               (do
                 (addDelim! sb1
                            "," (fmtSqlId vendor (dbcol fd)))
                 (.append sb1 (if (nil? v) "=null" "=?"))
                 (if v (conj! %1 v) %1))
               %1)) obj)]
    [(str sb1) ps]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private
  postFmtModelRow "" [obj model] `(bindModel ~obj ~model))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExec+
  "" [vendor conn sql pms options]
  (sqlExecWithOutput vendor conn sql pms options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExec
  "" ^long [vendor conn sql pms] (sqlExec vendor conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery+
  "" [vendor conn sql pms model]
  (sqls+ vendor
         conn
         sql
         pms
         (partial row2Obj
                  (partial modelInjtor model))
         #(postFmtModelRow % model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery
  "" [vendor conn sql pms] (sqlSelect vendor conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doCount
  "" [vendor conn model]
  (let
    [rc (doQuery vendor
                 conn
                 (str "select count(*) from "
                      (fmtSqlId vendor (dbtable model) )) [])]
    (if (empty? rc)
      0
      (last (first (seq (first rc)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doPurge
  ""
  [vendor conn model]
  (let [sql (str "delete from "
                 (fmtSqlId vendor (dbtable model)))]
    (sqlExec vendor conn sql [])
    0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doDelete
  ""
  ^long [vendor conn obj]

  (if-some [mcz (gmodel obj)]
    (doExec vendor
            conn
            (str "delete from "
                 (->> (dbtable mcz)
                      (fmtSqlId vendor ))
                 " where "
                 (fmtUpdateWhere vendor mcz))
            [(goid obj)])
    (dberr! "Unknown model for: %s" obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doInsert ""
  ^Object [vendor conn obj]

  (if-some [mcz (gmodel obj)]
    (let [pke (:pkey mcz)
          [s1 s2 pms]
          (insertFlds vendor obj (:fields mcz))]
      (if (hgl? s1)
        (let [out (doExec+
                    vendor
                    conn
                    (str "insert into "
                         (->> (dbtable mcz)
                              (fmtSqlId vendor ))
                         " (" s1 ") values (" s2 ")")
                    pms
                    {:pkey (dbcol pke mcz)})]
          (if (empty? out)
            (dberr! "rowid must be returned")
            (log/debug "Exec-with-out %s" out))
          (let [n (:1 out)]
            (if-not (number? n)
              (dberr! "rowid must be a Long"))
            (merge obj {pke  n})))))
    (dberr! "Unknown model for: %s" obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doUpdate ""
  ^long [vendor conn obj]

  (if-some [mcz (gmodel obj)]
    (let [[sb1 pms]
          (updateFlds vendor
                      obj (:fields mcz))]
      (if (hgl? sb1)
        (doExec vendor
                conn
                (str "update "
                     (->> (dbtable mcz)
                          (fmtSqlId vendor ))
                     " set " sb1 " where "
                     (fmtUpdateWhere vendor mcz))
                (conj pms (goid obj)))
        0))
    (dberr! "Unknown model for: %s" obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExtraSQL "" ^String [^String sql extra] sql)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object SQLrObj
  czlab.horde.core.SQLr
  (find-some [me typeid filters]
    (find-some me typeid filters _empty-map_))
  (find-all [me typeid extra]
    (find-some me typeid _empty-map_ extra))
  (find-all [me typeid]
    (find-all me typeid _empty-map_))
  (find-one [me typeid filters]
    (let [rs (find-some me typeid filters)]
      (if-not (empty? rs) (first rs))))
  (find-some [me typeid filters extraSQL]
    (let [{:keys [vendor models runc]} me]
      (if-some [mcz (models typeid)]
        (runc
          #(let [s (str "select * from "
                        (fmtSqlId vendor (:table mcz)))
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
        (dberr! "Unknown model: %s" typeid))))
  (fmt-id [me s] (fmtSqlId (:vendor me) s))
  (mod-obj [me obj]
    (let [{:keys [vendor runc]} me]
      (runc #(doUpdate vendor %1 obj))))
  (del-obj [me obj]
    (let [{:keys [vendor runc]} me]
      (runc #(doDelete vendor %1 obj))))
  (add-obj [me obj]
    (let [{:keys [vendor runc]} me]
      (runc #(doInsert vendor %1 obj))))
  (select-sql [me typeid sql params]
    (let [{:keys [runc models vendor]} me]
      (if-some [m (models typeid)]
        (runc #(doQuery+ vendor %1 sql params m))
        (dberr! "Unknown model: %s" typeid))))
  (select-sql [me sql params]
    (let [{:keys [vendor runc]} me]
      (runc #(doQuery vendor %1 sql params))))
  (exec-with-output [me sql pms]
    (let [{:keys [vendor runc]} me]
      (runc #(doExec+ vendor
                      %1
                      sql pms {:pkey *col-rowid*}))))
  (exec-sql [me sql pms]
    (let [{:keys [vendor runc]} me]
      (runc #(doExec vendor %1 sql pms))))
  (count-objs [me typeid]
    (let [{:keys [models vendor runc]} me]
      (if-some [m (models typeid)]
        (runc #(doCount vendor %1 m))
        (dberr! "Unknown model: %s" typeid))))
  (purge-objs [me typeid]
    (let [{:keys [models vendor runc]} me]
      (if-some [m (models typeid)]
        (runc #(doPurge vendor %1 m))
        (dberr! "Unknown model: %s" typeid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn sqlr<> ""
  ^czlab.horde.core.SQLr
  [db runc]
  {:pre [(fn? runc)]}

  (let [{:keys [schema vendor]} db]
    (object<> SQLrObj
              {:models (:models schema)
               :schema schema :vendor vendor :runc runc})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

