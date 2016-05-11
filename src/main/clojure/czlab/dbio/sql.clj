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

  czlab.xlib.dbio.sql

  (:require
    [czlab.xlib.util.meta :refer [BytesClass CharsClass]]
    [czlab.xlib.util.str
    :refer [sname ucase lcase hgl? AddDelim! strim]]
    [czlab.xlib.util.io :refer [ReadChars ReadBytes ]]
    [czlab.xlib.util.logging :as log]
    [czlab.xlib.util.core
    :refer [FlattenNil trap! NowJTstamp nnz]]
    [czlab.xlib.util.dates :refer [GmtCal]])

  (:use [czlab.xlib.dbio.core])

  (:import
    [java.util Calendar GregorianCalendar TimeZone]
    [com.zotohlab.frwk.dbio MetaCache
    SQLr DBIOError OptLockError]
    [java.math BigDecimal BigInteger]
    [java.io Reader InputStream]
    [com.zotohlab.frwk.dbio DBAPI]
    [com.zotohlab.frwk.io XData]
    [java.sql ResultSet Types SQLException
    DatabaseMetaData ResultSetMetaData
    Date Timestamp Blob Clob
    Statement PreparedStatement Connection]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fmtUpdateWhere ""

  ^String
  [lock mcz]

  (str (ese (Colname :rowid mcz))
       "=?"
       (if lock
           (str " AND "
                (ese (Colname :verid mcz)) "=?")
           "")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- lockError? ""

  [^String opcode cnt ^String table rowID]

  (when (== cnt 0)
    (trap! OptLockError opcode table rowID)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn SqlFilterClause

  "[sql-filter string, values]"

  [mcz filters]

  (let
    [flds (:fields (meta mcz))
     wc (reduce
          #(let [k (first %2)
                 fld (get flds k)
                 c (if (nil? fld)
                     (sname k)
                     (:column fld)) ]
             (AddDelim! %1
                        " AND "
                        (str (ese c)
                             (if (nil? (last %2))
                               " IS NULL "
                               " = ? "))))
          (StringBuilder.)
          (seq filters)) ]
    [ (str wc) (FlattenNil (vals filters)) ]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readCol ""

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
      (some? rdr) (with-open [r rdr] (ReadChars r))
      (some? inp) (with-open [p inp] (ReadBytes p))
      :else obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- readOneCol ""

  [sqlType pos ^ResultSet rset]

  (condp == (int sqlType)
    Types/TIMESTAMP (.getTimestamp rset (int pos) (GmtCal))
    Types/DATE (.getDate rset (int pos) (GmtCal))
    (readCol sqlType pos rset)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- modelInjtor

  "Row is a transient object"

  [mcz row cn ct cv]

  (let [fdef (-> (:columns (meta mcz))
                 (get (ucase cn))) ]
    (if (nil? fdef)
      row
      (assoc! row (:id fdef) cv))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stdInjtor

  "Generic resultset, no model defined
   Row is a transient object"

  [row cn ct cv]

  (assoc! row (keyword (ucase cn)) cv))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- row2Obj

  "Convert a jdbc row into object"

  [finj ^ResultSet rs ^ResultSetMetaData rsmeta]

  (with-local-vars [row (transient {}) ]
    (doseq [pos (range 1 (inc (.getColumnCount rsmeta)))
            :let [cn (.getColumnName rsmeta (int pos))
                  ct (.getColumnType rsmeta (int pos))
                  cv (readOneCol ct (int pos) rs) ]]
      (var-set row (finj @row cn ct cv)))
    (persistent! @row)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- insert? ""

  [^String sql]

  (.startsWith (lcase (strim sql)) "insert"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- setBindVar ""

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
    (CharsClass) (.setString ps pos (String. ^chars p))
    (BytesClass) (.setBytes ps pos ^bytes p)
    XData (.setBinaryStream ps pos (.stream ^XData p))
    Boolean (.setInt ps pos (if p 1 0))
    Double (.setDouble ps pos p)
    Float (.setFloat ps pos p)
    Timestamp (.setTimestamp ps pos p (GmtCal))
    Date (.setDate ps pos p (GmtCal))
    Calendar (.setTimestamp ps pos
                            (Timestamp. (.getTimeInMillis ^Calendar p))
                            (GmtCal))
    (DbioError (str "Unsupported param type: " (type p)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mssqlTweakSqlstr ""

  [^String sqlstr token cmd]

  (loop
    [stop false
     sql sqlstr]
    (if stop
      sql
      (let [pos (.indexOf (lcase sql)
                          (sname token))
            rc (if (< pos 0)
                 []
                 [(.substring sql 0 pos)
                  (.substring sql pos)]) ]
        (if (empty? rc)
          (recur true sql)
          (recur false (str (first rc)
                            " WITH ("
                            cmd
                            ") " (last rc)) ))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- jiggleSQL ""

  ^String
  [^DBAPI db ^String sqlstr]

  (let [sql (strim sqlstr)
        lcs (lcase sql)
        v (.vendor db)   ]
    (if (= :sqlserver (:id v))
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
(defn- buildStmt ""

  ^PreparedStatement
  [db ^Connection conn sqlstr params]

  (let [sql (jiggleSQL db sqlstr)
        ps (if (insert? sql)
             (.prepareStatement conn
                                sql
                                Statement/RETURN_GENERATED_KEYS)
             (.prepareStatement conn sql)) ]
    (log/debug "Building SQLStmt: %s" sql)
    (doseq [n (range 0 (count params)) ]
      (setBindVar ps (inc n) (nth params n)))
    ps))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- handleGKeys ""

  [^ResultSet rs cnt options]

  (let [rc (if (== cnt 1)
             (.getObject rs 1)
             (.getLong rs (str (:pkey options)))) ]
    {:1 rc}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExecWithOutput ""

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
(defn- sqlSelect+ ""

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
(defn- sqlSelect ""

  [db conn sql pms ]

  (sqlSelect+ db conn sql pms
              (partial row2Obj stdInjtor) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sqlExec ""

  [db conn sql pms]

  (with-open [stmt (buildStmt db conn sql pms) ]
    (.executeUpdate stmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- insertFlds

  "Format sql string for insert"

  [sb1 sb2 obj flds]

  (with-local-vars
    [ps (transient []) ]
    (doseq [[k v] obj
            :let [fdef (flds k) ]]
      (when (and (some? fdef)
                 (not (:auto fdef))
                 (not (:system fdef)))
        (AddDelim! sb1 "," (ese (Colname fdef)))
        (AddDelim! sb2 "," (if (nil? v) "NULL" "?"))
        (when (some? v)
          (var-set ps (conj! @ps v)))))
    (persistent! @ps)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- updateFlds

  "Format sql string for update"

  [^StringBuilder sb1 obj flds]

  (with-local-vars
    [ps (transient []) ]
    (doseq [[k v]  obj
            :let [fdef (flds k) ]]
      (when (and (some? fdef)
                 (:updatable fdef)
                 (not (:auto fdef))
                 (not (:system fdef)) )
        (doto sb1
          (AddDelim! "," (ese (Colname fdef)))
          (.append (if (nil? v) "=NULL" "=?")))
        (when (some? v)
          (var-set ps (conj! @ps v)))))
    (persistent! @ps)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- postFmtModelRow ""

  [model obj]

  (let [mm {:typeid model
            :verid (:verid obj)
            :rowid (:rowid obj)
            :last-modify (:last-modify obj) }]
    (with-meta (-> obj
                   (DbioClrFld :rowid)
                   (DbioClrFld :verid)
                   (DbioClrFld :last-modify)) mm)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExecWithOutput ""

  [db metas conn sql pms options]

  (sqlExecWithOutput db conn sql pms options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExec ""

  [db metas conn sql pms]

  (sqlExec db conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery+ ""

  [db metas conn sql pms model]

  (let [mcz (metas model) ]
    (when (nil? mcz)
          (DbioError (str "Unknown model " model)))
    (sqlSelect+ db conn sql pms
                (partial row2Obj
                         (partial modelInjtor mcz))
                #(postFmtModelRow model %))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doQuery ""

  [db metas conn sql pms]

  (sqlSelect db conn sql pms ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doCount ""

  [db metas conn model]

  (let
    [rc (doQuery db
                 metas
                 conn
                 (str "SELECT COUNT(*) FROM "
                      (ese (Tablename model metas)))
                 [])]
    (if (empty? rc)
      0
      (last (first (seq (first rc)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doPurge ""

  [db metas conn model]

  (let [sql (str "DELETE FROM "
                 (ese (Tablename model metas))) ]
    (sqlExec db conn sql [])
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doDelete ""

  [^DBAPI db metas conn obj]

  (let [info (meta obj)
        model (:typeid info)
        mcz (metas model) ]
    (when (nil? mcz)
      (DbioError (str "Unknown model " model)))
    (let [lock (.supportsLock db)
          table (Tablename mcz)
          rowid (:rowid info)
          verid (:verid info)
          p (if lock [rowid verid] [rowid] )
          w (fmtUpdateWhere lock mcz)
          cnt (doExec db
                      metas
                      conn
                      (str "DELETE FROM "
                           (ese table)
                           " WHERE " w) p) ]
      (when lock (lockError? "delete" cnt table rowid))
      cnt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doInsert ""

  [^DBAPI db metas conn obj]

  (let [info (meta obj)
        model (:typeid info)
        mcz (metas model) ]
    (when (nil? mcz)
      (DbioError (str "Unknown model " model)))
    (let [pkey {:pkey (Colname :rowid mcz)}
          lock (.supportsLock db)
          flds (:fields (meta mcz))
          table (Tablename mcz)
          s2 (StringBuilder.)
          s1 (StringBuilder.)
          now (NowJTstamp)
          pms (insertFlds s1 s2 obj flds) ]
      (when (> (.length s1) 0)
        (let [out (doExecWithOutput db
                                    metas
                                    conn
                                    (str "INSERT INTO "
                                         (ese table)
                                         "(" s1
                                         ") VALUES (" s2
                                         ")" )
                                    pms
                                    pkey)]
          (if (empty? out)
            (DbioError (str "Insert requires row-id to be returned."))
            (log/debug "Exec-with-out %s" out))
          (let [wm {:rowid (:1 out) :verid 0} ]
            (when-not (number? (:rowid wm))
                    (DbioError (str "RowID data-type must be Long.")))
            (vary-meta obj MergeMeta wm)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doUpdate ""

  [^DBAPI db metas conn obj]

  (let [info (meta obj)
        model (:typeid info)
        mcz (metas model) ]
    (when (nil? mcz)
      (DbioError (str "Unknown model " model)))
    (let [lock (.supportsLock db)
          flds (:fields (meta mcz))
          cver (nnz (:verid info))
          table (Tablename mcz)
          rowid (:rowid info)
          sb1 (StringBuilder.)
          now (NowJTstamp)
          nver (inc cver)
          pms (updateFlds sb1 obj flds) ]
      (when (> (.length sb1) 0)
        (with-local-vars
          [ ps (transient pms) ]
          (-> (AddDelim! sb1
                         ","
                         (ese (Colname :last-modify mcz)))
              (.append "=?"))
          (var-set ps (conj! @ps now))
          (when lock ;; up the version
            (-> (AddDelim! sb1
                           ","
                           (ese (Colname :verid mcz)))
                (.append "=?"))
            (var-set ps (conj! @ps nver)))
          ;; for the where clause
          (var-set ps (conj! @ps rowid))
          (when lock (var-set  ps (conj! @ps cver)))
          (let [cnt (doExec db
                            metas
                            conn
                            (str "UPDATE "
                                 (ese table)
                                 " SET "
                                 sb1
                                 " WHERE "
                                 (fmtUpdateWhere lock mcz))
                            (persistent! @ps)) ]
            (when lock (lockError? "update" cnt table rowid))
            (vary-meta obj MergeMeta
                       { :verid nver :last-modify now })))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- doExtraSQL ""

  ^String
  [^String sql extra]

  sql)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ReifySQLr

  "Create a SQLr"

  ^SQLr
  [^DBAPI db getc runc]

  {:pre [(fn? getc) (fn? runc)]}

  (let
    [metaz (-> db
               (.getMetaCache)
               (.getMetas))]
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
                           (GTable mcz))
                    [wc pms]
                    (SqlFilterClause mcz filters) ]
                   (if (hgl? wc)
                     (doQuery+ db metaz %1
                               (doExtraSQL (str s " WHERE " wc)
                                           extraSQL)
                               pms model)
                     (doQuery+ db metaz %1
                               (doExtraSQL s extraSQL) [] model)))]
          (runc (getc db) func)))

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

