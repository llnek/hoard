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
;; Copyright © 2013-2022, Kenneth Leung. All rights reserved.

(ns czlab.hoard.sql

  "Low level SQL JDBC functions."

  (:require [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.dates :as d]
            [czlab.basal.meta :as m]
            [czlab.basal.util :as u]
            [czlab.basal.io :as i]
            [czlab.basal.core :as c]
            [czlab.hoard.core :as h :refer [SQLr]])

  (:import [java.util Calendar TimeZone GregorianCalendar]
           [java.math BigDecimal BigInteger]
           [java.io File Reader InputStream]
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
(defn- fmt-where

  "Filter on primary key."
  [vendor model]

  (str (->> (:pkey model)
            (h/find-field model)
            h/find-col
            (h/fmt-sqlid vendor)) "=?"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- filter-clause

  "=> [sql-filter-string, values]"
  [vendor model filters]

  ;;returns the where clause and parameters
  (let [{:keys [fields]} model]
    [(c/sreduce<>
       #(let [[k v] %2
              c (-> (fields k)
                    h/find-col
                    (c/stror (c/sname k)))]
          (->> (str (h/fmt-sqlid vendor c)
                    (if (nil? v) " is null " " =? "))
               (c/sbf-join %1 " and "))) filters)
     (c/rnilv (vals filters))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- read-col

  "Read a db column."
  [sqlType pos ^ResultSet rset]

  (let [c (d/gmt<>)
        pos (int pos)]
    (condp == (int sqlType)
      Types/TIMESTAMP (.getTimestamp rset pos c)
      Types/DATE (.getDate rset pos c)
      (let [obj (.getObject rset pos)
            in (c/condp?? instance? obj
                 InputStream obj
                 Blob (.getBinaryStream ^Blob obj)
                 Reader obj
                 Clob (.getCharacterStream ^Clob obj))]
        (condp instance? in
          Reader (c/wo* [^Reader r in] (i/slurpc r))
          InputStream (c/wo* [^InputStream p in] (i/slurpb p)) obj)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- model-injtor

  "Row is a transient object."
  [model row cn ct cv]

  ;;if column is not defined in the model, ignore it
  (let
    [fdef (-> (meta model)
              :columns
              (get (c/ucase cn)))]
    (if (nil? fdef)
      row
      (assoc! row (:id fdef) cv))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- std-injtor

  "Generic resultset, no model defined
   Row is a transient object."
  [row cn ct cv] (assoc! row (keyword cn) cv))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- row->obj

  "Convert a jdbc row into object."
  [finj ^ResultSet rs ^ResultSetMetaData rsmeta]

  (merge (h/dbpojo<>)
         (c/preduce<map>
           #(let [pos (int %2)
                  ct (.getColumnType rsmeta pos)]
              (finj %1
                    (.getColumnName rsmeta pos)
                    ct
                    (read-col ct pos rs)))
           (range 1 (+ 1 (.getColumnCount rsmeta))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- insert?

  [sql] `(cs/starts-with? (c/lcase (c/strim ~sql)) "insert"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- set-bind-var

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
    u/CSCZ (.setString ps pos (String. ^chars p))
    u/BSCZ (.setBytes ps pos ^bytes p)
    Boolean (.setInt ps pos (if p 1 0))
    Double (.setDouble ps pos p)
    Float (.setFloat ps pos p)
    Timestamp (.setTimestamp ps pos p (d/gmt<>))
    Date (.setDate ps pos p (d/gmt<>))
    File (set-bind-var ps pos (io/input-stream p))
    Calendar (.setTimestamp ps pos
                            (Timestamp. (.getTimeInMillis ^Calendar p))
                            (d/gmt<>))
    (h/dberr! "Unsupported param-type: %s." (type p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- mssql-tweak

  [sqlstr token cmd]

  (loop [target (c/sname token)
         start 0 stop? false sql sqlstr]
    (if stop?
      sql
      (let [pos (cs/index-of (c/lcase sql)
                             target start)
            rc (if (number? pos)
                 (c/fmt "%s with (%s) %s"
                        (subs sql 0 pos)
                        cmd (subs sql pos)))]
        (if (nil? rc)
          (recur target 0 true sql)
          (recur target
                 (long (+ pos (count target))) stop? rc))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- jiggle-sql

  ^String [vendor sqlstr]

  (let [sql (c/strim sqlstr)
        lcs (c/lcase sql)]
    (c/stror (if (= h/SQLServer (:id vendor))
               (c/condp?? #(cs/starts-with? %2 %1) lcs
                 "select" (mssql-tweak sql :where "nolock")
                 "update" (mssql-tweak sql :set "rowlock")
                 "delete" (mssql-tweak sql :where "rowlock"))) sql)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- fmt-stmt

  ^PreparedStatement
  [vendor ^Connection conn sqlstr params]

  (let [sql (jiggle-sql vendor sqlstr)]
    (c/do-with
      [ps (if (insert? sql)
            (.prepareStatement
              conn sql Statement/RETURN_GENERATED_KEYS)
            (.prepareStatement conn sql))]
      (c/debug "SQLStmt: %s." sql)
      (doseq [n (range 0 (count params))]
        (set-bind-var ps (+ 1 n) (nth params n))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- handle-gkeys

  [^ResultSet rs cnt args]

  {:1 (if (== cnt 1)
        (.getObject rs 1)
        (.getLong rs (str (:pkey args))))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- exec->output

  [vendor conn sql pms args]

  (c/wo*
    [s (fmt-stmt vendor conn sql pms)]
    (if (pos? (.executeUpdate s))
      (c/wo* [rs (.getGeneratedKeys s)]
        (let [cnt (some-> rs
                          .getMetaData
                          .getColumnCount)]
          (if (and (c/spos? cnt)
                   (.next rs))
            (handle-gkeys rs cnt args)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sqls+

  [vendor conn sql pms func post]

  (c/wo* [s (fmt-stmt vendor
                      conn sql pms)
          rs (.executeQuery s)]
    (let [m (.getMetaData rs)]
      (loop [sum (c/tvec*)
             ok (.next rs)]
        (if-not ok
          (c/persist! sum)
          (recur (conj! sum
                        (post (func rs m)))
                 (.next rs)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sql-select

  [vendor conn sql pms]

  (sqls+ vendor
         conn
         sql pms (partial row->obj std-injtor) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sql-exec

  [vendor conn sql pms]

  (c/wo* [s (fmt-stmt vendor
                      conn sql pms)] (.executeUpdate s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- insert-flds

  "Format sql for insert."
  [vendor obj flds]

  (let [sb2 (c/sbf<>)
        sb1 (c/sbf<>)
        p (c/preduce<vec>
            #(let [[k v] %2
                   {:keys [auto?
                           system?] :as fd}
                   (get flds k)]
               (if-not (and fd
                            (not auto?)
                            (not system?))
                 %1
                 (do (c/sbf-join sb1
                                 "," (h/fmt-sqlid vendor
                                                  (h/find-col fd)))
                     (c/sbf-join sb2
                                 "," (if (nil? v) "null" "?"))
                     (if v (conj! %1 v) %1)))) obj)]
    [(str sb1)(str sb2) p]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- update-flds

  "Format sql for update."
  [vendor obj flds]

  (let [sb1 (c/sbf<>)
        ps (c/preduce<vec>
             #(let [[k v] %2
                    {:keys [auto?
                            system?
                            updatable?] :as fd}
                    (get flds k)]
                (if-not (and fd
                             updatable?
                             (not auto?)
                             (not system?))
                  %1
                  (do (c/sbf-join sb1
                                  "," (h/fmt-sqlid vendor
                                                   (h/find-col fd)))
                      (c/sbf+ sb1 (if (nil? v) "=null" "=?"))
                      (if v (conj! %1 v) %1)))) obj)]
    [(str sb1) ps]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- post-fmt-model-row

  [obj model] `(czlab.hoard.core/bind-model ~obj ~model))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-exec+

  [vendor conn sql pms options]

  (exec->output vendor conn sql pms options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-exec

  ^long [vendor conn sql pms] (sql-exec vendor conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-query+

  [vendor conn sql pms model]

  (sqls+ vendor
         conn
         sql
         pms
         (partial row->obj
                  (partial model-injtor model))
         #(post-fmt-model-row % model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-query

  [vendor conn sql pms] (sql-select vendor conn sql pms))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-count

  [vendor conn model]

  (c/if-some+
    [rc (do-query vendor
                  conn
                  (str "select count(*) from "
                       (h/fmt-sqlid vendor
                                    (h/find-table model))) [])]
    (c/_E (c/_1 (seq (c/_1 rc))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-purge

  [vendor conn model]

  (let [sql (str "delete from "
                 (h/fmt-sqlid vendor
                              (h/find-table model)))]
    (sql-exec vendor conn sql []) 0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-delete

  ^long [vendor conn obj]

  (if-some [mcz (h/gmodel obj)]
    (do-exec vendor
             conn
             (str "delete from "
                  (->> (h/find-table mcz)
                       (h/fmt-sqlid vendor))
                  " where "
                  (fmt-where vendor mcz))
             [(h/goid obj)])
    (h/dberr! "Unknown model for: %s." obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-insert

  ^Object [vendor conn obj]

  (if-some [{:keys [pkey fields]
             :as mcz}
            (h/gmodel obj)]
    (let [[s1 s2 pms]
          (insert-flds vendor obj fields)]
      (if (c/hgl? s1)
        (let [out (do-exec+
                    vendor
                    conn
                    (str "insert into "
                         (->> (h/find-table mcz)
                              (h/fmt-sqlid vendor))
                         " (" s1 ") values (" s2 ")")
                    pms
                    {:pkey (h/find-col (h/find-field mcz pkey))})]
          (if-not (empty? out)
            (c/debug "Exec-with-out %s." out)
            (h/dberr! "rowid must be returned."))
          (let [n (:1 out)]
            (if-not (number? n)
              (h/dberr! "rowid must be a Long."))
            (assoc obj pkey n)))))
    (h/dberr! "Unknown model for: %s." obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-update

  ^long [vendor conn obj]

  (if-some [{:keys [fields] :as mcz} (h/gmodel obj)]
    (let [[sb1 pms]
          (update-flds vendor obj fields)]
      (if-not (c/hgl? sb1)
        0
        (do-exec vendor
                 conn
                 (str "update "
                      (->> (h/find-table mcz)
                           (h/fmt-sqlid vendor))
                      " set " sb1 " where "
                      (fmt-where vendor mcz))
                 (conj pms (h/goid obj)))))
    (h/dberr! "Unknown model for: %s." obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-extra-sql

  ^String [sql extra] sql)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord SQLrImpl []
  SQLr
  (find-some [_ typeid filters]
    (h/find-some _ typeid filters {}))

  (find-all [_ typeid extra]
    (h/find-some _ typeid {} extra))

  (find-all [_ typeid]
    (h/find-all _ typeid {}))

  (find-some [me typeid filters extraSQL]
    (let [{:keys [runc models vendor]} me]
      (if-some [{:keys [table] :as mcz} (models typeid)]
        (runc #(let [s (str "select * from "
                            (h/fmt-sqlid vendor table))
                     [wc pms]
                     (filter-clause vendor mcz filters)]
                 (do-query+ vendor
                            %1
                            (do-extra-sql
                              (if (c/hgl? wc)
                                (str s " where " wc) s)
                              extraSQL)
                            pms mcz)))
        (h/dberr! "Unknown model: %s." typeid))))

  (fmt-id [me s] (h/fmt-sqlid (:vendor me) s))

  (mod-obj [me obj]
    (let [{:keys [runc vendor]} me]
      (runc #(do-update vendor %1 obj))))

  (del-obj [me obj]
    (let [{:keys [runc vendor]} me]
      (runc #(do-delete vendor %1 obj))))

  (add-obj [me obj]
    (let [{:keys [runc vendor]} me]
      (runc #(do-insert vendor %1 obj))))

  (select-sql [me typeid sql params]
    (let [{:keys [runc models vendor]} me]
      (if-some [m (models typeid)]
        (runc #(do-query+ vendor %1 sql params m))
        (h/dberr! "Unknown model: %s." typeid))))

  (select-sql [me sql params]
    (let [{:keys [runc vendor]} me]
      (runc #(do-query vendor %1 sql params))))

  (exec-with-output [me sql pms]
    (let [{:keys [runc ____meta models vendor]} me
          {:keys [col-rowid]} ____meta]
      (runc #(do-exec+ vendor
                       %1
                       sql pms {:pkey col-rowid}))))

  (exec-sql [me sql pms]
    (let [{:keys [runc vendor]} me]
      (runc #(do-exec vendor %1 sql pms))))

  (count-objs [me typeid]
    (let [{:keys [models runc vendor]} me]
      (if-some [m (models typeid)]
        (runc #(do-count vendor %1 m))
        (h/dberr! "Unknown model: %s." typeid))))

  (purge-objs [me typeid]
    (let [{:keys [models runc vendor]} me]
      (if-some [m (models typeid)]
        (runc #(do-purge vendor %1 m))
        (h/dberr! "Unknown model: %s." typeid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn sqlr<>

  "Create a sql."
  {:arglists '([db runc])}
  [db runc]
  {:pre [(fn? runc)]}

  (c/object<> SQLrImpl
              (merge {:runc runc}
                     (select-keys db [:schema :vendor])
                     (select-keys @(:schema db) [:models :____meta]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

