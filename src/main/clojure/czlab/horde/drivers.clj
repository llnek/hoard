;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Utility functions for DDL generation."
      :author "Kenneth Leung"}

  czlab.horde.drivers

  (:require [czlab.basal.log :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.horde.core :as h]
            [czlab.basal.core :as c]
            [czlab.basal.str :as s])

  (:import [clojure.lang Var]
           [java.io File]
           [czlab.basal Stateful]
           [java.sql DriverManager Connection Statement]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gSQLId
  "Format SQL identifier" {:tag String}

  ([idstr] (gSQLId idstr nil))
  ([idstr quote?]
   (let [{:keys [case-fn qstr]}
         h/*ddl-cfg*
         id (case-fn idstr)]
     (if (false? quote?) id (str qstr id qstr)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gtable
  "Get the table name (quoted)" {:tag String}

  ([model] (gtable model nil))
  ([model quote?]
   {:pre [(map? model)]} (gSQLId (:table model) quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gcolumn
  "Get the column name (quoted)" {:tag String}

  ([field] (gcolumn field nil))
  ([field quote?]
   {:pre [(map? field)]} (gSQLId (:column field) quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;(defmacro ^:private gcn "Get column name" [fields fid] `(genCol (get ~fields ~fid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genColDef
  "" ^String [vt db typedef field]

  (let [dft (first (:dft field))]
    (str (c/rvtbl vt :getPad db)
         (c/rvtbl vt :genCol db field)
         (str " " typedef " ")
         (c/rvtbl vt :nullClause db (:null? field))
         (if (s/hgl? dft) (str " default " dft) ""))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genExIndexes
  "external indexes"
  ^String [vt db schema fields model]

  (c/sreduce<>
    (fn [^StringBuilder b [k v]]
      (when-not (empty? v)
        (.append b
          (str "create index "
               (c/rvtbl vt :genIndex db model (name k))
               " on "
               (c/rvtbl vt :genTable db model)
               " ("
               (->> (map #(c/rvtbl vt :genCol db (fields %)) v)
                    (cs/join "," ))
               ") "
               (c/rvtbl vt :genExec db) "\n\n")))
      b)
    (:indexes model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques
  "" ^String [vt db schema fields model]

  (c/sreduce<>
    (fn [b [_ v]]
      (when-not (empty? v)
        (c/addDelim!
          b
          ",\n"
          (str (c/rvtbl vt :getPad db)
               "unique("
               (->> (map #(c/rvtbl vt :genCol db (fields %)) v)
                    (cs/join "," ))
               ")")))
      b)
    (:uniques model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPKey
  "" ^String [vt db model pks]
  (str (c/rvtbl vt :getPad db)
       "primary key("
       (cs/join "," (map #(c/rvtbl vt :genCol db %) pks)) ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody
  "" ^String [vt db schema model]

  (let [fields (:fields model)
        pke (:pkey model)
        bf (s/strbf<>)
        pkeys
        (c/preduce<vec>
          (fn [p [k fld]]
            (->>
              (case (:domain fld)
                :Timestamp (c/rvtbl vt :genTimestamp db fld)
                :Date (c/rvtbl vt :genDate db fld)
                :Calendar (c/rvtbl vt :genCaldr db fld)
                :Boolean (c/rvtbl vt :genBool db fld)
                :Int (if (:auto? fld)
                       (c/rvtbl vt :genAutoInteger db model fld)
                       (c/rvtbl vt :genInteger db fld))
                :Long (if (:auto? fld)
                        (c/rvtbl vt :genAutoLong db model fld)
                        (c/rvtbl vt :genLong db fld))
                :Double (c/rvtbl vt :genDouble db fld)
                :Float (c/rvtbl vt :genFloat db fld)
                (:Password :String)
                (c/rvtbl vt :genString db fld)
                :Bytes (c/rvtbl vt :genBytes db fld)
                (h/dberr! "Unsupported field: %s" fld))
              (c/addDelim! bf ",\n" ))
            (if (= pke
                   (:id fld)) (conj! p fld) p)) fields)]
    (when (> (.length bf) 0)
      (when-not (empty? pkeys)
        (.append bf (str ",\n"
                         (genPKey vt db model pkeys))))
      (let [s (genUniques vt db schema fields model)]
        (when (s/hgl? s)
          (.append bf (str ",\n" s)))))
    [(str bf)
     (genExIndexes vt db schema fields model)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable
  "" ^String [vt db schema model]

  (let [d (genBody vt db schema model)
        b (c/rvtbl vt :genBegin db model)
        e (c/rvtbl vt :genEnd db)]
    (str b
         (first d) e (last d)
         (c/rvtbl vt :genGrant db model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def ^:private ddl-base

  (c/defvtbl*

  :genExec #(str ";\n" (c/rvtbl %1 :genSep %2))

  :getNotNull "not null"
  :getNull "null"

  :nullClause #(if %3
                 (c/rvtbl %1 :getNull %2)
                 (c/rvtbl %1 :getNotNull %2))

  :genSep (fn [_ _]
            (if (:use-sep? h/*ddl-cfg*) h/ddl-sep ""))

  :genDrop
  #(str "drop table "
        (h/gtable %3) (c/rvtbl %1 :genExec %2) "\n\n")

  :genBegin
  #(str "create table " (h/gtable %3) " (\n")

  :genEnd
  #(str "\n) " (c/rvtbl %1 :genExec %2) "\n\n")

  :genEndSQL ""
  :genGrant ""

  :genIndex #(h/gSQLId (str (:table %3) "_" %4))

  :genTable #(h/gtable %3)

  :genCol #(h/gcolumn %3)

  :getPad "    "

  ;; data types

  :genBytes
  #(genColDef %1 %2 (c/rvtbl %1 :getBlobKwd %2) %3)

  :genString
  #(genColDef %1 %2
              (str (c/rvtbl %1 :getStringKwd %2)
                   "(" (:size %3) ")") %3)

  :genInteger
  #(genColDef %1 %2 (c/rvtbl %1 :getIntKwd %2) %3)

  :genAutoInteger ""

  :genDouble
  #(genColDef %1 %2 (c/rvtbl %1 :getDoubleKwd %2) %3)

  :genFloat
  #(genColDef %1 %2 (c/rvtbl %1 :getFloatKwd %2) %3)

  :genLong
  #(genColDef %1 %2 (c/rvtbl %1 :getLongKwd %2) %3)

  :genAutoLong ""

  :getTSDefault "CURRENT_TIMESTAMP"

  :genTimestamp
  #(genColDef %1 %2 (c/rvtbl %1 :getTSKwd %2) %3)

  :genDate
  #(genColDef %1 %2 (c/rvtbl %1 :getDateKwd %2) %3)

  :genCaldr
  #(c/rvtbl %1 :genTimestamp %2 %3)

  :genBool
  #(genColDef %1 %2 (c/rvtbl %1 :getBoolKwd %2) %3)

  ;; keywords
  :getDoubleKwd "double precision"
  :getStringKwd "varchar"
  :getFloatKwd "float"
  :getIntKwd "integer"
  :getTSKwd "timestamp"
  :getDateKwd "date"
  :getBoolKwd "integer"
  :getLongKwd "bigint"
  :getBlobKwd "blob"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;H2 database
(def ^:dynamic *h2-mem-url* "jdbc:h2:mem:{{dbid}};DB_CLOSE_DELAY=-1" )
(def ^:dynamic *h2-server-url* "jdbc:h2:tcp://host/path/db" )
(def ^:dynamic *h2-file-url* "jdbc:h2:{{path}};MVCC=TRUE" )
(def ^:dynamic *h2-driver* "org.h2.Driver" )
(def ^:dynamic *h2-mvcc* ";MVCC=TRUE" )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; H2
(def ^:private ddl-h2

  (c/defvtbl** ddl-base

  :id :h2

  :getDateKwd  "timestamp"
  :getDoubleKwd  "double"
  :getBlobKwd  "blob"
  :getFloatKwd   "float"

  :genAutoInteger
  (fn [vt db model field]
    (str (c/rvtbl vt :getPad db)
         (c/rvtbl vt :genCol db field)
         " "
         (c/rvtbl vt :getIntKwd db)
         (if (:pkey field)
           " identity(1) " " auto_increment(1) ")))

  :genAutoLong
  (fn [vt db model field]
    (str (c/rvtbl vt :getPad db)
         (c/rvtbl vt :genCol db field)
         " "
         (c/rvtbl vt :getLongKwd db)
         (if (:pkey field)
           " identity(1) " " auto_increment(1) ")))
  :genBegin
  #(str "create cached table " (h/gtable %3) " (\n" )
  :genDrop
  #(str "drop table "
        (h/gtable %3)
        " if exists cascade"
        (c/rvtbl %1 :genExec %2) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn H2Db
  "Create a H2 database"
  [dbDir ^String dbid ^String user ^String pwd]

  (c/test-some "file-dir" dbDir)
  (c/test-hgl "db-id" dbid)
  (c/test-hgl "user" user)

  (let [url (doto (io/file dbDir dbid) (.mkdirs))
        u (.getCanonicalPath url)
        dbUrl (cs/replace *h2-file-url* "{{path}}" u)]
    (log/debug "Creating H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        ;;(.execute s (str "create user " user " password \"" pwd "\" admin"))
        (.execute s "set default_table_type cached"))
      (with-open [s (.createStatement c1)]
        (.execute s "shutdown")))
    dbUrl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn closeH2Db
  "Close an existing H2 database"
  [dbDir ^String dbid ^String user ^String pwd]

  (c/test-some "file-dir" dbDir)
  (c/test-hgl "db-id" dbid)
  (c/test-hgl "user" user)

  (let [url (io/file dbDir dbid)
        u (.getCanonicalPath url)
        dbUrl (cs/replace *h2-file-url* "{{path}}" u)]
    (log/debug "Closing H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        (.execute s "shutdown")) )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;MySQL
(def ^:dynamic *mysql-driver* "com.mysql.jdbc.Driver")

(def ^:private ddl-mysql
  (c/defvtbl** ddl-base

  :id :mysql

  :getBlobKwd "longblob"
  :getTSKwd "timestamp"
  :getDoubleKwd "double"
  :getFloatKwd "double"
  :genEnd #(str "\n) type=InnoDB"
                (c/rvtbl %1 :genExec %2) "\n\n")
  :genAutoInteger #(str (c/rvtbl %1 :getPad %2)
                        (c/rvtbl %1 :genCol %2 %3)
                        " "
                        (c/rvtbl %1 :getIntKwd %2)
                        " not null auto_increment")
  :genAutoLong #(str (c/rvtbl %1 :getPad %2)
                     (c/rvtbl %1 :genCol %2 %3)
                     " "
                     (c/rvtbl %1 :getLongKwd %2)
                     " not null auto_increment")
  :genDrop #(str "drop table if exists "
                 (h/gtable %3)
                 (c/rvtbl %1 :genExec %2) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;PostgreSQL
(def ^:dynamic *postgresql-url* "jdbc:postgresql://{{host}}:{{port}}/{{db}}")
(def ^:dynamic *postgresql-driver* "org.postgresql.Driver")

(def ^:private ddl-postgres
  (c/defvtbl** ddl-base

  :id :postgres

  :getTSKwd "timestamp with time zone"
  :getBlobKwd "bytea"
  :getDoubleKwd "double precision"
  :getFloatKwd "real"
  :genCaldr #(c/rvtbl %1 :genTimestamp %2 %3)
  :genAutoInteger #(str (c/rvtbl %1 :getPad %2)
                        (c/rvtbl %1 :genCol %2 %3)
                        " serial"
                        " not null auto_increment")
  :genAutoLong #(str (c/rvtbl %1 :getPad %2)
                     (c/rvtbl %1 :genCol %2 %3)
                     " bigserial"
                     " not null auto_increment")
  :genDrop #(str "drop table if exists "
                 (h/gtable %3)
                 " cascade "
                 (c/rvtbl %1 :genExec %2) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQLServer

(def ^:private ddl-sqlserver
  (c/defvtbl** ddl-base

  :id :sqlserver

  :getDoubleKwd "float(53)"
  :getFloatKwd "float(53)"
  :getBlobKwd "image"
  :getTSKwd "datetime"
  :genAutoInteger #(str (c/rvtbl %1 :getPad %2)
                        (c/rvtbl %1 :genCol %2 %3)
                        " "
                        (c/rvtbl %1 :getIntKwd %2)
                        (if (:pkey %3)
                          " identity (1,1) "
                          " autoincrement "))
  :genAutoLong #(str (c/rvtbl %1 :getPad %2)
                     (c/rvtbl %1 :genCol %2 %3)
                     " "
                     (c/rvtbl %1 :getLongKwd %2)
                     (if (:pkey %3)
                       " identity (1,1) "
                       " autoincrement "))
  :genDrop #(str "if exists (select * from "
                 "dbo.sysobjects where id=object_id('"
                 (h/gtable %3 false)
                 "')) drop table "
                 (h/gtable %3)
                 (c/rvtbl %1 :genExec %2) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;Oracle
(defn- createSeq "" [vt db m fd]
  (let [s (h/gSQLId (str "S_"
                       (:table m) "_" (:column fd)))
        t (h/gSQLId (str "T_"
                       (:table m) "_" (:column fd)))]
    (str "create sequence "
         s
         " start with 1 increment by 1"
         (c/rvtbl vt :genExec db)
         "\n\n"
         "create or replace trigger "
         t
         "\n"
         "before insert on "
         (h/gtable m)
         "\n"
         "referencing new as new\n"
         "for each row\n"
         "begin\n"
         "select "
         s
         ".nextval into :new."
         (h/gcolumn fd) " from dual;\n"
         "end" (c/rvtbl vt :genExec db) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeTrackFields
  ""
  [model field]
  (if (or (= "12c+" (h/*ddl-cfg* :db-version))
          (= "12c" (h/*ddl-cfg* :db-version)))
    false
    (let [m (deref h/*ddl-bvs*)
          t (:id model)
          r (or (m t) {})]
      (->> (assoc r (:id field) field)
           (swap! h/*ddl-bvs*  assoc t))
      true)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- autoXXX
  "" [vt db model fld]
  (str (c/rvtbl vt :getPad db)
       (c/rvtbl vt :genCol db fld)
       " "
       "number generated by default on null as identity"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def ^:private ddl-oracle
  (c/defvtbl** ddl-base

  :id :oracle

  :getTSDefault "default systimestamp"
  :getStringKwd "varchar2"
  :getLongKwd "number(38)"
  :getDoubleKwd "binary_double"
  :getFloatKwd "binary_float"
  :genAutoInteger #(if (maybeTrackFields %3 %4)
                     (c/rvtbl %1 :genInteger %2 %4)
                     (c/rvtbl %1 :autoXXX %2 %3 %4))
  :genAutoLong #(if (maybeTrackFields %3 %4)
                  (c/rvtbl %1 :genLong %2 %4)
                  (autoXXX %2 %3 %4))
  :genEndSQL
  #(if (or (= "12c+" (h/*ddl-cfg* :db-version))
           (= "12c" (h/*ddl-cfg* :db-version)))
     ""
     (c/sreduce<>
       (fn [bd [model fields]]
         (reduce
           (fn [bd [_ fld]]
             (.append ^StringBuilder
                      bd
                      (createSeq %2 model fld)))
           bd fields))
       (deref h/*ddl-bvs*)))
  :genDrop #(str "drop table "
                 (h/gtable %3)
                 " cascade constraints purge"
                 (c/rvtbl %1 :genExec %2) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- findVtbl "" [dbID]
  (let [nsp "czlab.horde.drivers/ddl-"
        v (-> (str nsp
                   (name dbID))
              symbol resolve)]
    (.deref ^Var v)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDdl
  "Generate database DDL
  for this schema" {:tag String}

  ([schema db] (getDdl schema db nil))
  ([schema db dbver]
   (binding [h/*ddl-cfg* {:db-version (s/strim dbver)
                          :use-sep? true
                          :qstr ""
                          :case-fn clojure.string/upper-case}
             h/*ddl-bvs* (atom {})]
     (let [ms (:models schema)
           vt (if (keyword? db)
                (c/findVtbl db)
                (do (assert (map? db)) db))
           dbID (:id vt)
           drops (s/strbf<>)
           body (s/strbf<>)]
       (doseq [[id model] ms
               :let [tbl (:table model)]
               :when (and (not (:abstract? model))
                          (s/hgl? tbl))]
         (log/debug "model id: %s, table: %s" (name id) tbl)
         (.append drops (c/rvtbl vt :genDrop dbID model))
         (.append body (genOneTable vt dbID schema model)))
       (str drops body (c/rvtbl vt :genEndSQL dbID))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


