;; Copyright © 2013-2020, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.hoard.drivers

  "Utility functions for DDL generation."

  (:require [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.core :as c]
            [czlab.hoard.core :as h])

  (:import [clojure.lang Var]
           [java.io File]
           [java.sql DriverManager Connection Statement]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gsqlid

  "Format SQL identifier."
  {:tag String}

  ([idstr] (gsqlid idstr nil))

  ([idstr quote?]
   (let [{:keys [case-fn qstr]} h/*ddl-cfg*
         id (case-fn idstr)]
     (if (false? quote?) id (str qstr id qstr)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gtable

  "Get the table name (quoted)."
  {:tag String}

  ([model] (gtable model nil))

  ([model quote?]
   {:pre [(map? model)]} (gsqlid (:table model) quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gcolumn

  "Get the column name (quoted)."
  {:tag String}

  ([field] (gcolumn field nil))

  ([field quote?]
   {:pre [(map? field)]} (gsqlid (:column field) quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gen-col-def

  ^String [vt db typedef field]

  (let [dft (c/_1 (:dft field))]
    (str (c/vt-run?? vt :getPad [db])
         (c/vt-run?? vt :genCol [db field])
         " " typedef " "
         (c/vt-run?? vt :nullClause [db (:null? field)])
         (if (c/hgl? dft) (str " default " dft) ""))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gen-ex-indexes

  "External indexes."
  ^String [vt db schema fields model]

  (c/sreduce<>
    (fn [b [k v]]
      (if (empty? v)
        b
        (c/sbf+ b
                "create index "
                (c/vt-run?? vt :genIndex [db model (name k)])
                " on "
                (c/vt-run?? vt :genTable [db model])
                " ("
                (->> (map #(c/vt-run?? vt :genCol [db (fields %)]) v)
                     (cs/join "," ))
                ") "
                (c/vt-run?? vt :genExec [db]) "\n\n")))
    (:indexes model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gen-uniques

  ^String [vt db schema fields model]

  (c/sreduce<>
    (fn [b [_ v]]
      (if (empty? v)
        b
        (c/sbf-join b
                    ",\n"
                    (str (c/vt-run?? vt :getPad [db])
                         "unique("
                         (cs/join ","
                                  (map #(c/vt-run?? vt
                                                    :genCol [db (fields %)]) v))
                         ")"))))
    (:uniques model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gen-pkey

  ^String [vt db model pks]

  (str (c/vt-run?? vt :getPad [db])
       "primary key("
       (cs/join "," (map #(c/vt-run?? vt :genCol [db %]) pks)) ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gen-body

  ^String [vt db schema model]

  (let [{:keys [fields pkey]} model
        bf (c/sbf<>)
        pkeys
        (c/preduce<vec>
          (fn [p [k fld]]
            (c/sbf-join bf ",\n"
              (case (:domain fld)
                :Timestamp (c/vt-run?? vt :genTimestamp [db fld])
                :Date (c/vt-run?? vt :genDate [db fld])
                :Calendar (c/vt-run?? vt :genCaldr [db fld])
                :Boolean (c/vt-run?? vt :genBool [db fld])
                :Int (if (:auto? fld)
                       (c/vt-run?? vt :genAutoInteger [db model fld])
                       (c/vt-run?? vt :genInteger [db fld]))
                :Long (if (:auto? fld)
                        (c/vt-run?? vt :genAutoLong [db model fld])
                        (c/vt-run?? vt :genLong [db fld]))
                :Double (c/vt-run?? vt :genDouble [db fld])
                :Float (c/vt-run?? vt :genFloat [db fld])
                (:Password :String)
                (c/vt-run?? vt :genString [db fld])
                :Bytes (c/vt-run?? vt :genBytes [db fld])
                (h/dberr! "Unsupported field: %s." fld)))
            (if (not= pkey (:id fld))
              p
              (conj! p fld))) fields)]
    (when (pos? (.length bf))
      (when-not (empty? pkeys)
        (c/sbf+ bf ",\n" (gen-pkey vt db
                                   model pkeys)))
      (let [s (gen-uniques vt db
                           schema fields model)]
        (when (c/hgl? s)
          (c/sbf+ bf ",\n" s))))
    [(str bf)
     (gen-ex-indexes vt db schema fields model)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gen-one-table

  ^String [vt db schema model]

  (let [d (gen-body vt db schema model)
        b (c/vt-run?? vt :genBegin [db model])
        e (c/vt-run?? vt :genEnd [db])]
    (str b
         (c/_1 d) e (c/_E d)
         (c/vt-run?? vt :genGrant [db model]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/def- ddl-base
  (c/vtbl*
    :genExec #(str ";\n" (c/vt-run?? %1 :genSep [%2]))

    :getNotNull "not null"
    :getNull "null"

    :nullClause #(if %3
                   (c/vt-run?? %1 :getNull [%2])
                   (c/vt-run?? %1 :getNotNull [%2]))

    :genSep (c/fn_2
              (if (:use-sep? h/*ddl-cfg*) h/ddl-sep ""))

    :genDrop
    #(str "drop table "
          (gtable %3) (c/vt-run?? %1 :genExec [%2]) "\n\n")

    :genBegin
    #(str "create table " (gtable %3) " (\n")

    :genEnd
    #(str "\n) " (c/vt-run?? %1 :genExec [%2]) "\n\n")

    :genEndSQL ""
    :genGrant ""

    :genIndex #(gsqlid (str (:table %3) "_" %4))

    :genTable #(gtable %3)

    :genCol #(gcolumn %3)

    :getPad "    "

    ;; data types

    :genBytes
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getBlobKwd [%2]) %3)

    :genString
    #(gen-col-def %1 %2
                  (str (c/vt-run?? %1 :getStringKwd [%2])
                       "(" (:size %3) ")") %3)

    :genInteger
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getIntKwd [%2]) %3)

    :genAutoInteger ""

    :genDouble
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getDoubleKwd [%2]) %3)

    :genFloat
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getFloatKwd [%2]) %3)

    :genLong
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getLongKwd [%2]) %3)

    :genAutoLong ""

    :getTSDefault "CURRENT_TIMESTAMP"

    :genTimestamp
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getTSKwd [%2]) %3)

    :genDate
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getDateKwd [%2]) %3)

    :genCaldr
    #(c/vt-run?? %1 :genTimestamp [%2 %3])

    :genBool
    #(gen-col-def %1 %2 (c/vt-run?? %1 :getBoolKwd [%2]) %3)

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
(def h2-mem-url "jdbc:h2:mem:{{dbid}};DB_CLOSE_DELAY=-1")
(def h2-server-url "jdbc:h2:tcp://host/path/db")
;h2-database 1.4.199 works but 1.4.200 fails with MVCC
;(def h2-file-url "jdbc:h2:{{path}};MVCC=TRUE")
(def h2-file-url "jdbc:h2:{{path}}")
(def h2-driver "org.h2.Driver")
(def h2-mvcc ";MVCC=TRUE")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; H2
(c/def- ddl-h2
  (c/vtbl** ddl-base

    :id :h2

    :getDateKwd  "timestamp"
    :getDoubleKwd  "double"
    :getBlobKwd  "blob"
    :getFloatKwd   "float"

    :genAutoInteger
    (fn [vt db model field]
      (str (c/vt-run?? vt :getPad [db])
           (c/vt-run?? vt :genCol [db field])
           " "
           (c/vt-run?? vt :getIntKwd [db])
           (if (:pkey field)
             " identity(1) " " auto_increment(1) ")))

    :genAutoLong
    (fn [vt db model field]
      (str (c/vt-run?? vt :getPad [db])
           (c/vt-run?? vt :genCol [db field])
           " "
           (c/vt-run?? vt :getLongKwd [db])
           (if (:pkey field)
             " identity(1) " " auto_increment(1) ")))

    :genBegin
    #(str "create cached table " (gtable %3) " (\n" )

    :genDrop
    #(str "drop table "
          (gtable %3)
          " if exists cascade"
          (c/vt-run?? %1 :genExec [%2]) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn h2db

  "Create a H2 database"
  {:arglists '([dbDir dbid user pwd])}
  [dbDir ^String dbid ^String user ^String pwd]

  (c/test-some "file-dir" dbDir)
  (c/test-hgl "db-id" dbid)
  (c/test-hgl "user" user)

  (let [url (doto (io/file dbDir dbid) (.mkdirs))
        u (.getCanonicalPath url)
        dbUrl (cs/replace h2-file-url "{{path}}" u)]
    (c/debug "Creating H2: %s." dbUrl)
    (c/wo* [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (c/wo* [s (.createStatement c1)]
        ;;(.execute s (str "create user " user " password \"" pwd "\" admin"))
        (.execute s "set default_table_type cached"))
      (c/wo* [s (.createStatement c1)]
        (.execute s "shutdown")))
    dbUrl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn close-h2db

  "Close an existing H2 database"
  {:arglists '([dbDir dbid user pwd])}
  [dbDir ^String dbid ^String user ^String pwd]

  (c/test-some "file-dir" dbDir)
  (c/test-hgl "db-id" dbid)
  (c/test-hgl "user" user)
  (let [url (io/file dbDir dbid)
        u (.getCanonicalPath url)
        dbUrl (cs/replace h2-file-url "{{path}}" u)]
    (c/debug "Closing H2: %s." dbUrl)
    (c/wo* [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (c/wo* [s (.createStatement c1)] (.execute s "shutdown")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;MySQL
(def mysql-driver "com.mysql.jdbc.Driver")

(c/def- ddl-mysql
  (c/vtbl** ddl-base

    :id :mysql

    :getBlobKwd "longblob"
    :getTSKwd "timestamp"
    :getDoubleKwd "double"
    :getFloatKwd "double"
    :genEnd #(str "\n) type=InnoDB"
                  (c/vt-run?? %1 :genExec [%2]) "\n\n")

    :genAutoInteger #(str (c/vt-run?? %1 :getPad [%2])
                          (c/vt-run?? %1 :genCol [%2 %3])
                          " "
                          (c/vt-run?? %1 :getIntKwd [%2])
                          " not null auto_increment")

    :genAutoLong #(str (c/vt-run?? %1 :getPad [%2])
                       (c/vt-run?? %1 :genCol [%2 %3])
                       " "
                       (c/vt-run?? %1 :getLongKwd [%2])
                       " not null auto_increment")

    :genDrop #(str "drop table if exists "
                   (gtable %3)
                   (c/vt-run?? %1 :genExec [%2]) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;PostgreSQL
(def postgresql-url "jdbc:postgresql://{{host}}:{{port}}/{{db}}")
(def postgresql-driver "org.postgresql.Driver")

(c/def- ddl-postgres
  (c/vtbl** ddl-base

    :id :postgres

    :getTSKwd "timestamp with time zone"
    :getBlobKwd "bytea"
    :getDoubleKwd "double precision"
    :getFloatKwd "real"
    :genCaldr #(c/vt-run?? %1 :genTimestamp [%2 %3])

    :genAutoInteger #(str (c/vt-run?? %1 :getPad [%2])
                          (c/vt-run?? %1 :genCol [%2 %3])
                          " serial"
                          " not null auto_increment")

    :genAutoLong #(str (c/vt-run?? %1 :getPad [%2])
                       (c/vt-run?? %1 :genCol [%2 %3])
                       " bigserial"
                       " not null auto_increment")

    :genDrop #(str "drop table if exists "
                   (gtable %3)
                   " cascade "
                   (c/vt-run?? %1 :genExec [%2]) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQLServer
(c/def- ddl-sqlserver
  (c/vtbl** ddl-base

    :id :sqlserver

    :getDoubleKwd "float(53)"
    :getFloatKwd "float(53)"
    :getBlobKwd "image"
    :getTSKwd "datetime"

    :genAutoInteger #(str (c/vt-run?? %1 :getPad [%2])
                          (c/vt-run?? %1 :genCol [%2 %3])
                          " "
                          (c/vt-run?? %1 :getIntKwd [%2])
                          (if (:pkey %3)
                            " identity (1,1) "
                            " autoincrement "))

    :genAutoLong #(str (c/vt-run?? %1 :getPad [%2])
                       (c/vt-run?? %1 :genCol [%2 %3])
                       " "
                       (c/vt-run?? %1 :getLongKwd [%2])
                       (if (:pkey %3)
                         " identity (1,1) "
                         " autoincrement "))

    :genDrop #(str "if exists (select * from "
                   "dbo.sysobjects where id=object_id('"
                   (gtable %3 false)
                   "')) drop table "
                   (gtable %3)
                   (c/vt-run?? %1 :genExec [%2]) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;Oracle
(defn- create-seq

  [vt db m fd]

  (let [s (gsqlid (str "S_"
                       (:table m) "_" (:column fd)))
        t (gsqlid (str "T_"
                       (:table m) "_" (:column fd)))]

    (str "create sequence "
         s
         " start with 1 increment by 1"
         (c/vt-run?? vt :genExec [db])
         "\n\n"
         "create or replace trigger "
         t
         "\n"
         "before insert on "
         (gtable m)
         "\n"
         "referencing new as new\n"
         "for each row\n"
         "begin\n"
         "select "
         s
         ".nextval into :new."
         (gcolumn fd) " from dual;\n"
         "end" (c/vt-run?? vt :genExec [db]) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- maybe-track-fields

  [model field]

  (if-not (or (.equals "12c+" (h/*ddl-cfg* :db-version))
              (.equals "12c" (h/*ddl-cfg* :db-version)))
    (c/let->true [m (deref h/*ddl-bvs*)
                 t (:id model)
                 r (or (m t) {})]
      (swap! h/*ddl-bvs*
             assoc
             t
             (assoc r (:id field) field)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- auto-xxx

  [vt db model fld]

  (str (c/vt-run?? vt :getPad [db])
       (c/vt-run?? vt :genCol [db fld])
       " "
       "number generated by default on null as identity."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/def- ddl-oracle
  (c/vtbl** ddl-base

    :id :oracle

    :getTSDefault "default systimestamp"
    :getStringKwd "varchar2"
    :getLongKwd "number(38)"
    :getDoubleKwd "binary_double"
    :getFloatKwd "binary_float"

    :genAutoInteger #(if (maybe-track-fields %3 %4)
                       (c/vt-run?? %1 :genInteger [%2 %4])
                       (c/vt-run?? %1 :autoXXX [%2 %3 %4]))

    :genAutoLong #(if (maybe-track-fields %3 %4)
                    (c/vt-run?? %1 :genLong [%2 %4])
                    (auto-xxx %2 %3 %4))

    :genEndSQL
    #(if (or (.equals "12c+" (h/*ddl-cfg* :db-version))
             (.equals "12c" (h/*ddl-cfg* :db-version)))
       ""
       (c/sreduce<>
         (fn [bd [model fields]]
           (reduce
             (fn [bd [_ fld]]
               (c/sbf+ bd
                       (create-seq %2 model fld)))
             bd fields))
         (deref h/*ddl-bvs*)))

    :genDrop #(str "drop table "
                   (gtable %3)
                   " cascade constraints purge"
                   (c/vt-run?? %1 :genExec [%2]) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- find-vtbl

  [dbID]

  (let [nsp "czlab.hoard.drivers/ddl-"
        v (-> (str nsp
                   (name dbID))
              symbol resolve)]
    (.deref ^Var v)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-ddl

  "Generate database DDL for this schema."
  {:tag String
   :arglists '([schema db]
               [schema db dbver])}

  ([schema db]
   (get-ddl schema db nil))

  ([schema db dbver]
   (binding [h/*ddl-cfg* {:db-version (c/strim dbver)
                          :use-sep? true
                          :qstr ""
                          :case-fn clojure.string/upper-case}
             h/*ddl-bvs* (atom {})]
     (let [ms (:models @schema)
           vt (if (keyword? db)
                (find-vtbl db)
                (do (assert (map? db)) db))
           dbID (:id vt)
           drops (c/sbf<>)
           body (c/sbf<>)]
       (doseq [[id model] ms
               :let [tbl (:table model)]
               :when (and (not (:abstract? model))
                          (c/hgl? tbl))]
         (c/debug "model id: %s, table: %s." (name id) tbl)
         (c/sbf+ drops (c/vt-run?? vt :genDrop [dbID model]))
         (c/sbf+ body (gen-one-table vt dbID schema model)))
       (str drops body (c/vt-run?? vt :genEndSQL [dbID]))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

