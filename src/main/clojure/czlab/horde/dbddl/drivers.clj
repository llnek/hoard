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

  (:require [czlab.basal.logging :as log]
            [clojure.string :as cs])

  (:use [czlab.horde.core]
        [czlab.basal.core]
        [czlab.basal.str])

  (:import [java.io File]
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
         *ddl-cfg*
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
  "" ^String [me dbtype typedef field]

  (let [dft (first (:dft field))]
    (str (func getPad me dbtype)
         (func genCol me field)
         (str " " typedef " ")
         (func nullClause me dbtype (:null? field))
         (if (hgl? dft) (str " default " dft) ""))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genExIndexes
  "external indexes"
  ^String [me dbtype schema fields model]

  (sreduce<>
    (fn [^StringBuilder b [k v]]
      (when-not (empty? v)
        (.append b
          (str "create index "
               (func genIndex me model (name k))
               " on "
               (func genTable me model)
               " ("
               (->> (map #(func genCol me (fields %)) v)
                    (cs/join "," ))
               ") "
               (func genExec me dbtype) "\n\n")))
      b)
    (:indexes model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques
  "" ^String [me dbtype schema fields model]

  (sreduce<>
    (fn [b [_ v]]
      (when-not (empty? v)
        (addDelim!
          b
          ",\n"
          (str (func getPad me dbtype)
               "unique("
               (->> (map #(func genCol me (fields %)) v)
                    (cs/join "," ))
               ")")))
      b)
    (:uniques model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPKey
  "" ^String [me dbtype model pks]
  (str (func getPad me dbtype)
       "primary key(" (cs/join "," (map #(func genCol me %) pks)) ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody
  "" ^String [me dbtype schema model]

  (let [fields (:fields model)
        pke (:pkey model)
        bf (strbf<>)
        pkeys
        (preduce<vec>
          (fn [p [k fld]]
            (->>
              (case (:domain fld)
                :Timestamp (func genTimestamp me dbtype fld)
                :Date (func genDate me dbtype fld)
                :Calendar (func genCaldr me dbtype fld)
                :Boolean (func genBool me dbtype fld)
                :Int (if (:auto? fld)
                       (func genAutoInteger me dbtype model fld)
                       (func genInteger me dbtype fld))
                :Long (if (:auto? fld)
                        (func genAutoLong me dbtype model fld)
                        (func genLong me dbtype fld))
                :Double (func genDouble me dbtype fld)
                :Float (func genFloat me dbtype fld)
                (:Password :String) (func genString me dbtype fld)
                :Bytes (func genBytes me dbtype fld)
                (dberr! "Unsupported field: %s" fld))
              (addDelim! bf ",\n" ))
            (if (= pke (:id fld)) (conj! p fld) p)) fields)]
    (when (> (.length bf) 0)
      (when-not (empty? pkeys)
        (.append bf (str ",\n"
                         (genPKey me dbtype model pkeys))))
      (let [s (genUniques me dbtype schema fields model)]
        (when (hgl? s)
          (.append bf (str ",\n" s)))))
    [(str bf)
     (genExIndexes me dbtype schema fields model)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable
  "" ^String [me dbtype schema model]

  (let [d (genBody me dbtype schema model)
        b (func genBegin me dbtype model)
        e (func genEnd me dbtype model)]
    (str b (first d) e (last d) (func genGrant me dbtype model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDdl
  "Generate database DDL
  for this schema" {:tag String}

  ([schema dbID] (getDdl schema dbID nil))
  ([schema dbID dbver]
   (binding [*ddl-cfg* {:db-version (strim dbver)
                        :use-sep? true
                        :qstr ""
                        :case-fn clojure.string/upper-case}
             *ddl-bvs* (atom {})]
     (let [ms (:models @schema)
           drops (strbf<>)
           body (strbf<>)]
       (doseq [[id model] ms
               :let [tbl (:table model)]
               :when (and (not (:abstract? model))
                          (hgl? tbl))]
         (log/debug "model id: %s, table: %s" (name id) tbl)
         (.append drops (genDrop dbID model))
         (.append body (genOneTable dbID schema model)))
       (str drops body (genEndSQL dbID))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro func "" [sym me & xs] `(((keyword ~sym) ~me) ~me ~@xs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def ^:private ddl-map {

  :genExec
  (fn [me db] (str ";\n" (func genSep me db)))

  :getNotNull
  (fn [me db] "not null")

  :getNull
  (fn [me db] "null")

  :nullClause
  (fn [md db opt?]
    (if opt?
      (func getNull me db)
      (func getNotNull me db)))

  :genSep
  (fn [me db]
    (if (:use-sep? *ddl-cfg*) ddl-sep ""))

  :genDrop
  (fn [me db m]
    (str "drop table "
         (gtable m) (func genExec me db) "\n\n"))

  :genBegin
  (fn [me db m]
    (str "create table " (gtable m) " (\n"))

  :genEnd
  (fn [me db]
    (str "\n) " (func genExec me db) "\n\n"))

  :genEndSQL (fn [me db] "")
  :genGrant (fn [me db] "")

  :genIndex
  (fn [me db m fd]
    (gSQLId (str (:table m) "_" fd)))

  :genTable (fn [me db m] (gtable m))

  :genCol (fn [me db f] (gcolumn f))

  :getPad (fn [me db] "    ")

  ;; data types

  :genBytes
  (fn [me db f]
    (genColDef me db (getBlobKwd db) f))

  :genString
  (fn [me db fd]
    (genColDef me db
         (str (func getStringKwd me db)
              "(" (:size fd) ")") fd))

  :genInteger
  (fn [me db fd]
    (genColDef me db (func getIntKwd me db) fd))

  :genAutoInteger (fn [me db] "")

  :genDouble
  (fn [me db fd]
    (genColDef me db (func getDoubleKwd me db) fd))

  :genFloat
  (fn [me db fd]
    (genColDef me db (func getFloatKwd me db) fd))

  :genLong
  (fn [me db fd]
    (genColDef me db (func getLongKwd me db) fd))

  :genAutoLong (fn [me db] "")

  :getTSDefault (fn [me db] "CURRENT_TIMESTAMP")

  :genTimestamp
  (fn [me db fd]
    (genColDef me db (func getTSKwd me db) fd))

  :genDate
  (fn [me db fd]
    (genColDef me db (func getDateKwd me db) fd))

  :genCaldr
  (fn [me db fd]
    (func genTimestamp me db fd))

  :genBool
  (fn [me db fd]
    (genColDef me db (func getBoolKwd me db) fd))

  ;; keywords
  :getDoubleKwd (fn [a b] "double precision")
  :getStringKwd (fn [a b] "varchar")
  :getFloatKwd (fn [a b] "float")
  :getIntKwd (fn [a b] "integer")
  :getTSKwd (fn [a b] "timestamp")
  :getDateKwd (fn [a b] "date")
  :getBoolKwd (fn [a b] "integer")
  :getLongKwd (fn [a b] "bigint")
  :getBlobKwd (fn [a b] "blob")})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;H2 database
(def h2-mem-url "jdbc:h2:mem:{{dbid}};DB_CLOSE_DELAY=-1" )
(def h2-server-url "jdbc:h2:tcp://host/path/db" )
(def h2-file-url "jdbc:h2:{{path}};MVCC=TRUE" )
(def h2-driver "org.h2.Driver" )
(def h2-mvcc ";MVCC=TRUE" )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; H2
(def ^:private ddl-h2 {
  :getDateKwd (fn [a b] "timestamp")
  :getDoubleKwd (fn [a b] "double")
  :getBlobKwd (fn [a b] "blob")
  :getFloatKwd  (fn [a b] "float")
  :genAutoInteger
  (fn [me db model field]
    (str (func getPad me db)
         (func genCol me db field)
         " "
         (func getIntKwd me db)
         (if (:pkey field)
           " identity(1) " " auto_increment(1) ")))
  :genAutoLong
  (fn [me db model field]
    (str (func getPad me db)
         (func genCol me db field)
         " "
         (func getLongKwd me db)
         (if (:pkey field)
           " identity(1) " " auto_increment(1) ")))
  :genBegin
  (fn [me db m]
    (str "create cached table " (gtable m) " (\n" ))
  :genDrop
  (fn [me db model]
    (str "drop table "
         (gtable m)
         " if exists cascade"
         (func genExec me db) "\n\n")) })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn H2Db
  "Create a H2 database"
  [dbDir ^String dbid ^String user ^String pwd]

  (test-some "file-dir" dbDir)
  (test-hgl "db-id" dbid)
  (test-hgl "user" user)

  (let [url (doto (io/file dbDir dbid) (.mkdirs))
        u (.getCanonicalPath url)
        dbUrl (cs/replace h2-file-url "{{path}}" u)]
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

  (test-some "file-dir" dbDir)
  (test-hgl "db-id" dbid)
  (test-hgl "user" user)

  (let [url (io/file dbDir dbid)
        u (.getCanonicalPath url)
        dbUrl (cs/replace h2-file-url "{{path}}" u)]
    (log/debug "Closing H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        (.execute s "shutdown")) )))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


