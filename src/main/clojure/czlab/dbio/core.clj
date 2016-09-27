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

(ns ^{:doc "Database and modeling functions."
      :author "Kenneth Leung" }

  czlab.dbio.core

  (:require
    [czlab.xlib.format :refer [writeEdnString]]
    [czlab.xlib.logging :as log]
    [clojure.string :as cs]
    [clojure.set :as cset]
    [czlab.xlib.meta :refer [forname]])

  (:use [flatland.ordered.set]
        [czlab.xlib.core]
        [czlab.xlib.str])

  (:import
    [com.zaxxer.hikari HikariConfig HikariDataSource]
    [clojure.lang
     Keyword
     APersistentMap
     APersistentVector]
    [java.util
     HashMap
     TimeZone
     Properties
     GregorianCalendar]
    [czlab.dbio
     DBAPI
     Schema
     DBIOError
     SQLr
     JDBCPool
     JDBCInfo]
    [java.sql
     SQLException
     Connection
     Driver
     DriverManager
     DatabaseMetaData]
    [java.lang Math]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;(def ^String COL_LASTCHANGED "DBIO_LASTCHANGED")
;;(def ^String COL_CREATED_ON "DBIO_CREATED_ON")
;;(def ^String COL_CREATED_BY "DBIO_CREATED_BY")
;;(def ^String COL_LHS_TYPEID "DBIO_LHS_TYPEID")
;;(def ^String COL_RHS_TYPEID "DBIO_RHS_TYPEID")
(def ^String COL_LHS_ROWID "DBIO_LHS_ROWID")
(def ^String COL_RHS_ROWID "DBIO_RHS_ROWID")
(def ^String COL_ROWID "DBIO_ROWID")
;;(def ^String COL_VERID "DBIO_VERID")
(def DDL_SEP #"-- :")

(def ^:dynamic *DDL_CFG* nil)
(def ^:dynamic *DDL_BVS* nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro now<ts>

  "A java sql Timestamp"
  []

  `(java.sql.Timestamp. (.getTime (java.util.Date.))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cleanName

  ""
  [s]

  (-> (cs/replace (name s) #"[^a-zA-Z0-9_-]" "")
      (cs/replace  #"-" "_")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro set-oid

  ""
  {:no-doc true}
  [obj pkeyValue]

  `(let [o# ~obj
         pk# (:pkey (gmodel o#))]
     (assoc o# pk# ~pkeyValue)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro gmodel

  ""
  {:no-doc true}
  [obj]

  `(:model (meta ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro gtype

  ""
  {:no-doc true}
  [obj]

  `(:id (gmodel ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro goid

  ""
  {:no-doc true}
  [obj]

  `(let [o# ~obj
         pk# (:pkey (gmodel o#))]
     (pk# o#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gschema

  ""
  {:no-doc true
   :tag Schema}
  [model]

  (:schema (meta model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti fmtSQLId
  "Format SQL identifier"
  {:tag String} (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLId

  APersistentMap

  ([info idstr] (fmtSQLId info idstr nil))
  ([info idstr quote?]
   (let [ch (strim (:qstr info))
         id (cond
              (:ucs? info)
              (ucase idstr)
              (:lcs? info)
              (lcase idstr)
              :else idstr)]
     (if (false? quote?)
       id
       (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLId

  DBAPI

  ([^DBAPI db idstr] (fmtSQLId db idstr nil))
  ([^DBAPI db idstr quote?]
   (fmtSQLId (.vendor db) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLId

  DatabaseMetaData

  ([^DatabaseMetaData mt idstr] (fmtSQLId mt idstr nil))
  ([^DatabaseMetaData mt idstr quote?]
   (let [ch (strim (.getIdentifierQuoteString mt))
         id (cond
              (.storesUpperCaseIdentifiers mt)
              (ucase idstr)
              (.storesLowerCaseIdentifiers mt)
              (lcase idstr)
              :else idstr)]
     (if (false? quote?)
       id
       (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLId

  Connection

  ([^Connection conn idstr] (fmtSQLId conn idstr nil))
  ([^Connection conn idstr quote?]
   (fmtSQLId (.getMetaData conn) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; have to be function , not macro as this is passed into another higher
;; function - merge.
(defn- mergeMeta

  "Merge 2 meta maps"
  ^APersistentMap
  [m1 m2]
  {:pre [(map? m1) (or (nil? m2)(map? m2))]}

  (merge m1 m2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbtag

  "The id for this model"
  {:tag Keyword}

  ([model] (:id model))
  ([typeid schema]
   {:pre [(some? schema)]}
   (dbtag (.get ^Schema schema typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbtable

  "The table-name defined for this model"
  ^String

  ([model] (:table model))
  ([typeid schema]
   {:pre [(some? schema)]}
   (dbtable (.get ^Schema schema typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbcol

  "The column-name defined for this field"
  {:tag String}

  ([fdef] (:column fdef))
  ([fid model]
   {:pre [(map? model)]}
   (dbcol (-> (:fields model)
              (get fid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbspec<>

  "Basic jdbc parameters"
  ^JDBCInfo
  [cfg]
  {:pre [(map? cfg)]}

  (let [id (juid)]
    (reify JDBCInfo

      (url [_] (or (:server cfg) (:url cfg)))
      (id [_]  (or (:id cfg) id))
      (loadDriver [this]
        (if-some [s (.url this)]
          (if (hgl? s)
            (DriverManager/getDriver s))))
      (driver [_] (:driver cfg))
      (user [_] (:user cfg))
      (passwd [_] (str (:passwd cfg))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def Postgresql :postgresql)
(def Postgres :postgres)
(def SQLServer :sqlserver)
(def Oracle :oracle)
(def MySQL :mysql)
(def H2 :h2)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def ^:dynamic *DBTYPES*
  {SQLServer {:test-string "select count(*) from sysusers" }
   Postgresql {:test-string "select 1" }
   Postgres {:test-string "select 1" }
   MySQL {:test-string "select version()" }
   H2 {:test-string "select 1" }
   Oracle {:test-string "select 1 from DUAL" } })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dberr!

  "Throw a DBIOError execption"
  [fmt & more]

  (trap! DBIOError (str (apply format fmt more))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeGetVendor

  "Try to detect the database vendor"
  [product]

  (let [fc #(embeds? %2 %1)
        lp (lcase product)]
    (condp fc lp
      "microsoft" SQLServer
      "postgres" Postgresql
      "oracle" Oracle
      "mysql" MySQL
      "h2" H2
      (dberr! "Unknown db product: %s" product))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private fmtfkey

  "For o2o & o2m relations"
  [tn rn]

  `(keyword (str "fk_" (name ~tn) "_" (name ~rn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn matchSpec

  "Ensure the database type is supported"
  ^Keyword
  [^String spec]

  (let [kw (keyword (lcase spec))]
    (if (contains? *DBTYPES* kw)
      kw)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn matchUrl

  "From the jdbc url, get the database type"
  ^Keyword
  [^String url]

  (let [ss (.split url ":")]
    (if
      (> (alength ss) 1)
      (matchSpec (aget ss 1)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA MODELING
;;
;;(def JOINED-MODEL-MONIKER ::DBIOJoinedModel)
;;(def BASEMODEL-MONIKER ::DBIOBaseModel)
(def ^:private PKEY-DEF
  {:column COL_ROWID
   :domain :Long
   :id :rowid
   :auto true
   :system true
   :updatable false})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbobject<>

  "Define a generic model"
  ^APersistentMap
  [^String nm]

  {:table (cleanName nm)
   :id (asFQKeyword nm)
   :abstract false
   :system false
   :mxm false
   :pkey :rowid
   :indexes {}
   :uniques {}
   :rels {}
   :fields {:rowid PKEY-DEF} })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro dbmodel<>

  "Define a data model"
  [modelname & body]

  `(-> (dbobject<> ~(name modelname)) ~@body))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withJoined

  "A special model with 2 relations,
   left hand side and right hand side. *internal only*"
  {:tag APersistentMap
   :no-doc true}
  [pojo lhs rhs]
  {:pre [(map? pojo)
         (keyword? lhs) (keyword? rhs)]}

  (let [a2 {:lhs {:kind :MXM
                  :other lhs
                  :fkey :lhs-rowid}
            :rhs {:kind :MXM
                  :other rhs
                  :fkey :rhs-rowid} }]
    (-> pojo
        (assoc :rels a2)
        (assoc :mxm true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro dbjoined<>

  "Define a joined data model"
  [modelname lhs rhs]
  {:pre [(symbol? lhs)(symbol? rhs)]}

  `(-> (dbobject<> ~(name modelname))
       (dbfields
         {:lhs-rowid {:column COL_LHS_ROWID
                      :domain :Long
                      :null false}
          :rhs-rowid {:column COL_RHS_ROWID
                      :domain :Long
                      :null false} })
       (dbuniques
         {:i1 #{ :lhs-rowid :rhs-rowid }})
       (withJoined
         (asFQKeyword ~(name lhs))
         (asFQKeyword ~(name rhs)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withTable

  "Set the table name"
  ^APersistentMap
  [pojo table]
  {:pre [(map? pojo)]}

  (assoc pojo :table (cleanName table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- withXXXSets

  ""
  [pojo kvs fld]

  ;;merge new stuff onto old stuff
  (->>
    (persistent!
      (reduce
        #(assoc! %1
                 (first %2)
                 (into (ordered-set) (last %2)))
        (transient {})
        kvs))
    #(merge (%2 %1) )
    (interject pojo fld )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbindexes

  "Set indexes to the model"
  ^APersistentMap
  [pojo indexes]
  {:pre [(map? pojo) (map? indexes)]}

  ;;indices = { :a #{ :f1 :f2} ] :b #{:f3 :f4} }
  (withXXXSets pojo indexes :indexes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbkey

  "Declare your own primary key"
  [pojo pke]
  {:pre [(map? pojo)(map? pke)]}

  (let
    [m (select-keys pke [:domain
                         :column
                         :size
                         :auto])
     fs (:fields pojo)
     pk (:pkey pojo)
     p (pk fs)]
    (assert (some? (:column m)))
    (assert (some? (:domain m)))
    (assert (some? p))
    (->>
      (-> (if-not (:auto m)
            (dissoc p :auto)
            (assoc p :auto true))
          (assoc :size (or (:size m) 255))
          (assoc :domain (:domain m))
          (assoc :column (:column m)))
      (assoc fs pk)
      (assoc pojo :fields))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbuniques

  "Set uniques to the model"
  ^APersistentMap
  [pojo uniqs]
  {:pre [(map? pojo) (map? uniqs)]}

  ;;uniques = { :a #{ :f1 :f2 } :b #{ :f3 :f4 } }
  (withXXXSets pojo uniqs :uniques))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dftFld<>

  "The base field structure"
  ^APersistentMap
  [fid]

  {:column (cleanName fid)
   :id (keyword fid)
   :domain :String
   :size 255
   :rel-key false
   :null true
   :auto false
   :dft nil
   :updatable true
   :system false })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbfield

  "Add a new field"
  ^APersistentMap
  [pojo fid fdef]
  {:pre [(map? pojo) (map? fdef)]}

  (let [fd (merge (dftFld<> fid) fdef)
        k (:id fd)]
    (interject pojo :fields #(assoc (%2 %1) k fd))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbfields

  "Add a bunch of fields"
  ^APersistentMap
  [pojo flddefs]
  {:pre [(map? pojo) (map? flddefs)]}

  (reduce #(let [[k v] %2] (dbfield %1 k v)) pojo flddefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbassoc

  "Declare an association between 2 types"
  [pojo rid rel]

  (let
    [rd (merge {:cascade false
                :fkey nil} rel)
     r2 (case (:kind rd)
          (:O2O :O2M)
          (merge rd {:fkey (fmtfkey (:id pojo) rid) })
          (dberr! "Invalid relation: %s" rid))]
    (interject pojo :rels #(assoc (%2 %1) rid r2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbassocs

  "Declare a set of associations"
  [pojo reldefs]
  {:pre [(map? pojo) (map? reldefs)]}

  (reduce #(let [[k v] %2] (dbassoc %1 k v)) pojo reldefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private with-abstract

  ""
  [pojo flag]

  `(assoc ~pojo :abstract ~flag))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private withDBSystem

  ""
  [pojo]

  `(assoc ~pojo :system true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the base model here
(comment
(dbmodel<> DBIOBaseModel
  (with-abstract true)
  (withDBSystem)
  (dbfields {:rowid PKEY-DEF })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- checkField?

  ""
  [pojo fld]

  (boolean
    (if-some [f (-> (gmodel pojo)
                    (:fields )
                    (get fld))]
        (not (or (:auto f)
                 (not (:updatable f)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(comment
(dbmodel<> DBIOJoinedModel
  (with-abstract true)
  (withDBSystem)
  (dbfields
    {:lhs-rowid {:column COL_LHS_ROWID
                 :domain :Long
                 :null false}
     :rhs-rowid {:column COL_RHS_ROWID
                 :domain :Long
                 :null false} })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private mkfkdef<>

  ""
  [fid ktype]

  `(merge (dftFld<> ~fid)
          {:rel-key true :domain ~ktype }))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolveAssocs

  "Walk through all models, for each model, study its relations.
  For o2o or o2m assocs, we need to artificially inject a new
  field/column into the (other/rhs) model (foreign key)"
  [metas]

  ;; 1st, create placeholder maps for each model,
  ;; to hold new fields from rels
  (with-local-vars
    [phd (transient (zipmap (keys metas) (repeat {})))
     xs (transient {})]
    ;; as we find new relation fields,
    ;; add them to the placeholders
    (doseq [[_ m] metas
            :let [pkey (:pkey m)
                  rs (:rels m)
                  kt (:domain (pkey (:fields m)))]
            :when (and (not (:abstract m))
                       (not (empty? rs)))]
      (doseq [[_ r] rs
              :let [{:keys [other
                            kind
                            fkey]} r]
              :when (or (= :O2O kind)
                        (= :O2M kind))]
        (var-set
          phd
          (assoc! @phd
                  other
                  (->> (mkfkdef<> fkey kt)
                       (assoc (@phd other) fkey))))))
    ;; now walk through all the placeholder maps and merge those new
    ;; fields to the actual models
    (doseq [[k v] (persistent! @phd)
            :let [mcz (metas k)]]
      (->> (assoc! @xs
                   k
                   (assoc mcz
                          :fields
                          (merge (:fields mcz) v)))
           (var-set xs )))
    (persistent! @xs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolveMXMs

  ""
  [metas]

  (with-local-vars [mms (transient {})]
    (doseq [[k m] metas
            :let [{:keys [fields
                          rels]} m
                  fs (:fields m)
                  rs (:rels m)]
            :when (:mxm m)]
      (->>
        (persistent!
          (reduce
            #(let
               [[side kee] %2
                other (get-in rs [side :other])
                mz (metas other)
                pke ((:fields mz) (:pkey mz))
                d (merge (kee fs)
                         (select-keys pke
                                      [:domain :size]))]
               (assoc! %1 kee d))
            (transient {})
            [[:lhs :lhs-rowid]
             [:rhs :rhs-rowid]]))
        (merge fs)
        (assoc m :fields )
        (assoc! @mms k )
        (var-set mms )))
    (merge metas (persistent! @mms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- colmapFields

  "Create a map of fields keyed by the column name"
  [flds]

  (pcoll!
    (reduce
      #(let [[_ v] %2]
         (assoc! %1 (ucase (:column v)) v))
      (transient {})
      flds)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- metaModels

  "Inject extra meta-data properties into each model.  Each model will have
   its (complete) set of fields keyed by column nam or field id"
  [metas schema]

  (pcoll!
    (reduce
      #(let [[k m] %2]
         (->> [schema (->> (:fields m)
                           (colmapFields ))]
              (zipmap [:schema :columns])
              (with-meta m)
              (assoc! %1 k)))
      (transient {})
      metas)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbschema<>

  "A cache storing meta-data for all models"
  ^Schema
  [& models]

  (let
    [data (atom {})
     sch (reify Schema
           (get [_ id] (@data id))
           (models [_] @data))
     ms (if-not (empty? models)
          (reduce
            #(assoc! %1 (:id %2) %2)
            (transient {})
            models))
     m2 (if (== 0 (count ms))
          {}
          (-> (pcoll! ms)
              (resolveAssocs )
              (resolveMXMs )
              (metaModels sch)))]
    (reset! data m2)
    sch))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbgShowSchema

  ""
  ^String
  [^Schema mc]
  {:pre [(some? mc)]}

  (str
    (reduce
      #(addDelim! %1
                  "\n"
                  (writeEdnString
                    {:TABLE (:table %2)
                     :DEFN %2
                     :META (meta %2)}))
      (strbf<>)
      (vals (.models mc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- safeGetConn

  "Safely connect to database referred by this jdbc"
  ^Connection
  [^JDBCInfo jdbc]

  (let
    [user (.user jdbc)
     dv (.driver jdbc)
     url (.url jdbc)
     d (.loadDriver jdbc)
     p (Properties.)]
    (if (hgl? user)
      (doto p
        (.put "password" (str (.passwd jdbc)))
        (.put "user" user)
        (.put "username" user)))
    (if (nil? d)
      (dberr! "Can't load Jdbc Url: %s" url))
    (if (and (hgl? dv)
             (not= (-> d
                       (.getClass)
                       (.getName)) dv))
      (log/warn "want %s, got %s" dv (class d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbconnect<>

  "Connect to database referred by this jdbc"
  ^Connection
  [^JDBCInfo jdbc]
  {:pre [(some? jdbc)]}

  (let
    [url (.url jdbc)
     conn (if (hgl? (.user jdbc))
            (safeGetConn jdbc)
            (DriverManager/getConnection url))]
    (if (nil? conn)
      (dberr! "Failed to connect: %s" url))
    (doto ^Connection
      conn
      (.setTransactionIsolation
        Connection/TRANSACTION_SERIALIZABLE))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn testConnect?

  "true if able to connect to the database, as a test"
  [jdbc]

  (try
    (.close (dbconnect<> jdbc))
    true
    (catch SQLException _# false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti resolveVendor
  "Find type of database"
  ^APersistentMap (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod resolveVendor

  JDBCInfo
  [jdbc]

  (with-open
    [conn (dbconnect<> jdbc)]
    (resolveVendor conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod resolveVendor

  Connection
  [conn]

  (let
    [md (.getMetaData ^Connection conn)
     rc {:id (maybeGetVendor (.getDatabaseProductName md))
         :qstr (strim (.getIdentifierQuoteString md))
         :version (.getDatabaseProductVersion md)
         :name (.getDatabaseProductName md)
         :url (.getURL md)
         :user (.getUserName md)
         :lcs? (.storesLowerCaseIdentifiers md)
         :ucs? (.storesUpperCaseIdentifiers md)
         :mcs? (.storesMixedCaseIdentifiers md)}]
    (assoc rc :fmtId (partial fmtSQLId rc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti tableExist?
  "Is this table defined in db?" (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?

  JDBCPool
  [^JDBCPool pool ^String table]

  (with-open
    [conn (.nextFree pool) ]
    (tableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?

  JDBCInfo
  [jdbc ^String table]

  (with-open
    [conn (dbconnect<> jdbc)]
    (tableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?

  Connection
  [^Connection conn ^String table]

  (log/debug "testing table: %s" table)
  (try!!
    false
    (let [mt (.getMetaData conn)]
      (with-open
        [res (.getColumns
               mt nil nil
               (fmtSQLId conn table) nil)]
        (and (some? res) (.next res))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti rowExist?
  "Is there any rows in the table?" (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod rowExist?

  JDBCInfo
  [^JDBCInfo jdbc ^String table]

  (with-open
    [conn (dbconnect<> jdbc)]
    (rowExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod rowExist?

  Connection
  [^Connection conn ^String table]

  (try!!
    false
    (let
      [sql (str "select count(*) from "
                (fmtSQLId conn table))]
      (with-open [stmt (.createStatement conn)
                  res (.executeQuery stmt sql)]
        (and (some? res)
             (.next res)
             (> (.getInt res (int 1)) 0))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- loadColumns

  "Read each column's metadata"
  [^DatabaseMetaData mt
   ^String catalog
   ^String schema ^String table]

  (with-local-vars
    [pkeys #{} cms {}]
    (with-open
      [rs (.getPrimaryKeys
            mt catalog schema table)]
      (loop [sum (transient #{})
             more (.next rs)]
        (if-not more
          (var-set pkeys (pcoll! sum))
          (recur
            (conj! sum (.getString rs (int 4)))
            (.next rs)))))
    (with-open
      [rs (.getColumns
            mt catalog schema table "%")]
      (loop [sum (transient {})
             more (.next rs)]
        (if-not more
          (var-set cms (pcoll! sum))
          (let [opt (not= (.getInt rs (int 11))
                          DatabaseMetaData/columnNoNulls)
                n (.getString rs (int 4))
                cn (ucase n)
                ctype (.getInt rs (int 5))]
            (recur
              (assoc! sum
                      (keyword cn)
                      {:column n
                       :sql-type ctype
                       :null opt
                       :pkey (contains? @pkeys n) })
              (.next rs))))))
    (with-meta @cms
               {:supportsGetGeneratedKeys
                (.supportsGetGeneratedKeys mt)
                :primaryKeys
                @pkeys
                :supportsTransactions
                (.supportsTransactions mt) })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn loadTableMeta

  "Fetch metadata of this table from db"
  [^Connection conn ^String table]
  {:pre [(some? conn)]}

  (let [dbv (resolveVendor conn)
        mt (.getMetaData conn)
        catalog nil
        schema (if (= (:id dbv) :oracle) "%" nil)
        tbl (fmtSQLId conn table false)]
    ;; not good, try mixed case... arrrrrrrrrrhhhhhhhhhhhhhh
    ;;rs = m.getTables( catalog, schema, "%", null)
    (loadColumns mt catalog schema tbl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- makePool<>

  ""
  ^JDBCPool
  [^JDBCInfo jdbc ^HikariDataSource impl]

  (let [dbv (resolveVendor jdbc)]
    (test-some "database-vendor" dbv)
    (reify JDBCPool

      (shutdown [_]
        (log/debug "shutting down pool impl: %s" impl)
        (.close impl))

      (dbUrl [_] (.url jdbc))
      (vendor [_] dbv)

      (nextFree [_]
        (try
            (.getConnection impl)
          (catch Throwable e#
            (log/error e# "")
            (dberr! "No free connection")))))))

      ;;Object
      ;;Clojure CLJ-1347
      ;;finalize won't work *correctly* in reified objects - document
      ;;(finalize [this]
        ;;(try!
          ;;(log/debug "DbPool finalize() called.")
          ;;(.shutdown this)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbpool<>

  "Create a db connection pool"
  ^JDBCPool
  [^JDBCInfo jdbc options]

  (let [options (or options {})
        dv (.driver jdbc)
        hc (HikariConfig.) ]

    ;;(log/debug "URL: %s"  (.url jdbc))
    ;;(log/debug "Driver: %s" dv)
    ;;(log/debug "Options: %s" options)

    (if (hgl? dv) (forname dv))
    (doto hc
          (.setPassword (str (.passwd jdbc)))
          (.setUsername (.user jdbc))
          (.setJdbcUrl (.url jdbc)))
    (log/debug "[hikari]\n%s" (.toString hc))
    (makePool<> jdbc (HikariDataSource. hc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeOK?

  ""
  [^String dbn ^Throwable e]

  (let [ee (cast? SQLException (rootCause e))
        ec (some-> ee (.getErrorCode))]
    (if
      (and (embeds? (str dbn) "oracle")
           (some? ec)
           (== 942 ec)
           (== 1418 ec)
           (== 2289 ec)
           (== 0 ec))
      true
      (throw e))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti uploadDdl
  "Upload DDL to DB" (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl

  JDBCPool
  [^JDBCPool pool ^String ddl]

  (with-open
    [conn (.nextFree pool)]
    (uploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl

  JDBCInfo
  [^JDBCInfo jdbc ^String ddl]

  (with-open
    [conn (dbconnect<> jdbc)]
    (uploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl

  Connection
  [^Connection conn ^String ddl]
  {:pre [(some? conn)] }

  (let
    [lines (map #(strim %) (cs/split ddl DDL_SEP))
     dbn (lcase (-> (.getMetaData conn)
                    (.getDatabaseProductName)))]
    (.setAutoCommit conn true)
    (log/debug "\n%s" ddl)
    (doseq [^String s (seq lines)
            :let [ln (strimAny s ";" true)]
            :when (and (hgl? ln)
                       (not= (lcase ln) "go"))]
      (with-open
        [s (.createStatement conn)]
        (try
          (.executeUpdate s ln)
        (catch SQLException e#
          (maybeOK? dbn e#)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbpojo<>

  "Creates a blank object of the given type"
  ^APersistentMap
  [model]
  {:pre [(some? model)]}

  (with-meta {} {:model model} ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro mockPojo<>

  ""
  [obj]

  `(let [o# ~obj
         pk# (:pkey (gmodel o#))]
     (with-meta {pk# (goid o#)} (meta o#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetFld

  "Set value to a field"
  [pojo fld value]
  {:pre [(map? pojo) (keyword? fld)]}

  (if (checkField? pojo fld)
    (assoc pojo fld value)
    pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetFlds*

  "Set many field values such as f1 v1 f2 v2 ... fn vn"
  ^APersistentMap
  [pojo fvs]
  {:pre [(map? fvs)]}

  (reduce
    #(dbSetFld %1
               (first %2)
               (last %2))
    pojo
    fvs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- setMxMFlds*

  ""
  [pojo & fvs]

  (reduce
    #(assoc %1 (first %2) (last %2))
    pojo
    (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbClrFld

  "Remove a field"
  [pojo fld]
  {:pre [(map? pojo) (:keyword? fld)]}

  (if (checkField? pojo fld)
    (dissoc pojo fld)
    pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbGetFld

  "Get value of a field"
  [pojo fld]
  {:pre [(map? pojo) (:keyword? fld)]}

  (get pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbioGetRelation

  "Get the relation definition"
  [model rid kind]

  (if-some
    [r (get (:rels model) rid)]
    (if (= (:kind r) kind) r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- selectSide+

  ""
  [mxm obj]

  (let [rhs (get-in mxm [:rels :rhs])
        lhs (get-in mxm [:rels :lhs])
        t (gtype obj)
        rt (:other rhs)
        lf (:other lhs)]
    (cond
      (= t rt)
      [:rhs-rowid :lhs-rowid lf]
      (= t lf)
      [:lhs-rowid :rhs-rowid rt]
      :else
      (if (some? obj)
        (dberr! "Unknown mxm relation for: %s" t)
        [nil nil nil]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private selectSide

  ""
  [mxm obj]

  `(first (selectSide+ ~mxm  ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; handling assocs
(defn- dbioGetO2X

  ""
  [ctx lhsObj kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        rid (:as ctx)
        mcz (gmodel lhsObj)]
    (if-some [r (dbioGetRelation mcz rid kind)]
      r
      (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbioSetO2X

  ""
  [ctx lhsObj rhsObj kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        mcz (gmodel lhsObj)
        rid (:as ctx)]
    (if-some [r (dbioGetRelation mcz rid kind)]
      (let [fv (goid lhsObj)
            fid (:fkey r)
            y (-> (mockPojo<> rhsObj)
                  (dbSetFld fid fv))
            cnt (.update sqlr y)]
        [ lhsObj (merge rhsObj y) ])
      (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbGetO2M

  "One to many assocs"
  [ctx lhsObj]
  {:pre [(map? ctx)(map? lhsObj)]}

  (if-some
    [r (dbioGetO2X ctx lhsObj :O2M)]
    (-> ^SQLr
        (:with ctx)
        (.findSome (or (:cast ctx)
                       (:other r))
                   {(:fkey r) (goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetO2M

  ""
  [ctx lhsObj rhsObj]
  {:pre [(map? ctx)(map? lhsObj)(map? rhsObj)]}

  (dbioSetO2X ctx lhsObj rhsObj :O2M))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetO2M*

  ""
  ^APersistentVector
  [ctx lhsObj & rhsObjs]

  (pcoll!
    (reduce
      #(conj! %1 (last (dbioSetO2X
                         ctx
                         lhsObj
                         %2
                         :O2M)))
      (transient [])
      rhsObjs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbGetO2O

  "One to one relation"
  ^APersistentMap
  [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]}

  (if-some
    [r (dbioGetO2X ctx lhsObj :O2O)]
    (-> ^SQLr
        (:with ctx)
        (.findOne (or (:cast ctx)
                      (:other r))
                  {(:fkey r) (goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetO2O

  ""
  [ctx lhsObj rhsObj]
  {:pre [(map? ctx) (map? lhsObj) (map? rhsObj)]}

  (dbioSetO2X ctx lhsObj rhsObj :O2O))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbioClrO2X

  ""
  [ctx objA kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        rid (:as ctx)
        mA (gmodel objA)]
    (if-some [r (dbioGetRelation mA rid kind)]
      (let [rt (or (:cast ctx)
                   (:other r))
            mB (.get schema rt)
            tn (dbtable mB)
            cn (dbcol (:fkey r) mB)]
        (.exec
          sqlr
          (if-not (:cascade r)
            (format
              "update %s set %s= null where %s=?"
              (.fmtId sqlr tn)
              (.fmtId sqlr cn)
              (.fmtId sqlr cn))
            (format
              "delete from %s where %s=?"
              (.fmtId sqlr tn)
              (.fmtId sqlr cn)))
          [(goid objA)])
        objA)
      (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbClrO2M

  ""
  [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]}

  (dbioClrO2X ctx lhsObj :O2M))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbClrO2O

  ""
  [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]}

  (dbioClrO2X ctx lhsObj :O2O))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- xrefColType

  ""
  [col]

  (case col
    :rhs-rowid :rhs-typeid
    :lhs-rowid :lhs-typeid
    (dberr! "Invaid column key: %s" col)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetM2M

  "Many to many relations"
  ^APersistentMap
  [ctx objA objB]
  {:pre [(map? ctx) (map? objA) (map? objB)]}

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        jon (:joined ctx)]
    (if-some [mm (.get schema jon)]
      (let [ka (selectSide mm objA)
            kb (selectSide mm objB)]
        (->> (-> (dbpojo<> mm)
                 (setMxMFlds*
                   ka (goid objA)
                   kb (goid objB)))
             (.insert sqlr )))
      (dberr! "Unkown relation: %s" jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbClrM2M

  ""

  ([ctx obj] (dbClrM2M ctx obj nil))
  ([ctx objA objB]
   {:pre [(some? objA)]}
    (let [^SQLr sqlr (:with ctx)
          schema (.metas sqlr)
          jon (:joined ctx)]
      (if-some [mm (.get schema jon)]
        (let [fs (:fields mm)
              ka (selectSide mm objA)
              kb (selectSide mm objB)]
          (if (nil? objB)
            (.exec sqlr
                   (format
                     "delete from %s where %s=?"
                     (.fmtId sqlr (dbtable mm))
                     (.fmtId sqlr (dbcol (fs ka))))
                   [ (goid objA) ])
            (.exec sqlr
                   (format
                     "delete from %s where %s=? and %s=?"
                     (.fmtId sqlr (dbtable mm))
                     (.fmtId sqlr (dbcol (fs ka)))
                     (.fmtId sqlr (dbcol (fs kb))))
                   [ (goid objA) (goid objB) ])))
        (dberr! "Unkown relation: %s" jon)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbGetM2M

  ""
  [ctx obj]
  {:pre [(map? ctx)(map? obj)]}

  (let [^SQLr sqlr (:with ctx)
        RS (.fmtId sqlr "RES")
        MM (.fmtId sqlr "MM")
        schema (.metas sqlr)
        jon (:joined ctx)]
    (if-some [mm (.get schema jon)]
      (let [[ka kb t]
            (selectSide+ mm obj)
            t2 (or (:cast ctx) t)
            fs (:fields mm)
            tm (.get schema t2)]
        (if (nil? tm)
          (dberr! "Unknown model: %s" t2))
        (.select
          sqlr
          t2
          (format
            (str "select distinct %s.* from %s %s "
                 "join %s %s on "
                 "%s.%s=? and %s.%s=%s.%s"),
            RS
            (.fmtId sqlr (dbtable tm))
            RS
            (.fmtId sqlr (dbtable mm))
            MM
            MM (.fmtId sqlr (dbcol (ka fs)))
            MM (.fmtId sqlr (dbcol (kb fs)))
            RS (.fmtId sqlr COL_ROWID))
          [ (goid obj) ]))
      (dberr! "Unknown joined model: %s" jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


