;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database and modeling functions."
      :author "Kenneth Leung"}

  czlab.horde.dbio.core

  (:require [czlab.basal.format :refer [writeEdnStr]]
            [czlab.basal.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [clojure.set :as cset]
            [czlab.basal.meta :refer [forname]])

  (:use [flatland.ordered.set]
        [czlab.basal.core]
        [czlab.basal.str])

  (:import [java.util HashMap TimeZone Properties GregorianCalendar]
           [clojure.lang Keyword APersistentMap APersistentVector]
           [com.zaxxer.hikari HikariConfig HikariDataSource]
           [czlab.horde
            DbApi
            Schema
            DbioError
            SQLr
            JdbcPool
            JdbcSpec]
           [java.sql
            SQLException
            Connection
            Driver
            DriverManager
            DatabaseMetaData]
           [java.lang Math]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(def ^String col-lhs-rowid "CZLAB_LHS_ROWID")
(def ^String col-rhs-rowid "CZLAB_RHS_ROWID")
(def ^String col-rowid "CZLAB_ROWID")
(def ddl-sep #"-- :")
(def ^:dynamic *ddl-cfg* nil)
(def ^:dynamic *ddl-bvs* nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro tstamp<>
  "Sql timestamp" [] `(java.sql.Timestamp. (.getTime (java.util.Date.))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cleanName
  "" [s] (-> (cs/replace (name s)
                         #"[^a-zA-Z0-9_-]" "")
             (cs/replace  #"-" "_")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro gmodel
  "" {:no-doc true} [obj] `(:$model (meta ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro gtype "Get object's type" [obj] `(:id (gmodel ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro goid
  "Get object's id" [obj] `(let [o# ~obj
                                 pk# (:pkey (gmodel o#))] (pk# o#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gschema "Get schema" ^Schema [model] (:schema (meta model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti fmtSqlId
  "Format SQL identifier" {:tag String} (fn [a & _] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSqlId

  APersistentMap

  ([info idstr] (fmtSqlId info idstr nil))
  ([info idstr quote?]
   (let [ch (strim (:qstr info))
         id (cond
              (:ucs? info)
              (ucase idstr)
              (:lcs? info)
              (lcase idstr)
              :else idstr)]
     (if (false? quote?) id (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSqlId

  DbApi

  ([db idstr] (fmtSqlId db idstr nil))
  ([^DbApi db idstr quote?]
   (fmtSqlId (.vendor db) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSqlId

  DatabaseMetaData

  ([mt idstr] (fmtSqlId mt idstr nil))
  ([^DatabaseMetaData mt idstr quote?]
   (let [ch (strim (.getIdentifierQuoteString mt))
         id (cond
              (.storesUpperCaseIdentifiers mt)
              (ucase idstr)
              (.storesLowerCaseIdentifiers mt)
              (lcase idstr)
              :else idstr)]
     (if (false? quote?) id (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSqlId

  Connection

  ([conn idstr] (fmtSqlId conn idstr nil))
  ([^Connection conn idstr quote?]
   (fmtSqlId (.getMetaData conn) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; have to be function , not macro as this is passed into another higher
;; function - merge.
(defn- mergeMeta
  "Merge 2 meta maps"
  ^APersistentMap
  [m1 m2] {:pre [(map? m1)
                 (or (nil? m2)(map? m2))]} (merge m1 m2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbtag
  "The id for this model" {:tag Keyword}
  ([model] (:id model))
  ([typeid schema]
   {:pre [(ist? Schema schema)]}
   (dbtag (. ^Schema schema get typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbtable
  "The table-name for this model" {:tag String}

  ([model] (:table model))
  ([typeid schema]
   {:pre [(ist? Schema schema)]}
   (dbtable (. ^Schema schema get typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbcol
  "The column-name for this field" {:tag String}

  ([fdef] (:column fdef))
  ([fid model]
   {:pre [(map? model)]}
   (dbcol (get (:fields model) fid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbspec<>
  "Basic jdbc parameters"
  ^JdbcSpec [cfg] {:pre [(map? cfg)]}

  (let [impl (muble<>
               (dissoc cfg
                       :id  :passwd
                       :url :server))
        pwd (str (:passwd cfg))]
    (->> (or (:server cfg)
             (:url cfg))
         (.setv impl :url))
    (->> (or (:id cfg)
             (jid<>))
         (.setv impl :id))
    (if (hgl? pwd)
      (.setv impl :passwd pwd))
    (reify JdbcSpec
      (url [_] (.getv impl :url))
      (id [_]  (.getv impl :id))
      (intern [_] (.intern impl))
      (loadDriver [me]
        (if-some+ [s (.url me)]
          (DriverManager/getDriver s)))
      (driver [_] (.getv impl :driver))
      (user [_] (.getv impl :user))
      (passwd [_] (.getv impl :passwd)))))

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
(def ^:dynamic *db-types*
  {SQLServer {:test-string "select count(*) from sysusers" }
   Postgresql {:test-string "select 1" }
   Postgres {:test-string "select 1" }
   MySQL {:test-string "select version()" }
   H2 {:test-string "select 1" }
   Oracle {:test-string "select 1 from DUAL" } })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dberr!
  "Throw a DbioError execption"
  [fmt & more] (trap! DbioError (str (apply format fmt more))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeGetVendor
  "Detect the database vendor" [product]

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
  [tn rn] `(keyword (str "fk_" (name ~tn) "_" (name ~rn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn matchSpec
  "Ensure db-type is supported" ^Keyword [^String spec]
  (let [kw (keyword (lcase spec))] (if (contains? *db-types* kw) kw)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn matchUrl
  "From jdbc url, get db-type" ^Keyword [dburl]

  (if-some+ [ss (.split (str dburl) ":")]
    (if (> (alength ss) 1) (matchSpec (aget ss 1)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA MODELING
(def ^:private pkey-meta
  {:column col-rowid
   :domain :Long
   :id :rowid
   :auto? true
   :system? true
   :updatable? false})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbdef<>
  "Define a generic model"
  ^APersistentMap [modelName] {:pre [(isFQKeyword? modelName)]}

  {:table (cleanName (name modelName))
   :id modelName
   :abstract? false
   :system? false
   :mxm? false
   :pkey :rowid
   :indexes {}
   :uniques {}
   :rels {}
   :fields {:rowid pkey-meta}})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro dbmodel<>
  "Define a data model"
  [modelname & body] `(-> (dbdef<> ~modelname) ~@body))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withJoined
  "A special model with 2 relations,
   left hand side and right hand side. *internal only*"
  {:tag APersistentMap
   :no-doc true}
  [pojo lhs rhs]
  {:pre [(map? pojo)
         (isFQKeyword? lhs) (isFQKeyword? rhs)]}

  (let [a2 {:lhs {:kind :MXM
                  :other lhs
                  :fkey :lhs-rowid}
            :rhs {:kind :MXM
                  :other rhs
                  :fkey :rhs-rowid} }]
    (-> pojo
        (assoc :rels a2)
        (assoc :mxm? true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro dbjoined<>
  "Define a joined data model" [modelname lhs rhs]

  `(-> (dbdef<> ~modelname)
       (dbfields
         {:lhs-rowid {:column col-lhs-rowid
                      :domain :Long
                      :null? false}
          :rhs-rowid {:column col-rhs-rowid
                      :domain :Long
                      :null? false} })
       (dbuniques
         {:i1 #{ :lhs-rowid :rhs-rowid }})
       (withJoined ~lhs ~rhs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withTable
  "Set the table name"
  ^APersistentMap [pojo table]
  {:pre [(map? pojo)]} (assoc pojo :table (cleanName table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;merge new stuff onto old stuff
(defn- withXXXSets "" [pojo kvs fld]
  (->> (preduce<map>
         #(assoc! %1
                  (first %2)
                  (into (ordered-set) (last %2))) kvs)
       (update-in pojo [fld] merge )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;indices = { :a #{ :f1 :f2} ] :b #{:f3 :f4} }
(defn dbindexes
  "Set indexes to the model"
  ^APersistentMap [pojo indexes]
  {:pre [(map? pojo) (map? indexes)]} (withXXXSets pojo indexes :indexes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbkey
  "Declare your own primary key"
  [pojo pke] {:pre [(map? pojo)(map? pke)]}

  (let [{:keys [domain id column size auto?]}
        pke
        {:keys [fields pkey]}
        pojo
        p (pkey fields)
        oid (or id pkey)
        fields (dissoc fields pkey)]
    (assert (some? column))
    (assert (some? domain))
    (assert (some? p))
    (assert (= pkey (:id p)))
    (->
      (->>
        (-> (if-not auto?
              (dissoc p :auto?)
              (assoc p :auto? true))
            (assoc :size (or size 255))
            (assoc :id oid)
            (assoc :domain domain)
            (assoc :column column))
        (assoc fields oid)
        (assoc pojo :fields))
      (assoc :pkey oid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;uniques = { :a #{ :f1 :f2 } :b #{ :f3 :f4 } }
(defn dbuniques
  "Set uniques to the model"
  ^APersistentMap [pojo uniqs]
  {:pre [(map? pojo) (map? uniqs)]} (withXXXSets pojo uniqs :uniques))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dftFld<> "" ^APersistentMap [fid]

  {:column (cleanName fid)
   :id fid
   :domain :String
   :size 255
   :rel-key? false
   :null? true
   :auto? false
   :dft nil
   :updatable? true
   :system? false})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbfield
  "Add a new field"
  ^APersistentMap [pojo fid fdef]
  {:pre [(keyword? fid) (map? pojo) (map? fdef)]}

  (let [fd (merge (dftFld<> fid)
                  (dissoc fdef :id))]
    (update-in pojo
               [:fields] assoc fid fd)))

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
  "Define an relation between 2 models"
  [pojo rid rel]
  (let
    [rd (merge {:cascade? false :fkey nil} rel)
     r2 (case (:kind rd)
          (:O2O :O2M)
          (merge rd {:fkey (fmtfkey (:id pojo) rid) })
          (dberr! "Invalid relation: %s" rid))]
    (update-in pojo [:rels] assoc rid r2)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbassocs
  "Define a set of associations"
  [pojo reldefs]
  {:pre [(map? pojo) (map? reldefs)]}

  (reduce #(let [[k v] %2] (dbassoc %1 k v)) pojo reldefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private with-abstract
  "" [pojo flag] `(assoc ~pojo :abstract? ~flag))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private with-system "" [pojo] `(assoc ~pojo :system? true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the base model here
(comment
(dbmodel<> ::DBIOBaseModel
  (with-abstract true)
  (with-system)
  (dbfields {:rowid pkey-meta })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- checkField? "" [pojo fld]
  (bool!
    (if-some [f (-> (gmodel pojo)
                    :fields
                    (get fld))]
      (not (or (:auto? f)
               (not (:updatable? f)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(comment
(dbmodel<> ::DBIOJoinedModel
  (with-abstract true)
  (with-system)
  (dbfields
    {:lhs-rowid {:column col-lhs-rowid :domain :Long :null? false}
     :rhs-rowid {:column col-rhs-rowid :domain :Long :null? false} })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private mkfkdef<>
  "" [fid ktype] `(merge (dftFld<> ~fid)
                         {:rel-key? true :domain ~ktype }))

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
            :when (and (not (:abstract? m))
                       (not (empty? rs)))]
      (doseq [[_ r] rs
              :let [{:keys [other kind fkey]} r]
              :when (or (= :O2O kind)(= :O2M kind))]
        (var-set
          phd
          (assoc! @phd
                  other
                  (->> (mkfkdef<> fkey kt)
                       (assoc (@phd other) fkey))))))
    ;; now walk through all the placeholder maps and merge those new
    ;; fields to the actual models
    (doseq [[k v] (pcoll! @phd)
            :let [mcz (metas k)]]
      (->> (assoc! @xs
                   k
                   (update-in mcz
                              [:fields] merge v))
           (var-set xs )))
    (pcoll! @xs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolveMXMs "" [metas]
  (with-local-vars
    [mms (transient {})]
    (doseq [[k m] metas
            :let [fs (:fields m)
                  rs (:rels m)]
            :when (:mxm? m)]
      (->>
        (preduce<map>
          #(let
             [[side kee] %2
              other (get-in rs [side :other])
              mz (metas other)
              pke ((:fields mz) (:pkey mz))
              d (merge (kee fs)
                       (select-keys pke
                                    [:domain :size]))]
             (assoc! %1 kee d))
          [[:lhs :lhs-rowid]
           [:rhs :rhs-rowid]])
        (merge fs)
        (assoc m :fields )
        (assoc! @mms k )
        (var-set mms )))
    (merge metas (pcoll! @mms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- colmapFields
  "Create a map of fields keyed by the column name"
  [flds]
  (preduce<map> #(let [[_ v] %2] (assoc! %1 (ucase (:column v)) v)) flds))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- metaModels
  "Inject extra meta-data properties into each model.  Each model will have
   its (complete) set of fields keyed by column nam or field id"
  [metas schema]
  (preduce<map>
    #(let [[k m] %2]
       (->> [schema (->> (:fields m)
                         (colmapFields ))]
            (zipmap [:schema :columns])
            (with-meta m)
            (assoc! %1 k)))
    metas))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbschema<>
  "Stores metadata for all models" ^Schema [& models]

  (let [data (atom {})
        sch (reify Schema
              (get [_ id] (@data id))
              (models [_] @data))
        ms (if-not (empty? models)
             (preduce<map>
               #(assoc! %1 (:id %2) %2) models))
        m2 (if (empty? ms)
             {}
             (-> (resolveAssocs ms)
                 resolveMXMs
                 (metaModels sch)))]
    (reset! data m2)
    sch))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbgShowSchema
  "" ^String [^Schema mc] {:pre [(some? mc)]}

  (sreduce<>
    #(addDelim! %1
                "\n"
                (writeEdnStr {:TABLE (:table %2)
                              :DEFN %2
                              :META (meta %2)})) (vals (.models mc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- safeGetConn
  "Safely connect to db" ^Connection [^JdbcSpec jdbc]

  (let [{:keys [url driver
                passwd user]}
        (.intern jdbc)
        p (Properties.)
        d (.loadDriver jdbc)]
    (if (hgl? user)
      (doto p
        (.put "password" passwd)
        (.put "user" user)
        (.put "username" user)))
    (if (nil? d)
      (dberr! "Can't load Jdbc Url: %s" url))
    (if (and (hgl? driver)
             (not= (-> d
                       .getClass
                       .getName) driver))
      (log/warn "want %s, got %s" driver (class d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbconnect<>
  "Connect to db"
  ^Connection
  [^JdbcSpec jdbc] {:pre [(some? jdbc)]}

  (let [url (.url jdbc)
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
  "If able to connect to the database, as a test"
  [jdbc] (try (do->true (. (dbconnect<> jdbc) close))
              (catch SQLException _ false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti resolveVendor
  "Find type of database" ^APersistentMap (fn [a] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod resolveVendor
  JdbcSpec
  [jdbc] (with-open [conn (dbconnect<> jdbc)] (resolveVendor conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod resolveVendor
  Connection
  [conn]
  (let [md (. ^Connection conn getMetaData)
        rc {:id (maybeGetVendor (.getDatabaseProductName md))
            :qstr (strim (.getIdentifierQuoteString md))
            :version (.getDatabaseProductVersion md)
            :name (.getDatabaseProductName md)
            :url (.getURL md)
            :user (.getUserName md)
            :lcs? (.storesLowerCaseIdentifiers md)
            :ucs? (.storesUpperCaseIdentifiers md)
            :mcs? (.storesMixedCaseIdentifiers md)}]
    (assoc rc :fmtId (partial fmtSqlId rc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti tableExist?
  "Is this table defined in db?" (fn [a b] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?
  JdbcPool
  [^JdbcPool pool ^String table]
  (with-open [conn (.nextFree pool) ] (tableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?
  JdbcSpec
  [jdbc ^String table]
  (with-open [conn (dbconnect<> jdbc)] (tableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?
  Connection
  [^Connection conn ^String table]
  (log/debug "testing table: %s" table)
  (try!!
    false
    (let [dbv (resolveVendor conn)
          mt (.getMetaData conn)
          s (if (= (:id dbv) :oracle) "%")]
      (with-open
        [res (.getColumns
               mt
               nil
               s
               (fmtSqlId conn table false) "%")]
        (and res (.next res))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti rowExist?
  "Is there any rows in the table?" (fn [a _] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod rowExist?
  JdbcSpec
  [^JdbcSpec jdbc ^String table]
  (with-open [conn (dbconnect<> jdbc)] (rowExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod rowExist?
  Connection
  [^Connection conn ^String table]
  (try!!
    false
    (let
      [sql (str "select count(*) from "
                (fmtSqlId conn table))]
      (with-open [stmt (.createStatement conn)
                  res (.executeQuery stmt sql)]
        (and res
             (.next res)
             (> (.getInt res (int 1)) 0))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- loadColumns
  "Read each column's metadata"
  [^DatabaseMetaData mt
   ^String catalog ^String schema ^String table]

  (with-local-vars [pkeys #{} cms {}]
    (with-open
      [rs (. mt getPrimaryKeys catalog schema table)]
      (loop [sum (transient #{})
             more (.next rs)]
        (if-not more
          (var-set pkeys (pcoll! sum))
          (recur
            (conj! sum (.getString rs (int 4)))
            (.next rs)))))
    (with-open
      [rs (. mt getColumns catalog schema table "%")]
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
                      {:sql-type ctype
                       :column n
                       :null? opt
                       :pkey? (contains? @pkeys n) })
              (.next rs))))))
    (with-meta @cms
               {:supportsGetGeneratedKeys?
                (.supportsGetGeneratedKeys mt)
                :primaryKeys
                @pkeys
                :supportsTransactions?
                (.supportsTransactions mt) })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn loadTableMeta
  "Fetch metadata of this table from db"
  [^Connection conn ^String table] {:pre [(some? conn)]}

  (let [dbv (resolveVendor conn)
        mt (.getMetaData conn)
        catalog nil
        schema (if (= (:id dbv) :oracle) "%")
        tbl (fmtSqlId conn table false)]
    ;; not good, try mixed case... arrrrrrrrrrhhhhhhhhhhhhhh
    ;;rs = m.getTables( catalog, schema, "%", null)
    (loadColumns mt catalog schema tbl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- makePool<>
  "" ^JdbcPool [^JdbcSpec jdbc
                ^HikariDataSource impl]

  (let [dbv (resolveVendor jdbc)]
    (test-some "database-vendor" dbv)
    (reify JdbcPool

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
  "Create a db connection pool" {:tag JdbcPool}

  ([jdbc] (dbpool<> jdbc nil))
  ([^JdbcSpec jdbc options]
   (let [{:keys [driver url passwd user]}
         (.intern jdbc)
         options (or options {})
         hc (HikariConfig.)]
     ;;(log/debug "jdbc: %s" (.intern jdbc))
     ;;(log/debug "options: %s" options)
     (if (hgl? driver) (forname driver))
     (.setJdbcUrl hc url)
     (when (hgl? user)
       (.setPassword hc passwd)
       (.setUsername hc user))
     (log/debug "[hikari]\n%s" (str hc))
     (makePool<> jdbc (HikariDataSource. hc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeOK?
  "" [^String dbn ^Throwable e]

  (let [ee (cast? SQLException (rootCause e))
        ec (some-> ee .getErrorCode)]
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
(defmulti uploadDdl "Upload DDL to DB" (fn [a _] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl
  JdbcPool
  [^JdbcPool pool ^String ddl]
  (with-open [conn (.nextFree pool)] (uploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl
  JdbcSpec
  [^JdbcSpec jdbc ^String ddl]
  (with-open [conn (dbconnect<> jdbc)] (uploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl
  Connection
  [^Connection conn ddlstr] {:pre [(some? conn)] }

  (let [lines (mapv #(strim %)
                    (cs/split ddlstr ddl-sep))
        dbn (lcase (-> conn
                       .getMetaData
                       .getDatabaseProductName))]
    (.setAutoCommit conn true)
    (log/debug "\n%s" ddlstr)
    (doseq [s lines
            :let [ln (strimAny s ";" true)]
            :when (and (hgl? ln) (not= (lcase ln) "go"))]
      (with-open [s (.createStatement conn)]
        (try (.executeUpdate s ln)
             (catch SQLException _ (maybeOK? dbn _)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn bindModel
  "Internal" [pojo model]
  {:pre [(map? pojo)(map? model)]} (with-meta pojo {:$model model}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbpojo<>
  "Create object of type" ^APersistentMap [model] (bindModel {} model))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro mockPojo<>
  "" [obj]
  `(let [o# ~obj
         pk# (:pkey (gmodel o#))] (with-meta {pk# (goid o#)} (meta o#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetFld
  "Set value to a field"
  [pojo fld value]
  {:pre [(map? pojo) (keyword? fld)]}
  (if (checkField? pojo fld) (assoc pojo fld value) pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetFlds*
  "Set field+values as: f1 v1 f2 v2 ... fn vn"
  ^APersistentMap
  [pojo fvs] {:pre [(map? fvs)]}
  (reduce #(dbSetFld %1 (first %2) (last %2)) pojo fvs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- setMxMFlds* "" [pojo & fvs]
  (reduce #(assoc %1 (first %2) (last %2)) pojo (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbClrFld
  "Remove a field"
  [pojo fld]
  {:pre [(map? pojo) (:keyword? fld)]}
  (if (checkField? pojo fld) (dissoc pojo fld) pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbGetFld
  "Get value of a field"
  [pojo fld] {:pre [(map? pojo) (:keyword? fld)]} (get pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbioGetRelation
  "Get the relation definition"
  [model rid kind]
  (if-some [r (get (:rels model) rid)] (if (= (:kind r) kind) r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- selectSide+ "" [mxm obj]

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
      (if obj
        (dberr! "Unknown mxm relation for: %s" t)
        [nil nil nil]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private selectSide
  "" [mxm obj] `(first (selectSide+ ~mxm  ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; handling assocs
(defn- dbioGetO2X
  "" [ctx lhsObj kind]

  (let [sqlr (:with ctx)
        rid (:as ctx)
        mcz (gmodel lhsObj)]
    (if-some
      [r (dbioGetRelation mcz rid kind)]
      r
      (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbioSetO2X
  "" [ctx lhsObj rhsObj kind]

  (let [^SQLr sqlr (:with ctx)
        mcz (gmodel lhsObj)
        rid (:as ctx)]
    (if-some
      [r (dbioGetRelation mcz rid kind)]
      (let [fv (goid lhsObj)
            fid (:fkey r)
            y (-> (mockPojo<> rhsObj)
                  (dbSetFld fid fv))
            cnt (.update sqlr y)]
        [lhsObj (merge rhsObj y)])
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
  "" [ctx lhsObj rhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)(map? rhsObj)]} (dbioSetO2X ctx lhsObj rhsObj :O2M))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetO2M*
  "" ^APersistentVector [ctx lhsObj & rhsObjs]
  (preduce<vec> #(conj! %1 (last (dbioSetO2X ctx lhsObj %2 :O2M))) rhsObjs))

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
  "Set One to one relation"
  [ctx lhsObj rhsObj]
  {:pre [(map? ctx)
         (map? lhsObj) (map? rhsObj)]} (dbioSetO2X ctx lhsObj rhsObj :O2O))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbioClrO2X "" [ctx objA kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        rid (:as ctx)
        mA (gmodel objA)]
    (if-some
      [r (dbioGetRelation mA rid kind)]
      (let [rt (or (:cast ctx)
                   (:other r))
            mB (.get schema rt)
            tn (dbtable mB)
            cn (dbcol (:fkey r) mB)]
        (.exec
          sqlr
          (if-not (:cascade? r)
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
  "Clear one to many relation" [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]} (dbioClrO2X ctx lhsObj :O2M))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbClrO2O
  "Clear one to one relation" [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]} (dbioClrO2X ctx lhsObj :O2O))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- xrefColType "" [col]
  (case col
    :rhs-rowid :rhs-typeid
    :lhs-rowid :lhs-typeid
    (dberr! "Invaid column key: %s" col)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbSetM2M
  "Set many to many relations"
  ^APersistentMap [ctx objA objB]
  {:pre [(map? ctx) (map? objA) (map? objB)]}

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        jon (:joined ctx)]
    (if-some
      [mm (.get schema jon)]
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
  "Clear many to many relations"

  ([ctx obj] (dbClrM2M ctx obj nil))
  ([ctx objA objB]
   {:pre [(some? objA)]}
    (let [^SQLr sqlr (:with ctx)
          schema (.metas sqlr)
          jon (:joined ctx)]
      (if-some
        [mm (.get schema jon)]
        (let [fs (:fields mm)
              ka (selectSide mm objA)
              kb (selectSide mm objB)]
          (if (nil? objB)
            (.exec sqlr
                   (format
                     "delete from %s where %s=?"
                     (.fmtId sqlr (dbtable mm))
                     (.fmtId sqlr (dbcol (fs ka))))
                   [(goid objA)])
            (.exec sqlr
                   (format
                     "delete from %s where %s=? and %s=?"
                     (.fmtId sqlr (dbtable mm))
                     (.fmtId sqlr (dbcol (fs ka)))
                     (.fmtId sqlr (dbcol (fs kb))))
                   [(goid objA) (goid objB)])))
        (dberr! "Unkown relation: %s" jon)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbGetM2M
  "" [ctx obj]
  {:pre [(map? ctx)(map? obj)]}

  (let [^SQLr sqlr (:with ctx)
        RS (.fmtId sqlr "RES")
        MM (.fmtId sqlr "MM")
        schema (.metas sqlr)
        jon (:joined ctx)]
    (if-some
      [mm (.get schema jon)]
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
            RS (.fmtId sqlr col-rowid))
          [(goid obj)]))
      (dberr! "Unknown joined model: %s" jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


