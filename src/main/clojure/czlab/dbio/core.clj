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

(ns ^{:doc "Database and modeling utilties"
      :author "kenl" }

  czlab.dbio.core

  (:import
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
     BoneCPHook
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
    [java.lang Math]
    [czlab.crypto PasswordAPI]
    [com.jolbox.bonecp BoneCP BoneCPConfig])

  (:require
    [czlab.xlib.format :refer [writeEdnString]]
    [czlab.xlib.str
     :refer [lcase
             ucase
             hgl?
             sname
             lcase
             ucase
             strimAny
             strim
             embeds?
             addDelim!
             hasNoCase?]]
    [czlab.xlib.core
     :refer [asFQKeyword
             test-nonil
             cast?
             tryc
             try!
             trylet!
             trap!
             interject
             nnz
             nbf
             juid
             rootCause]]
    [czlab.xlib.logging :as log]
    [clojure.string :as cs]
    [clojure.set :as cset]
    [czlab.crypto.codec :as codec]
    [czlab.xlib.meta :refer [forname]])

  (:use [flatland.ordered.set]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(defonce ^String COL_LASTCHANGED "DBIO_LASTCHANGED")
(defonce ^String COL_CREATED_ON "DBIO_CREATED_ON")
(defonce ^String COL_CREATED_BY "DBIO_CREATED_BY")
(defonce ^String COL_LHS_TYPEID "DBIO_LHS_TYPEID")
(defonce ^String COL_LHS_ROWID "DBIO_LHS_ROWID")
(defonce ^String COL_RHS_TYPEID "DBIO_RHS_TYPEID")
(defonce ^String COL_RHS_ROWID "DBIO_RHS_ROWID")
(defonce ^String COL_ROWID "DBIO_ROWID")
(defonce ^String COL_VERID "DBIO_VERID")
(defonce DDL_SEP #"-- :")

(def ^:dynamic *DDL_CFG* nil)
(def ^:dynamic *DDL_BVS* nil)

(def ^:private ^String _NSP "czc.dbio.core" )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cleanName "" [s]
  (-> (cs/replace (name s) #"[^a-zA-Z0-9_-]" "")
      (cs/replace  #"-" "_")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro set-oid "" ^:private [pojo oid] `(assoc ~pojo :rowid ~oid))
(defmacro gtype "" ^:no-doc [obj]  `(:id (:model (meta ~obj))))
(defmacro gschema "" ^:no-doc [obj]  `(:schema (meta ~obj)))
(defmacro gmodel "" ^:no-doc [obj]  `(:model (meta ~obj)))
(defmacro goid "" ^:no-doc [obj]  `(:rowid ~obj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti fmtSQLIdStr
  "Format SQL identifier" ^String (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLIdStr

  APersistentMap

  ^String
  [info idstr & [quote?]]

  (let [ch (strim (:qstr info))
        id (cond
             (:ucs? info)
             (ucase idstr)
             (:lcs? info)
             (lcase idstr)
             :else idstr)]
    (if (false? quote?)
      id
      (str ch id ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLIdStr

  DBAPI

  ^String
  [^DBAPI db idstr & [quote?]]

  (fmtSQLIdStr (.vendor db) idstr quote?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLIdStr

  DatabaseMetaData

  ^String
  [^DatabaseMetaData mt idstr & [quote?]]

  (let [ch (strim (.getIdentifierQuoteString mt))
        id (cond
             (.storesUpperCaseIdentifiers mt)
             (ucase idstr)
             (.storesLowerCaseIdentifiers mt)
             (lcase idstr)
             :else idstr)]
    (if (false? quote?)
      id
      (str ch id ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod fmtSQLIdStr

  Connection

  ^String
  [^Connection conn idstr & [quote?]]

  (fmtSQLIdStr (.getMetaData conn) idstr quote?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn canAssignFrom?

  "Check if cur type is a subclass of the root type"

  [root model]

  {:pre [(keyword? root) ]}

  (if (= root (:id model))
    true
    (let [^Schema s (gschema model)
          p (:parent model)]
      (if (some? p)
        (canAssignFrom? root (.get s p))
        false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; have to be function , not macro as this is passed into another higher
;; function - merge.
(defn mergeMeta

  "Merge 2 meta maps"

  ^APersistentMap
  [m1 m2]

  {:pre [(map? m1) (or (nil? m2)(map? m2))]}

  (merge m1 m2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbTablename

  "The table-name defined for this model"

  (^String [model] (:table model))

  (^String [model-id ^Schema schema]
   {:pre [(some? schema)]}
   (dbTablename (.get schema model-id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbColname

  "The column-name defined for this field"

  (^String [fdef] (:column fdef))

  (^String [fld-id model]
   {:pre [(map? model)]}
   (-> (:fields (meta model))
       (get fld-id)
       (dbColname ))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn mkJdbc

  "Basic jdbc parameters"

  ^JDBCInfo
  [cfg]

  {:pre [(map? cfg)]}

  ;;(log/debug "JDBC id= %s, cfg = %s" id cfg)
  (let [id (juid)]
    (reify

      JDBCInfo

      (getUrl [_] (or (:server cfg) (:url cfg)))
      (getId [_]  (or (:id cfg) id))

      (loadDriver [this]
        (when-let [s (.getUrl this)]
          (when (hgl? s)
            (DriverManager/getDriver s))))

      (getDriver [_] (:driver cfg))
      (getUser [_] (:user cfg))
      (getPwd [_] (str (:passwd cfg))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defonce Postgresql :postgresql)
(defonce SQLServer :sqlserver)
(defonce Oracle :oracle)
(defonce MySQL :mysql)
(defonce H2 :h2)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defonce DBTYPES
  {SQLServer {:test-string "select count(*) from sysusers" }
   Postgresql { :test-string "select 1" }
   MySQL { :test-string "select version()" }
   H2 { :test-string "select 1" }
   Oracle { :test-string "select 1 from DUAL" } })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn throwDBError

  "Throw a DBIOError execption"

  [^String msg]

  (trap! DBIOError msg))

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
      (throwDBError (str "Unknown db product " product)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fmtfkey

  "For o2o & o2m relations"

  ^Keyword
  [tn rn]

  (keyword (str "fk_" (name tn) "_" (name rn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA MODELING
;;
(defonce JOINED-MODEL-MONIKER ::DBIOJoinedModel)
(defonce BASEMODEL-MONIKER ::DBIOBaseModel)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioModel

  "Define a generic model"

  ^APersistentMap
  [^String nm]

  {:table (cleanName nm)
   :id (asFQKeyword nm)
   :parent nil
   :abstract false
   :system false
   :mxm false
   :indexes {}
   :uniques {}
   :fields {}
   :rels {} })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro defModel

  "Define a data model"

  [modelname & body]

  `(def ~modelname
     (-> (dbioModel ~(name modelname))
         ~@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro defJoined

  "Define a joined data model"

  [modelname lhs rhs]

  `(def ~modelname
      (-> (dbioModel ~(name modelname))
          (assoc :parent JOINED-MODEL-MONIKER)
          (withJoined ~lhs ~rhs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withParent

  "Link a parent to the model"

  ^APersistentMap
  [pojo par]

  {:pre [(map? pojo)
         (keyword? par)
         (not= JOINED-MODEL-MONIKER par)
         (not= BASEMODEL-MONIKER par)]}

  (assoc pojo :parent par))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withJoined

  "A special model with 2 relations,
   left hand side and right hand side"

  ^APersistentMap
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
(defn withTablename

  "Set the table name"

  ^APersistentMap
  [pojo table]

  {:pre [(map? pojo)]}

  (assoc pojo :table (cleanName table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- with-xxx-sets

  ""

  [pojo kvs fld]

  (let [m (persistent!
            (reduce
              (fn [out [k v]]
                (assoc! out
                       k
                       (into (ordered-set) v)))
              (transient {})
              kvs))]
    ;;merge new stuff onto old stuff
    (interject pojo fld #(merge (%2 %1) m))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withIndexes

  "Set indexes to the model"

  ^APersistentMap
  [pojo indices]

  {:pre [(map? pojo) (map? indices)]}

  ;;indices = { :a #{ :f1 :f2} ] :b #{:f3 :f4} }
  (with-xxx-sets pojo indices :indexes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withUniques

  "Set uniques to the model"

  ^APersistentMap
  [pojo uniqs]

  {:pre [(map? pojo) (map? uniqs)]}

  ;;uniques = { :a #{ :f1 :f2 } :b #{ f3 f4 } }
  (with-xxx-sets pojo uniqs :uniques))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- getDftFldObj

  "The base field structure"

  ^APersistentMap
  [fid]

  {:column (cleanName fid)
   :domain :String
   :size 255
   :id (keyword fid)
   :rel-key false
   :pkey false
   :null true
   :auto false
   :dft nil
   :updatable true
   :system false
   :index "" })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withField

  "Add a new field"

  ^APersistentMap
  [pojo fid fdef]

  {:pre [(map? pojo) (map? fdef)]}

  (let [fd (merge (getDftFldObj fid) fdef)
        k (:id fd)]
    (interject pojo :fields #(assoc (%2 %1) k fd))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withFields

  "Add a bunch of fields"

  ^APersistentMap
  [pojo flddefs]

  {:pre [(map? pojo) (map? flddefs)]}

  (with-local-vars [rcmap pojo]
    (doseq [[k v] flddefs]
      (->> (withField @rcmap k v)
           (var-set rcmap)))
    @rcmap))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- withRelation

  ""

  [pojo rid rhs kind & [cascade?]]

  {:pre [(map? pojo) (keyword? rid) (keyword? rhs)]}

  (let [rd {:fkey nil :cascade false
            :kind nil :other nil}
        r2 (case kind
             (:O2O :O2M)
             (merge rd {:fkey (fmtfkey (:id pojo) rid)
                        :cascade (true? cascade?)
                        :kind kind
                        :other rhs})
             (throwDBError (str "Invalid relation " rid)))]
    (interject pojo :rels #(assoc (%2 %1) rid r2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withO2M

  "Add a one to many relation"

  ^APersistentMap
  [pojo rid rhs & [cascade?]]

  (withRelation pojo rid rhs :O2M cascade?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withO2O

  "Add a one to one relation"

  ^APersistentMap
  [pojo rid rhs & [cascade?]]

  (withRelation pojo rid rhs :O2O cascade?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn withAbstract

  "Set the model as abstract"

  ^APersistentMap
  [pojo flag]

  {:pre [(map? pojo)]}

  (assoc pojo :abstract flag))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- with-db-system

  "This is a built-in system level model"

  [pojo]

  (assoc pojo :system true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the base model here
(defModel DBIOBaseModel
  (withAbstract true)
  (with-db-system)
  (withFields
    {:rowid {:column COL_ROWID :pkey true :domain :Long
             :auto true :system true :updatable false} }))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- checkField?

  ""
  [fld]

  (not
    (clojure.core/contains?
      #{:lhs-rowid
        :rhs-rowid
        :rowid
        :lhs-typeid
        :rhs-typeid} fld)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defModel DBIOJoinedModel
  (withAbstract true)
  (with-db-system)
  (withFields
    {:lhs-typeid {:column COL_LHS_TYPEID }
     :lhs-rowid {:column COL_LHS_ROWID :domain :Long :null false}
     :rhs-typeid {:column COL_RHS_TYPEID }
     :rhs-rowid {:column COL_RHS_ROWID :domain :Long :null false} }))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro mkfkdef

  ""

  ^:private
  [fid]

  `(merge (getDftFldObj ~fid)
          {:rel-key true :domain :Long }))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolve-rels

  "Walk through all models, for each model, study its relations.
  For o2o or o2m assocs, we need to artificially inject a new
  field/column into the (other/rhs) model (foreign key)"

  [metas]

  ;; 1st, create placeholder maps for each model,
  ;; to hold new fields from rels
  (with-local-vars [phd (reduce
                          #(assoc! %1 %2 {})
                          (transient {}) (keys metas))
                    xs (transient {})]
    ;; as we find new relation fields,
    ;; add them to the placeholders
    (doseq [[_ m] metas
            :let [rs (:rels m)]
            :when (and (not (:abstract m))
                       (not (empty? rs)))]
      (doseq [[_ r] rs
              :let [rid (:other r)
                    t (:kind r)
                    fid (:fkey r)]
              :when (or (= :O2O t)
                        (= :O2M t))]
        (var-set phd
                 (assoc! @phd
                         rid
                         (->> (mkfkdef fid)
                              (assoc (@phd rid) fid ))))))
    ;; now walk through all the placeholder maps and merge those new
    ;; fields to the actual models
    (doseq [[k v] (persistent! @phd)
            :let [mcz (metas k)]]
      (->> (assoc! @xs k
                       (assoc mcz
                              :fields
                              (merge (:fields mcz) v)))
           (var-set xs )))
    (persistent! @xs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti collect-db-xxx

  ""
  ^:private

  (fn [_ _ b]

    (cond
    (keyword? b)
    :keyword

    (map? b)
    :map

    :else
    (throwDBError (str "Invalid arg " b)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod collect-db-xxx

  :keyword

  [kw metas model-id]

  (if-some [mcz (metas model-id)]
    (collect-db-xxx kw metas mcz)
    (log/warn "Unknown model id: %s" model-id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod collect-db-xxx

  :map

  [kw metas model-def]

  (if-some [par (:parent model-def)]
    (merge {} (collect-db-xxx kw metas par)
              (kw model-def))
    (merge {} (kw model-def))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn collectDbXXX

  ""

  ^:no-doc
  [kw model]

  {:pre [(map? model)]}

  (let [^Schema s (gschema model)]
    (collect-db-xxx kw (.getModels s) model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- colmap-fields

  "Create a map of fields keyed by the column name"

  [flds]

  (persistent!
    (reduce
      (fn [m [_ v]]
        (assoc! m (ucase (:column v)) v))
      (transient {})
      flds)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- meta-models

  "Inject extra meta-data properties into each model.  Each model will have
   its (complete) set of fields keyed by column nam or field id"

  [metas schema]

  (with-local-vars [sum (transient {})]
    (doseq [[k m] metas]
      (let [flds (collect-db-xxx :fields metas m)
            cols (colmap-fields flds)]
        (var-set sum
                 (assoc! @sum
                         k
                         (with-meta m
                                    {:schema schema
                                     :columns cols
                                     :fields flds } ) ))))
    (persistent! @sum)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolve-parent

  "Ensure that all user defined models are
   all derived from the system base model"

  [metas model]

  (let [par (:parent model)]
    (cond
      (keyword? par)
      (if (nil? (metas par))
        (throwDBError (str "Unknown parent model " par))
        model)

      (nil? par)
      (assoc model :parent BASEMODEL-MONIKER)

      :else
      (throwDBError (str "Invalid parent " par)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Idea is walk through all models and ultimately
;; link it's root to base-model
(defn- resolve-parents

  ""

  [metas]

  (persistent!
    (reduce
      (fn [sum [_ v]]
        (let [rc (resolve-parent metas v)]
         (assoc! sum (:id rc) rc)))
      (transient {})
      metas)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn mkDbSchema

  "A cache storing meta-data for all models"

  ^Schema
  [& models]

  (let [data (atom {})
        schema (reify Schema
                 (get [_ id] (get @data id))
                 (getModels [_] @data))
        ms (if (empty? models)
             {}
             (persistent!
               (reduce #(assoc! %1 (:id %2) %2)
                       (transient {})
                       models)))
        m2 (if (empty? ms)
             {}
             (-> (assoc ms JOINED-MODEL-MONIKER DBIOJoinedModel)
                 (resolve-parents)
                 (resolve-rels)
                 (assoc BASEMODEL-MONIKER DBIOBaseModel)
                 (meta-models schema)))]
    (reset! data m2)
    schema))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbgShowSchema

  ""

  ^String
  [^Schema mc]

  {:pre [(some? mc)]}

  (reduce #(addDelim! %1
                      "\n"
                      (writeEdnString {:TABLE (:table %2)
                                       :DEFN %2
                                       :META (meta %2)}))
          (StringBuilder.)
          (vals (.getModels mc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- safeGetConn

  "Safely connect to database referred by this jdbc"

  ^Connection
  [^JDBCInfo jdbc]

  (let [user (.getUser jdbc)
        dv (.getDriver jdbc)
        url (.getUrl jdbc)
        d (.loadDriver jdbc)
        p (let [pps (Properties.)]
            (if (hgl? user)
              (doto pps
                (.put "password" (str (.getPwd jdbc)))
                (.put "user" user)
                (.put "username" user)))
            pps)]
    (when (nil? d) (throwDBError (str "Can't load Jdbc Url: " url)))
    (when (and (hgl? dv)
               (not= (-> d (.getClass) (.getName)) dv))
      (log/warn "expected %s, loaded with driver: %s" dv (.getClass d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn mkDbConnection

  "Connect to database referred by this jdbc"

  ^Connection
  [^JDBCInfo jdbc]

  {:pre [(some? jdbc)]}

  (let [url (.getUrl jdbc)
        ^Connection
        conn (if (hgl? (.getUser jdbc))
               (safeGetConn jdbc)
               (DriverManager/getConnection url))]
    (when (nil? conn)
      (throwDBError (str "Failed to create db connection: " url)))
    (doto conn
      (.setTransactionIsolation Connection/TRANSACTION_SERIALIZABLE))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn testConnection?

  "true if able to connect to the database, as a test"

  [jdbc]

  (try
    (.close (mkDbConnection jdbc))
    true
    (catch SQLException _# false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti resolveVendor
  "Find type of database" ^APersistentMap (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod resolveVendor

  JDBCInfo

  ^APersistentMap
  [jdbc]

  (with-open
    [conn (mkDbConnection jdbc)]
    (resolveVendor conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod resolveVendor

  Connection

  ^APersistentMap
  [conn]

  (let [md (.getMetaData ^Connection conn) ]
    {:id (maybeGetVendor (.getDatabaseProductName md))
     :qstr (strim (.getIdentifierQuoteString md))
     :version (.getDatabaseProductVersion md)
     :name (.getDatabaseProductName md)
     :url (.getURL md)
     :user (.getUserName md)
     :lcs? (.storesLowerCaseIdentifiers md)
     :ucs? (.storesUpperCaseIdentifiers md)
     :mcs? (.storesMixedCaseIdentifiers md)}))

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
    [conn (mkDbConnection jdbc)]
    (tableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod tableExist?

  Connection

  [^Connection conn ^String table]

  (with-local-vars [rc false]
    (log/debug "testing table: %s" table)
    (trylet!
      [mt (.getMetaData conn)]
      (with-open [res (.getColumns mt
                                   nil
                                   nil
                                   (fmtSQLIdStr conn table)
                                   nil)]
        (when (and (some? res)
                   (.next res))
          (var-set rc true))))
    @rc))

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
    [conn (mkDbConnection jdbc)]
    (rowExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod rowExist?

  Connection

  [^Connection conn ^String table]

  (with-local-vars [rc false]
    (trylet!
      [sql (str
             "SELECT COUNT(*) FROM  "
             (fmtSQLIdStr conn table))]
      (with-open [stmt (.createStatement conn)
                  res (.executeQuery stmt sql)]
        (when (and (some? res)
                   (.next res))
          (var-set rc (> (.getInt res (int 1)) 0)))))
    @rc))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- load-columns

  "Read each column's metadata"

  [^DatabaseMetaData mt
   ^String catalog
   ^String schema ^String table]

  (with-local-vars [pkeys #{} cms {}]
    (with-open [rs (.getPrimaryKeys mt
                                    catalog
                                    schema table)]
      (loop [sum (transient #{})
             more (.next rs)]
        (if-not more
          (var-set pkeys (persistent! sum))
          (recur
            (conj! sum (.getString rs (int 4)))
            (.next rs)))))
    (with-open [rs (.getColumns mt
                                catalog
                                schema table "%")]
      (loop [sum (transient {})
             more (.next rs)]
        (if-not more
          (var-set cms (persistent! sum))
          (let [opt (not= (.getInt rs (int 11))
                          DatabaseMetaData/columnNoNulls)
                n (.getString rs (int 4))
                cn (ucase n)
                ctype (.getInt rs (int 5))]
            (recur
              (assoc! sum
                      (keyword cn)
                      {:column n :sql-type ctype :null opt
                       :pkey (clojure.core/contains? @pkeys n) })
              (.next rs))))))
    (with-meta @cms {:supportsGetGeneratedKeys
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
        tbl (fmtSQLIdStr conn table false)]
    ;; not good, try mixed case... arrrrrrrrrrhhhhhhhhhhhhhh
    ;;rs = m.getTables( catalog, schema, "%", null)
    (load-columns mt catalog schema tbl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- makePool

  ""

  ^JDBCPool
  [^JDBCInfo jdbc ^BoneCP impl]

  (let [dbv (resolveVendor jdbc)]
    (test-nonil "database-vendor" dbv)
    (reify

      JDBCPool

      (shutdown [_]
        (log/debug "About to shut down the pool impl: %s" impl)
        (.shutdown impl))

      (dbUrl [_] (.getUrl jdbc))
      (vendor [_] dbv)

      (nextFree [_]
        (try
            (.getConnection impl)
          (catch Throwable e#
            (log/error e# "")
            (throwDBError "No free connection")))))))

      ;;Object
      ;;Clojure CLJ-1347
      ;;finalize won't work *correctly* in reified objects - document
      ;;(finalize [this]
        ;;(try!
          ;;(log/debug "DbPool finalize() called.")
          ;;(.shutdown this)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn mkDbPool

  "Create a db connection pool"

  ^JDBCPool
  [^JDBCInfo jdbc options]

  (let [options (or options {})
        dv (.getDriver jdbc)
        bcf (BoneCPConfig.) ]

    ;;(log/debug "URL: %s"  (.getUrl jdbc))
    ;;(log/debug "Driver: %s" dv)
    ;;(log/debug "Options: %s" options)

    (when (hgl? dv) (forname dv))
    (doto bcf
          (.setPartitionCount (Math/max 1 (nnz (:partitions options))))
          (.setLogStatementsEnabled (nbf (:debug options)))
          (.setPassword (str (.getPwd jdbc)))
          (.setJdbcUrl (.getUrl jdbc))
          (.setUsername (.getUser jdbc))
          (.setIdleMaxAgeInSeconds (* 60 60 2)) ;; 2 hrs
          (.setMaxConnectionsPerPartition (Math/max 1 (nnz (:max-conns options))))
          (.setMinConnectionsPerPartition (Math/max 1 (nnz (:min-conns options))))
          (.setPoolName (juid))
          (.setAcquireRetryDelayInMs 5000)
          (.setConnectionTimeoutInMs  (Math/max 5000 (nnz (:max-conn-wait options))))
          (.setDefaultAutoCommit false)
          ;;(.setConnectionHook (BoneCPHook.))
          (.setAcquireRetryAttempts 1))
    (log/debug "[bonecp]\n%s" (.toString bcf))
    (makePool jdbc (BoneCP. bcf))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeOK?

  ""

  [^String dbn ^Throwable e]

  (if-let [ee (->> (rootCause e)
                   (cast? SQLException ))]
    (let [ec (.getErrorCode
               ^SQLException ee)]
      (cond
        (and (embeds? (str dbn) "oracle")
             (== 942 ec)
             (== 1418 ec)
             (== 2289 ec)(== 0 ec))
        true
        :else
        (throw e)))
    (throw e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti uploadDdl "Upload DDL to DB" (fn [a & xs] (class a)))

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
    [conn (mkDbConnection jdbc)]
     (uploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod uploadDdl

  Connection

  [^Connection conn ^String ddl]

  {:pre [(some? conn)] }

  (let [lines (map #(strim %) (cs/split ddl DDL_SEP))
        dbn (lcase (-> (.getMetaData conn)
                       (.getDatabaseProductName)))]
    (.setAutoCommit conn true)
    (log/debug "\n%s" ddl)
    (doseq [^String s (seq lines)
            :let [ln (strimAny s ";" true)]
            :when (and (hgl? ln)
                       (not= (lcase ln) "go"))]

      (with-open [s (.createStatement conn)]
        (try
          (.executeUpdate s ln)
        (catch SQLException e#
          (maybeOK? dbn e#)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioCreateObj

  "Creates a blank object of the given type"

  ^APersistentMap
  [model]

  {:pre [(some? model)]}

  (with-meta {} { :model model } ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro mockObj "" ^:private [obj]
  `(with-meta {:rowid (goid ~obj)} (meta ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioSetFld

  "Set value to a field"

  [pojo fld value]

  {:pre [(map? pojo) (:keyword? fld)]}

  (if (checkField? fld)
    (assoc pojo fld value)
    pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioSetFlds*

  "Set many field values such as f1 v1 f2 v2 ... fn vn"

  ^APersistentMap
  [pojo fld value & fvs]

  {:pre [(or (empty? fvs)
             (even? (count fvs)))]}

  (reduce
    (fn [p [f v]]
      (dbioSetFld p f v))
    (dbioSetFld pojo fld value)
    (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioClrFld

  "Remove a field"

  [pojo fld]

  {:pre [(map? pojo) (:keyword? fld)]}

  (if (checkField? fld)
    (dissoc pojo fld)
    pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioGetFld

  "Get value of a field"

  [pojo fld]

  {:pre [(map? pojo) (:keyword? fld)]}

  (get pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioGetRelation

  "Get the relation definition"

  [model rid kind]

  (if-let [r (get (:rels model) rid)]
    (when (= (:kind r) kind)
      r)))

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
    (case t
      rt
      [:rhs-rowid :lhs-rowid lf]
      lf
      [:lhs-rowid :rhs-rowid rt]
      (throwDBError (str "Mismatched mxm relation for " t)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro selectSide
  ""
  ^:private
  [mxm obj]
  `(first (selectSide+ ~mxm  ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; handling assocs
(defn- dbio-get-o2x

  ""

  [ctx lhsObj kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        rid (:as ctx)
        mcz (gmodel lhsObj)]
    (if-let [r (dbioGetRelation mcz rid kind)]
      r
      (throwDBError (str "Unknown relation " rid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbio-set-o2x

  ""

  [ctx lhsObj rhsObj kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        mcz (gmodel lhsObj)
        rid (:as ctx)]
    (if-let [r (dbioGetRelation mcz (:as ctx) kind)]
      (let [fv (goid lhsObj)
            fid (:fkey r)
            y (->> (-> (mockObj rhsObj)
                       (dbioSetFld fid fv))
                   (.update sqlr))]
        [ lhsObj (merge rhsObj y) ])
      (throwDBError (str "Unknown assoc " rid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioGetO2M

  "One to many assocs"

  [ctx lhsObj]

  {:pre [(map? ctx)(map? lhsObj)]}

  (when-let [r (dbio-get-o2x ctx lhsObj :O2M)]
    (-> ^SQLr
        (:with ctx)
        (.findSome (or (:cast ctx)
                       (:other r))
                   {(:fkey r) (goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioSetO2M

  ""

  [ctx lhsObj rhsObj]

  {:pre [(map? ctx)(map? lhsObj)(map? rhsObj)]}

  (dbio-set-o2x ctx lhsObj rhsObj :O2M))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioSetO2M*

  ""

  ^APersistentVector
  [ctx lhsObj & rhsObjs]

  (persistent!
    (reduce
      #(conj! %1 (last (dbio-set-o2x
                         ctx
                         lhsObj
                         %2)))
      (transient [])
      rhsObjs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioGetO2O

  "One to one relation"

  ^APersistentMap
  [ctx lhsObj]

  {:pre [(map? ctx) (map? lhsObj)]}

  (when-let [r (dbio-get-o2x ctx lhsObj :O2O)]
    (-> ^SQLr
        (:with ctx)
        (.findOne (or (:cast ctx)
                      (:other r))
                  {(:fkey r) (goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioSetO2O

  ""

  [ctx lhsObj rhsObj]

  {:pre [(map? ctx) (map? lhsObj) (map? rhsObj)]}

  (dbio-set-o2x ctx lhsObj rhsObj :O2O))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbio-clr-o2x

  ""

  [ctx objA kind]

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        rid (:as ctx)
        mA (gmodel objA)]
    (if-let [r (dbioGetRelation mA rid kind)]
      (let [rt (or (:cast ctx)
                   (:other r))
            mB (.get schema rt)
            tn (dbTablename mB)
            cn (dbColname (:fkey r) mB)]
        (.exec
          sqlr
          (if-not (:cascade r)
            (format
              "UPDATE %s SET %s= NULL WHERE %s=?"
              (.escId sqlr tn)
              (.escId sqlr cn)
              (.escId sqlr cn))
            (format
              "DELETE FROM %s WHERE %s=?"
              (.escId sqlr tn)
              (.escId sqlr cn)))
          [(goid objA)])
        objA)
      (throwDBError (str "Unknown assoc " rid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioClrO2M

  ""

  [ctx lhsObj]

  {:pre [(map? ctx) (map? lhsObj)]}

  (dbio-clr-o2x ctx lhsObj :O2M))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioClrO2O

  ""

  [ctx lhsObj]

  {:pre [(map? ctx) (map? lhsObj)]}

  (dbio-clr-o2x ctx lhsObj :O2O))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- xrefColType

  ""

  [col]

  (case col
    :rhs-rowid :rhs-typeid
    :lhs-rowid :lhs-typeid
    (throwDBError (str "Invaid column key " col))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioSetM2M

  "Many to many relations"

  ^APersistentMap
  [ctx objA objB]

  {:pre [(map? ctx) (map? objA) (map? objB)]}

  (let [^SQLr sqlr (:with ctx)
        schema (.metas sqlr)
        jon (:joined ctx)]
    (if-let [mm (.get schema jon)]
      (let [ka (selectSide mm objA)
            kb (selectSide mm objB)]
        (->> (-> (dbioCreateObj mm)
                 (dbioSetFlds*
                   ka (goid objA)
                   kb (goid objB)))
             (.insert sqlr )))
      (throwDBError (str "Unkown relation " jon)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioClrM2M

  ""

  ([ctx obj] (dbioClrM2M ctx obj nil))

  ([ctx objA objB]
   {:pre [(some? objA)]}
    (let [^SQLr sqlr (:with ctx)
          schema (.metas sqlr)
          jon (:joined ctx)]
      (if-let [mm (.get schema jon)]
        (let [fs (:fields (meta mm))
              ka (selectSide mm objA)
              kb (selectSide mm objB)]
          (if (nil? objB)
            (.exec sqlr
                   (format
                     "DELETE FROM %s WHERE %s=?"
                     (.escId sqlr (dbTablename mm))
                     (.escId sqlr (dbColname (fs ka))))
                   [ (goid objA) ])
            (.exec sqlr
                   (format
                     "DELETE FROM %s WHERE %s=? AND %s=?"
                     (.escId sqlr (dbTablename mm))
                     (.escId sqlr (dbColname (fs ka)))
                     (.escId sqlr (dbColname (fs kb))))
                   [ (goid objA) (goid objB) ])))
        (throwDBError (str "Unkown assoc " jon))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioGetM2M

  ""

  [ctx obj]

  {:pre [(map? ctx)(map? obj)]}

  (let [^SQLr sqlr (:with ctx)
        RS (.escId sqlr "RES")
        MM (.escId sqlr "MM")
        schema (.metas sqlr)
        jon (:joined ctx)]
    (if-let [mm (.get schema jon)]
      (let [[ka kb t]
            (selectSide+ mm obj)
            t2 (or (:cast ctx) t)
            fs (:fields (meta mm))
            tm (.get schema t2)]
        (when (nil? tm)
          (throwDBError (str "Unknown model " t2)))
        (.select
          sqlr
          t2
          (format
            (str "SELECT DISTINCT %s.* FROM %s %s "
                 "JOIN %s %s ON "
                 "%s.%s=? AND %s.%s=%s.%s"),
            RS
            (.escId sqlr (dbTablename tm))
            RS
            (.escId sqlr (dbTablename mm))
            MM
            MM (.escId sqlr (dbColname (ka fs)))
            MM
            MM (.escId sqlr (dbColname (kb fs)))
            RS (.escId sqlr COL_ROWID))
          [ (goid obj) ]))
      (throwDBError (str "Unknown joined model " jon)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


