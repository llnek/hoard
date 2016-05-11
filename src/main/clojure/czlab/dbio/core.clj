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

  czlab.xlib.dbio.core

  (:require
    [czlab.xlib.util.format :refer [WriteEdnString]]
    [czlab.xlib.util.str
    :refer [lcase ucase strim
    Embeds? AddDelim! HasNocase? hgl?]]
    [czlab.xlib.util.core
    :refer [tryc try! trylet! trap!
    RootCause StripNSPath
    GetTypeId Interject nnz nbf juid]]
    [czlab.xlib.util.logging :as log]
    [clojure.string :as cs]
    [clojure.set :as cset]
    [czlab.xlib.crypto.codec :as codec ]
    [czlab.xlib.util.meta :refer [ForName]])

  (:use [flatland.ordered.set])

  (:import
    [java.util HashMap GregorianCalendar
    TimeZone Properties]
    [com.zotohlab.frwk.dbio MetaCache
    Schema BoneCPHook DBIOError SQLr JDBCPool JDBCInfo]
    [java.sql SQLException
    DatabaseMetaData Connection Driver DriverManager]
    [java.lang Math]
    [com.zotohlab.frwk.crypto PasswordAPI]
    [com.jolbox.bonecp BoneCP BoneCPConfig]
    [org.apache.commons.lang3 StringUtils]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(def ^:dynamic ^MetaCache *META-CACHE* nil)
(def ^:dynamic *USE_DDL_SEP* true)
(def ^:dynamic *DDL_BVS* nil)
(def ^:dynamic *JDBC-INFO* nil)
(def ^:dynamic *JDBC-POOL* nil)

(defonce ^String COL_LASTCHANGED "DBIO_LASTCHANGED")
(defonce ^String COL_CREATED_ON "DBIO_CREATED_ON")
(defonce ^String COL_CREATED_BY "DBIO_CREATED_BY")
(defonce ^String COL_LHS_TYPEID "LHS_TYPEID")
(defonce ^String COL_LHS_ROWID "LHS_ROWID")
(defonce ^String COL_RHS_TYPEID "RHS_TYPEID")
(defonce ^String COL_RHS_ROWID "RHS_ROWID")
(defonce ^String COL_ROWID "DBIO_ROWID")
(defonce ^String COL_VERID "DBIO_VERID")
(defonce ^String DDL_SEP "-- :")

(def ^:private ^String _NSP "czc.dbio.core" )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro uc-ent ^String [ent] `(cs/upper-case (name ~ent)))
(defmacro lc-ent ^String [ent] `(cs/lower-case (name ~ent)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ese

  "Escape string entity for sql"

  (^String [c1 ent c2] (str c1 (uc-ent ent) c2))
  (^String [ent] (uc-ent ent))
  (^String [ch ent] (str ch (uc-ent ent) ch)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn GTable

  "Get the table name (escaped) of this model"

  ^String
  [model]

  (ese (:table model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; have to be function , not macro as this is passed into another higher
;; function - merge.
(defn MergeMeta

  "Merge 2 meta maps"

  [m1 m2]

  (merge m1 m2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn Tablename

  "the table-name defined for this model"

  (^String
    [mdef]
    (:table mdef))

  (^String
    [mid cache]
    (:table (cache mid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn Colname

  "the column-name defined for this field"

  (^String
    [fdef]
    (:column fdef))

  (^String
    [fid mcz]
    (-> (:fields (meta mcz))
        (get fid)
        (:column))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro eseLHS [] `(ese COL_LHS_ROWID))
(defmacro eseRHS [] `(ese COL_RHS_ROWID))
(defmacro eseOID [] `(ese COL_ROWID))
(defmacro eseVID [] `(ese COL_VERID))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn Jdbc*

  "Make a JDBCInfo record"

  ^JDBCInfo
  [^String id cfg ^PasswordAPI pwdObj]

  ;;(log/debug "JDBC id= %s, cfg = %s" id cfg)
  (let [server (:server cfg)]
    (reify

      JDBCInfo

      (getDriver [_] (:driver cfg))
      (getUser [_] (:user cfg))
      (getId [_] id)
      (getUrl [_] (if-not (empty? server)
                    server
                    (:url cfg)))
      (getPwd [_] (str pwdObj)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defonce Postgresql :postgresql)
(defonce SQLServer :sqlserver)
(defonce Oracle :oracle)
(defonce MySQL :mysql)
(defonce H2 :h2)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defonce DBTYPES {
    :sqlserver { :test-string "select count(*) from sysusers" }
    :postgresql { :test-string "select 1" }
    :mysql { :test-string "select version()" }
    :h2  { :test-string "select 1" }
    :oracle { :test-string "select 1 from DUAL" }
  })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ^:no-doc DbioError

  "Throw a DBIOError execption"

  [^String msg]

  (trap! DBIOError msg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioScopeType

  "Scope a type id"

  [t]

  (keyword (str *ns* "/" t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeGetVendor

  "Try to detect the database vendor"

  [^String product]

  (let [fc #(Embeds? %2 %1)
        lp (lcase product)]
    (condp fc lp
      "microsoft" :sqlserver
      "postgres" :postgresql
      "oracle" :oracle
      "mysql" :mysql
      "h2" :h2
      (DbioError (str "Unknown db product " product)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn Concise

  "Extract key attributes from this object"

  [obj]

  {:rowid (:rowid (meta obj))
   :verid (:verid (meta obj)) } )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fmtfkey ""

  [p1 p2]

  (keyword (str "fk_" (name p1) "_" (name p2))) )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn MatchDbType

  "Ensure the database type is supported"

  [^String dbtype]

  (let [kw (keyword (lcase dbtype)) ]
    (when
      (some? (DBTYPES kw)) kw)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn MatchJdbcUrl

  "From the jdbc url, get the database type"

  [^String url]

  (let [ss (StringUtils/split url \:) ]
    (when
      (> (alength ss) 1)
      (MatchDbType (aget ss 1)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; data modelling
;;
(defonce JOINED-MODEL-MONIKER :czc.dbio.core/DBIOJoinedModel)
(defonce BASEMODEL-MONIKER :czc.dbio.core/DBIOBaseModel)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioModel

  "Define a generic database model"

  ([^String nm] (DbioModel (str *ns*) nm))

  ([^String nsp ^String nm]
   {
    :id (keyword (str nsp "/" nm))
    :table (ucase nm)
    :parent nil
    :abstract false
    :system false
    :mxm false
    :indexes {}
    :uniques {}
    :fields {}
    :assocs {} }) )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro DefModel2

  "Define a data model (with namespace)"

  [nsp modelname & body]

  `(def ~modelname
     (-> (DbioModel ~nsp
                    ~(name modelname))
         ~@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro DefModel

  "Define a data model"

  [modelname & body]

  `(def ~modelname
     (-> (DbioModel ~(name modelname))
         ~@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro DefJoined2

  "Define a joined data model"

  [nsp modelname lhs rhs]

  `(def ~modelname
      (-> (DbioModel ~nsp ~(name modelname))
          (WithDbParentModel JOINED-MODEL-MONIKER)
          (WithDbJoinedModel ~lhs ~rhs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro DefJoined

  "Define a joined data model"

  [modelname lhs rhs]

  `(def ~modelname
      (-> (DbioModel ~(name modelname))
          (WithDbParentModel JOINED-MODEL-MONIKER)
          (WithDbJoinedModel ~lhs ~rhs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbParentModel

  "Give a parent to the model"

  [pojo par]

  {:pre [(map? pojo) (keyword? par)]}

  (assoc pojo :parent par))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbJoinedModel

  "A special model with 2 assocs,
   left hand side and right hand side"

  [pojo lhs rhs]

  {:pre [(map? pojo) (keyword? lhs) (keyword? rhs)]}

  (let [a1 {:kind :MXM :other lhs :fkey :lhs_rowid}
        a2 {:kind :MXM :other rhs :fkey :rhs_rowid}
        m2 (-> (:assocs pojo)
               (assoc :lhs a1)
               (assoc :rhs a2)) ]
    (-> pojo
        (assoc :assocs m2)
        (assoc :mxm true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbTablename

  "Set the table name"

  [pojo tablename]

  {:pre [(map? pojo)]}

  (assoc pojo :table (ucase tablename)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbIndexes

  "Set indexes to the model"

  [pojo indices]

  {:pre [(map? pojo) (map? indices)]}

  ;;turn indices from array to set
  (let [m (reduce
            #(assoc %1
                    (first %2)
                    (into (ordered-set) (last %2)))
            {}
            indices)]
    (Interject pojo :indexes #(merge (%2 %1) m))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbUniques

  "Set uniques to the model"

  [pojo uniqs]

  {:pre [(map? pojo) (map? uniqs)]}

  (let [m (reduce
            #(assoc %1
                    (first %2)
                    (into (ordered-set) (last %2)))
            {}
            uniqs)]
    (Interject pojo :uniques #(merge (%2 %1) m))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- getDftFldObj

  "The base field structure"

  [fid]

  {:column (ucase (name fid))
   :domain :String
   :size 255
   :id (keyword fid)
   :assoc-key false
   :pkey false
   :null true
   :auto false
   :dft nil
   :updatable true
   :system false
   :index "" } )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbField

  "Create a new field"

  [pojo fid fdef]

  {:pre [(map? pojo) (map? fdef)]}

  (let [fd (merge (getDftFldObj fid) fdef)
        k (:id fd)]
    (Interject pojo :fields #(assoc (%2 %1) k fd))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbFields

  "Create a batch of fields"

  [pojo flddefs]

  {:pre [(map? pojo) (coll? flddefs)]}

  (with-local-vars [rcmap pojo]
    (doseq [[k v] (seq flddefs) ]
      (var-set rcmap (WithDbField @rcmap k v)))
    @rcmap))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbAssoc

  "Set an association"

  [pojo aid adef]

  {:pre [(map? pojo) (map? adef)]}

  (let [ad (merge {:kind nil :other nil
                   :fkey nil :cascade false} adef)
        a2 (case (:kind ad)
             (:O2O :O2M)
             (assoc ad :fkey (fmtfkey (:id pojo) aid))
             (:M2M :MXM)
             ad
             ;;else
             (DbioError (str "Invalid assoc def " adef)))
        k (keyword aid)]
    (Interject pojo :assocs #(assoc (%2 %1) k a2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbAssocs

  "Set a batch of associations"

  [pojo assocs]

  {:pre [(map? pojo) (coll? assocs)]}

  (with-local-vars [rcmap pojo ]
    (doseq [[k v] (seq assocs) ]
      (var-set rcmap (WithDbAssoc @rcmap k v)))
    @rcmap))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn WithDbAbstract

  "Set the model as abstract"

  [pojo]

  {:pre [(map? pojo)]}

  (assoc pojo :abstract true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- WithDbSystem

  "This is a built-in system level model"

  [pojo]

  (assoc pojo :system true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nested-merge

  "Merge either a set or map"

  [src des]

  (cond
    (and (set? src)(set? des)) (cset/union src des)
    (and (map? src)(map? des)) (merge src des)
    :else des))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the base model here
(DefModel2 _NSP DBIOBaseModel
  (WithDbAbstract)
  (WithDbSystem)
  (WithDbFields
    {:rowid {:column COL_ROWID :pkey true :domain :Long
             :auto true :system true :updatable false}
     :verid {:column COL_VERID :domain :Long :system true
             :dft [ 0 ] }
     :last-modify {:column COL_LASTCHANGED :domain :Timestamp
                   :system true :dft [""] }
     :created-on {:column COL_CREATED_ON :domain :Timestamp
                  :system true :dft [""] :updatable false}
     :created-by {:column COL_CREATED_BY :system true :domain :String } }))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(DefModel2 _NSP DBIOJoinedModel
  (WithDbAbstract)
  (WithDbSystem)
  (WithDbFields
    {:lhs-typeid {:column COL_LHS_TYPEID }
     :lhs-oid {:column COL_LHS_ROWID :domain :Long :null false}
     :rhs-typeid {:column COL_RHS_TYPEID }
     :rhs-oid {:column COL_RHS_ROWID :domain :Long :null false} }) )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbSchema*

  "A schema holds the set of models"

  ^Schema
  [theModels]

  (reify

    Schema

    (getModels [_] theModels)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolve-assocs

  "Walk through all models, for each model, study its assocs.
  For o2o or o2m assocs, we need to artificially inject a new
  field/column into the model (foreign key)"

  ;; map of models
  [ms]

  (with-local-vars
    [fdef {:domain :Long :assoc-key true }
     rc (transient {})
     xs (transient {}) ]
    ;; create placeholder maps for each model,
    ;; to hold new fields from assocs
    (doseq [[k m] ms]
      (var-set rc (assoc! @rc k {} )))
    ;; as we find new assoc fields, add them to the placeholder maps
    (doseq [[k m]  ms]
      (doseq [[x s] (:assocs m) ]
        (case (:kind s)
          (:O2O :O2M)
          (let [fid (keyword (:fkey s))
                rhs (@rc (:other s))
                ft (merge (getDftFldObj fid) @fdef) ]
            (var-set rc (assoc! @rc (:other s) (assoc rhs fid ft))))
          nil)))
    ;; now walk through all the placeholder maps and merge those new
    ;; fields to the actual models
    (let [tm (persistent! @rc) ]
      (doseq [[k v] tm]
        (let [mcz (ms k)
              fs (:fields mcz) ]
          (var-set xs (assoc! @xs
                              k
                              (assoc mcz :fields (merge fs v))))))
      (persistent! @xs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- resolve-parent

  "Ensure that all user defined models are
   all derived from the system base model"

  ;; map of models
  ;; model defn
  [mcz model]

  (let [par (:parent model) ]
    (cond
      (keyword? par)
      (if (nil? (mcz par))
        (DbioError (str "Unknown parent model " par))
        model)

      (nil? par)
      (assoc model :parent BASEMODEL-MONIKER)

      :else
      (DbioError (str "Invalid parent " par)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Idea is walk through all models and ultimately
;; link it's root to base-model
(defn- resolve-parents ""

  ;; map of models
  [ms]

  (persistent!
    (reduce
      #(let [rc (resolve-parent ms (last %2)) ]
         (assoc! %1 (:id rc) rc))
      (transient {})
      (seq ms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mapize-models

  "Turn the list of models into a map of models,
   keyed by the model id"

  ;; list of models
  [ms]

  (persistent!
    (reduce #(assoc! %1 (:id %2) %2)
            (transient {})
            (seq ms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- collect-db-xxx-filter ""

  [k a b]

  (cond
    (keyword? b)
    :keyword

    (map? b)
    :map

    :else
    (DbioError (str "Invalid arg " b))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti CollectDbXXX collect-db-xxx-filter)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod CollectDbXXX :keyword

  [kw cache modelid]

  (if-some [mcz (cache modelid) ]
    (CollectDbXXX kw cache mcz)
    (log/warn "Unknown database model id: %s" modelid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod CollectDbXXX :map

  [kw cache mcz]

  (if-some [par (:parent mcz) ]
    (merge {} (CollectDbXXX kw cache par)
              (kw mcz))
    (merge {} (kw mcz))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- colmap-fields

  "Create a map of fields keyed by the column name"

  [flds]

  (with-local-vars [sum (transient {}) ]
    (doseq [[k v] flds]
      (let [cn (ucase (str (:column v))) ]
        (var-set sum (assoc! @sum cn v))))
    (persistent! @sum)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- meta-models

  "Inject extra meta-data properties into each model.  Each model will have
   its (complete) set of fields keyed by column nam or field id"

  [cache]

  (with-local-vars [sum (transient {}) ]
    (doseq [[k m] cache]
      (let [flds (CollectDbXXX :fields cache m)
            cols (colmap-fields flds) ]
        (var-set sum
                 (assoc! @sum k
                         (with-meta m
                                    {:columns cols
                                     :fields flds } ) ))))
    (persistent! @sum)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn MetaCache*

  "A cache storing meta-data for all models"

  ^MetaCache
  [^Schema schema]

  (let [ms (if (nil? schema)
             {}
             (mapize-models (.getModels schema)))
        m2 (if (empty? ms)
             {}
             (-> (assoc ms JOINED-MODEL-MONIKER DBIOJoinedModel)
                 (resolve-parents)
                 (resolve-assocs)
                 (assoc BASEMODEL-MONIKER DBIOBaseModel)
                 (meta-models))) ]
    (reify

      MetaCache

      (getMetas [_] m2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbgShowMetaCache ""

  ^String
  [^MetaCache mcache]

  (reduce #(AddDelim! %1
                      "\n"
                      (WriteEdnString {:TABLE (:table %2)
                                       :DEFN %2
                                       :META (meta %2)}))
          (StringBuilder.)
          (vals (.getMetas mcache))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- safeGetConn

  "Safely connect to database referred by this jdbc"

  ^Connection
  [^JDBCInfo jdbc]

  (let [user (.getUser jdbc)
        dv (.getDriver jdbc)
        url (.getUrl jdbc)
        d (if (hgl? url) (DriverManager/getDriver url))
        p (if (hgl? user)
            (doto (Properties.)
              (.put "password" (str (.getPwd jdbc)))
              (.put "user" user)
              (.put "username" user))
            (Properties.)) ]
    (when (nil? d) (DbioError (str "Can't load Jdbc Url: " url)))
    (when (and (hgl? dv)
               (not= (-> d (.getClass) (.getName)) dv))
      (log/warn "expected %s, loaded with driver: %s" dv (.getClass d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbConnection*

  "Connect to database referred by this jdbc"

  ^Connection
  [^JDBCInfo jdbc]

  (let [url (.getUrl jdbc)
        ^Connection
        conn (if (hgl? (.getUser jdbc))
               (safeGetConn jdbc)
               (DriverManager/getConnection url)) ]
    (when (nil? conn)
          (DbioError (str "Failed to create db connection: " url)))
    (doto conn
      (.setTransactionIsolation Connection/TRANSACTION_SERIALIZABLE))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn TestConnection ""

  [jdbc]

  (tryc (.close (DbConnection* jdbc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti ResolveVendor "Find out the type of database" class)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod ResolveVendor JDBCInfo

  [jdbc]

  (with-open [conn (DbConnection* jdbc) ]
    (ResolveVendor conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod ResolveVendor Connection

  [^Connection conn]

  (let [md (.getMetaData conn) ]
    (-> {:id (maybeGetVendor (.getDatabaseProductName md)) }
        (assoc :version (.getDatabaseProductVersion md))
        (assoc :name (.getDatabaseProductName md))
        (assoc :quote-string (.getIdentifierQuoteString md))
        (assoc :url (.getURL md))
        (assoc :user (.getUserName md))
        (assoc :lcis (.storesLowerCaseIdentifiers md))
        (assoc :ucis (.storesUpperCaseIdentifiers md))
        (assoc :mcis (.storesMixedCaseIdentifiers md)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti TableExist?
  "Is this table defined in db?" (fn [a b] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod TableExist? JDBCPool

  [^JDBCPool pool ^String table]

  (with-open [conn (.nextFree pool) ]
    (TableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod TableExist? JDBCInfo

  [jdbc ^String table]

  (with-open [conn (DbConnection* jdbc) ]
    (TableExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod TableExist? Connection

  [^Connection conn ^String table]

  (with-local-vars [rc false]
    (log/debug "testing the existence of table: %s" table)
    (trylet!
      [mt (.getMetaData conn)
       tbl (cond
             (.storesUpperCaseIdentifiers mt)
             (ucase table)
             (.storesLowerCaseIdentifiers mt)
             (lcase table)
             :else table) ]
      (with-open [res (.getColumns mt nil nil tbl nil) ]
        (when (and (some? res)
                   (.next res))
          (var-set rc true))))
    @rc))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti RowExist?
  "Is there any rows in the table?" (fn [a b] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod RowExist? JDBCInfo

  [jdbc ^String table]

  (with-open [conn (DbConnection* jdbc) ]
    (RowExist? conn table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod RowExist? Connection

  [^Connection conn ^String table]

  (with-local-vars [rc false ]
    (trylet!
      [sql (str "SELECT COUNT(*) FROM  "
                (ucase table)) ]
      (with-open [stmt (.createStatement conn)
                  res (.executeQuery stmt sql) ]
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

  (with-local-vars
    [pkeys #{} cms {} ]
    (with-open [rs (.getPrimaryKeys mt
                                    catalog
                                    schema table) ]
      (loop [sum (transient #{})
             more (.next rs) ]
        (if (not more)
          (var-set pkeys (persistent! sum))
          (recur
            (conj! sum (ucase (.getString rs (int 4))) )
            (.next rs)))))
    (with-open [rs (.getColumns mt
                                catalog
                                schema table "%") ]
      (loop [sum (transient {})
             more (.next rs) ]
        (if (not more)
          (var-set cms (persistent! sum))
          (let [opt (not= (.getInt rs (int 11))
                          DatabaseMetaData/columnNoNulls)
                cn (ucase (.getString rs (int 4)))
                ctype (.getInt rs (int 5)) ]
            (recur
              (assoc! sum (keyword cn)
                      {:column cn :sql-type ctype :null opt
                       :pkey (clojure.core/contains? @pkeys cn) })
              (.next rs))))))
    (with-meta @cms {:supportsGetGeneratedKeys
                     (.supportsGetGeneratedKeys mt)
                     :supportsTransactions
                     (.supportsTransactions mt) })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn LoadTableMeta

  "Fetch metadata of this table from db"

  [^Connection conn ^String table]

  (let [dbv (ResolveVendor conn)
        mt (.getMetaData conn)
        catalog nil
        schema (if (= (:id dbv) :oracle) "%" nil)
        tbl (cond
              (.storesUpperCaseIdentifiers mt)
              (ucase table)
              (.storesLowerCaseIdentifiers mt)
              (lcase table)
              :else table) ]
    ;; not good, try mixed case... arrrrrrrrrrhhhhhhhhhhhhhh
    ;;rs = m.getTables( catalog, schema, "%", null)
    (load-columns mt catalog schema tbl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- makePool ""

  ^JDBCPool
  [^JDBCInfo jdbc ^BoneCP impl]

  (let [dbv (ResolveVendor jdbc) ]
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
            (DbioError (str "No free connection"))))))))

      ;;Object
      ;;Clojure CLJ-1347
      ;;finalize won't work *correctly* in reified objects - document
      ;;(finalize [this]
        ;;(try!
          ;;(log/debug "DbPool finalize() called.")
          ;;(.shutdown this)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbPool*

  "Create a db connection pool"

  [^JDBCInfo jdbc options]

  (let [options (or options {})
        dv (.getDriver jdbc)
        bcf (BoneCPConfig.) ]

    ;;(log/debug "URL: %s"  (.getUrl jdbc))
    ;;(log/debug "Driver: %s" dv)
    ;;(log/debug "Options: %s" options)

    (when (hgl? dv) (ForName dv))
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
(defn- splitLines ""

  [^String lines]

  (with-local-vars
    [w (.length DDL_SEP)
     rc []
     s2 lines ]
    (loop [sum (transient [])
           ddl lines
           pos (.indexOf ddl DDL_SEP) ]
      (if (< pos 0)
        (do (var-set rc (persistent! sum))
            (var-set s2 (strim ddl)))
        (let [nl (strim (.substring ddl 0 pos))
              d2 (.substring ddl (+ pos @w))
              p2 (.indexOf d2 DDL_SEP) ]
          (recur (conj! sum nl) d2 p2))))
    (if (hgl? @s2)
      (conj @rc @s2)
      @rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeOK ""

  [^String dbn ^Throwable e]

  (let [oracle (Embeds? (str dbn) "oracle")
        ee (RootCause e)
        ec (when (instance? SQLException ee)
             (.getErrorCode ^SQLException ee)) ]
    (if (nil? ec)
      (throw e)
      (cond
        (and oracle (== 942 ec)
             (== 1418 ec)
             (== 2289 ec)(== 0 ec))
        true
        :else
        (throw e)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti UploadDdl "Upload DDL to DB" (fn [a b] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod UploadDdl JDBCPool

  [^JDBCPool pool ^String ddl]

  (with-open [conn (.nextFree pool) ]
     (UploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod UploadDdl JDBCInfo

  [jdbc ^String ddl]

  (with-open [conn (DbConnection* jdbc) ]
     (UploadDdl conn ddl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod UploadDdl Connection

  [^Connection conn ^String ddl]

  (log/debug "\n%s" ddl)
  (let [dbn (lcase (-> (.getMetaData conn)
                       (.getDatabaseProductName)))
        lines (splitLines ddl) ]
    (.setAutoCommit conn true)
    (doseq [^String line (seq lines)
            :let
            [ln (StringUtils/strip (strim line) ";") ]
            :when (and (hgl? ln)
                       (not= (lcase ln) "go"))]
      (try
        (with-open [stmt (.createStatement conn) ]
          (.executeUpdate stmt ln))
        (catch SQLException e#
          (maybeOK dbn e#))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioCreateObj

  "Creates a blank object of the given type
   model : keyword, the model type id"

  [modelid]

  {:pre [(keyword? modelid)]}

  (with-meta {} { :typeid modelid } ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioSetFlds

  "Set many field values"

  [pojo fld value & fvs]

  (apply assoc pojo fld value fvs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioSetFld*

  "Set many field values"

  [pojo fvs]

  {:pre [(map? fvs)]}

  (if-not (empty? fvs)
    (let [a (flatten (seq fvs))]
      (apply DbioSetFlds pojo (first a)(second a) (drop 2 a)))
    pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioSetFld

  "Set value to a field"

  [pojo fld value]

  (assoc pojo fld value))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioClrFld

  "Remove a field"

  [pojo fld]

  (dissoc pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioGetFld

  "Get value of a field"

  [pojo fld]

  (get pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioGetAssoc

  "Get the assoc definition
   cache : meta cache
   mcz : the model
   id : assoc id"

  [cache mcz id]

  (when (some? mcz)
    (let [rc ((:assocs mcz) id) ]
      (if (nil? rc)
        (DbioGetAssoc cache (cache (:parent mcz)) id)
        rc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; handling assocs
(defn- dbio-get-o2x ""

  [ctx lhsObj]

  (let [^SQLr sqlr (:with ctx)
        mcache (.metas sqlr)
        mcz (mcache (GetTypeId lhsObj))
        ac (DbioGetAssoc mcache mcz (:as ctx))
        rt (:cast ctx)
        fid (:fkey ac)
        fv (:rowid (meta lhsObj)) ]
    (if (nil? ac)
      (DbioError "Unknown assoc " (:as ctx))
      [ sqlr (or rt (:other ac)) {fid fv} ])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- dbio-set-o2x ""

  [ctx lhsObj rhsObj]

  (let [^SQLr sqlr (:with ctx)
        mcache (.metas sqlr)
        mcz (mcache (GetTypeId lhsObj))
        ac (DbioGetAssoc mcache mcz (:as ctx))
        fv (:rowid (meta lhsObj))
        fid (:fkey ac) ]
    (when (nil? ac) (DbioError "Unknown assoc " (:as ctx)))
    (let [x (-> (DbioCreateObj (GetTypeId rhsObj))
                (DbioSetFld fid fv)
                (vary-meta MergeMeta (Concise rhsObj)))
          y (.update sqlr x) ]
      [ lhsObj (merge y (dissoc rhsObj fid)) ])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioGetO2M

  "One to many assocs"

  [ctx lhsObj]

  (let [[sql rt pms] (dbio-get-o2x ctx lhsObj) ]
    (when (some? rt)
      (.findSome ^SQLr sql rt pms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioSetO2M ""

  [ctx lhsObj rhsObj]

  (dbio-set-o2x ctx lhsObj rhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioAddO2M ""

  [ctx lhsObj rhsObjs ]

  (with-local-vars [rc (transient []) ]
    (doseq [r rhsObjs]
      (var-set rc
               (conj! @rc
                      (last (dbio-set-o2x ctx lhsObj r)))))
    [ lhsObj (persistent! @rc) ]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioClrO2M ""

  [ctx lhsObj]

  (let [^SQLr sqlr (:with ctx)
        mcache (.metas sqlr)
        mcz (mcache (GetTypeId lhsObj))
        ac (DbioGetAssoc mcache mcz (:as ctx))
        fv (:rowid (meta lhsObj))
        rt (:cast ctx)
        rp (or rt (:other ac))
        fid (:fkey ac) ]
    (when (nil? ac)(DbioError "Unknown assoc " (:as ctx)))
    (if (:cascade ac)
      (.exec sqlr (str "delete from "
                       (GTable (mcache rp))
                       " where " (ese fid) " = ?") [fv])
      (.exec sqlr (str "update "
                       (GTable (mcache rp))
                       " set " (ese fid) " = NULL "
                       " where " (ese fid) " = ?") [fv]))
    lhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioGetO2O

  "One to one assocs"

  [ctx lhsObj]

  (let [[sql rt pms] (dbio-get-o2x ctx lhsObj) ]
    (.findOne ^SQLr sql rt pms)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioSetO2O ""

  [ctx lhsObj rhsObj]

  (dbio-set-o2x ctx lhsObj rhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioClrO2O ""

  [ctx lhsObj]

  (let [^SQLr sqlr (:with ctx)
        mcache (.metas sqlr)
        mcz (mcache (GetTypeId lhsObj))
        ac (DbioGetAssoc mcache mcz (:as ctx))
        fv (:rowid (meta lhsObj))
        rt (:cast ctx)
        fid (:fkey ac) ]
    (when (nil? ac) (DbioError "Unknown assoc " (:as ctx)))
    (let [y (.findOne sqlr
                      (or rt (:other ac))
                      { fid fv } ) ]
      (when (some? y)
        (let [x (vary-meta (-> (DbioCreateObj (GetTypeId y))
                               (DbioSetFld fid nil))
                            MergeMeta (meta y)) ]
          (if (:cascade ac)
            (.delete sqlr x)
            (.update sqlr x)))))
    lhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioSetM2M

  "Many to many assocs"

  [ctx lhsObj rhsObj]

  (let [^SQLr sqlr (:with ctx)
        mcache (.metas sqlr)
        rv (:rowid (meta rhsObj))
        lv (:rowid (meta lhsObj))
        lid (GetTypeId lhsObj)
        rid (GetTypeId rhsObj)
        mcz (mcache lid)
        ac (DbioGetAssoc mcache mcz (:as ctx))
        mm (mcache (:joined ac))
        rl (:other (:rhs (:assocs mm)))
        ml (:other (:lhs (:assocs mm)))
        x (DbioCreateObj (:id mm))
        y  (if (= ml lid)
              (-> x
                  (DbioSetFld :lhs-typeid (StripNSPath lid))
                  (DbioSetFld :lhs-oid lv)
                  (DbioSetFld :rhs-typeid (StripNSPath rid))
                  (DbioSetFld :rhs-oid rv))
              (-> x
                  (DbioSetFld :lhs-typeid (StripNSPath rid))
                  (DbioSetFld :lhs-oid rv)
                  (DbioSetFld :rhs-typeid (StripNSPath lid))
                  (DbioSetFld :rhs-oid lv))) ]
    (.insert sqlr y)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioClrM2M

  ([ctx lhsObj] (DbioClrM2M ctx lhsObj nil))

  ([ctx lhsObj rhsObj]
    (let [^SQLr sqlr (:with ctx)
          mcache (.metas sqlr)
          rv (:rowid (meta rhsObj))
          lv (:rowid (meta lhsObj))
          lid (GetTypeId lhsObj)
          rid (GetTypeId rhsObj)
          mcz (mcache lid)
          ac (DbioGetAssoc mcache mcz (:as ctx))
          mm (mcache (:joined ac))
          flds (:fields (meta mm))
          rl (:other (:rhs (:assocs mm)))
          ml (:other (:lhs (:assocs mm)))
          [x y a b]
          (if (= ml lid)
              [ (:column (:lhs-oid flds)) (:column (:lhs-typeid flds))
                (:column (:rhs-oid flds)) (:column (:rhs-typeid flds)) ]
              [ (:column (:rhs-oid flds)) (:column (:rhs-typeid flds))
                (:column (:lhs-oid flds)) (:column (:lhs-typeid flds)) ]) ]
      (when (nil? ac) (DbioError "Unkown assoc " (:as ctx)))
      (if (nil? rhsObj)
        (.exec sqlr
               (str "delete from "
                    (GTable mm)
                    " where " (ese x) " =? and " (ese y) " =?")
               [ lv (StripNSPath lid) ] )
        (.exec sqlr
               (str "delete from "
                    (GTable mm)
                    " where " (ese x) " =? and " (ese y) " =? and "
                    (ese a)
                    " =? and "
                    (ese b)
                    " =?" )
               [ lv (StripNSPath lid) rv (StripNSPath rid) ])) )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioGetM2M ""

  [ctx lhsObj]

  (let [^SQLr sqlr (:with ctx)
        mcache (.metas sqlr)
        lv (:rowid (meta lhsObj))
        lid (GetTypeId lhsObj)
        eseRES (ese "RES")
        eseMM (ese "MM")
        mcz (mcache lid)
        ac (DbioGetAssoc mcache mcz (:as ctx))
        mm (mcache (:joined ac))
        flds (:fields (meta mm))
        rl (:other (:rhs (:assocs mm)))
        ml (:other (:lhs (:assocs mm)))
        [x y z k a t]
        (if (= ml lid)
          [:lhs-typeid :rhs-typeid :lhs-oid :rhs-oid ml rl]
          [:rhs-typeid :lhs-typeid :rhs-oid :lhs-oid rl ml] ) ]
    (when (nil? ac) (DbioError "Unknown assoc " (:as ctx)))
    (.select sqlr
             t
             (str "select distinct "
                  eseRES
                  ".* from "
                  (GTable (get mcache t))
                  " " eseRES " join " (GTable mm) " " eseMM " on "
                  (str eseMM "." (ese (:column (x flds))))
                  "=? and "
                  (str eseMM "." (ese (:column (y flds))))
                  "=? and "
                  (str eseMM "." (ese (:column (z flds))))
                  "=? and "
                  (str eseMM "." (ese (:column (k flds))))
                  " = " (str eseRES "." (ese (:column (:rowid flds)))))
                  [ (StripNSPath a) (StripNSPath t) lv ])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

