;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database and modeling functions."
      :author "Kenneth Leung"}

  czlab.horde.core

  (:require [czlab.basal.util :as u]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.str :as s]
            [czlab.basal.io :as i]
            [czlab.basal.log :as l]
            [czlab.basal.meta :as m]
            [czlab.basal.core :as c])

  (:use [flatland.ordered.set])

  (:import [java.util HashMap TimeZone Properties GregorianCalendar]
           [clojure.lang Keyword APersistentMap APersistentVector]
           [com.zaxxer.hikari HikariConfig HikariDataSource]
           [java.sql
            SQLException
            Connection
            Driver
            DriverManager
            DatabaseMetaData]
           [java.lang Math]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
(def ^:private REL-TYPES #{:o2o :o2m})
(def ^:dynamic *ddl-cfg* nil)
(def ^:dynamic *ddl-bvs* nil)
(def ddl-sep "-- :")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Transactable
  (transact! [_ func]
             [_ func cfg] "Run this function inside a transaction"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol SQLr
  (find-some [_ model filters]
             [_ model filters extras] "")
  (find-all [_ model]
            [_ model extras] "")
  (find-one [_ model filters] "")
  (fmt-id [_ s] "")
  (mod-obj [_ obj] "")
  (del-obj [_ obj] "")
  (add-obj [_ obj] "")
  (exec-with-output [_ sql params] "")
  (exec-sql [_ sql params] "")
  (count-objs [_ model] "")
  (purge-objs [_ model] "")
  (select-sql [_ sql params]
              [_ model sql params] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol JdbcPoolApi
  (p-close [_] "Shut down this pool")
  (p-next [_] "Next free connection from the pool"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DbioField [])
(defrecord DbioModel [])
(defrecord JdbcSpec [])
(defrecord DbioAssoc [])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private relok? [x] `(contains? REL-TYPES ~x))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dberr!
  "Throw a SQL execption."
  [fmt & more]
  (c/trap! SQLException (str (apply format fmt more))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private mkrel
  [& args] `(merge (dft-rel<>) (hash-map ~@args )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private mkfld
  [& args] `(merge (dft-fld<>) (hash-map ~@args )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbmodel<>
  "Define a data model inside dbschema<>."
  [name & body]
  `(-> (czlab.horde.core/dbdef<> ~name) ~@body))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro tstamp<>
  "Sql timestamp."
  [] `(java.sql.Timestamp. (.getTime (java.util.Date.))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- clean-name
  [s]
  (str (some-> s name (cs/replace #"[^a-zA-Z0-9_-]" "") (cs/replace  #"-" "_"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gmodel
  "Get object's model." [obj] `(:model (meta ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gtype
  "Get object's type." [obj] `(:id (czlab.horde.core/gmodel ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro goid
  "Get object's id."
  [obj] `(let [o# ~obj
               pk# (:pkey (czlab.horde.core/gmodel o#))] (pk# o#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gschema "Get schema." [model] `(:schema (meta ~model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro find-model
  "Get model from schema."
  [schema typeid] `(get (:models (deref ~schema)) ~typeid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro find-field
  "Get field-def from model."
  [model fieldid] `(get (:fields ~model) ~fieldid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro find-assoc
  "Get assoc-def from model."
  [model relid] `(get (:rels ~model) ~relid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti fmt-sqlid
  "Format SQL identifier." {:tag String} (fn [a & _] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod fmt-sqlid
  APersistentMap
  ([info idstr] (fmt-sqlid info idstr nil))
  ([info idstr quote?]
   (let [{:keys [qstr ucs? lcs?]} info
         ch (s/strim qstr)
         id (cond ucs? (s/ucase idstr)
                  lcs? (s/lcase idstr) :else idstr)]
     (if (false? quote?) id (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod fmt-sqlid
  DatabaseMetaData
  ([mt idstr] (fmt-sqlid mt idstr nil))
  ([^DatabaseMetaData mt idstr quote?]
   (let [ch (s/strim (.getIdentifierQuoteString mt))
         id (cond
              (.storesUpperCaseIdentifiers mt) (s/ucase idstr)
              (.storesLowerCaseIdentifiers mt) (s/lcase idstr)
              :else idstr)]
     (if (false? quote?) id (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod fmt-sqlid
  Connection
  ([conn idstr] (fmt-sqlid conn idstr nil))
  ([^Connection conn idstr quote?]
   (fmt-sqlid (.getMetaData conn) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; have to be function , not macro as this is passed into another higher
;; function - merge.
(defn- merge-meta
  "Merge 2 meta maps"
  [m1 m2] {:pre [(map? m1)
                 (or (nil? m2)(map? m2))]} (merge m1 m2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbtag
  "The id (type) for this model."
  {:tag Keyword}
  ([model] (:id model))
  ([typeid schema]
   (dbtag (find-model schema typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbtable
  "The table-name for this model."
  {:tag String}
  ([model] (:table model))
  ([typeid schema]
   (dbtable (find-model schema typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbcol
  "The column-name for this field."
  {:tag String}
  ([fdef] (:column fdef))
  ([fid model]
   (dbcol (find-field model fid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord JdbcPool []
  JdbcPoolApi
  (p-close [me]
    (c/let#nil [{:keys [impl]} me]
      (l/debug "finz: %s" impl)
      (.close ^HikariDataSource impl)))
  (p-next [me]
    (let [{:keys [impl]} me]
      (try (.getConnection ^HikariDataSource impl)
           (catch Throwable _
             (dberr! "No free connection.") nil)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn jdbc-pool<>
  "A Connection Pool." [vendor jdbc impl]
  (assoc (JdbcPool.)
         :vendor vendor :jdbc jdbc :impl impl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbspec<>
  "Basic jdbc parameters."
  ([url] (dbspec<> nil url nil nil))
  ([driver url user passwd]
   (assoc (JdbcSpec.)
          :driver (str driver)
          :user (str user)
          :url (str url)
          :passwd (i/x->chars passwd)
          :id (str "jdbc#" (u/seqint2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn load-driver ^Driver [spec]
  (c/if-string [s (:url spec)] (DriverManager/getDriver s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def Postgresql :postgresql)
(def Postgres :postgres)
(def SQLServer :sqlserver)
;;(def SQLServer :mssql)
(def Oracle :oracle)
(def MySQL :mysql)
(def H2 :h2)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:dynamic *db-types*
  {SQLServer {:test-string "select count(*) from sysusers" }
   Postgresql {:test-string "select 1" }
   Postgres {:test-string "select 1" }
   MySQL {:test-string "select version()" }
   H2 {:test-string "select 1" }
   Oracle {:test-string "select 1 from DUAL" } })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- maybe-get-vendor
  "Detect the database vendor."
  [product]
  (let [fc #(s/embeds? %2 %1)
        lp (s/lcase product)]
    (condp fc lp
      "microsoft" SQLServer
      "postgres" Postgresql
      "oracle" Oracle
      "mysql" MySQL
      "h2" H2
      (dberr! "Unknown db product: %s." product))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private fmt-fkey
  "For o2o & o2m relations"
  [tn rn] `(s/x->kw "fk_" (name ~tn) "_" (name ~rn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn match-spec??
  "Ensure db-type is supported."
  ^Keyword [spec]
  (let [kw (keyword (s/lcase spec))]
    (if (contains? *db-types* kw) kw nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn match-url??
  "From jdbc url, get db-type."
  ^Keyword [dburl]
  (c/if-some+ [ss (.split (str dburl) ":")]
    (if (> (alength ss) 1) (match-spec?? (aget ss 1)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA MODELING
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dft-fld<>
  ([] (dft-fld<> nil))
  ([fid]
   (merge (DbioField.)
          {:domain :String
           :id fid
           :size 255
           :rel-key? false
           :null? true
           :auto? false
           :dft nil
           :system? false
           :updatable? true
           :column (clean-name fid)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:private pkey-meta (mkfld :updatable? false
                                :domain :Long
                                :id :rowid
                                :auto? true
                                :system? true
                                :column "must be set!"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbdef<>
  "Define a generic model. *internal*"
  ([mname] (dbdef<> mname nil))
  ([mname options]
   {:pre [(c/is-scoped-keyword? mname)]}
   (merge (DbioModel.)
          {:abstract? false
           :system? false
           :mxm? false
           :pkey :rowid
           :indexes {}
           :rels {}
           :uniques {}
           :id mname
           :fields {:rowid pkey-meta}
           :table (clean-name mname)} options)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dft-rel<>
  ([] (dft-rel<> nil))
  ([id]
   (merge (DbioAssoc.)
          {:other nil :fkey nil :kind nil})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbfield<>
  "Add a new field."
  [pojo fid fdef]
  {:pre [(keyword? fid)(map? fdef)]}
  (update-in pojo
             [:fields]
             assoc
             fid
             (merge (dft-fld<> fid)
                    (dissoc fdef :id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbfields
  "Add a bunch of fields."
  [pojo flddefs]
  {:pre [(map? flddefs)]}
  (reduce #(let [[k v] %2] (dbfield<> %1 k v)) pojo flddefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn with-joined
  "A special model with 2 relations,
   left hand side and right hand side. *internal*"
  [pojo lhs rhs]
  {:pre [(c/is-scoped-keyword? lhs)
         (c/is-scoped-keyword? rhs)]}
  (let [{{:keys [col-lhs-rowid
                 col-rhs-rowid]} :config} pojo]
    (->
      (dbfields pojo
              {:lhs-rowid (mkfld :column col-lhs-rowid
                                 :domain :Long :null? false)
               :rhs-rowid (mkfld :column col-rhs-rowid
                                 :domain :Long :null? false)})
      (assoc :rels
             {:lhs (mkrel :kind :mxm
                          :other lhs
                          :fkey :lhs-rowid)
              :rhs (mkrel :kind :mxm
                          :other rhs
                          :fkey :rhs-rowid)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbjoined<>
  "Define a joined data model."
  ([modelname lhs rhs]
   `(dbjoined<> modelname
                (czlab.horde.core/dbcfg nil) lhs rhs))
  ([modelname options lhs rhs]
   `(-> (czlab.horde.core/dbdef<> ~modelname {:mxm? true})
        ~options
        (czlab.horde.core/with-joined ~lhs ~rhs)
        (czlab.horde.core/dbuniques {:i1 #{:lhs-rowid :rhs-rowid}}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn with-table
  "Set the table name"
  [pojo table]
  {:pre [(map? pojo)]}
  (assoc pojo :table (clean-name table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;merge new stuff onto old stuff
(defn- with-xxx-sets [pojo kvs fld]
  (update-in pojo
             [fld]
             merge
             (c/preduce<map>
               #(assoc! %1
                        (c/_1 %2)
                        (into (ordered-set) (c/_E %2))) kvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;indices = { :a #{ :f1 :f2} ] :b #{:f3 :f4} }
(defn dbindexes
  "Set indexes to the model."
  [pojo indexes]
  {:pre [(map? indexes)]}
  (with-xxx-sets pojo indexes :indexes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbkey
  "Declare your own primary key"
  [pojo pke]
  (let [{:keys [fields pkey]} pojo
        {:keys [domain id
                auto?
                column size]} pke
        p (pkey fields)
        oid (or id pkey)
        fields (dissoc fields pkey)]
    (assert (and column domain p (= pkey (:id p))))
    (-> (->> (assoc (if-not auto?
                      (dissoc p :auto?)
                      (assoc p :auto? true))
                    :id oid
                    :domain domain
                    :column column
                    :size (c/num?? size 255))
             (assoc fields oid)
             (assoc pojo :fields)) (assoc :pkey oid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;uniques = { :a #{ :f1 :f2 } :b #{ :f3 :f4 } }
(defn dbuniques
  "Set uniques to the model"
  [pojo uniqs]
  {:pre [(map? uniqs)]}
  (with-xxx-sets pojo uniqs :uniques))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbassoc
  "Define an relation between 2 models"
  [{:keys [id] :as pojo} rid rel]
  (let [rd (merge {:cascade? false :fkey nil} rel)]
    (update-in pojo
               [:rels]
               assoc
               rid
               (if (relok? (:kind rd))
                 (assoc rd
                        :fkey (fmt-fkey id rid))
                 (dberr! "Invalid relation: %s." rid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbassocs
  "Define a set of associations"
  [pojo reldefs]
  {:pre [(map? reldefs)]}
  (reduce #(let [[k v] %2] (dbassoc %1 k v)) pojo reldefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private with-abstract
  [pojo flag] `(assoc ~pojo :abstract? ~flag))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private with-system
  [pojo] `(assoc ~pojo :system? true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- check-field? [pojo fld]
  (boolean
    (if-some [f (find-field (gmodel pojo) fld)]
      (not (or (:auto? f)
               (not (:updatable? f)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private mkfkdef<>
  [fid ktype] `(merge (dft-fld<> ~fid)
                      {:rel-key? true :domain ~ktype}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- resolve-assocs
  "Walk through all models, for each model, study its relations.
  For o2o or o2m assocs, we need to artificially inject a new
  field/column into the (other/rhs) model (foreign key)"
  [metas]
  ;; 1st, create placeholder maps for each model,
  ;; to hold new fields from rels
  (with-local-vars
    [xs (c/tmap*)
     phd (c/tmap* (zipmap (keys metas)
                          (repeat {})))]
    ;;as we find new relation fields,
    ;;add them to the placeholders
    (doseq [[_ m] metas
            :let [{:keys [pkey fields
                          rels abstract?]} m
                  kt (:domain (pkey fields))]
            :when (and (not abstract?)
                       (not (empty? rels)))]
      (doseq [[_ r] rels
              :let [{:keys [other kind fkey]} r]
              :when (or (= :o2o kind)
                        (= :o2m kind))]
        (var-set
          phd
          (assoc! @phd
                  other
                  (->> (mkfkdef<> fkey kt)
                       (assoc (@phd other) fkey))))))
    ;;now walk through all the placeholder maps and merge those new
    ;;fields to the actual models
    (doseq [[k v] (c/ps! @phd)
            :let [mcz (metas k)]]
      (var-set xs
               (assoc! @xs
                       k
                       (update-in mcz
                                  [:fields] merge v))))
    (c/ps! @xs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- resolve-mxms [metas]
  (with-local-vars [mms (c/tmap*)]
    (doseq [[k m] metas
            :let [{:keys [mxm?
                          rels
                          fields]} m] :when mxm?]
      (->>
        (c/preduce<map>
          #(let
             [[side kee] %2
              other (get-in rels [side :other])
              mz (metas other)
              pke ((:fields mz) (:pkey mz))
              d (merge (kee fields)
                       (select-keys pke
                                    [:domain :size]))]
             (assoc! %1 kee d))
          [[:lhs :lhs-rowid]
           [:rhs :rhs-rowid]])
        (merge fields)
        (assoc m :fields )
        (assoc! @mms k)
        (var-set mms)))
    (merge metas
           (c/ps! @mms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- colmap-fields
  "Create a map of fields keyed by the column name"
  [flds]
  (c/preduce<map> #(let [[_ v] %2]
                     (assoc! %1 (s/ucase (:column v)) v)) flds))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- meta-models
  "Inject extra meta-data properties into each model.  Each model will have
   its (complete) set of fields keyed by column nam or field id"
  [metas schema]
  (c/preduce<map>
    #(let [[k m] %2
           {:keys [fields]} m]
       (assoc! %1
               k
               (with-meta m
                          {:schema schema
                           :columns (colmap-fields fields)}))) metas))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:private dft-options {:col-rowid "CZLAB_ROWID"
                            :col-lhs-rowid "CZLAB_LHS_ROWID"
                            :col-rhs-rowid "CZLAB_RHS_ROWID"})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbcfg
  "*internal*" [pojo options]
  (let [{:keys [pkey]} pojo
        {:keys [col-rowid] :as cfg}
        (merge dft-options options)]
    (-> (assoc pojo :config cfg)
        (update-in [:fields pkey] assoc :column col-rowid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbschema<> "" [& args]
  (let [[p1 p2] (take 2 args)
        [options models]
        (if (and (= :meta p1)
                 (map? p2))
          [p2 (drop 2 args)] [nil args])]
    `(czlab.horde.core/dbschema*
       :meta ~options
       ~@(map #(let [[p1 p2 & more] %]
                 (cons p1
                       (cons p2
                             (cons `(czlab.horde.core/dbcfg ~options) more)))) models))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbschema*
  "Stores metadata for all models"
  [& args]
  (let [[p1 p2] (take 2 args)
        [options models]
        (if (= :meta p1)
          [p2 (drop 2 args)] [nil args])
        ms (if (not-empty models)
             (c/preduce<map>
               #(assoc! %1 (:id %2) %2) models))
        sch (atom {:config (merge dft-options options)})
        m2 (if (not-empty ms)
             (-> (resolve-assocs ms) (resolve-mxms) (meta-models nil)))]
    (c/assoc!! sch
               :models
               (c/preduce<map>
                 #(let [[k m] %2]
                    (assoc! %1
                            k
                            (vary-meta m assoc :schema sch))) m2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbg-show-schema ""
  ^String [schema]
  {:pre [(some? schema)]}
  (s/sreduce<>
    #(s/sbf-join %1
                 "\n"
                 (i/fmt->edn {:TABLE (:table %2)
                              :DEFN %2
                              :META (meta %2)})) (vals (:models @schema))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- safe-get-conn
  ^Connection [jdbc]
  (let [d (load-driver jdbc)
        p (Properties.)
        {:keys [url user
                driver passwd]} jdbc]
    (when (s/hgl? user)
      (doto p
        (.put "user" user)
        (.put "username" user))
      (if (some? passwd)
        (.put p "password" (i/x->str passwd))))
    (if (nil? d)
      (dberr! "Can't load Jdbc Url: %s" url))
    (if (and (s/hgl? driver)
             (not= (-> d
                       .getClass
                       .getName) driver))
      (l/warn "want %s, got %s" driver (class d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbconnect<>
  "Connect to db"
  ^Connection
  [{:keys [url user] :as jdbc}]
  (let [^Connection
        conn (if (s/hgl? user)
               (safe-get-conn jdbc)
               (DriverManager/getConnection url))]
    (if (nil? conn)
      (dberr! "Failed to connect: %s" url))
    (doto conn
      (.setTransactionIsolation
        Connection/TRANSACTION_SERIALIZABLE))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn test-connect?
  "Test connect to the database?"
  [jdbc] (try (c/do#true (.close (dbconnect<> jdbc)))
              (catch SQLException _ false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn resolve-vendor
  "Find type of database."
  ^APersistentMap [in]
  (condp instance? in
    JdbcSpec
    (c/wo* [c (dbconnect<> in)] (resolve-vendor c))
    Connection
    (let [m (.getMetaData ^Connection in)
          rc {:id (maybe-get-vendor (.getDatabaseProductName m))
              :qstr (s/strim (.getIdentifierQuoteString m))
              :version (.getDatabaseProductVersion m)
              :name (.getDatabaseProductName m)
              :url (.getURL m)
              :user (.getUserName m)
              :lcs? (.storesLowerCaseIdentifiers m)
              :ucs? (.storesUpperCaseIdentifiers m)
              :mcs? (.storesMixedCaseIdentifiers m)}]
      (assoc rc :fmtId (partial fmt-sqlid rc)))
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn table-exist?
  "Is this table defined in db?"
  [in ^String table]
  (condp instance? in
    JdbcPool
    (c/wo* [^Connection
            c (p-next in)]
      (table-exist? c table))
    JdbcSpec
    (c/wo* [c (dbconnect<> in)] (table-exist? c table))
    Connection
    (u/try!!
      false
      (let [dbv (resolve-vendor in)
            m (.getMetaData ^Connection in)]
        (c/wo* [res (.getColumns m
                                 nil
                                 (if (= (:id dbv) :oracle) "%")
                                 (fmt-sqlid in table false) "%")]
          (and res (.next res)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn row-exist?
  "Is there any rows in the table?"
  [in table]
  (condp instance? in
    JdbcSpec
    (c/wo* [c (dbconnect<> in)] (row-exist? c table))
    Connection
    (u/try!!
      false
      (let [sql (str "select count(*) from "
                     (fmt-sqlid in table))]
        (c/wo* [res (-> (.createStatement ^Connection in)
                        (.executeQuery sql))]
          (and res
               (.next res)
               (pos? (.getInt res (int 1)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- load-columns
  "Read each column's metadata"
  [^DatabaseMetaData m
   ^String catalog ^String schema ^String table]
  (with-local-vars [pkeys #{} cms {}]
    (c/wo* [rs (.getPrimaryKeys m
                                catalog schema table)]
      (loop [sum (c/tset*)
             more (.next rs)]
        (if-not more
          (var-set pkeys (c/ps! sum))
          (recur
            (conj! sum
                   (.getString rs
                               (int 4))) (.next rs)))))
    (c/wo* [rs (.getColumns m catalog schema table "%")]
      (loop [sum (c/tmap*)
             more (.next rs)]
        (if-not more
          (var-set cms (c/ps! sum))
          (let [opt (not= (.getInt rs (int 11))
                          DatabaseMetaData/columnNoNulls)
                n (.getString rs (int 4))
                cn (s/ucase n)
                ctype (.getInt rs (int 5))]
            (recur (assoc! sum
                           (keyword cn)
                           {:sql-type ctype
                            :column n
                            :null? opt
                            :pkey? (contains? @pkeys n)})
                   (.next rs))))))
    (with-meta @cms
               {:supportsGetGeneratedKeys?
                (.supportsGetGeneratedKeys m)
                :primaryKeys
                @pkeys
                :supportsTransactions?
                (.supportsTransactions m)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn load-table-meta
  "Fetch metadata of this table from db"
  [^Connection conn ^String table]
  {:pre [(some? conn)]}
  (let [dbv (resolve-vendor conn)
        mt (.getMetaData conn)
        catalog nil
        schema (if (= (:id dbv) :oracle) "%")
        tbl (fmt-sqlid conn table false)]
    ;; not good, try mixed case... arrrrrrrrrrhhhhhhhhhhhhhh
    ;;rs = m.getTables( catalog, schema, "%", null)
    (load-columns mt catalog schema tbl)))

;;Object
;;Clojure CLJ-1347
;;finalize won't work *correctly* in reified objects - document
;;(finalize [this]
;;(try!
;;(log/debug "DbPool finalize() called.")
;;(.shutdown this)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbpool<>
  "Create a db connection pool"
  ([jdbc] (dbpool<> jdbc nil))
  ([jdbc options]
   (let [dbv (resolve-vendor jdbc)
         options (or options {})
         {:keys [driver url
                 passwd user]} jdbc
         hc (HikariConfig.)]
     ;;(l/debug "pool-options: %s" options)
     ;;(l/debug "pool-jdbc: %s" jdbc)
     (if (s/hgl? driver)
       (m/forname driver))
     (c/test-some "db-vendor" dbv)
     (.setJdbcUrl hc ^String url)
     (when (s/hgl? user)
       (.setUsername hc ^String user)
       (if (some? passwd)
         (.setPassword hc (i/x->str passwd))))
     (l/debug "[hikari]\n%s" (str hc))
     (jdbc-pool<> dbv jdbc (HikariDataSource. hc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- maybe-ok?
  [dbn ^Throwable e]
  (let [ee (c/cast? SQLException (u/root-cause e))
        ec (some-> ee .getErrorCode)]
    (or (and (s/embeds? dbn "oracle")
             (some? ec)
             (== 942 ec) (== 1418 ec) (== 2289 ec) (== 0 ec))
        (throw e))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn upload-ddl
  "Upload DDL to DB."
  [in ^String ddl]
  (condp instance? in
    JdbcPool
    (c/wo* [^Connection
            c (p-next in)] (upload-ddl c ddl))
    JdbcSpec
    (c/wo* [c (dbconnect<> in)] (upload-ddl c ddl))
    Connection
    (let [lines (mapv #(s/strim %)
                      (cs/split ddl (re-pattern ddl-sep)))
          dbn (s/lcase (-> ^Connection
                           in
                           .getMetaData .getDatabaseProductName))]
      (.setAutoCommit ^Connection in true)
      (l/debug "\n%s" ddl)
      (doseq [s lines
              :let [ln (s/strim-any s ";" true)]
              :when (and (s/hgl? ln) (not= (s/lcase ln) "go"))]
        (c/wo* [s (.createStatement ^Connection in)]
          (try (.executeUpdate s ln)
               (catch SQLException _ (maybe-ok? dbn _))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn bind-model
  "*internal*"
  [pojo model]
  {:pre [(map? pojo)(map? model)]} (with-meta pojo {:model model}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbpojo<>
  "Create object of type"
  ^APersistentMap [model] (bind-model {} model))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro mock-pojo<>
  "Clone object with pkey only" [obj]
  `(let [o# ~obj
         pk# (:pkey (czlab.horde.core/gmodel o#))]
     (with-meta {pk# (czlab.horde.core/goid o#)} (meta o#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-fld
  "Set value to a field."
  ^APersistentMap
  [pojo fld value]
  {:pre [(keyword? fld)]}
  (if (check-field? pojo fld)
    (assoc pojo fld value)
    (u/throw-BadData "Invalid field %s" fld)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-flds*
  "Set field+values as: f1 v1 f2 v2 ... fn vn."
  ^APersistentMap
  [pojo & fvs] {:pre [(even? (count fvs))]}
  (reduce #(db-set-fld %1
                       (c/_1 %2)
                       (c/_2 %2))
          pojo
          (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- set-mxm-flds* [pojo & fvs]
  (reduce #(assoc %1 (first %2) (last %2)) pojo (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-fld
  "Remove a field."
  [pojo fld]
  {:pre [(map? pojo) (:keyword? fld)]}
  (if (check-field? pojo fld) (dissoc pojo fld) pojo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-fld
  "Get value of a field."
  [pojo fld] {:pre [(map? pojo) (:keyword? fld)]} (get pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-get-relation
  "Get the relation definition."
  [model rid kind]
  (if-some [r (find-assoc model rid)] (if (= (:kind r) kind) r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- select-side+ [mxm obj]
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
        (dberr! "Unknown many-to-many for: %s" t)
        [nil nil nil]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private select-side
  [mxm obj] `(first (select-side+ ~mxm  ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; handling assocs
(defn- dbio-get-o2x [ctx lhsObj kind]
  (let [{rid :as sqlr :with} ctx]
    (or (dbio-get-relation (gmodel lhsObj) rid kind)
        (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-set-o2x [ctx lhsObj rhsObj kind]
  (let [{rid :as sqlr :with} ctx]
    (if-some
      [r (dbio-get-relation
           (gmodel lhsObj) rid kind)]
      (let [fv (goid lhsObj)
            fid (:fkey r)
            y (-> (mock-pojo<> rhsObj)
                  (db-set-fld fid fv))
            cnt (mod-obj sqlr y)]
        [lhsObj (merge rhsObj y)])
      (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-o2m
  "One to many assocs."
  [ctx lhsObj]
  {:pre [(map? ctx)(map? lhsObj)]}
  (let [{sqlr :with kast :cast} ctx]
    (if-some
      [r (dbio-get-o2x ctx lhsObj :o2m)]
      (find-some sqlr
                 (or kast
                     (:other r))
                 {(:fkey r) (goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-o2m
  "" [ctx lhsObj rhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)
         (map? rhsObj)]}
  (dbio-set-o2x ctx lhsObj rhsObj :o2m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-o2m*
  "" ^APersistentVector
  [ctx lhsObj & rhsObjs]
  (c/preduce<vec>
    #(conj! %1
            (last (dbio-set-o2x ctx lhsObj %2 :o2m))) rhsObjs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-o2o
  "One to one relation."
  ^APersistentMap
  [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]}
  (let [{sqlr :with kast :cast} ctx]
    (if-some
      [r (dbio-get-o2x ctx lhsObj :o2o)]
      (find-one sqlr
                (or kast
                    (:other r))
                {(:fkey r) (goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-o2o
  "Set One to one relation."
  [ctx lhsObj rhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)
         (map? rhsObj)]}
  (dbio-set-o2x ctx lhsObj rhsObj :o2o))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-clr-o2x [ctx objA kind]
  (let [{sqlr :with kast :cast rid :as} ctx
        s (:schema sqlr)
        mA (gmodel objA)]
    (if-some
      [r (dbio-get-relation mA rid kind)]
      (let [rt (or kast (:other r))
            mB (find-model s rt)
            tn (dbtable mB)
            cn (dbcol (:fkey r) mB)]
        (exec-sql
          sqlr
          (if-not (:cascade? r)
            (format
              "update %s set %s= null where %s=?"
              (fmt-id sqlr tn)
              (fmt-id sqlr cn)
              (fmt-id sqlr cn))
            (format
              "delete from %s where %s=?"
              (fmt-id sqlr tn)
              (fmt-id sqlr cn)))
          [(goid objA)])
        objA)
      (dberr! "Unknown relation: %s" rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-o2m
  "Clear one to many relation."
  [ctx lhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)]} (dbio-clr-o2x ctx lhsObj :o2m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-o2o
  "Clear one to one relation."
  [ctx lhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)]} (dbio-clr-o2x ctx lhsObj :o2o))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- xref-col-type [col]
  (case col
    :rhs-rowid :rhs-typeid
    :lhs-rowid :lhs-typeid
    (dberr! "Invaid column key: %s" col)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-m2m
  "Set many to many relations."
  ^APersistentMap
  [ctx objA objB]
  {:pre [(map? ctx)
         (map? objA) (map? objB)]}
  (let [{sqlr :with jon :joined} ctx
        s (:schema sqlr)]
    (if-some
      [mm (find-model s jon)]
      (let [ka (select-side mm objA)
            kb (select-side mm objB)]
        (add-obj sqlr
                 (-> (dbpojo<> mm)
                     (set-mxm-flds*
                       ka (goid objA)
                       kb (goid objB)))))
      (dberr! "Unkown relation: %s" jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-m2m
  "Clear many to many relations."
  ([ctx obj] (db-clr-m2m ctx obj nil))
  ([ctx objA objB]
   {:pre [(some? objA)]}
    (let [{sqlr :with jon :joined} ctx
          s (:schema sqlr)]
      (if-some
        [mm (find-model s jon)]
        (let [fs (:fields mm)
              ka (select-side mm objA)
              kb (select-side mm objB)]
          (if (nil? objB)
            (exec-sql sqlr
                      (format
                        "delete from %s where %s=?"
                        (fmt-id sqlr (dbtable mm))
                        (fmt-id sqlr (dbcol (fs ka))))
                      [(goid objA)])
            (exec-sql sqlr
                      (format
                        "delete from %s where %s=? and %s=?"
                        (fmt-id sqlr (dbtable mm))
                        (fmt-id sqlr (dbcol (fs ka)))
                        (fmt-id sqlr (dbcol (fs kb))))
                      [(goid objA) (goid objB)])))
        (dberr! "Unkown relation: %s" jon)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-m2m
  "" [ctx pojo]
  {:pre [(map? ctx)
         (map? pojo)]}
  (let [{sqlr :with kast :cast jon :joined} ctx
        s (:schema sqlr)
        MM (fmt-id sqlr "MM")
        RS (fmt-id sqlr "RES")]
    (if-some
      [mm (find-model s jon)]
      (let [{{:keys [col-rowid]} :config} @s
            {:keys [fields]} mm
            [ka kb t]
            (select-side+ mm pojo)
            t2 (or kast t)
            tm (find-model s t2)]
        (if (nil? tm)
          (dberr! "Unknown model: %s." t2))
        (select-sql sqlr
                    t2
                    (s/fmt
                      (str "select distinct %s.* from %s %s "
                           "join %s %s on "
                           "%s.%s=? and %s.%s=%s.%s")
                      RS
                      (fmt-id sqlr (dbtable tm))
                      RS
                      (fmt-id sqlr (dbtable mm))
                      MM
                      MM (fmt-id sqlr (dbcol (ka fields)))
                      MM (fmt-id sqlr (dbcol (kb fields)))
                      RS (fmt-id sqlr col-rowid))
                    [(goid pojo)]))
      (dberr! "Unknown joined model: %s" jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


