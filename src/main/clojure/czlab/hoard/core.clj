;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database and modeling functions."
      :author "Kenneth Leung"}

  czlab.hoard.core

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
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:private dft-options {:col-rowid "CZLAB_ROWID"
                            :col-lhs-rowid "CZLAB_LHS_ROWID"
                            :col-rhs-rowid "CZLAB_RHS_ROWID"})
(def ^:dynamic *ddl-cfg* nil)
(def ^:dynamic *ddl-bvs* nil)
(def ddl-sep "-- :")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare dbo2o dbo2m dbm2m dbfields dft-fld<>)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol PojoAPI
  "Functions relating to a Pojo."
  (bind-model [_ model] "")
  (db-get-fld [_ fld] "Get field.")
  (db-clr-fld [_ fld] "Remove field.")
  (db-set-fld [_ fld value] "Set value to a field."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol JdbcSpecAPI
  "Functions relating to jdbc properties."
  (testing? [_] "Test connect to db.")
  (conn<> [_] "")
  (load-driver [_] "Load the jdbc driver class."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Matchers
  "Functions relating to jdbc protocol line."
  (match-url?? [_] "")
  (match-spec?? [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol SchemaAPI
  "Functions relating to a schema."
  (find-model [_ id] "Find model."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol FieldAPI
  "Functions relating to a field."
  (find-col [_] "Find column name,"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol ModelAPI
  "Functions relating to a model."
  (find-assoc [_ id] "")
  (gschema [_] "")
  (gmxm [_] "")
  (find-table [_] "")
  (find-id [_]  "")
  (find-field [_ id] "")
  (with-table [_ table] "")
  (with-joined [_ lhs rhs] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol ConnectionAPI
  "Functions relating to a jdbc connection."
  (row-exist? [_ table] "")
  (upload-ddl [_ ddl] "")
  (db-vendor [_] "")
  (db-meta [_] "")
  (table-meta [_ table] "")
  (table-exist? [_ table] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Transactable
  "Functions relating to a db transaction."
  (tx-transact! [_ func]
                [_ func cfg]
                "Run function inside a transaction."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol SQLr
  "Functions relating to SQL."
  (sq-find-some [_ model filters]
                [_ model filters extras] "")
  (sq-find-all [_ model]
               [_ model extras] "")
  (sq-find-one [_ model filters] "")
  (sq-fmt-id [_ s] "")
  (sq-mod-obj [_ obj] "")
  (sq-del-obj [_ obj] "")
  (sq-add-obj [_ obj] "")
  (sq-exec-with-output [_ sql params] "")
  (sq-exec-sql [_ sql params] "")
  (sq-count-objs [_ model] "")
  (sq-purge-objs [_ model] "")
  (sq-select-sql [_ sql params]
                 [_ model sql params] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol JdbcPool
  "Functions relating to a jdbc connection pool."
  (jp-close [_] "Shut down this pool.")
  (jp-next [_] "Next free connection from the pool."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DbioModel [])
(defrecord DbioField [])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DbioM2MRel [])
(defrecord DbioO2ORel [])
(defrecord DbioO2MRel [])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord VendorGist [])
(defrecord JdbcSpec [])
(defrecord DbioPojo [])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dberr!
  "Throw a SQL execption."
  [fmt & more]
  (c/trap! SQLException (str (apply format fmt more))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private mkfld
  [& args] `(merge (dft-fld<>) (hash-map ~@args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbmodel<>
  "Define a data model inside a schema."
  [name & body]
  (let [p1 (first body)
        [options defs]
        (if (map? p1)
          [p1 (drop 1 body)] [nil body])]
  `(-> (czlab.hoard.core/dbdef<> ~name ~options) ~@defs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro tstamp<>
  "Sql timestamp."
  [] `(java.sql.Timestamp. (.getTime (java.util.Date.))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- clean-name
  [s]
  (str (some-> s
               name
               (cs/replace #"[^a-zA-Z0-9_-]" "")
               (cs/replace  #"-" "_"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gmodel
  "Get object's model."
  [pojo] `(:model (meta ~pojo)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gtype
  "Get object's type."
  [pojo] `(:id (czlab.hoard.core/gmodel ~pojo)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro goid
  "Get object's id."
  [pojo]
  `(let [o# ~pojo
         pk# (:pkey (czlab.hoard.core/gmodel o#))] (pk# o#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol SchemaAPI
  clojure.lang.Atom

  (find-model [schema typeid]
    (or (get (:models @schema) typeid)
        (l/warn "find-model %s failed!" typeid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol FieldAPI
  DbioField

  (find-col [_] (:column _)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol ModelAPI
  DbioModel

  (with-joined [model lhs rhs]
    {:pre [(c/is-scoped-keyword? lhs)
           (c/is-scoped-keyword? rhs)]}
    ;meta was injected by our framework
    (let [{{:keys [col-lhs-rowid
                   col-rhs-rowid]} :____meta} model]
      ;create the fields to store the pkeys of
      ;both lhs & rhs
      (-> (dbfields model
                    {:lhs-rowid
                     (mkfld :domain :Long
                            :null? false
                            :column col-lhs-rowid)
                     :rhs-rowid
                     (mkfld :domain :Long
                            :null? false
                            :column col-rhs-rowid)})
          ;create a mxm relation
          (dbm2m lhs rhs))))

  (with-table [_ table]
    (assoc _ :table (clean-name table)))

  (gschema [_]
    (:schema (meta _)))

  (find-field [model fieldid]
    (get (:fields model) fieldid))

  (find-table [_] (:table _))

  (find-id [_] (:id _))

  (gmxm [model]
    (assert (:mxm? model)
            "Not a joined model.")
    (get (:rels model) :mxm))

  (find-assoc [model relid]
    (get (:rels model) relid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn fmt-sqlid
  "Format SQL identifier."
  {:tag String}
  ([info idstr]
   (fmt-sqlid info idstr nil))
  ([info idstr quote?]
   (cond
     (map? info)
     (let [{:keys [qstr ucs? lcs?]} info
           ch (s/strim qstr)
           id (cond ucs? (s/ucase idstr)
                    lcs? (s/lcase idstr) :else idstr)]
       (if (false? quote?) id (str ch id ch)))
     (c/is? DatabaseMetaData info)
     (let [mt (c/cast? DatabaseMetaData info)
           ch (s/strim
                (.getIdentifierQuoteString mt))
           id (cond
                (.storesUpperCaseIdentifiers mt)
                (s/ucase idstr)
                (.storesLowerCaseIdentifiers mt)
                (s/lcase idstr)
                :else idstr)]
       (if (false? quote?) id (str ch id ch)))
     (c/is? Connection info)
     (fmt-sqlid (.getMetaData ^Connection info) idstr quote?))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; have to be function , not macro as this is passed into another higher
;; function - merge.
(defn- merge-meta
  "Merge 2 meta maps."
  [m1 m2] {:pre [(map? m1)
                 (or (nil? m2)(map? m2))]} (merge m1 m2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord JdbcPoolImpl []
  java.io.Closeable
  (close [_] (jp-close _))
  JdbcPool
  (jp-close [me]
    (c/let#nil [{:keys [impl]} me]
      (l/debug "finz: %s." impl)
      (.close ^HikariDataSource impl)))
  (jp-next [me]
    (try (.getConnection ^HikariDataSource (:impl me))
         (catch Throwable _ (dberr! "No free connection.") nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- jdbc-pool<>
  [vendor jdbc impl]
  (c/object<> JdbcPoolImpl
              :vendor vendor :jdbc jdbc :impl impl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbspec<>
  "Basic jdbc parameters."
  ([url] (dbspec<> nil url nil nil))
  ([driver url user passwd]
   (c/object<> JdbcSpec
               :driver (str driver)
               :user (str user)
               :url (str url)
               :passwd (i/x->chars passwd)
               :id (str (u/jid<>) "#" (u/seqint2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- safe-get-conn
  ^Connection [jdbc]
  (let [^Driver d (load-driver jdbc)
        p (Properties.)
        {:keys [url user
                driver passwd]} jdbc]
    (when (s/hgl? user)
      (doto p
        (.put "user" user)
        (.put "username" user))
      (if passwd
        (.put p "password" (i/x->str passwd))))
    (if (nil? d)
      (dberr! "Can't load Jdbc Url: %s." url))
    (if (and (s/hgl? driver)
             (not= (-> d
                       .getClass
                       .getName) driver))
      (l/warn "want %s, got %s." driver (class d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol JdbcSpecAPI
  JdbcSpec
  (testing? [_]
    (try (c/do#true (.close ^Connection (conn<> _)))
         (catch SQLException _ false)))
  (conn<> [jdbc]
    (let [{:keys [url user]} jdbc
          ^Connection
          c (if (s/hgl? user)
              (safe-get-conn jdbc)
              (DriverManager/getConnection url))]
      (if (nil? c)
        (dberr! "Failed to connect: %s." url))
      (doto c
        (.setTransactionIsolation
          Connection/TRANSACTION_SERIALIZABLE))))
  (load-driver [_]
    (c/if-string [s (:url _)] (DriverManager/getDriver s))))

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
  "For o2o & o2m relations."
  [tn rn] `(s/x->kw "fk_" (name ~tn) "_" (name ~rn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol Matchers
  String
  (match-spec?? [spec]
    (let [kw (keyword (s/lcase spec))]
      (if (contains? *db-types* kw) kw)))
  (match-url?? [dburl]
    (c/if-some+
      [ss (s/split (str dburl) ":")]
      (if (> (count ss) 1) (match-spec?? (second ss))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA MODELING
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dft-fld<>
  ([] (dft-fld<> nil))
  ([fid]
   (c/object<> DbioField
               :domain :String
               :id fid
               :size 255
               :rel-key? false
               :null? true
               :auto? false
               :dft nil
               :system? false
               :updatable? true
               :column (clean-name fid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:private pkey-meta (mkfld :updatable? false
                                :domain :Long
                                :id :rowid
                                :auto? true
                                :system? true
                                :column "must be set!"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbcfg
  "Set the column name for the primary key.  *internal*"
  [model]
  (let [{:keys [pkey]
         {:keys [col-rowid]} :____meta} model]
    (if (s/nichts? col-rowid)
      model
      (update-in model [:fields pkey] assoc :column col-rowid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbdef<>
  "Define a generic model. *internal*"
  ([mname] (dbdef<> mname nil))
  ([mname options]
   {:pre [(c/is-scoped-keyword? mname)]}
   (dbcfg (c/object<> DbioModel
                      (merge {:abstract? false
                              :system? false
                              :mxm? false
                              :pkey :rowid
                              :indexes {}
                              :rels {}
                              :uniques {}
                              :id mname
                              :fields {:rowid pkey-meta}
                              :table (clean-name mname)} options)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbfield<>
  "Add a new field."
  [model fid fdef]
  {:pre [(keyword? fid)(map? fdef)]}
  (update-in model
             [:fields]
             assoc
             fid
             (merge (dft-fld<> fid) (dissoc fdef :id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbfields
  "Add a bunch of fields."
  [model flddefs]
  {:pre [(map? flddefs)]}
  (reduce #(dbfield<> %1 (c/_1 %2) (c/_E %2)) model flddefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbjoined<>
  "Define a joined data model."
  ([modelname lhs rhs]
   `(dbjoined<> ~modelname nil ~lhs ~rhs))
  ([modelname options lhs rhs]
   (let [options' (merge options {:mxm? true})]
     `(-> (czlab.hoard.core/dbdef<> ~modelname ~options')
          (czlab.hoard.core/with-joined ~lhs ~rhs)
          (czlab.hoard.core/dbuniques {:i1 #{:lhs-rowid :rhs-rowid}})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;merge new stuff onto old stuff
(defn- with-xxx-sets [model kvs fld]
  (update-in model
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
  [model indexes]
  {:pre [(map? indexes)]}
  (with-xxx-sets model indexes :indexes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbkey
  "Declare your own primary key."
  [model pke]
  (let [{:keys [fields pkey]} model
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
             (assoc model :fields))
        (assoc :pkey oid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;uniques = { :a #{ :f1 :f2 } :b #{ :f3 :f4 } }
(defn dbuniques
  "Set uniques to the model."
  [model uniqs]
  {:pre [(map? uniqs)]}
  (with-xxx-sets model uniqs :uniques))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbassoc<>
  "Define an relation between 2 models."
  [{:keys [id] :as model} rel args]
  (let [{fk :fkey rid :id :as R}
        (merge rel {:fkey nil
                    :cascade? false} args)]
    (update-in model
               [:rels]
               assoc
               rid
               (assoc R
                      :fkey
                      (or fk
                          (fmt-fkey id rid))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbm2m
  [model lhs rhs]
  (dbassoc<> model
             (DbioM2MRel.)
             {:owner (find-id model)
              :id :mxm
              :lhs [lhs :lhs-rowid]
              :rhs [rhs :rhs-rowid]}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbo2m
  "Define a one to many association."
  [model id & args]
  {:pre [(not-empty args)]}
  (dbassoc<> model
             (DbioO2MRel.)
             (assoc (c/kvs->map args) :id id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbo2o
  "Define a one to one association."
  [model id & args]
  {:pre [(not-empty args)]}
  (dbassoc<> model
             (DbioO2ORel.)
             (assoc (c/kvs->map args) :id id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private with-abstract
  [model] `(assoc ~model :abstract? true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private with-system
  [model] `(assoc ~model :system? true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- check-field? [pojo fld]
  (boolean
    (if-some [f (find-field (gmodel pojo) fld)]
      (not (or (:auto? f)
               (not (:updatable? f)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private mkfkdef<>
  [fid ktype] `(assoc (dft-fld<> ~fid)
                      :rel-key? true :domain ~ktype))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- resolve-assocs
  "Walk through all models, for each model, study its relations.
  For o2o or o2m assocs, we need to artificially inject a new
  field/column into the (other/rhs) model (foreign key)."
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
                       (not-empty rels))]
      ;only deal with o2o, o2m assocs
      (doseq [[_ r] rels
              :let [{:keys [other fkey]} r]
              :when (not (c/is? DbioM2MRel r))]
        ;inject a new field to the *other* type
        (var-set phd
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
    (persistent! @xs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- resolve-mxms [metas]
  ;deal with dbjoined<> decls
  (with-local-vars [mms (c/tmap*)]
    (doseq [[k m] metas
            :let [{:keys [mxm? rels fields]} m
                  {:keys [lhs rhs] :as R}
                  (get rels :mxm)]
            :when (and mxm? R)]
      ;make foreign keys to have the same attributes
      ;as the linked tables primary keys.
      (->>
        (c/preduce<map>
          #(let
             [[side kee] %2
              mz (metas side)
              pke ((:pkey mz)
                   (:fields mz))
              d (merge (kee fields)
                       (select-keys pke
                                    [:domain :size]))]
             (assoc! %1 kee d))
          [lhs rhs])
        (merge fields)
        (assoc m :fields)
        (assoc! @mms k)
        (var-set mms)))
    (merge metas (persistent! @mms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- colmap-fields
  "Create a map of fields keyed by the column name."
  [flds]
  (c/preduce<map>
    #(let [[_ v] %2]
       (assoc! %1 (s/ucase (:column v)) v)) flds))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- meta-models
  "Inject extra meta-data properties into each model.  Each model will have
   its (complete) set of fields keyed by column name or field id."
  [metas schema]
  (c/preduce<map>
    #(let [[k m] %2
           {:keys [fields]} m]
       (assoc! %1
               k
               (with-meta m
                          {:schema schema
                           :columns (colmap-fields fields)}))) metas))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro defschema
  "" [name & models]
  (let [m (meta name)
        options (merge m dft-options)
        options' {:____meta options}]
    `(def
       ~name
       (czlab.hoard.core/dbschema*
         ~options
         ~@(map #(let [[p1 p2 & more] %]
                   (cons p1
                         (cons p2
                               (cons options' more)))) models)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbschema*
  "Stores metadata for all models.  *internal*"
  [options & models]
  (let [ms (if-not (empty? models)
             (c/preduce<map>
               #(assoc! %1 (:id %2) %2) models))
        sch (atom {:____meta
                   (merge options dft-options)})
        m2 (if-not (empty? ms)
             (-> ms resolve-assocs resolve-mxms (meta-models nil)))]
    (c/assoc!! sch
               :models
               (c/preduce<map>
                 #(let [[k m] %2]
                    (assoc! %1
                            k
                            (vary-meta m assoc :schema sch))) m2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbg-schema
  "" {:tag String}
  ([schema] (dbg-schema schema true))
  ([schema simple?]
   {:pre [(some? schema)]}
   (if simple?
     (i/fmt->edn (:models @schema))
     (s/sreduce<>
       #(s/sbf-join %1
                    "\n"
                    (i/fmt->edn {:TABLE (:table %2)
                                 :DEFN %2
                                 :META (meta %2)}))
       (vals (:models @schema))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- maybe-ok?
  [dbn ^Throwable e]
  (let [ee (c/cast? SQLException (u/root-cause e))
        ec (some-> ee .getErrorCode)]
    (or (and (s/embeds? dbn "oracle")
             (some? ec)
             (== 942 ec) (== 1418 ec) (== 2289 ec) (== 0 ec)) (throw e))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- load-columns
  "Read each column's metadata."
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
          (let [opt? (not= (.getInt rs (int 11))
                           DatabaseMetaData/columnNoNulls)
                n (.getString rs (int 4))
                cn (s/ucase n)
                ctype (.getInt rs (int 5))]
            (recur (assoc! sum
                           (keyword cn)
                           {:sql-type ctype
                            :column n
                            :null? opt?
                            :pkey? (contains? @pkeys n)})
                   (.next rs))))))
    (with-meta @cms
               {:supportsGetGeneratedKeys?
                (.supportsGetGeneratedKeys m)
                :primaryKeys
                @pkeys
                :supportsTransactions?
                (.supportsTransactions m)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol ConnectionAPI
  Connection
  (upload-ddl [_ ddl]
    (let [lines (mapv #(s/strim %)
                      (cs/split ddl (re-pattern ddl-sep)))
          ^Connection conn _
          dbn (s/lcase (some-> conn
                               .getMetaData
                               .getDatabaseProductName))]
      (.setAutoCommit conn true)
      ;(l/debug "\n%s" ddl)
      (doseq [s lines
              :let [ln (s/strim-any s ";" true)]
              :when (and (s/hgl? ln)
                         (not= (s/lcase ln) "go"))]
        (c/wo* [s (.createStatement conn)]
          (try (.executeUpdate s ln)
               (catch SQLException _ (maybe-ok? dbn _)))))))
  (table-exist? [c table]
    (c/try!
      (let [m (db-meta c)
            dbv (db-vendor c)]
        (c/wo* [res (.getColumns ^DatabaseMetaData m
                                 nil
                                 (if (= (:id dbv) :oracle) "%")
                                 (fmt-sqlid c table false) "%")]
          (and res (.next res))))))
  (row-exist? [c table]
    (c/try!
      (let [sql (str "select count(*) from "
                     (fmt-sqlid c table))]
        (c/wo* [res (-> ^Connection c
                        .createStatement (.executeQuery sql))]
          (and res
               (.next res)
               (pos? (.getInt res (int 1))))))))
  (db-vendor [_]
    (let [m (.getMetaData ^Connection _)
          rc (c/object<> VendorGist
                         :id (maybe-get-vendor (.getDatabaseProductName m))
                         :qstr (s/strim (.getIdentifierQuoteString m))
                         :version (.getDatabaseProductVersion m)
                         :name (.getDatabaseProductName m)
                         :url (.getURL m)
                         :user (.getUserName m)
                         :lcs? (.storesLowerCaseIdentifiers m)
                         :ucs? (.storesUpperCaseIdentifiers m)
                         :mcs? (.storesMixedCaseIdentifiers m))]
      (assoc rc :fmt-id (partial fmt-sqlid rc))))
  (db-meta [_]
    (.getMetaData ^Connection _))
  (table-meta [conn table]
    (let [mt (.getMetaData ^Connection conn)
          tbl (fmt-sqlid conn table false)
          dbv (db-vendor conn)
          catalog nil
          schema (if (= (:id dbv) :oracle) "%")]
      ;; not good, try mixed case... arrrrhhhhhh
      ;;rs = m.getTables( catalog, schema, "%", null)
      (load-columns mt catalog schema tbl))))

;;Object
;;Clojure CLJ-1347
;;finalize won't work *correctly* in reified objects - document
;;(finalize [this]
;;(try!
;;(log/debug "DbPool finalize() called.")
;;(.shutdown this)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbpool<>
  "Create a db connection pool."
  ([jdbc] (dbpool<> jdbc nil))
  ([jdbc options]
   (let [dbv (c/wo* [^Connection
                     c (conn<> jdbc)] (db-vendor c))
         {:keys [driver url passwd user]} jdbc
         options (or options {})
         hc (HikariConfig.)]
     ;;(l/debug "pool-options: %s." options)
     ;;(l/debug "pool-jdbc: %s." jdbc)
     (if (s/hgl? driver)
       (m/forname driver))
     (c/test-some "db-vendor" dbv)
     (.setJdbcUrl hc ^String url)
     (when (s/hgl? user)
       (.setUsername hc ^String user)
       (if passwd
         (.setPassword hc (i/x->str passwd))))
     (l/debug "[hikari]\n%s." (str hc))
     (jdbc-pool<> dbv jdbc (HikariDataSource. hc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbpojo<>
  "Create object of type."
  ([] (DbioPojo.))
  ([model]
   {:pre [(some? model)]}
   (bind-model (DbioPojo.) model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn mock-pojo<>
  "Clone object with pkey only." [obj]
  (let [out (DbioPojo.)
        pk (:pkey (gmodel obj))]
    (with-meta (assoc out pk (goid obj)) (meta obj))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol PojoAPI
  DbioPojo
  (bind-model [p _model]
    {:pre [(c/is? DbioModel _model)]}
    (let [m (meta p)
          {:keys [model]} m]
      (if (nil? model)
        (with-meta p (assoc m :model _model))
        (c/raise! "Cannot bind model %s twice!" model))))
  (db-clr-fld [pojo fld] (dissoc pojo fld))
  (db-get-fld [pojo fld] (get pojo fld))
  (db-set-fld [pojo fld value]
    {:pre [(keyword? fld)]}
    (if (check-field? pojo fld)
      (assoc pojo fld value)
      (u/throw-BadData "Invalid field %s." fld))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-flds*
  "Set field+values as: f1 v1 f2 v2 ... fn vn."
  [pojo & fvs]
  {:pre [(c/n#-even? fvs)]}
  (reduce #(db-set-fld %1
                       (c/_1 %2)
                       (c/_E %2))
          pojo (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


