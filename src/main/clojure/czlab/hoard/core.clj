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
;; Copyright © 2013-2024, Kenneth Leung. All rights reserved.

(ns czlab.hoard.core

  "Database and modeling functions."

  (:refer-clojure :exclude [next])

  (:use [flatland.ordered.set])

  (:require [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.io :as i]
            [czlab.basal.meta :as m]
            [czlab.basal.util :as u]
            [czlab.basal.core :as c])

  (:import [java.util
            HashMap
            TimeZone
            Properties
            GregorianCalendar]
           [clojure.lang
            Keyword
            APersistentMap
            APersistentVector]
           [com.zaxxer.hikari
            HikariConfig
            HikariDataSource]
           [java.lang Math]
           [java.sql
            SQLException
            Connection
            Driver
            DriverManager
            DatabaseMetaData]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/def- dft-options {:col-rowid "CZLAB_ROWID"
                     :col-lhs-rowid "CZLAB_LHS_ROWID"
                     :col-rhs-rowid "CZLAB_RHS_ROWID"})
(def ^:dynamic *ddl-cfg* nil)
(def ^:dynamic *ddl-bvs* nil)
(def ddl-sep "-- :")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare dbo2o dbo2m dbm2m dbfields dft-fld<>)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Transactable
  "Functions relating to a db transaction."
  (transact! [_ func]
             [_ func cfg]
             "Run function inside a transaction."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol SQLr
  "Functions relating to SQL."
  (find-some [_ model filters]
             [_ model filters extras] "")
  (find-all [_ model]
            [_ model extras] "")
  ;(find-one [_ model filters] "")
  (fmt-id [_ s] "")
  (mod-obj [_ obj] "")
  (del-obj [_ obj] "")
  (add-obj [_ obj] "")
  (count-objs [_ model] "")
  (purge-objs [_ model] "")
  (exec-sql [_ sql params] "")
  (exec-with-output [_ sql params] "")
  (select-sql [_ sql params]
              [_ model sql params] ""))

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
  {:arglists '([fmt & more])}
  [fmt & more]

  (c/trap! SQLException (c/fmt fmt more)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- mkfld

  [& args] `(merge (dft-fld<>) (array-map ~@args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbmodel<>

  "Define a data model inside a schema."
  {:arglists '([name & body])}
  [name & body]

  (let [p1 (first body)
        [options defs]
        (if-not (map? p1)
          [nil body] [p1 (drop 1 body)])]
    `(-> (czlab.hoard.core/dbdef<> ~name ~options) ~@defs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro tstamp<>

  "Sql timestamp."
  {:arglists '([])}
  []

  `(java.sql.Timestamp. (.getTime (java.util.Date.))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- clean-name

  [s] (str (some-> s
                   name
                   (cs/replace #"[^a-zA-Z0-9_-]" "")
                   (cs/replace  #"-" "_"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gmodel

  "Get object's model."
  {:arglists '([pojo])}
  [pojo]

  `(:model (meta ~pojo)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro gtype

  "Get object's type."
  {:arglists '([pojo])}
  [pojo]

  `(:id (czlab.hoard.core/gmodel ~pojo)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro goid

  "Get object's id."
  {:arglists '([pojo])}
  [pojo]

  `(let [o# ~pojo
         pk# (:pkey (czlab.hoard.core/gmodel o#))] (pk# o#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn find-model

  "Find a model from the schema."
  {:arglists '([schema typeid])}
  [schema typeid]
  {:pre [(c/atom? schema)]}

  (or (get (:models @schema) typeid)
      (c/warn "find-model %s failed!" typeid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn find-col

  "Look up the field's column name."
  {:arglists '([f])}
  [f]
  {:pre [(c/is? DbioField f)]}

  (:column f))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn with-joined

  "Define a joined table."
  {:arglists '([model lhs rhs])}
  [model lhs rhs]
  {:pre [(c/is? DbioModel model)]}

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn with-table

  "Define the name of a table."
  {:arglists '([m table])}
  [m table]
  {:pre [(c/is? DbioModel m)]}

  (assoc m :table (clean-name table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn gschema

  "Get the schema containing this model."
  {:arglists '([m])}
  [m]
  {:pre [(c/is? DbioModel m)]}

  (:schema (meta m)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn find-field

  "Look up a field definition from the model."
  {:arglists '([model fieldid])}
  [model fieldid]
  {:pre [(c/is? DbioModel model)]}

  (get (:fields model) fieldid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn find-table

  "Get the table name from the model."
  {:arglists '([m])}
  [m]
  {:pre [(c/is? DbioModel m)]}

  (:table m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn find-id

  "Get the id of this model."
  {:arglists '([m])}
  [m]
  {:pre [(c/is? DbioModel m)]}

  (:id m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn find-assoc

  "Find the relation from the model."
  {:arglists '([model relid])}
  [m relid]
  {:pre [(c/is? DbioModel m)]}

  (get (:rels m) relid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn gmxm

  "Get the many-to-many relation from the model."
  {:arglists '([model])}
  [m]
  {:pre [(c/is? DbioModel m)
         (:mxm? m)]}

  (find-assoc m :mxm))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn fmt-sqlid

  "Format SQL identifier."
  {:tag String
   :arglists '([info idstr]
               [info idstr quote?])}

  ([info idstr]
   (fmt-sqlid info idstr nil))

  ([info idstr quote?]
   (cond
     (map? info)
     (let [{:keys [qstr ucs? lcs?]} info
           ch (c/strim qstr)
           id (cond ucs? (c/ucase idstr)
                    lcs? (c/lcase idstr) :else idstr)]
       (if (false? quote?) id (str ch id ch)))
     (c/is? DatabaseMetaData info)
     (let [mt (c/cast? DatabaseMetaData info)
           ch (c/strim
                (.getIdentifierQuoteString mt))
           id (cond
                (.storesUpperCaseIdentifiers mt)
                (c/ucase idstr)
                (.storesLowerCaseIdentifiers mt)
                (c/lcase idstr)
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
(defrecord JdbcPool []
  java.io.Closeable
  (close [me]
    (let [s (:impl me)]
      (c/debug "finz: %s." s)
      (.close ^HikariDataSource s)))
  c/Finzable
  (finz [_] (.close _))
  c/Nextable
  (next [me]
    (try (.getConnection ^HikariDataSource (:impl me))
         (catch Throwable _ (dberr! "No free connection.") nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- jdbc-pool<>
  [vendor jdbc impl]
  (c/object<> JdbcPool
              :vendor vendor :jdbc jdbc :impl impl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbspec<>

  "Basic jdbc parameters."
  {:arglists '([url]
               [driver url user passwd])}

  ([url]
   (dbspec<> nil url nil nil))

  ([driver url user passwd]
   (c/object<> JdbcSpec
               :driver (str driver)
               :user (str user)
               :url (str url)
               :passwd (i/x->chars passwd)
               :id (str (u/jid<>) "#" (u/seqint2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn load-driver

  "Load a jdbc driver."
  {:tag Driver
   :arglists '([jdbc])}
  [jdbc]
  {:pre [(c/is? JdbcSpec jdbc)]}

  (c/if-string
    [s (:url jdbc)]
    (DriverManager/getDriver s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- safe-get-conn

  ^Connection [jdbc]

  (let [d (load-driver jdbc)
        p (Properties.)
        {:keys [url user
                driver passwd]} jdbc]
    (when (c/hgl? user)
      (doto p
        (.put "user" user)
        (.put "username" user))
      (if passwd
        (.put p "password" (i/x->str passwd))))
    (if (nil? d)
      (dberr! "Can't load Jdbc Url: %s." url))
    (if (and (c/hgl? driver)
             (not= (-> d
                       .getClass
                       .getName) driver))
      (c/warn "want %s, got %s." driver (class d)))
    (.connect d url p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn conn<>

  "Make a jdbc connection."
  {:tag Connection
   :arglists '([jdbc])}
  [jdbc]
  {:pre [(c/is? JdbcSpec jdbc)]}

  (let [{:keys [url user]} jdbc
        ^Connection
        c (if (c/hgl? user)
            (safe-get-conn jdbc)
            (DriverManager/getConnection url))]
    (if (nil? c)
      (dberr! "Failed to connect: %s." url))
    (doto c
      (.setTransactionIsolation
        Connection/TRANSACTION_SERIALIZABLE))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn testing?

  "Check if jdbc spec is valid?"
  {:arglists '([s])}
  [s]
  {:pre [(c/is? JdbcSpec s)]}

  (try (c/do->true
         (.close (conn<> s)))
       (catch SQLException _ false)))

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

  (let [fc #(c/embeds? %2 %1)
        lp (c/lcase product)]
    (condp fc lp
      "microsoft" SQLServer
      "postgres" Postgresql
      "oracle" Oracle
      "mysql" MySQL
      "h2" H2
      (dberr! "Unknown db product: %s." product))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- fmt-fkey

  "For o2o & o2m relations."
  [tn rn] `(c/x->kw "fk_" (name ~tn) "_" (name ~rn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn match-spec??

  "If the database is supported?"
  {:arglists '([spec])}
  [spec]

  (let [kw (if-not (string? spec)
             spec (keyword (c/lcase spec)))]
    (if (contains? *db-types* kw) kw)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn match-url??

  "If the referred database is supported?"
  {:arglists '([dburl])}
  [dburl]

  (c/if-some+
    [ss (c/split (str dburl) ":")]
    (if (> (count ss) 1) (match-spec?? (second ss)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA MODELING
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dft-fld<>

  ([]
   (dft-fld<> nil))

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
(c/def- pkey-meta (mkfld :updatable? false
                         :domain :Long
                         :id :rowid
                         :auto? true
                         :system? true
                         :column "must be set!"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbcfg

  "Set the column name for the primary key.  *Internal*"
  [model]

  (let [{:keys [pkey]
         {:keys [col-rowid]} :____meta} model]
    (if (c/nichts? col-rowid)
      model
      (update-in model [:fields pkey] assoc :column col-rowid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbdef<>

  "Define a generic model. *internal*"
  {:arglists '([mname]
               [mname options])}

  ([mname]
   (dbdef<> mname nil))

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
  {:arglists '([model fid fdef])}
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
  {:arglists '([model flddefs])}
  [model flddefs]
  {:pre [(map? flddefs)]}

  (reduce #(dbfield<> %1 (c/_1 %2) (c/_E %2)) model flddefs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro dbjoined<>

  "Define a joined data model."
  {:arglists '([modelname lhs rhs]
               [modelname options lhs rhs])}

  ([modelname lhs rhs]
   `(dbjoined<> ~modelname nil ~lhs ~rhs))

  ([modelname options lhs rhs]
   (let [options' (merge options {:mxm? true})]
     `(-> (czlab.hoard.core/dbdef<> ~modelname ~options')
          (czlab.hoard.core/with-joined ~lhs ~rhs)
          (czlab.hoard.core/dbuniques {:i1 #{:lhs-rowid :rhs-rowid}})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;merge new stuff onto old stuff
(defn- with-xxx-sets

  [model kvs fld]

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
  {:arglists '([model indexes])}
  [model indexes]
  {:pre [(map? indexes)]}

  (with-xxx-sets model indexes :indexes))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbkey

  "Declare your own primary key."
  {:arglists '([model pke])}
  [model pke]

  (let [{:keys [fields pkey]} model
        {:keys [domain id
                auto?
                column size]} pke
        p (pkey fields)
        oid (or id pkey)
        fields (dissoc fields pkey)]
    (assert (and column
                 domain
                 p
                 (= pkey (:id p))))
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
  {:arglists '([model uniqs])}
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
  {:arglists '([model id & args])}
  [model id & args]
  {:pre [(not-empty args)]}

  (dbassoc<> model
             (DbioO2MRel.)
             (assoc (c/kvs->map args) :id id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbo2o

  "Define a one to one association."
  {:arglists '([model id & args])}
  [model id & args]
  {:pre [(not-empty args)]}

  (dbassoc<> model
             (DbioO2ORel.)
             (assoc (c/kvs->map args) :id id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- with-abstract

  [model] `(assoc ~model :abstract? true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- with-system

  [model] `(assoc ~model :system? true))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- check-field?

  [pojo fld]

  (boolean
    (if-some [f (find-field (gmodel pojo) fld)]
      (not (or (:auto? f)
               (not (:updatable? f)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- mkfkdef<>

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
    (doseq [[k v] (c/persist! @phd)
            :let [mcz (metas k)]]
      (var-set xs
               (assoc! @xs
                       k
                       (update-in mcz
                                  [:fields] merge v))))
    (persistent! @xs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- resolve-mxms

  [metas]

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
       (assoc! %1 (c/ucase (:column v)) v)) flds))

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

  "Define a schema."
  {:arglists '([name & model])}
  [name & models]

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
  {:arglists '([options & models])}
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

  "Debug print a schema."
  {:tag String
   :arglists '([schema]
               [schema simple?])}

  ([schema]
   (dbg-schema schema true))

  ([schema simple?]
   {:pre [(some? schema)]}
   (if simple?
     (i/fmt->edn (:models @schema))
     (c/sreduce<>
       #(c/sbf-join %1
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
    (or (and (c/embeds? (c/lcase dbn) "oracle")
             (some? ec)
             (== 942 ec)
             (== 1418 ec)
             (== 2289 ec) (== 0 ec)) (throw e))))

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
          (var-set pkeys (c/persist! sum))
          (recur
            (conj! sum
                   (.getString rs
                               (int 4))) (.next rs)))))
    (c/wo* [rs (.getColumns m catalog schema table "%")]
      (loop [sum (c/tmap*)
             more (.next rs)]
        (if-not more
          (var-set cms (c/persist! sum))
          (let [opt? (not= (.getInt rs (int 11))
                           DatabaseMetaData/columnNoNulls)
                n (.getString rs (int 4))
                cn (c/ucase n)
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
(defn upload-ddl

  "Update DDL commands to database."
  {:arglists '([conn ddl])}
  [c ddl]
  {:pre [(c/is? Connection c)]}

  (let [lines (map #(c/strim %)
                   (cs/split ddl (re-pattern ddl-sep)))
        c (c/cast? Connection c)
        dbn (.. c
                getMetaData
                getDatabaseProductName)]
    (.setAutoCommit c true)
    ;(c/debug "\n%s" ddl)
    (doseq [s lines
            :let [ln (c/strim-any s ";" true)]
            :when (and (c/hgl? ln)
                       (not= (c/lcase ln) "go"))]
      (c/wo* [s (.createStatement c)]
        (try (.executeUpdate s ln)
             (catch SQLException _ (maybe-ok? dbn _)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-meta

  "Get the meta-data on the database."
  {:arglists '([c])
   :tag DatabaseMetaData}
  [c]
  {:pre [(c/is? Connection c)]}

  (.getMetaData ^Connection c))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-vendor

  "Get information about the database."
  {:arglists '([c])}
  [c]
  {:pre [(c/is? Connection c)]}

  (let [m (.getMetaData ^Connection c)
        rc (c/object<> VendorGist
                       :id (maybe-get-vendor (.getDatabaseProductName m))
                       :qstr (c/strim (.getIdentifierQuoteString m))
                       :version (.getDatabaseProductVersion m)
                       :name (.getDatabaseProductName m)
                       :url (.getURL m)
                       :user (.getUserName m)
                       :lcs? (.storesLowerCaseIdentifiers m)
                       :ucs? (.storesUpperCaseIdentifiers m)
                       :mcs? (.storesMixedCaseIdentifiers m))]
    (assoc rc :fmt-id (partial fmt-sqlid rc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn table-exist?

  "If table is defined in the database?"
  {:arglists '([c])}
  [c table]
  {:pre [(c/is? Connection c)]}

  (c/try!
    (let [m (db-meta c)
          dbv (db-vendor c)]
      (c/wo* [res (.getColumns m
                               nil
                               (if (= :oracle
                                      (:id dbv)) "%")
                               (fmt-sqlid c table false) "%")]
        (and res (.next res))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn row-exist?

  "If table in database is not empty?"
  {:arglists '([c])}
  [c table]
  {:pre [(c/is? Connection c)]}

  (c/try!
    (let [sql (c/fmt "select %s from %s"
                     "count(*)"
                     (fmt-sqlid c table))]
      (c/wo* [res (-> ^Connection c
                      .createStatement
                      (.executeQuery sql))]
        (and res
             (.next res)
             (pos? (.getInt res (int 1))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn table-meta

  "Get the meta-data on the table."
  {:arglists '([conn table])}
  [c table]
  {:pre [(c/is? Connection c)]}

  (let [mt (.getMetaData ^Connection c)
        tbl (fmt-sqlid c table false)
        dbv (db-vendor c)
        catalog nil
        schema (if (= (:id dbv) :oracle) "%")]
    ;; not good, try mixed case... arrrrhhhhhh
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

  "Create a db connection pool."
  {:arglists '([jdbc]
               [jdbc options])}

  ([jdbc]
   (dbpool<> jdbc nil))

  ([jdbc options]
   (let [dbv (c/wo* [^Connection
                     c (conn<> jdbc)] (db-vendor c))
         {:keys [driver url passwd user]} jdbc
         options (or options {})
         hc (HikariConfig.)]
     ;;(c/debug "pool-options: %s." options)
     ;;(c/debug "pool-jdbc: %s." jdbc)
     (if (c/hgl? driver)
       (m/forname driver))
     (c/test-some "db-vendor" dbv)
     (.setJdbcUrl hc ^String url)
     (when (c/hgl? user)
       (.setUsername hc ^String user)
       (if passwd
         (.setPassword hc (i/x->str passwd))))
     (c/debug "[hikari]\n%s." (str hc))
     (jdbc-pool<> dbv jdbc (HikariDataSource. hc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn bind-model

  "Ensure pojo is bound to a model."
  {:arglists '([p model])}
  [p _model]
  {:pre [(c/is? DbioPojo p)
         (c/is? DbioModel _model)]}

  (let [{:as m
         :keys [model]} (meta p)]
    (if (nil? model)
      (c/wm* p (assoc m :model _model))
      (c/raise! "Cannot bind model %s twice!" model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbpojo<>

  "Create object of type."
  {:arglists '([][model])}

  ([]
   (DbioPojo.))

  ([model]
   {:pre [(some? model)]}
   (bind-model (DbioPojo.) model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn mock-pojo<>

  "Clone object with pkey only."
  {:arglists '([obj])}
  [obj]

  (let [out (DbioPojo.)
        pk (:pkey (gmodel obj))]
    (c/wm* (assoc out pk (goid obj)) (meta obj))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn clr-fld

  "Clear a field in the pojo."
  {:arglists '([pojo fld])}
  [pojo fld]

  (dissoc pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-fld

  "Get a field value from the pojo."
  {:arglists '([pojo fld])}
  [pojo fld]

  (get pojo fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn set-fld

  "Assign a value to a field in the pojo."
  {:arglists '([pojo fld value])}
  [pojo fld value]
  {:pre [(keyword? fld)]}

  (if (check-field? pojo fld)
    (assoc pojo fld value)
    (u/throw-BadData "Invalid field %s." fld)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-flds*

  "Set field+values as: f1 v1 f2 v2 ... fn vn."
  [pojo & fvs]
  {:pre [(c/n#-even? fvs)]}

  (reduce #(set-fld %1
                    (c/_1 %2)
                    (c/_E %2)) pojo (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-flds

  "Set field+values as: f1 v1 f2 v2 ... fn vn."
  [pojo fvs]
  {:pre [(map? fvs)]}

  (reduce #(set-fld %1
                    (c/_1 %2)
                    (c/_E %2)) pojo fvs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

