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

  czlab.dbio.drivers

  (:import
    [czlab.dbio
     MetaCache
     DBAPI
     DBIOError])

  (:require
    [czlab.xlib.logging :as log]
    [clojure.string :as cs]
    [czlab.xlib.str
     :refer [lcase
             ucase
             hgl?
             addDelim!]])

  (:use [czlab.dbio.core]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn genCol ""

  ^String
  [field]

  (ucase (:column field)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gcn "Get column name"

  ^String
  [fields fid]

  (genCol (get fields fid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNotNull  "" [db] "NOT NULL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNull  "" [db] "NULL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getPad  ""

  ^String
  [db]

  "  ")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro nullClause ""

  [db opt?]

  `(let [d# ~db]
    (if ~opt? (getNull d#) (getNotNull d#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro genSep ""

  ^String
  [db]

  `(if *USE_DDL_SEP* DDL_SEP ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti genBegin (fn [a & more] a))
(defmulti genExec (fn [a & more] a))
(defmulti genDrop (fn [a & more] a))

(defmulti genEndSQL (fn [a & more] a))
(defmulti genGrant (fn [a & more] a))
(defmulti genEnd (fn [a & more] a))

(defmulti genAutoInteger (fn [a & more] a))
(defmulti genDouble (fn [a & more] a))
(defmulti genLong (fn [a & more] a))
(defmulti genFloat (fn [a & more] a))
(defmulti genAutoLong (fn [a & more] a))
(defmulti getTSDefault (fn [a & more] a))
(defmulti genTimestamp (fn [a & more] a))
(defmulti genDate (fn [a & more] a))
(defmulti genCal (fn [a & more] a))
(defmulti genBool (fn [a & more] a))
(defmulti genInteger (fn [a & more] a))

(defmulti getFloatKwd (fn [a & more] a))
(defmulti getIntKwd (fn [a & more] a))
(defmulti getTSKwd (fn [a & more] a))
(defmulti getDateKwd (fn [a & more] a))
(defmulti getBoolKwd (fn [a & more] a))
(defmulti getLongKwd (fn [a & more] a))
(defmulti getDoubleKwd (fn [a & more] a))
(defmulti getStringKwd (fn [a & more] a))
(defmulti getBlobKwd (fn [a & more] a))
(defmulti genBytes (fn [a & more] a))
(defmulti genString (fn [a & more] a))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genExec :default

  ^String
  [db]

  (str ";\n" (genSep db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop :default

  ^String
  [db table]

  (str "DROP TABLE " table (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin :default

  ^String
  [db table]

  (str "CREATE TABLE " table " (\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEnd :default

  ^String
  [db table]

  (str "\n) " (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genGrant :default

  ^String
  [db table]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEndSQL :default

  ^String
  [db]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn genColDef ""

  ^String
  [db typedef field]

  (let [dft (first (:dft field))]
    (str (getPad db)
         (genCol field)
         " "
         typedef
         " "
         (nullClause db (:null field))
         (if (nil? dft) "" (str " DEFAULT " dft)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod getFloatKwd :default [db] "FLOAT")
(defmethod getIntKwd :default [db] "INTEGER")
(defmethod getTSKwd :default [db] "TIMESTAMP")
(defmethod getDateKwd :default [db] "DATE")
(defmethod getBoolKwd :default [db] "INTEGER")
(defmethod getLongKwd :default [db] "BIGINT")
(defmethod getDoubleKwd :default [db] "DOUBLE PRECISION")
(defmethod getStringKwd :default [db] "VARCHAR")
(defmethod getBlobKwd :default [db] "BLOB")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBytes :default

  [db field]

  (genColDef db (getBlobKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genString :default

  [db field]

  (genColDef db
             (str (getStringKwd db)
                  "(" (:size field) ")")
             field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genInteger :default

  [db field]

  (genColDef db
             (getIntKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger :default

  [db table field]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDouble :default

  [db field]

  (genColDef db
             (getDoubleKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genFloat :default

  [db field]

  (genColDef db
             (getFloatKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genLong :default

  [db field]

  (genColDef db
             (getLongKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong :default

  [db table field]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod getTSDefault :default

  [db]

  "CURRENT_TIMESTAMP")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genTimestamp :default

  [db field]

  (genColDef db
             (getTSKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDate :default

  [db field]

  (genColDef db
             (getDateKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCal :default

  [db field]

  (genTimestamp db field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBool :default

  [db field]

  (genColDef db
             (getBoolKwd db) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genExIndexes ""

  ^String
  [db cache table fields model]

  (let [m (collectDbXXX :indexes cache model)
        bf (StringBuilder.)]
    (doseq [[nm nv] m
            :let [cols (map #(gcn flds %) nv)]]
      (when (empty? cols)
        (mkDbioError (str "Cannot have empty index: " nm)))
      (.append bf
               (str "CREATE INDEX "
                    (lcase (str table "_" (name nm)))
                    " ON "
                    table
                    " ("
                    (cs/join "," cols)
                    ")"
                    (genExec db) "\n\n")))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques ""

  [db cache fields model]

  (let [m (collectDbXXX :uniques cache model)
        bf (StringBuilder.)]
    (doseq [[nm nv] m
            :let [cols (map #(gcn fields %) nv)]]
      (when (empty? cols)
        (mkDbioError (str "Illegal empty unique: " (name nm))))
      (addDelim! bf ",\n"
          (str (getPad db) "UNIQUE(" (cs/join "," cols) ")")))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPrimaryKey ""

  [db model pks]

  (str (getPad db)
       "PRIMARY KEY("
       (ucase (cs/join "," pks))
       ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody ""

  [db cache table model]

  (let [fields (collectDbXXX :fields cache model)
        inx (StringBuilder.)
        bf (StringBuilder.)]
    (with-local-vars [pkeys (transient #{})]
      ;; 1st do the columns
      (doseq [[fid fld] fields]
        (let [dt (:domain fld)
              cn (genCol fld)
              col (case dt
                    :Boolean (genBool db fld)
                    :Timestamp (genTimestamp db fld)
                    :Date (genDate db fld)
                    :Calendar (genCal db fld)
                    :Int (if (:auto fld)
                           (genAutoInteger db table fld)
                           (genInteger db fld))
                    :Long (if (:auto fld)
                            (genAutoLong db table fld)
                            (genLong db fld))
                    :Double (genDouble db fld)
                    :Float (genFloat db fld)
                    (:Password :String) (genString db fld)
                    :Bytes (genBytes db fld)
                    (mkDbioError (str "Unsupported domain type " dt))) ]
          (when (:pkey fld) (var-set pkeys (conj! @pkeys cn)))
          (addDelim! bf ",\n" col)))
      ;; now do the assocs
      ;; now explicit indexes
      (-> inx (.append (genExIndexes db cache table fields model)))
      ;; now uniques, primary keys and done
      (when (> (.length bf) 0)
        (when (> (count @pkeys) 0)
          (.append bf (str ",\n" (genPrimaryKey db model (persistent! @pkeys)))))
        (let [s (genUniques db cache fields model)]
          (when (hgl? s)
            (.append bf (str ",\n" s)))))
    [(.toString bf) (.toString inx)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable ""

  [db mcache model]

  (let [table (ucase (:table model))
        b (genBegin db table)
        d (genBody db mcache table model)
        e (genEnd db table)
        s1 (str b (first d) e)
        inx (last d) ]
    (str s1
         (if (hgl? inx) inx "")
         (genGrant db table))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDDL  ""

  ^String
  [^MetaCache metaCache db]

  (binding [*DDL_BVS* (atom {})]
    (let [ms (.getMetas metaCache)
          drops (StringBuilder.)
          body (StringBuilder.)]
      (doseq [[id tdef] ms
              :let [tbl (:table tdef)]]
        (when (and (not (:abstract tdef))
                   (hgl? tbl))
          (log/debug "Model Id: %s table: %s" (name id) tbl)
          (-> drops (.append (genDrop db (ucase tbl) )))
          (-> body (.append (genOneTable db ms tdef)))))
      (str "" drops body (genEndSQL db)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

