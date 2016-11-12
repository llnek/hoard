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

(ns ^{:doc "Utility functions for DDL generation."
      :author "Kenneth Leung" }

  czlab.dbddl.drivers

  (:require
    [czlab.xlib.core :refer [try!]]
    [czlab.xlib.logging :as log]
    [czlab.xlib.str
     :refer [lcase
             ucase
             strbf<>
             strim
             hgl?
             addDelim!]]
    [clojure.string :as cs])

  (:use [czlab.dbio.core])

  (:import
    [czlab.dbio
     Schema
     DBAPI
     DBIOError]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- disp [a & xs] a)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti genBegin disp)
(defmulti genExec disp)
(defmulti genDrop disp)
(defmulti genEndSQL disp)
(defmulti genGrant disp)
(defmulti genEnd disp)
(defmulti genTable disp)
(defmulti genCol disp)
(defmulti genIndex disp)
;; data types
(defmulti genAutoInteger disp)
(defmulti genDouble disp)
(defmulti genLong disp)
(defmulti genFloat disp)
(defmulti genAutoLong disp)
(defmulti getTSDefault disp)
(defmulti genTimestamp disp)
(defmulti genDate disp)
(defmulti genCaldr disp)
(defmulti genBool disp)
(defmulti genInteger disp)
(defmulti genBytes disp)
(defmulti genString disp)
;; keywords
(defmulti getFloatKwd disp)
(defmulti getIntKwd disp)
(defmulti getTSKwd disp)
(defmulti getDateKwd disp)
(defmulti getBoolKwd disp)
(defmulti getLongKwd disp)
(defmulti getDoubleKwd disp)
(defmulti getStringKwd disp)
(defmulti getBlobKwd disp)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod getDoubleKwd :default [db] "double precision")
(defmethod getStringKwd :default [db] "varchar")
(defmethod getFloatKwd :default [db] "float")
(defmethod getIntKwd :default [db] "integer")
(defmethod getTSKwd :default [db] "timestamp")
(defmethod getDateKwd :default [db] "date")
(defmethod getBoolKwd :default [db] "integer")
(defmethod getLongKwd :default [db] "bigint")
(defmethod getBlobKwd :default [db] "blob")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gSQLId

  "Format SQL identifier"

  ^String
  [idstr & [quote?]]

  (let [f  (:case-fn *DDL_CFG*)
        ch (:qstr *DDL_CFG*)
        id (f idstr)]
    (if (false? quote?)
      id
      (str ch id ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gtable

  "Get the table name (quoted) of this model"

  ^String
  [model & [quote?]]
  {:pre [(map? model)]}

  (gSQLId (:table model) quote?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gcolumn

  "Get the column name (quoted) of this field"
  ^String
  [field & [quote?]]
  {:pre [(map? field)]}

  (gSQLId (:column field) quote?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gcn

  "Get column name"
  [fields fid]

  (genCol (get fields fid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNotNull "" ^String [_] "not null")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNull "" ^String [_] "null")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getPad

  ""
  {:no-doc true
   :tag String}

  [_] "  ")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro nullClause

  ""
  ^String
  [dbtype opt?]

  `(let [d# ~dbtype]
    (if ~opt? (getNull d#) (getNotNull d#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro genSep

  ""
  ^String
  [_]

  `(if (:use-sep? *DDL_CFG*) DDL_SEP ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genExec

  :default

  [dbtype]

  (str ";\n" (genSep dbtype)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop

  :default

  [dbtype model]

  (str "drop table "
       (gtable model) (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin

  :default

  [_ model]

  (str "create table " (gtable model) " (\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEnd

  :default

  [dbtype model]

  (str "\n) " (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genGrant :default [_ _] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEndSQL :default [_] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genIndex

  :default

  [model xn]

  (gSQLId (str (:table model) "_" xn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genTable :default [model] (gtable model))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCol :default [field] (gcolumn field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn genColDef

  ""
  ^String
  [dbtype typedef field]

  (let [dft (first (:dft field))]
    (str (getPad dbtype)
         (genCol field)
         (str " " typedef " ")
         (nullClause dbtype (:null? field))
         (if (hgl? dft) (str " default " dft) ""))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBytes

  :default

  [dbtype field]

  (genColDef dbtype (getBlobKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genString

  :default

  [dbtype field]

  (genColDef dbtype
             (str (getStringKwd dbtype)
                  "(" (:size field) ")")
             field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genInteger

  :default

  [dbtype field]

  (genColDef dbtype
             (getIntKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger :default [_ _ _] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDouble

  :default

  [dbtype field]

  (genColDef dbtype
             (getDoubleKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genFloat

  :default

  [dbtype field]

  (genColDef dbtype
             (getFloatKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genLong

  :default

  [dbtype field]

  (genColDef dbtype
             (getLongKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong :default [_ _ _] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod getTSDefault :default [_] "CURRENT_TIMESTAMP")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genTimestamp

  :default

  [dbtype field]

  (genColDef dbtype
             (getTSKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDate

  :default

  [dbtype field]

  (genColDef dbtype
             (getDateKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCaldr

  :default

  [dbtype field]

  (genTimestamp dbtype field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBool

  :default

  [dbtype field]

  (genColDef dbtype
             (getBoolKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genExIndexes

  "Generate external index definitions"
  ^String
  [dbtype schema fields model]

  (str
    (reduce
      (fn [b [k v]]
        (when-not (empty? v)
          (-> ^StringBuilder
              b
              (.append
                (str "create index "
                     (genIndex model (name k))
                     " on "
                     (genTable model)
                     " ("
                     (->> (map #(genCol (fields %)) v)
                          (cs/join "," ))
                     ") "
                     (genExec dbtype) "\n\n"))))
        b)
      (strbf<>)
      (:indexes model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques

  ""
  ^String
  [dbtype schema fields model]

  (str
    (reduce
      (fn [b [_ v]]
        (when-not (empty? v)
          (addDelim!
            b
            ",\n"
            (str (getPad dbtype)
                 "unique("
                 (->> (map #(genCol (fields %)) v)
                      (cs/join "," ))
                 ")")))
        b)
      (strbf<>)
      (:uniques model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPKey

  ""
  ^String
  [dbtype model pks]

  (str (getPad dbtype)
       "primary key("
       (cs/join "," (map #(genCol %) pks))
       ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody

  ""
  ^String
  [dbtype schema model]

  (let
    [fields (:fields model)
     pke (:pkey model)
     bf (strbf<>)
     pkeys
     (persistent!
       (reduce
         (fn [p [k fld]]
           (->> (case (:domain fld)
                  :Timestamp (genTimestamp dbtype fld)
                  :Date (genDate dbtype fld)
                  :Calendar (genCaldr dbtype fld)
                  :Boolean (genBool dbtype fld)
                  :Int (if (:auto? fld)
                         (genAutoInteger dbtype model fld)
                         (genInteger dbtype fld))
                  :Long (if (:auto? fld)
                          (genAutoLong dbtype model fld)
                          (genLong dbtype fld))
                  :Double (genDouble dbtype fld)
                  :Float (genFloat dbtype fld)
                  (:Password :String) (genString dbtype fld)
                  :Bytes (genBytes dbtype fld)
                  (dberr! "Unsupported field: %s" fld))
                (addDelim! bf ",\n" ))
           (if (= pke (:id fld))
             (conj! p fld)
             p))
         (transient [])
         fields))]
    (when (> (.length bf) 0)
      (when-not (empty? pkeys)
        (.append bf (str ",\n"
                         (genPKey dbtype model pkeys))))
      (let [s (genUniques dbtype schema fields model)]
        (when (hgl? s)
          (.append bf (str ",\n" s)))))
    [(str bf)
     (genExIndexes dbtype schema fields model)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable

  ""
  ^String
  [dbtype schema model]

  (let [d (genBody dbtype schema model)
        b (genBegin dbtype model)
        e (genEnd dbtype model)]
    (str b
         (first d)
         e
         (last d)
         (genGrant dbtype model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDDL

  "Generate database DDL for this schema"
  ^String
  [^Schema schema dbID & [dbver]]

  (binding [*DDL_CFG* {:db-version (strim dbver)
                       :use-sep? true
                       :qstr ""
                       :case-fn clojure.string/upper-case}
            *DDL_BVS* (atom {})]
    (let [ms (.models schema)
          drops (strbf<>)
          body (strbf<>)]
      (doseq [[id model] ms
              :let [tbl (:table model)]
              :when (and (not (:abstract? model))
                         (hgl? tbl))]
        (log/debug "model id: %s, table: %s" (name id) tbl)
        (.append drops (genDrop dbID model))
        (.append body (genOneTable dbID schema model)))
      (str drops body (genEndSQL dbID)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


