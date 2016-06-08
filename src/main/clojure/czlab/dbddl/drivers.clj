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

  czlab.dbddl.drivers

  (:require
    [czlab.xlib.logging :as log]
    [czlab.xlib.str
     :refer [lcase
             ucase
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
(defn- disp [a & more] a)

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
(defmethod getDoubleKwd :default [db] "DOUBLE PRECISION")
(defmethod getFloatKwd :default [db] "FLOAT")
(defmethod getIntKwd :default [db] "INTEGER")
(defmethod getTSKwd :default [db] "TIMESTAMP")
(defmethod getDateKwd :default [db] "DATE")
(defmethod getBoolKwd :default [db] "INTEGER")
(defmethod getLongKwd :default [db] "BIGINT")
(defmethod getStringKwd :default [db] "VARCHAR")
(defmethod getBlobKwd :default [db] "BLOB")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gSQLId

  "Format SQL identifier"

  [idstr & [quote?]]

  (let [ch (:qstr *DDL_CFG*)]
    (if (false? quote?)
      idstr
      (str ch idstr ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gtable

  "Get the table name (escaped) of this model"

  [model & [quote?]]

  {:pre [(map? model)]}

  (gSQLId ((:case-fn *DDL_CFG*) (:table model)) quote?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gcolumn

  "Get the column name (escaped) of this field"

  [field & [quote?]]

  {:pre [(map? field)]}

  (gSQLId ((:case-fn *DDL_CFG*) (:column field)) quote?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gcn

  "Get column name"

  [fields fid]

  (genCol (get fields fid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNotNull "" [_] "NOT NULL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNull "" [_] "NULL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getPad  "" ^:no-doc [_] "  ")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro nullClause

  ""

  [dbtype opt?]

  `(let [d# ~dbtype]
    (if ~opt? (getNull d#) (getNotNull d#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro genSep

  ""

  [_]

  `(if (:use-sep *DDL_CFG*) DDL_SEP ""))

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

  (str "DROP TABLE "
       (gtable model) (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin

  :default

  [_ model]

  (str "CREATE TABLE " (gtable model) " (\n"))

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

  (let [fu (:case-fn *DDL_CFG*)
        ch (:qstr *DDL_CFG*)]
    (str ch
         (fu (str (:table model) "_" xn))
         ch)))

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

  [dbtype typedef field]

  ;;(println "field = " field)
  (let [dft (first (:dft field))]
    (str (getPad dbtype)
         (genCol field)
         (str " " typedef " ")
         (nullClause dbtype (:null field))
         (if (hgl? dft) (str " DEFAULT " dft) ""))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBytes

  :default

  [dbtype field]

  (genColDef dbtype (getBlobKwd db) field))

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

  [dbtype schema fields model]

  (str
    (reduce
      (fn [b [k v]]
        (when-not (empty? v)
          (.append
            b
            (str "CREATE INDEX "
                 (genIndex model (name k))
                 " ON "
                 (genTable model)
                 " ("
                 (->> (map #(genCol (fields %)) v)
                      (cs/join "," ))
                 ") "
                 (genExec dbtype) "\n\n")))
        b)
      (StringBuilder.)
      (collectDbXXX :indexes schema model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques

  ""

  [dbtype schema fields model]

  (str
    (reduce
      (fn [b [_ v]]
        (when-not (empty? v)
          (addDelim!
            b
            ",\n"
            (str (getPad dbtype)
                 "UNIQUE("
                 (->> (map #(genCol (fields %)) v)
                      (cs/join "," ))
                 ")")))
        b)
      (StringBuilder.)
      (collectDbXXX :uniques schema model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPKey

  ""

  [dbtype model pks]

  (str (getPad dbtype)
       "PRIMARY KEY("
       (cs/join "," (map #(genCol %) pks))
       ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody

  ""

  [dbtype schema model]

  (let
    [fields (collectDbXXX :fields schema model)
     bf (StringBuilder.)
     pkeys
     (persistent!
       (reduce
         (fn [p [_ fld]]
           (->> (case (:domain fld)
                  :Timestamp (genTimestamp dbtype fld)
                  :Date (genDate dbtype fld)
                  :Calendar (genCaldr dbtype fld)
                  :Boolean (genBool dbtype fld)
                  :Int (if (:auto fld)
                         (genAutoInteger dbtype model fld)
                         (genInteger dbtype fld))
                  :Long (if (:auto fld)
                          (genAutoLong dbtype model fld)
                          (genLong dbtype fld))
                  :Double (genDouble dbtype fld)
                  :Float (genFloat dbtype fld)
                  (:Password :String) (genString dbtype fld)
                  :Bytes (genBytes dbtype fld)
                  (throwDBError (str "Unsupported field " fld)))
                (addDelim! bf ",\n" ))
           (if (:pkey fld)
             (conj! p fld)
             p))
         (transient {})
         fields))]
    (when (> (.length bf) 0)
      (when-not (empty? pkeys)
        (.append bf (str ",\n"
                         (genPKey dbtype model pkeys))))
      (let [s (genUniques dbtype schema fields model)]
        (when (hgl? s)
          (.append bf (str ",\n" s)))))
    [(str bf) (genExIndexes dbtype schema fields model)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable

  ""

  [dbtype schema model]

  (let [b (genBegin dbtype model)
        d (genBody dbtype schema model)
        e (genEnd dbtype model)]
    (str b
         (first d)
         e
         (last d)
         (genGrant dbtype model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDDL

  ""

  ^String
  [^Schema schema dbID & [dbver]]

  (binding [*DDL_CFG* {:db-version (strim dbver)
                       :use-sep true
                       :qstr ""
                       :case-fn ucase}
            *DDL_BVS* (atom {})]
    (let [drops (StringBuilder.)
          body (StringBuilder.)
          ms (.getModels schema)]
      (doseq [[id model] ms
              :let [tbl (:table model)]
              :when (and (not (:abstract model))
                         (hgl? tbl))]
        (log/debug "Model Id: %s, table: %s" (name id) tbl)
        (.append drops (genDrop dbID model))
        (.append body (genOneTable dbID schema model)))
      (str drops body (genEndSQL dbID)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


