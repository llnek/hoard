;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Utility functions for DDL generation."
      :author "Kenneth Leung"}

  czlab.horde.dbddl.drivers

  (:require [czlab.basal.logging :as log]
            [clojure.string :as cs])

  (:use [czlab.horde.dbio.core]
        [czlab.basal.core]
        [czlab.basal.str])

  (:import [czlab.horde Schema DbApi DbioError]))

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
  "Format SQL identifier" {:tag String}

  ([idstr] (gSQLId idstr nil))
  ([idstr quote?]
   (let [f  (:case-fn *ddl-cfg*)
         ch (:qstr *ddl-cfg*)
         id (f idstr)]
     (if (false? quote?) id (str ch id ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gtable
  "Get the table name (quoted)" {:tag String}

  ([model] (gtable model nil))
  ([model quote?]
   {:pre [(map? model)]} (gSQLId (:table model) quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gcolumn
  "Get the column name (quoted)" {:tag String}

  ([field] (gcolumn field nil))
  ([field quote?]
   {:pre [(map? field)]} (gSQLId (:column field) quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private gcn
  "Get column name" [fields fid] `(genCol (get ~fields ~fid)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNotNull "" [_] "not null")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNull "" [_] "null")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getPad "" {:no-doc true} [_] "  ")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro nullClause
  "" [dbtype opt?]
  `(let [d# ~dbtype] (if ~opt? (getNull d#) (getNotNull d#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro genSep "" [_] `(if (:use-sep? *ddl-cfg*) ddl-sep ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genExec :default [dbtype] (str ";\n" (genSep dbtype)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop
  :default
  [dbtype model] (str "drop table "
                      (gtable model) (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin :default [_ model] (str "create table " (gtable model) " (\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEnd :default [dbtype _] (str "\n) " (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genGrant :default [_ _] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEndSQL :default [_] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genIndex :default [model xn] (gSQLId (str (:table model) "_" xn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genTable :default [model] (gtable model))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCol :default [field] (gcolumn field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn genColDef
  "" ^String [dbtype typedef field]

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
  [dbtype field] (genColDef dbtype (getBlobKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genString
  :default
  [dbtype field]
  (genColDef dbtype
             (str (getStringKwd dbtype)
                  "(" (:size field) ")") field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genInteger
  :default
  [dbtype field] (genColDef dbtype (getIntKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger :default [_ _ _] "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDouble
  :default
  [dbtype field] (genColDef dbtype (getDoubleKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genFloat
  :default
  [dbtype field] (genColDef dbtype (getFloatKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genLong
  :default
  [dbtype field] (genColDef dbtype (getLongKwd dbtype) field))

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
  [dbtype field] (genColDef dbtype (getTSKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDate
  :default
  [dbtype field] (genColDef dbtype (getDateKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCaldr :default [dbtype field] (genTimestamp dbtype field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBool :default [dbtype field] (genColDef dbtype (getBoolKwd dbtype) field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genExIndexes
  "external indexes"
  ^String [dbtype schema fields model]

  (sreduce<>
    (fn [^StringBuilder b [k v]]
      (when-not (empty? v)
        (.append b
          (str "create index "
               (genIndex model (name k))
               " on "
               (genTable model)
               " ("
               (->> (map #(genCol (fields %)) v)
                    (cs/join "," ))
               ") "
               (genExec dbtype) "\n\n")))
      b)
    (:indexes model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques
  "" ^String [dbtype schema fields model]

  (sreduce<>
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
    (:uniques model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPKey
  "" ^String [dbtype model pks]
  (str (getPad dbtype)
       "primary key(" (cs/join "," (map #(genCol %) pks)) ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody
  "" ^String [dbtype schema model]

  (let [fields (:fields model)
        pke (:pkey model)
        bf (strbf<>)
        pkeys
        (preduce<vec>
          (fn [p [k fld]]
            (->>
              (case (:domain fld)
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
            (if (= pke (:id fld)) (conj! p fld) p)) fields)]
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
  "" ^String [dbtype schema model]

  (let [d (genBody dbtype schema model)
        b (genBegin dbtype model)
        e (genEnd dbtype model)]
    (str b (first d) e (last d) (genGrant dbtype model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDdl
  "Generate database DDL
  for this schema" {:tag String}

  ([schema dbID] (getDdl schema dbID nil))
  ([^Schema schema dbID dbver]
   (binding [*ddl-cfg* {:db-version (strim dbver)
                        :use-sep? true
                        :qstr ""
                        :case-fn clojure.string/upper-case}
             *ddl-bvs* (atom {})]
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
       (str drops body (genEndSQL dbID))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

