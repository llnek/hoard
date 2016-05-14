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
    [czlab.xlib.str :refer [lcase ucase hgl? addDelim!]]
    [czlab.xlib.logging :as log]
    [clojure.string :as cs])

  (:use [czlab.dbio.core])

  (:import
    [czlab.dbio DBAPI DBIOError]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

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
(defmulti genTable (fn [a & more] a))
(defmulti genCol (fn [a & more] a))
(defmulti genIndex (fn [a & more] a))

(defmulti genAutoInteger (fn [a & more] a))
(defmulti genDouble (fn [a & more] a))
(defmulti genLong (fn [a & more] a))
(defmulti genFloat (fn [a & more] a))
(defmulti genAutoLong (fn [a & more] a))
(defmulti getTSDefault (fn [a & more] a))
(defmulti genTimestamp (fn [a & more] a))
(defmulti genDate (fn [a & more] a))
(defmulti genCaldr (fn [a & more] a))
(defmulti genBool (fn [a & more] a))
(defmulti genInteger (fn [a & more] a))
(defmulti genBytes (fn [a & more] a))
(defmulti genString (fn [a & more] a))

(defmulti getFloatKwd (fn [a] a))
(defmulti getIntKwd (fn [a] a))
(defmulti getTSKwd (fn [a] a))
(defmulti getDateKwd (fn [a] a))
(defmulti getBoolKwd (fn [a] a))
(defmulti getLongKwd (fn [a] a))
(defmulti getDoubleKwd (fn [a] a))
(defmulti getStringKwd (fn [a] a))
(defmulti getBlobKwd (fn [a] a))

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
  [db model]

  (str "DROP TABLE " (gtable model) (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin :default

  ^String
  [db model]

  (str "CREATE TABLE " (gtable model) " (\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEnd :default

  ^String
  [db model]

  (str "\n) " (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genGrant :default

  ^String
  [db model]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEndSQL :default

  ^String
  [db]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genIndex :default

  ^String
  [model xn]

  (ese (str (:table model) "_" xn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genTable :default

  ^String
  [model]

  (ese (:table model)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCol :default

  ^String
  [field]

  (ese (:column field)))

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
(defmethod genCaldr :default

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
(defn- genExIndexes "Generate external index definitions"

  ^String
  [db fields model]

  (let [m (collectDbXXX :indexes (.getMetas db) model)
        bf (StringBuilder.)]
    (doseq [[nm nv] m
            :let [cols (map #(genCol (get fields %)) nv)]]
      (when (empty? cols)
        (mkDbioError (str "Cannot have empty index: " nm)))
      (.append bf
               (str "CREATE INDEX "
                    (genIndex model (name nm))
                    " ON "
                    (genTable model)
                    " ("
                    (cs/join "," cols)
                    ") "
                    (genExec db) "\n\n")))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques ""

  [db fields model]

  (let [m (collectDbXXX :uniques (.getMetas db) model)
        bf (StringBuilder.)]
    (doseq [[nm nv] m
            :let [cols (map #(genCol (get fields %)) nv)]]
      (when (empty? cols)
        (mkDbioError (str "Illegal empty unique: " (name nm))))
      (addDelim! bf ",\n"
          (str (getPad db) "UNIQUE(" (cs/join "," cols) ")")))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPKey ""

  [db model pks]

  (str (getPad db)
       "PRIMARY KEY("
       (cs/join "," (map #(genCol %) pks))
       ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody ""

  [db model]

  (let [fields (collectDbXXX :fields (.getMetas db) model)
        inx (StringBuilder.)
        bf (StringBuilder.)]
    (with-local-vars [pkeys (transient #{})]
      ;; 1st do the columns
      (doseq [[fid fld] fields]
        (let [dt (:domain fld)
              col (case dt
                    :Boolean (genBool db fld)
                    :Timestamp (genTimestamp db fld)
                    :Date (genDate db fld)
                    :Calendar (genCaldr db fld)
                    :Int (if (:auto fld)
                           (genAutoInteger db model fld)
                           (genInteger db fld))
                    :Long (if (:auto fld)
                            (genAutoLong db model fld)
                            (genLong db fld))
                    :Double (genDouble db fld)
                    :Float (genFloat db fld)
                    (:Password :String) (genString db fld)
                    :Bytes (genBytes db fld)
                    (mkDbioError (str "Unsupported domain type " dt))) ]
          (when (:pkey fld) (var-set pkeys (conj! @pkeys fld)))
          (addDelim! bf ",\n" (genCol fld))))
      ;; now do the assocs
      ;; now explicit indexes
      (.append inx (genExIndexes db fields model))
      ;; now uniques, primary keys and done
      (when (> (.length bf) 0)
        (when (> (count @pkeys) 0)
          (.append bf (str ",\n"
                           (genPKey db model (persistent! @pkeys)))))
        (let [s (genUniques db fields model)]
          (when (hgl? s)
            (.append bf (str ",\n" s)))))
    [(.toString bf) (.toString inx)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable ""

  [db model]

  (let [b (genBegin db model)
        d (genBody db model)
        e (genEnd db model)
        s1 (str b (first d) e)
        inx (last d) ]
    (str s1
         (if (hgl? inx) inx "")
         (genGrant db model))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn getDDL  ""

  ^String
  [db]

  (binding [*DDL_BVS* (atom {})]
    (let [drops (StringBuilder.)
          body (StringBuilder.)
          ms (.getMetas db)]
      (doseq [[id model] ms
              :let [tbl (:table model)]]
        (when (and (not (:abstract model))
                   (hgl? tbl))
          (log/debug "Model Id: %s table: %s" (name id) tbl)
          (.append drops (genDrop db model))
          (.append body (genOneTable db model))))
      (str "" drops body (genEndSQL db)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

