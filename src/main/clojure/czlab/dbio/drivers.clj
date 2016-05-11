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

  czlab.xlib.dbio.drivers

  (:require
    [czlab.xlib.util.str
    :refer [lcase ucase hgl? AddDelim! ]]
    [czlab.xlib.util.logging :as log]
    [clojure.string :as cs])

  (:use [czlab.xlib.dbio.core])

  (:import
    [com.zotohlab.frwk.dbio MetaCache DBAPI DBIOError]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gcn ""

  ^String
  [flds fid]

  (let [c (:column (get flds fid)) ]
    (if (hgl? c) (ucase c) c)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNotNull  "" [db] "NOT NULL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro getNull  "" [db] "NULL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn GetPad  ""

  ^String
  [db]

  "    ")

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn GenCol ""

  ^String
  [fld]

  (ucase (:column fld)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti GenBegin (fn [a & more] a))
(defmulti GenExec (fn [a & more] a))
(defmulti GenDrop (fn [a & more] a))

(defmulti GenEndSQL (fn [a & more] a))
(defmulti GenGrant (fn [a & more] a))
(defmulti GenEnd (fn [a & more] a))

(defmulti GenAutoInteger (fn [a & more] a))
(defmulti GenDouble (fn [a & more] a))
(defmulti GenLong (fn [a & more] a))
(defmulti GenFloat (fn [a & more] a))
(defmulti GenAutoLong (fn [a & more] a))
(defmulti GetTSDefault (fn [a & more] a))
(defmulti GenTimestamp (fn [a & more] a))
(defmulti GenDate (fn [a & more] a))
(defmulti GenCal (fn [a & more] a))
(defmulti GenBool (fn [a & more] a))
(defmulti GenInteger (fn [a & more] a))

(defmulti GetFloatKeyword (fn [a & more] a))
(defmulti GetIntKeyword (fn [a & more] a))
(defmulti GetTSKeyword (fn [a & more] a))
(defmulti GetDateKeyword (fn [a & more] a))
(defmulti GetBoolKeyword (fn [a & more] a))
(defmulti GetLongKeyword (fn [a & more] a))
(defmulti GetDoubleKeyword (fn [a & more] a))
(defmulti GetStringKeyword (fn [a & more] a))
(defmulti GetBlobKeyword (fn [a & more] a))
(defmulti GenBytes (fn [a & more] a))
(defmulti GenString (fn [a & more] a))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenExec :default

  ^String
  [db]

  (str ";\n" (genSep db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenDrop :default

  ^String
  [db table]

  (str "DROP TABLE " table (GenExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenBegin :default

  ^String
  [db table]

  (str "CREATE TABLE " table "\n(\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenEnd :default

  ^String
  [db table]

  (str "\n)" (GenExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenGrant :default

  ^String
  [db table]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenEndSQL :default

  ^String
  [db]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn GenColDef

  ^String
  [db ^String col ty opt? dft]

  (str (GetPad db)
       (ucase col)
       " " ty " "
       (nullClause db opt?)
       (if (nil? dft) "" (str " DEFAULT " dft))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GetFloatKeyword :default [db] "FLOAT")
(defmethod GetIntKeyword :default [db] "INTEGER")
(defmethod GetTSKeyword :default [db] "TIMESTAMP")
(defmethod GetDateKeyword :default [db] "DATE")
(defmethod GetBoolKeyword :default [db] "INTEGER")
(defmethod GetLongKeyword :default [db] "BIGINT")
(defmethod GetDoubleKeyword :default [db] "DOUBLE PRECISION")
(defmethod GetStringKeyword :default [db] "VARCHAR")
(defmethod GetBlobKeyword :default [db] "BLOB")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenBytes :default

  [db fld]

  (GenColDef db (:column fld) (GetBlobKeyword db) (:null fld) nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenString :default

  [db fld]

  (GenColDef db
             (:column fld)
             (str (GetStringKeyword db)
                  "("
                  (:size fld) ")")
             (:null fld)
             (if (:dft fld) (first (:dft fld)) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenInteger :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetIntKeyword db)
             (:null fld)
             (if (:dft fld) (first (:dft fld)) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoInteger :default

  [db table fld]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenDouble :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetDoubleKeyword db)
             (:null fld)
             (if (:dft fld) (first (:dft fld)) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenFloat :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetFloatKeyword db)
             (:null fld)
             (if (:dft fld) (first (:dft fld)) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenLong :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetLongKeyword db)
             (:null fld)
             (if (:dft fld) (first (:dft fld)) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoLong :default

  [db table fld]

  "")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GetTSDefault :default

  [db]

  "CURRENT_TIMESTAMP")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenTimestamp :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetTSKeyword db)
             (:null fld)
             (if (:dft fld) (GetTSDefault db) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenDate :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetDateKeyword db)
             (:null fld)
             (if (:dft fld) (GetTSDefault db) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenCal :default

  [db fld]

  (GenTimestamp db fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenBool :default

  [db fld]

  (GenColDef db
             (:column fld)
             (GetBoolKeyword db)
             (:null fld)
             (if (:dft fld) (first (:dft fld)) nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genExIndexes ""

  ^String
  [db cache table flds mcz]

  (let [m (CollectDbXXX :indexes cache mcz)
        bf (StringBuilder.) ]
    (doseq [[nm nv] m
            :let [cols (map #(gcn flds %) nv) ]]
      (when (empty? cols)
        (DbioError (str "Cannot have empty index: " nm)))
      (.append bf (str "CREATE INDEX "
                       (lcase (str table "_" (name nm)))
                       " ON " table
                       " ( "
                       (cs/join "," cols)
                       " )"
                       (GenExec db) "\n\n" )))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genUniques ""

  [db cache flds mcz]

  (let [m (CollectDbXXX :uniques cache mcz)
        bf (StringBuilder.) ]
    (doseq [[nm nv] m
            :let [cols (map #(gcn flds %) nv) ]]
      (when (empty? cols)
        (DbioError (str "Illegal empty unique: " (name nm))))
      (AddDelim! bf ",\n"
          (str (GetPad db) "UNIQUE(" (cs/join "," cols) ")")))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genPrimaryKey ""

  [db mcz pks]

  (str (GetPad db)
       "PRIMARY KEY("
       (ucase (cs/join "," pks) )
       ")"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genBody ""

  [db cache table mcz]

  (let [flds (CollectDbXXX :fields cache mcz)
        bf (StringBuilder.)
        inx (StringBuilder.) ]
    (with-local-vars [pkeys (transient #{}) ]
      ;; 1st do the columns
      (doseq [[fid fld] flds]
        (let [cn (ucase (:column fld))
              dt (:domain fld)
              col (case dt
                    :Boolean (GenBool db fld)
                    :Timestamp (GenTimestamp db fld)
                    :Date (GenDate db fld)
                    :Calendar (GenCal db fld)
                    :Int (if (:auto fld)
                           (GenAutoInteger db table fld)
                           (GenInteger db fld))
                    :Long (if (:auto fld)
                            (GenAutoLong db table fld)
                            (GenLong db fld))
                    :Double (GenDouble db fld)
                    :Float (GenFloat db fld)
                    (:Password :String) (GenString db fld)
                    :Bytes (GenBytes db fld)
                    (DbioError (str "Unsupported domain type " dt))) ]
          (when (:pkey fld) (var-set pkeys (conj! @pkeys cn)))
          (AddDelim! bf ",\n" col)))
      ;; now do the assocs
      ;; now explicit indexes
      (-> inx (.append (genExIndexes db cache table flds mcz)))
      ;; now uniques, primary keys and done.
      (when (> (.length bf) 0)
        (when (> (count @pkeys) 0)
          (.append bf (str ",\n" (genPrimaryKey db mcz (persistent! @pkeys)))))
        (let [s (genUniques db cache flds mcz) ]
          (when (hgl? s)
            (.append bf (str ",\n" s)))))
    [ (.toString bf) (.toString inx) ] )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- genOneTable ""

  [db mcache mcz]

  (let [table (ucase (:table mcz))
        b (GenBegin db table)
        d (genBody db mcache table mcz)
        e (GenEnd db table)
        s1 (str b (first d) e)
        inx (last d) ]
    (str s1
         (if (hgl? inx) inx "")
         (GenGrant db table))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn GetDDL  ""

  ^String
  [^MetaCache metaCache db ]

  (binding [*DDL_BVS* (atom {})]
    (let [ms (.getMetas metaCache)
          drops (StringBuilder.)
          body (StringBuilder.) ]
      (doseq [[id tdef] ms
              :let [tbl (:table tdef) ]]
        (when (and (not (:abstract tdef))
                   (hgl? tbl))
          (log/debug "Model Id: %s table: %s" (name id) tbl)
          (-> drops (.append (GenDrop db (ucase tbl) )))
          (-> body (.append (genOneTable db ms tdef)))))
      (str "" drops body (GenEndSQL db)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

