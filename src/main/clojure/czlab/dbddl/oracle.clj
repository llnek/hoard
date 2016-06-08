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

  czlab.dbddl.oracle

  (:require [czlab.xlib.logging :as log])

  (:use [czlab.dbddl.drivers]
        [czlab.dbio.core]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createSeq

  ""
  [dbtype model field]

  (let [s (gSQLId (str "S_" (:table model) "_" (:column field)))
        t (gSQLId (str "T_" (:table model) "_" (:column field)))]
    (str "CREATE SEQUENCE "
         s
         " START WITH 1 INCREMENT BY 1"
         (genExec dbtype)
         "\n\n"
         "CREATE OR REPLACE TRIGGER "
         t
         "\n"
         "BEFORE INSERT ON "
         (gtable model)
         "\n"
         "REFERENCING NEW AS NEW\n"
         "FOR EACH ROW\n"
         "BEGIN\n"
         "SELECT "
         s
         ".NEXTVAL INTO :NEW."
         (gcolumn field) " FROM DUAL;\n"
         "END" (genExec dbtype) "\n\n")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Oracle
(defmethod getTSDefault Oracle [_] "DEFAULT SYSTIMESTAMP")
(defmethod getStringKwd Oracle [_] "VARCHAR2")
(defmethod getLongKwd Oracle [_] "NUMBER(38)")
(defmethod getDoubleKwd Oracle [_] "BINARY_DOUBLE")
(defmethod getFloatKwd Oracle [_] "BINARY_FLOAT")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- maybeTrackFields

  ""
  [model field]

  (if-not (or (= "12c+" (*DDL_CFG* :db-version))
              (= "12c" (*DDL_CFG* :db-version)))
    (let [m (deref *DDL_BVS*)
          t (:id model)
          r (or (m t) {})]
      (->> (assoc r (:id field) field)
           (swap! *DDL_BVS*  assoc t)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger

  Oracle

  [dbtype model field]

  (maybeTrackFields model field)
  (genInteger dbtype field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong

  Oracle

  [dbtype model field]

  (maybeTrackFields model field)
  (genLong dbtype field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEndSQL

  Oracle

  [dbtype]

  (if (or (= "12c+" (*DDL_CFG* :db-version))
          (= "12c" (*DDL_CFG* :db-version)))
    ""
    (str
      (reduce
        (fn [bd [model fields]]
          (reduce
            (fn [bd [_ fld]]
              (.append bd (createSeq dbtype model fld)))
            bd
            fields))
        (StringBuilder.)
        (deref *DDL_BVS*)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop

  Oracle

  [dbtype model]

  (str "DROP TABLE "
       (gtable model)
       " CASCADE CONSTRAINTS PURGE"
       (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


