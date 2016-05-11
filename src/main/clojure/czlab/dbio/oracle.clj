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

  czlab.xlib.dbio.oracle

  (:require [czlab.xlib.util.logging :as log])

  (:use [czlab.xlib.dbio.drivers]
        [czlab.xlib.dbio.core]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createSequenceTrigger

  [db table col]

  (str "CREATE OR REPLACE TRIGGER TRIG_" table "\n"
       "BEFORE INSERT ON " table "\n"
       "REFERENCING NEW AS NEW\n"
       "FOR EACH ROW\n"
       "BEGIN\n"
       "SELECT SEQ_" table ".NEXTVAL INTO :NEW."
       col " FROM DUAL;\n"
       "END" (GenExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createSequence

  [db table]

  (str "CREATE SEQUENCE SEQ_" table
       " START WITH 1 INCREMENT BY 1"
       (GenExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Oracle
(defmethod GetStringKeyword Oracle [db] "VARCHAR2")
(defmethod GetTSDefault Oracle [db] "DEFAULT SYSTIMESTAMP")
(defmethod GetLongKeyword Oracle [db] "NUMBER(38)")
(defmethod GetDoubleKeyword Oracle [db] "BINARY_DOUBLE")
(defmethod GetFloatKeyword Oracle [db] "BINARY_FLOAT")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoInteger Oracle

  [db table fld]

  (swap! *DDL_BVS* assoc table (:column fld))
  (GenInteger db fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoLong Oracle

  [db table fld]

  (swap! *DDL_BVS* assoc table (:column fld))
  (GenLong db fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenEndSQL Oracle

  [db]

  (let [bf (StringBuilder.) ]
    (doseq [en (deref *DDL_BVS*)]
      (doto bf
        (.append (createSequence db (first en)))
        (.append (createSequenceTrigger db
                                        (first en)
                                        (last en)))))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenDrop Oracle

  [db table]

  (str "DROP TABLE " table " CASCADE CONSTRAINTS PURGE" (GenExec db) "\n\n"))

;;(println (GetDDL (MakeMetaCache testschema) (Oracle.) ))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

