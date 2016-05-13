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

  czlab.dbio.oracle

  (:require [czlab.xlib.logging :as log])

  (:use [czlab.dbio.drivers]
        [czlab.dbio.core]))


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
       "END" (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createSequence

  [db table]

  (str "CREATE SEQUENCE SEQ_"
       table
       " START WITH 1 INCREMENT BY 1"
       (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Oracle
(defmethod getStringKwd Oracle [db] "VARCHAR2")
(defmethod getTSDefault Oracle [db] "DEFAULT SYSTIMESTAMP")
(defmethod getLongKwd Oracle [db] "NUMBER(38)")
(defmethod getDoubleKwd Oracle [db] "BINARY_DOUBLE")
(defmethod getFloatKwd Oracle [db] "BINARY_FLOAT")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger Oracle

  [db table field]

  (swap! *DDL_BVS* assoc table (:column field))
  (genInteger db field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong Oracle

  [db table field]

  (swap! *DDL_BVS* assoc table (:column field))
  (genLong db field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEndSQL Oracle

  [db]

  (let [bf (StringBuilder.)]
    (doseq [en (deref *DDL_BVS*)]
      (doto bf
        (.append (createSequence db (first en)))
        (.append (createSequenceTrigger db
                                        (first en)
                                        (last en)))))
    (.toString bf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop Oracle

  [db table]

  (str "DROP TABLE "
       table
       " CASCADE CONSTRAINTS PURGE"
       (genExec db) "\n\n"))

;;(println (getDDL (reifyMetaCache testschema) (Oracle.) ))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

