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

  czlab.xlib.dbio.postgresql

  (:require [czlab.xlib.util.logging :as log])

  (:use [czlab.xlib.dbio.drivers]
        [czlab.xlib.dbio.core :as dbcore]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defonce POSTGRESQL-URL "jdbc:postgresql://{{host}}:{{port}}/{{db}}" )
(defonce POSTGRESQL-DRIVER "org.postgresql.Driver")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Postgresql
(defmethod GetTSKeyword :postgresql [db] "TIMESTAMP WITH TIME ZONE")
(defmethod GetBlobKeyword :postgresql [db] "BYTEA")
(defmethod GetDoubleKeyword :postgresql [db] "DOUBLE PRECISION")
(defmethod GetFloatKeyword :postgresql [db] "REAL")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenCal :postgresql

  [db fld]

  (GenTimestamp db fld))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoInteger :postgresql

  [db table fld]

  (GenColDef db (GenCol fld) "SERIAL" false nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoLong :postgresql

  [db table fld]

  (GenColDef db (GenCol fld) "BIGSERIAL" false nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenDrop :postgresql

  [db table]

  (str "DROP TABLE IF EXISTS " table " CASCADE" (GenExec db) "\n\n"))

;;(def XXX (.getMetas (MakeMetaCache testschema)))
;;(println (GetDDL (MakeMetaCache testschema) (Postgresql.) ))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

