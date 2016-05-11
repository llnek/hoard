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

  czlab.xlib.dbio.sqlserver

  (:require
    [czlab.xlib.util.logging :as log])

  (:use [czlab.xlib.dbio.drivers]
        [czlab.xlib.dbio.core]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; SQLServer
(defmethod GetBlobKeyword SQLServer [db] "IMAGE")
(defmethod GetTSKeyword SQLServer [db] "DATETIME")
(defmethod GetDoubleKeyword SQLServer [db] "FLOAT(53)")
(defmethod GetFloatKeyword SQLServer [db] "FLOAT(53)")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoInteger SQLServer

  [db table fld]

  (str (GetPad db) (GenCol fld)
       " " (GetIntKeyword db)
       (if (:pkey fld) " IDENTITY (1,1) " " AUTOINCREMENT ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenAutoLong SQLServer

  [db table fld]

  (str (GetPad db) (GenCol fld)
       " " (GetLongKeyword db)
       (if (:pkey fld) " IDENTITY (1,1) " " AUTOINCREMENT ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod GenDrop SQLServer

  [db table]

  (str "IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id=object_id('"
       table "')) DROP TABLE "
       table (GenExec db) "\n\n"))

;;(println (GetDDL (MakeMetaCache testschema) (SQLServer.) ))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

