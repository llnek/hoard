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


(ns ^{:doc "DDL functions for SQL Server"
      :author "Kenneth Leung" }

  czlab.dbddl.sqlserver

  (:require
    [czlab.xlib.logging :as log])

  (:use [czlab.dbddl.drivers]
        [czlab.dbio.core]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; SQLServer
(defmethod getDoubleKwd SQLServer [_] "float(53)")
(defmethod getFloatKwd SQLServer [_] "float(53)")
(defmethod getBlobKwd SQLServer [_] "image")
(defmethod getTSKwd SQLServer [_] "datetime")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger
  SQLServer
  [dbtype model fld]
  (str (getPad dbtype)
       (genCol fld)
       " "
       (getIntKwd dbtype)
       (if (:pkey fld)
         " identity (1,1) "
         " autoincrement ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong
  SQLServer
  [dbtype model fld]
  (str (getPad dbtype)
       (genCol fld)
       " "
       (getLongKwd dbtype)
       (if (:pkey fld)
         " identity (1,1) "
         " autoincrement ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop
  SQLServer
  [dbtype model]
  (str "if exists (select * from dbo.sysobjects where id=object_id('"
       (gtable model false)
       "')) drop table "
       (gtable model)
       (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


