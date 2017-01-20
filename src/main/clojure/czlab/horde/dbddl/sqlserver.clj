;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "DDL functions for SQL Server"
      :author "Kenneth Leung"}

  czlab.horde.dbddl.sqlserver

  (:require
    [czlab.basal.logging :as log])

  (:use [czlab.horde.dbddl.drivers]
        [czlab.horde.dbio.core]))

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


