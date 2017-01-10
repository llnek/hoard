;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "DDL functions for MySQL."
      :author "Kenneth Leung"}

  czlab.horde.dbddl.mysql

  (:require [czlab.xlib.logging :as log])

  (:use [czlab.horde.dbddl.drivers]
        [czlab.horde.dbio.core]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(def MYSQL-DRIVER "com.mysql.jdbc.Driver")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; MySQL
(defmethod getBlobKwd MySQL [_] "longblob")
(defmethod getTSKwd MySQL [_] "timestamp")
(defmethod getDoubleKwd MySQL [_] "double")
(defmethod getFloatKwd MySQL [_]  "double")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEnd
  MySQL
  [dbtype model]
  (str "\n) type=InnoDB" (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger
  MySQL
  [dbtype model field]
  (str (getPad dbtype)
       (genCol field)
       " "
       (getIntKwd dbtype)
       " not null auto_increment"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong
  MySQL
  [dbtype model field]
  (str (getPad dbtype)
       (genCol field)
       " "
       (getLongKwd dbtype)
       " not null auto_increment"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop
  MySQL
  [dbtype model]
  (str "drop table if exists "
       (gtable model)
       (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


