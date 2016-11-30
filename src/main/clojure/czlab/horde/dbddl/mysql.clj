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

(ns ^{:doc "DDL functions for MySQL."
      :author "Kenneth Leung" }

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


