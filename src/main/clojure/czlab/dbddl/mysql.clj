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

  czlab.dbddl.mysql

  (:require [czlab.xlib.logging :as log])

  (:use [czlab.dbddl.drivers]
        [czlab.dbio.core]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(def MYSQL-DRIVER "com.mysql.jdbc.Driver")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; MySQL
(defmethod getBlobKwd MySQL [db] "LONGBLOB")
(defmethod getTSKwd MySQL [db] "TIMESTAMP")
(defmethod getDoubleKwd MySQL [db] "DOUBLE")
(defmethod getFloatKwd MySQL [db]  "DOUBLE")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genEnd MySQL

  [db table]

  (str "\n) Type=InnoDB" (genExec db) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger MySQL

  [db table field]

  (str (getPad db)
       (genCol field)
       " "
       (getIntKwd db)
       " NOT NULL AUTO_INCREMENT"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong MySQL

  [db table field]

  (str (getPad db)
       (genCol field)
       " "
       (getLongKwd db)
       " NOT NULL AUTO_INCREMENT"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop MySQL

  [db table]

  (str "DROP TABLE IF EXISTS "
       table
       (genExec db) "\n\n"))

;;(println (getDDL (reifyMetaCache testschema) (MySQL.) ))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


