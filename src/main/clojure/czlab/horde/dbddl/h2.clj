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

(ns ^{:doc "DDL functions for H2."
      :author "Kenneth Leung" }

  czlab.horde.dbddl.h2

  (:require [czlab.xlib.core :refer [test-some test-hgl]]
            [czlab.xlib.logging :as log]
            [clojure.string :as cs]
            [clojure.java.io :as io])

  (:use [czlab.horde.dbddl.drivers]
        [czlab.horde.dbio.core])

  (:import [czlab.horde DBIOError]
           [java.io File]
           [java.sql DriverManager Connection Statement]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def H2-SERVER-URL "jdbc:h2:tcp://host/path/db" )
(def H2-DRIVER "org.h2.Driver" )

(def H2-MEM-URL "jdbc:h2:mem:{{dbid}};DB_CLOSE_DELAY=-1" )
(def H2-FILE-URL "jdbc:h2:{{path}};MVCC=TRUE" )

(def H2_MVCC ";MVCC=TRUE" )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; H2
(defmethod getDateKwd H2 [_] "timestamp")
(defmethod getDoubleKwd H2 [_] "double")
(defmethod getBlobKwd H2 [_] "blob")
(defmethod getFloatKwd H2 [_] "float")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger
  H2
  [dbtype model field]
  (str (getPad dbtype)
       (genCol field)
       " "
       (getIntKwd dbtype)
       (if (:pkey field)
         " identity(1) "
         " auto_increment(1) ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong
  H2
  [dbtype model field]
  (str (getPad dbtype)
       (genCol field)
       " "
       (getLongKwd dbtype)
       (if (:pkey field)
         " identity(1) "
         " auto_increment(1) ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin
  H2
  [_ model]
  (str "create cached table " (gtable model) " (\n" ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop
  H2
  [dbtype model]
  (str "drop table "
       (gtable model)
       " if exists cascade" (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn H2Db
  "Create a H2 database"
  [dbDir ^String dbid ^String user ^String pwd]

  (test-some "file-dir" dbDir)
  (test-hgl "db-id" dbid)
  (test-hgl "user" user)

  (let [url (doto (io/file dbDir dbid) (.mkdirs))
        u (.getCanonicalPath url)
        dbUrl (cs/replace H2-FILE-URL "{{path}}" u)]
    (log/debug "Creating H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        ;;(.execute s (str "create user " user " password \"" pwd "\" admin"))
        (.execute s "set default_table_type cached"))
      (with-open [s (.createStatement c1)]
        (.execute s "shutdown")))
    dbUrl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn closeH2Db
  "Close an existing H2 database"
  [dbDir ^String dbid ^String user ^String pwd]

  (test-some "file-dir" dbDir)
  (test-hgl "db-id" dbid)
  (test-hgl "user" user)

  (let [url (io/file dbDir dbid)
        u (.getCanonicalPath url)
        dbUrl (cs/replace H2-FILE-URL "{{path}}" u)]
    (log/debug "Closing H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        (.execute s "shutdown")) )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


