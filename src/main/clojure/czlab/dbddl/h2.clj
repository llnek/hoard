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

  czlab.dbddl.h2

  (:import
    [czlab.crypto PasswordAPI]
    [czlab.dbio DBIOError]
    [java.io File]
    [java.sql DriverManager Connection Statement])

  (:require
    [czlab.xlib.core :refer [test-nonil test-nestr]]
    [czlab.xlib.logging :as log]
    [clojure.string :as cs]
    [clojure.java.io :as io])

  (:use [czlab.dbddl.drivers]
        [czlab.dbio.core]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defonce H2-SERVER-URL "jdbc:h2:tcp://host/path/db" )
(defonce H2-DRIVER "org.h2.Driver" )

(defonce H2-MEM-URL "jdbc:h2:mem:{{dbid}};DB_CLOSE_DELAY=-1" )
(defonce H2-FILE-URL "jdbc:h2:{{path}};MVCC=TRUE" )

(defonce H2_MVCC ";MVCC=TRUE" )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; H2
(defmethod getDateKwd H2 [_] "TIMESTAMP")
(defmethod getDoubleKwd H2 [_] "DOUBLE")
(defmethod getBlobKwd H2 [_] "BLOB")
(defmethod getFloatKwd H2 [_] "FLOAT")

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
         " IDENTITY(1) "
         " AUTO_INCREMENT(1) ")))

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
         " IDENTITY(1) "
         " AUTO_INCREMENT(1) ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genBegin

  H2

  [_ model]

  (str "CREATE CACHED TABLE " (gtable model) " (\n" ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop

  H2

  [dbtype model]

  (str "DROP TABLE "
       (gtable model)
       " IF EXISTS CASCADE" (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn H2Db

  "Create a H2 database"

  [^File dbFileDir
   ^String dbid
   ^String user
   ^PasswordAPI pwdObj]

  (test-nonil "file-dir" dbFileDir)
  (test-nestr "db-id" dbid)
  (test-nestr "user" user)

  (let [url (io/file dbFileDir dbid)
        u (.getCanonicalPath url)
        pwd (str pwdObj)
        dbUrl (cs/replace H2-FILE-URL "{{path}}" u)]
    (log/debug "Creating H2: %s" dbUrl)
    (.mkdir dbFileDir)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        ;;(.execute s (str "CREATE USER " user " PASSWORD \"" pwd "\" ADMIN"))
        (.execute s "SET DEFAULT_TABLE_TYPE CACHED"))
      (with-open [s (.createStatement c1)]
        (.execute s "SHUTDOWN")))
    dbUrl))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn closeH2Db

  "Close an existing H2 database"

  [^File dbFileDir
   ^String dbid
   ^String user
   ^PasswordAPI pwdObj]

  (test-nonil "file-dir" dbFileDir)
  (test-nestr "db-id" dbid)
  (test-nestr "user" user)

  (let [url (io/file dbFileDir dbid)
        u (.getCanonicalPath url)
        pwd (str pwdObj)
        dbUrl (cs/replace H2-FILE-URL "{{path}}" u)]
    (log/debug "Closing H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        (.execute s "SHUTDOWN")) )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


