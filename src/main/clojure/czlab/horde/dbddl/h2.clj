;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "DDL functions for H2."
      :author "Kenneth Leung"}

  czlab.horde.dbddl.h2

  (:require [czlab.basal.core :refer [test-some test-hgl]]
            [czlab.basal.logging :as log]
            [clojure.string :as cs]
            [clojure.java.io :as io])

  (:use [czlab.horde.dbddl.drivers]
        [czlab.horde.dbio.core])

  (:import [czlab.horde DbioError]
           [java.io File]
           [java.sql DriverManager Connection Statement]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def h2-server-url "jdbc:h2:tcp://host/path/db" )
(def h2-driver "org.h2.Driver" )

(def h2-mem-url "jdbc:h2:mem:{{dbid}};DB_CLOSE_DELAY=-1" )
(def h2-file-url "jdbc:h2:{{path}};MVCC=TRUE" )

(def h2-mvcc ";MVCC=TRUE" )

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
        dbUrl (cs/replace h2-file-url "{{path}}" u)]
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
        dbUrl (cs/replace h2-file-url "{{path}}" u)]
    (log/debug "Closing H2: %s" dbUrl)
    (with-open [c1 (DriverManager/getConnection dbUrl user pwd)]
      (.setAutoCommit c1 true)
      (with-open [s (.createStatement c1)]
        (.execute s "shutdown")) )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


