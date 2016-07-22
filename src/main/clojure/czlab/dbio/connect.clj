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

(ns ^{:doc "Database connections."
      :author "Kenneth Leung" }

  czlab.dbio.connect

  (:require
    [czlab.xlib.core :refer [try! test-nonil]]
    [czlab.xlib.logging :as log])

  (:use [czlab.dbio.core]
        [czlab.dbio.sql])

  (:import
    [java.sql Connection]
    [czlab.dbio
     DBAPI
     SQLr
     Schema
     JDBCPool
     JDBCInfo
     DBIOLocal
     Transactable]
    [java.util Map]))


;;The calculation of pool size in order to avoid deadlock is a
;;fairly simple resource allocation formula:
;;pool size = Tn x (Cm - 1) + 1
;;Where Tn is the maximum number of threads,
;;and Cm is the maximum number of simultaneous connections
;;held by a single thread.


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(def ^:private POOL_CFG
  {:connectionTimeout 30000 ;; how long in millis caller will wait
   :idleTimeout 600000 ;; idle time in pool
   :maximumPoolSize 10
   :minimumIdle 10
   :poolName "" })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- registerJdbcTL

  "Add a thread-local db pool"
  ^JDBCPool
  [^JDBCInfo jdbc options]

  (let [^Map
        c (-> (DBIOLocal/getCache)
              (.get))
        hc (.id jdbc)]
    (when-not (.containsKey c hc)
      (log/debug "No db-pool in DBIO-thread-local, creating one")
      (->> (merge POOL_CFG options)
           (dbpool jdbc )
           (.put c hc )))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- openDB

  "Connect to a database"
  ^Connection
  [^DBAPI db cfg]

  (let [how (or (:isolation cfg)
                Connection/TRANSACTION_SERIALIZABLE)
        auto? (not (false? (:auto? cfg)))]
  (doto (.open db)
    (.setTransactionIsolation (int how))
    (.setAutoCommit auto?))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- simSQLr

  "Non transactional SQL object"
  ^SQLr
  [^DBAPI db]

  (let [how Connection/TRANSACTION_SERIALIZABLE]
    (reifySQLr
      db
      #(with-open [c2 (openDB db {})] (% c2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undo

  ""
  [^Connection conn]

  (try! (.rollback conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- commit

  ""
  [^Connection conn]

  (.commit conn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- txSQLr

  "A composite supports transactions"
  ^Transactable
  [^DBAPI db]

  (reify

    Transactable

    (execWith [_ cb cfg]
      (with-open
        [c (openDB db
                   (->> {:auto? false}
                        (merge cfg)))]
          (try
            (let
              [rc (cb (reifySQLr
                        db
                        #(% c)))]
              (commit c)
              rc)
            (catch Throwable e#
              (undo c)
              (log/warn e# "")
              (throw e#)))))

    (execWith [this cb]
      (.execWith this cb {}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen

  "Open/access to a datasource"
  ^DBAPI
  [^JDBCInfo jdbc schema & [options]]

  (let [v (resolveVendor jdbc)
        s (atom nil)
        t (atom nil)
        db
        (reify
          DBAPI
          (compositeSQLr [this] @t)
          (simpleSQLr [this] @s)
          (getMetas [_] schema)
          (vendor [_] v)
          (finz [_] )
          (open [_] (dbconnect jdbc)))]
    (test-nonil "database-vendor" v)
    (reset! s (simSQLr db))
    (reset! t (txSQLr db))
    db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen+

  "Open/access to a datasource using pooled connections"
  ^DBAPI
  [^JDBCInfo jdbc schema & [options]]

  (let [pool (->> (merge POOL_CFG options)
                  (dbpool jdbc ))
        v (.vendor pool)
        s (atom nil)
        t (atom nil)
        db
        (reify
          DBAPI
          (compositeSQLr [this] @t)
          (simpleSQLr [this] @s)
          (getMetas [_] schema)
          (vendor [_] v)
          (finz [_] (.shutdown pool))
          (open [_] (.nextFree pool)))]
    (test-nonil "database-vendor" v)
    (reset! s (simSQLr db))
    (reset! t (txSQLr db))
    db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


