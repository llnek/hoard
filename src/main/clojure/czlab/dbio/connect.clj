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

(ns ^{:doc "Database connections"
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

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
      (->> {:max-conns 1
            :min-conns 1
            :partitions 1}
           (merge options )
           (mkDbPool jdbc )
           (.put c hc )))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
(defn- undo

  ""
  [^Connection conn]

  (try! (.rollback conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
(defn dbioConnect

  "Connect to a datasource"

  ^DBAPI
  [^JDBCInfo jdbc schema & [options]]

  (let [options (or options {})
        v (resolveVendor jdbc)
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
          (open [_] (mkDbConnection jdbc)))]
    (test-nonil "database-vendor" v)
    (reset! s (simSQLr db))
    (reset! t (txSQLr db))
    db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioConnectViaPool

  "Connect to a datasource"

  ^DBAPI
  [^JDBCInfo jdbc schema & [options]]

  (let [pool (mkDbPool jdbc options)
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


