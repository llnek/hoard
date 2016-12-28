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

  czlab.horde.dbio.connect

  (:require [czlab.xlib.logging :as log])

  (:use [czlab.horde.dbio.core]
        [czlab.xlib.core]
        [czlab.horde.dbio.sql])

  (:import [java.sql Connection]
           [czlab.horde
            DbApi
            SQLr
            Schema
            JdbcPool
            JdbcInfo
            DbioLocal
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

(def ^:private pool-cfg
  {:connectionTimeout 30000 ;; how long in millis caller will wait
   :idleTimeout 600000 ;; idle time in pool
   :maximumPoolSize 10
   :minimumIdle 10
   :poolName "" })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- registerJdbcTL
  "Add a thread-local db pool"
  ^JdbcPool
  [^JdbcInfo jdbc options]
  (let [^Map
        c (-> (DbioLocal/cache)
              (.get))
        hc (.id jdbc)]
    (when-not (.containsKey c hc)
      (log/debug "No db-pool in DBIO-thread-local, creating one")
      (->> (merge pool-cfg options)
           (dbpool<> jdbc )
           (.put c hc )))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- openDB
  "Connect to a database"
  ^Connection
  [^DbApi db cfg]
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
  [^DbApi db]
  (let [cfg {:isolation Connection/TRANSACTION_SERIALIZABLE}]
    (sqlr<>
      db
      #(with-open [c2 (openDB db cfg)] (% c2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private undo
  "" [c] `(try! (.rollback ~(with-meta c {:tag 'Connection}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private commit
  "" [c] `(.commit ~(with-meta c {:tag 'Connection})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- txSQLr
  "A composite supports transactions"
  ^Transactable
  [^DbApi db]
  (reify Transactable
    (execWith [_ cb cfg]
      (with-open
        [c (openDB db
                   (->> {:auto? false}
                        (merge cfg)))]
        (try
          (let
            [rc (cb (sqlr<> db #(% c)))]
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
(defn dbopen<>
  "Open/access to a datasource"
  {:tag DbApi}

  ([jdbc _schema] (dbopen<> jdbc _schema nil))
  ([^JdbcInfo jdbc _schema options]
   (let [v (resolveVendor jdbc)
         s (atom nil)
         t (atom nil)
         db
         (reify DbApi
           (compositeSQLr [this] @t)
           (simpleSQLr [this] @s)
           (schema [_] _schema)
           (vendor [_] v)
           (finx [_] )
           (open [_] (dbconnect<> jdbc)))]
     (test-some "database-vendor" v)
     (reset! s (simSQLr db))
     (reset! t (txSQLr db))
     db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<+>
  "Open/access to a datasource using pooled connections"
  {:tag DbApi}

  ([jdbc _schema] (dbopen<+> jdbc _schema nil))
  ([^JdbcInfo jdbc _schema options]
   (let [pool (->> (merge pool-cfg options)
                   (dbpool<> jdbc ))
         v (.vendor pool)
         s (atom nil)
         t (atom nil)
         db
         (reify DbApi
           (compositeSQLr [this] @t)
           (simpleSQLr [this] @s)
           (schema [_] _schema)
           (vendor [_] v)
           (finx [_] (.shutdown pool))
           (open [_] (.nextFree pool)))]
     (test-some "database-vendor" v)
     (reset! s (simSQLr db))
     (reset! t (txSQLr db))
     db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


