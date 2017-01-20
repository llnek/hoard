;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database connections."
      :author "Kenneth Leung"}

  czlab.horde.dbio.connect

  (:require [czlab.basal.logging :as log])

  (:use [czlab.horde.dbio.core]
        [czlab.basal.core]
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


