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
            JdbcSpec
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
  ^JdbcPool [^JdbcSpec jdbc options]

  (let [^Map c (-> (DbioLocal/cache) .get)
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
  "Connect to db" ^Connection [db cfg]

  (let [how (or (:isolation cfg)
                Connection/TRANSACTION_SERIALIZABLE)
        auto? (!false? (:auto? cfg))]
  (doto (. ^DbApi db open)
    (.setTransactionIsolation (int how))
    (.setAutoCommit auto?))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- simSQLr "" ^SQLr [db]

  (let [cfg {:isolation
             Connection/TRANSACTION_SERIALIZABLE}]
    (sqlr<> db #(with-open [c2 (openDB db cfg)] (% c2)))))

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
(defn- txSQLr "" ^Transactable [db]

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
          (catch Throwable _
            (undo c) (log/warn _ "") (throw _)))))

    (execWith [this cb]
      (.execWith this cb nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<>
  "Open/access to a datasource" {:tag DbApi}

  ([jdbc _schema] (dbopen<> jdbc _schema nil))
  ([^JdbcSpec jdbc _schema options]
   (let [v (resolveVendor jdbc)
         s (atom nil)
         t (atom nil)
         db
         (reify DbApi
           (compositeSQLr [this] @t)
           (simpleSQLr [this] @s)
           (schema [_] _schema)
           (vendor [_] v)
           (dispose [_] )
           (open [_] (dbconnect<> jdbc)))]
     (test-some "database-vendor" v)
     (reset! s (simSQLr db))
     (reset! t (txSQLr db))
     db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbapi<>
  "Open/access to a datasource (pooled)" {:tag DbApi}

  ([pool _schema] (dbapi<> pool _schema nil))
  ([^JdbcPool pool _schema options]
   (let [v (.vendor pool)
         s (atom nil)
         t (atom nil)
         db
         (reify DbApi
           (compositeSQLr [this] @t)
           (simpleSQLr [this] @s)
           (schema [_] _schema)
           (vendor [_] v)
           (dispose [_] (.shutdown pool))
           (open [_] (.nextFree pool)))]
     (test-some "database-vendor" v)
     (reset! s (simSQLr db))
     (reset! t (txSQLr db))
     db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<+>
  "Open/access to a datasource (pooled)" {:tag DbApi}

  ([jdbc _schema] (dbopen<+> jdbc _schema nil))
  ([^JdbcSpec jdbc _schema options]
   (dbapi<> (->> (merge pool-cfg options)
                 (dbpool<> jdbc )) _schema)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

