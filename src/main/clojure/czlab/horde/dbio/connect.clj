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
  ^czlab.horde.dbio.core.JdbcPool
  [jdbc options]

  (let [^Map c (-> (DbioLocal/cache) .get)
        hc (:id @jdbc)]
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
(defstateful Transactable
  ITransactable
  (execWith [_ cb cfg]
    (let [{:keys [db]} @data]
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
            (undo c) (log/warn _ "") (throw _))))))
  (execWith [me cb]
    (.execWith me cb _empty-map_)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- txSQLr "" [db]
  (entity<> Transactable {:db db}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IDbApi
  ""
  (compositeSQLr [_] "")
  (simpleSQLr [_] "")
  (schema [_] "")
  (vendor [_] "")
  (open [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful DbApi
  Disposable
  (dispose [_] (if-fn? [f (:finz @data)] (f @data)))
  IDbApi
  (compositeSQLr [_] @t)
  (simpleSQLr [_] @s)
  (schema [_] (:schema @data))
  (vendor [_] (:vendor @data))
  (open [_] (if-fn? [f (:open @data)] (f @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<>
  "Open/access to a datasource"
  {:tag czlab.horde.dbio.connect.DbApi}

  ([jdbc _schema]
   (dbopen<> jdbc _schema nil))

  ([jdbc _schema options]
   (let [v (resolveVendor jdbc)
         _ (test-some "db-vendor" v)
         db (entity<> DbApi
               {:open #(dbconnect<> (:jdbc %))
                :schema _schema
                :jdbc jdbc
                :sim nil
                :tx nil
                :vendor v})]
     (doto db
       (.update {:sim (simSQLr db) :tx (txSQLr db)})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbapi<>
  "Open/access to a datasource (pooled)"
  ^czlab.horde.dbio.connect.DbApi

  ([pool _schema]
   (dbapi<> pool _schema _empty-map_))

  ([pool _schema options]
   (let [v (:vendor @pool)
         _ (test-some "db-vendor" v)
         db
         (entity<> DbApi
                   {:finz #(.shutdown ^czlab.horde.dbio.core.JdbcPool (:pool %))
                    :open #(.nextFree ^czlab.horde.dbio.core.JdbcPool (:pool %))
                    :schema _schema
                    :vendor v
                    :pool pool
                    :sim nil
                    :tx nil})]
     (doto db
       (.update {:sim (simSQLr db) :tx (txSQLr db)})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<+>
  "Open/access to a datasource (pooled)"
  ^czlab.horde.dbio.connect.DbApi

  ([jdbc _schema]
   (dbopen<+> jdbc _schema _empty-map_))

  ([jdbc _schema options]
   (dbapi<> (->> (merge pool-cfg options)
                 (dbpool<> jdbc )) _schema)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF
