;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database connections."
      :author "Kenneth Leung"}

  czlab.horde.connect

  (:require [czlab.basal.logging :as log])

  (:use [czlab.horde.core]
        [czlab.basal.core]
        [czlab.horde.sql])

  (:import [java.sql SQLException Connection]
           [czlab.jasal Disposable TLocalMap]
           [czlab.basal Stateful]
           [java.util Map]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IDbApi
  ""
  (compositeSQLr [_] "")
  (simpleSQLr [_] "")
  (^Connection open [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defcontext DbApi
  Disposable
  (dispose [me] (if-fn? [f (:finz @me)] (f @me)))
  IDbApi
  (compositeSQLr [me] (.getv (.getx me) :tx))
  (simpleSQLr [me] (.getv (.getx me) :sm))
  (open [me] (if-fn? [f (:open @me)] (f @me))))

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
(defmethod fmtSqlId DbApi

  ([db idstr] (fmtSqlId db idstr nil))
  ([db idstr quote?]
   (fmtSqlId (:vendor @db) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- registerJdbcTL
  "Add a thread-local db pool"
  ^czlab.horde.core.JdbcPool
  [jdbc options]

  (let [^Map c (-> (TLocalMap/cache) .get)
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
  "Connect to db"
  ^Connection [db cfg]

  (let [how (or (:isolation cfg)
                Connection/TRANSACTION_SERIALIZABLE)
        auto? (!false? (:auto? cfg))]
  (doto
    ^Connection
    (.open ^czlab.horde.connect.DbApi db)
    (.setTransactionIsolation (int how))
    (.setAutoCommit auto?))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- simSQLr "" [db]
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
(defobject TransactableObj
  czlab.horde.core.Transactable
  (execWith [me cb cfg]
    (let [{:keys [db]} @me]
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
  (object<> TransactableObj {:db db}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<>
  "Open/access to a datasource"
  {:tag czlab.horde.connect.DbApi}

  ([jdbc schema]
   (dbopen<> jdbc schema nil))

  ([jdbc schema options]
   (let [v (resolveVendor jdbc)
         _ (test-some "db-vendor" v)
         db (context<> DbApi
                       {:open #(dbconnect<> (:jdbc %))
                        :vendor v
                        :jdbc jdbc
                        :schema schema})
         cx (.getx db)]
     (.setv cx :sm (simSQLr db))
     (.setv cx :tx (txSQLr db))
     db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbapi<>
  "Open/access to a datasource (pooled)"
  ^czlab.horde.connect.DbApi

  ([pool schema]
   (dbapi<> pool schema _empty-map_))

  ([pool schema options]
   (let [db
         (context<> DbApi
                    {:finz #(.shutdown
                              ^czlab.horde.core.JdbcPool (:pool %))
                     :open #(.nextFree
                              ^czlab.horde.core.JdbcPool (:pool %))
                     :vendor (:vendor @pool)
                     :jdbc (:jdbc @pool)
                     :schema schema
                     :pool pool})
         cx (.getx db)]
     (.setv cx :sm (simSQLr db))
     (.setv cx :tx (txSQLr db))
     db)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<+>
  "Open/access to a datasource (pooled)"
  ^czlab.horde.connect.DbApi

  ([jdbc schema]
   (dbopen<+> jdbc schema _empty-map_))

  ([jdbc schema options]
   (dbapi<> (->> (merge pool-cfg options)
                 (dbpool<> jdbc )) schema)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

