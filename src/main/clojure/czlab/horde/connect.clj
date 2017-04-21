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

  (:import [czlab.jasal Settable Disposable TLocalMap]
           [java.sql SQLException Connection]
           [java.util Map]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IDbApi
  ""
  (^czlab.horde.core.Transactable compositeSQLr [_] "")
  (^czlab.horde.core.SQLr simpleSQLr [_] "")
  (^Connection opendb [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object DbApi
  Disposable
  (dispose [me] (if-fn? [f (:finz me)] (f me)))
  IDbApi
  (compositeSQLr [me] (:tx me))
  (simpleSQLr [me] (:sm me))
  (opendb [me] (if-fn? [f (:open me)] (f me))))

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
   (fmtSqlId (:vendor db) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- registerJdbcTL
  "Add a thread-local db pool"
  [jdbc options]

  (let [^Map c (.get (TLocalMap/cache))
        hc (:id jdbc)]
    (when-not (.containsKey c hc)
      (log/debug "No db-pool in thread-local, creating one")
      (->> (merge pool-cfg options) (dbpool<> jdbc ) (.put c hc )))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- connectToDB
  "Connect to db"
  ^Connection [db cfg]

  (let [how (or (:isolation cfg)
                Connection/TRANSACTION_SERIALIZABLE)
        f (:open db)
        auto? (!false? (:auto? cfg))]
  (doto
    ^Connection
    (f db)
    (.setTransactionIsolation (int how))
    (.setAutoCommit auto?))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- simSQLr "" [db]
  (let [cfg {:isolation
             Connection/TRANSACTION_SERIALIZABLE}]
    (sqlr<> db #(with-open [c2 (connectToDB db cfg)] (% c2)))))

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
(decl-object TransactableObj
  czlab.horde.core.Transactable
  (transact! [me cb cfg]
    (let [{:keys [db]} me]
      (with-open
        [c (connectToDB db
                   (->> {:auto? false}
                        (merge cfg)))]
        (try
          (let
            [rc (cb (sqlr<> db #(% c)))]
            (commit c)
            rc)
          (catch Throwable _
            (undo c) (log/warn _ "") (throw _))))))
  (transact! [me cb]
    (transact! me cb _empty-map_)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- txSQLr "" [db]
  (object<> TransactableObj {:db db}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<>
  "Open/access to a datasource"

  ([jdbc schema]
   (dbopen<> jdbc schema _empty-map_))

  ([jdbc schema options]
   (let [opener #(dbconnect<> (:jdbc %))
         vendor (resolveVendor jdbc)
         db {:vendor vendor
             :open opener
             :jdbc jdbc
             :schema schema}
         tx (txSQLr db)
         sm (simSQLr db)]
     (object<> DbApi (assoc db :tx tx :sm sm)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbapi<>
  "Open/access to a datasource (pooled)"
  ^czlab.horde.connect.DbApi

  ([pool schema]
   (dbapi<> pool schema _empty-map_))

  ([pool schema options]
   (let [{:keys [vendor jdbc]} pool
         finzer #(shut-down (:pool %))
         opener #(next-free (:pool %))
         db {:finz finzer
             :open opener
             :schema schema
             :vendor vendor
             :jdbc jdbc
             :pool pool}
         sm (simSQLr db)
         tx (txSQLr db)]
     (object<> DbApi (assoc db :tx tx :sm sm)))))

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

