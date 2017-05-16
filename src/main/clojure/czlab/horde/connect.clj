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

  (:require [czlab.horde.core :as h :refer [fmtSqlId]]
            [czlab.basal.log :as log]
            [czlab.basal.core :as c]
            [czlab.horde.sql :as q])

  (:import [java.sql
            Connection
            SQLException]
           [czlab.jasal
            Settable
            Disposable
            TLocalMap]
           [java.util Map]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol DbApi
  ""
  (composite-sqlr [_] "Transaction enabled session")
  (simple-sqlr [_] "Auto commit session")
  (^Connection opendb [_] "Jdbc connection"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object DbApiObj
  Disposable
  (dispose [me] (c/if-fn? [f (:finz me)] (f me)))
  DbApi
  (composite-sqlr [me] (:tx me))
  (simple-sqlr [me] (:sm me))
  (opendb [me] (c/if-fn? [f (:open me)] (f me))))

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
(defmethod fmtSqlId DbApiObj

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
      (->> (merge pool-cfg options) (h/dbpool<> jdbc) (.put c hc)))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- connectToDB
  "Connect to db"
  ^Connection [db cfg]

  (let [how (or (:isolation cfg)
                Connection/TRANSACTION_SERIALIZABLE)
        f (:open db)
        auto? (c/!false? (:auto? cfg))]
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
    (q/sqlr<> db #(with-open [c2 (connectToDB db cfg)] (% c2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private undo
  "" [c] `(c/try! (.rollback ~(with-meta c {:tag 'Connection}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private commit
  "" [c] `(.commit ~(with-meta c {:tag 'Connection})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object TransactableObj
  czlab.horde.core.Transactable
  (transact! [me cb cfg]
    (let [{:keys [db]} me]
      (with-open
        [c (connectToDB db
                   (->> {:auto? false}
                        (merge cfg)))]
        (try
          (let
            [rc (cb (q/sqlr<> db #(% c)))]
            (commit c)
            rc)
          (catch Throwable _
            (undo c) (log/warn _ "") (throw _))))))
  (transact! [me cb]
    (.transact! me cb nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- txSQLr "" [db]
  (c/object<> TransactableObj {:db db}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<>
  "Open/access to a datasource"

  ([jdbc schema]
   (dbopen<> jdbc schema nil))

  ([jdbc schema options]
   (let [opener #(h/dbconnect<> (:jdbc %))
         vendor (h/resolveVendor jdbc)
         db {:vendor vendor
             :open opener
             :jdbc jdbc
             :schema schema}
         tx (txSQLr db)
         sm (simSQLr db)]
     (c/object<> DbApiObj (assoc db :tx tx :sm sm)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbapi<>
  "Open/access to a datasource (pooled)"

  ([pool schema]
   (dbapi<> pool schema nil))

  ([pool schema options]
   (let [{:keys [vendor jdbc]} pool
         finzer #(h/shut-down (:pool %))
         opener #(h/next-free (:pool %))
         db {:finz finzer
             :open opener
             :schema schema
             :vendor vendor
             :jdbc jdbc
             :pool pool}
         sm (simSQLr db)
         tx (txSQLr db)]
     (c/object<> DbApiObj (assoc db :tx tx :sm sm)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbopen<+>
  "Open/access to a datasource (pooled)"

  ([jdbc schema]
   (dbopen<+> jdbc schema nil))

  ([jdbc schema options]
   (dbapi<> (->> (merge pool-cfg options)
                 (h/dbpool<> jdbc)) schema)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

