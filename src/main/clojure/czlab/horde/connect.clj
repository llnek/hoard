;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database connections."
      :author "Kenneth Leung"}

  czlab.horde.connect

  (:require [czlab.horde.sql :as q]
            [czlab.basal.log :as l]
            [czlab.basal.core :as c]
            [czlab.horde.core
             :as h :refer [fmt-sqlid]])

  (:import [java.sql
            Connection
            SQLException]
           [java.util Map]
           [czlab.horde TLocalMap]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol DbApi
  (db-composite [_] "Transaction enabled session")
  (db-simple [_] "Auto commit session")
  (db-finz [_])
  (db-open [_] "Jdbc connection"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DbObj []
  DbApi
  (db-composite [me] (:tx me))
  (db-simple [me] (:sm me))
  (db-finz [me] (c/if-fn? [f (:finz me)] (f me)))
  (db-open [me] (c/if-fn? [f (:open me)] (f me))))

;;The calculation of pool size in order to avoid deadlock is a
;;fairly simple resource allocation formula:
;;pool size = Tn x (Cm - 1) + 1
;;Where Tn is the maximum number of threads,
;;and Cm is the maximum number of simultaneous connections
;;held by a single thread.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(def ^:private pool-cfg {:connectionTimeout 30000 ;; how long in millis caller will wait
                         :idleTimeout 600000 ;; idle time in pool
                         :maximumPoolSize 10
                         :minimumIdle 10
                         :poolName "" })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod fmt-sqlid DbObj
  ([db idstr] (fmt-sqlid db idstr nil))
  ([db idstr quote?]
   (fmt-sqlid (:vendor db) idstr quote?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- register-jdbc-tl
  "Add a thread-local db pool."
  [jdbc options]
  (let [^Map c (.get (TLocalMap/cache))
        hc (:id jdbc)]
    (when-not (.containsKey c hc)
      (l/debug "No db-pool in thread-local, creating one.")
      (->> (merge pool-cfg options) (h/dbpool<> jdbc) (.put c hc)))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- connect-db "Connect to db." ^Connection [db cfg]
  (let [{:keys [isolation auto?]} cfg
        ^Connection c ((:open db) db)
        how (or isolation
                Connection/TRANSACTION_SERIALIZABLE)]
    (doto c
      (.setAutoCommit (c/!false? auto?))
      (.setTransactionIsolation (int how)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sim-sqlr [db]
  (let [cfg {:isolation
             Connection/TRANSACTION_SERIALIZABLE}]
    (q/sqlr<> db #(c/wo* [c2 (connect-db db cfg)] (% c2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private undo
  [c] `(c/try! (.rollback ~(with-meta c {:tag 'Connection}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private commit
  [c] `(.commit ~(with-meta c {:tag 'Connection})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord TransactableObj []
  czlab.horde.core.Transactable
  (transact! [me cb cfg]
    (let [{:keys [db]} me]
      (c/wo* [c (connect-db db (assoc cfg :auto? false))]
        (try (let [rc (cb (q/sqlr<> db #(% c)))]
               (commit c)
               rc)
             (catch Throwable _
               (undo c) (l/warn _ "") (throw _))))))
  (transact! [me cb]
    (.transact! me cb nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- tx-sqlr [db] (assoc (TransactableObj.) :db db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbopen<>
  "Open/access to a datasource"
  ([jdbc schema]
   (dbopen<> jdbc schema nil))
  ([jdbc schema options]
   (let [opener #(h/dbconnect<> (:jdbc %))
         vendor (h/resolve-vendor jdbc)
         db {:vendor vendor
             :open opener
             :jdbc jdbc
             :schema schema}
         tx (tx-sqlr db)
         sm (sim-sqlr db)]
     (merge (DbObj.) (assoc db :tx tx :sm sm)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbapi<>
  "Open/access to a datasource (pooled)"
  ([pool schema]
   (dbapi<> pool schema nil))
  ([pool schema options]
   (let [{:keys [vendor jdbc]} pool
         finzer #(h/p-close (:pool %))
         opener #(h/p-next (:pool %))
         db {:finz finzer
             :open opener
             :schema schema
             :vendor vendor
             :jdbc jdbc
             :pool pool}
         sm (sim-sqlr db)
         tx (tx-sqlr db)]
     (merge (DbObj.) (assoc db :tx tx :sm sm)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbopen<+>
  "Open/access to a datasource (pooled)"
  ([jdbc schema]
   (dbopen<+> jdbc schema nil))
  ([jdbc schema options]
   (dbapi<> (h/dbpool<> jdbc
                        (merge pool-cfg options)) schema)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

