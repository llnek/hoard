;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Database connections."
      :author "Kenneth Leung"}

  czlab.hoard.connect

  (:require [czlab.hoard.sql :as q]
            [czlab.basal.log :as l]
            [czlab.basal.core :as c]
            [czlab.hoard.core :as h])

  (:import [java.sql
            Connection
            SQLException]
           [java.util Map]
           [czlab.hoard TLocalMap]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol DbObj ""
  (db-simple [_] "Auto commit session")
  (db-open [_] "Jdbc connection")
  (db-finz [_])
  (db-composite [_] "Transaction enabled session"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
(defn- register-jdbc-tl
  "Add a thread-local db pool."
  [spec options]
  (let [hc (:id spec)
        ^Map c (.get (TLocalMap/cache))]
    (when-not (.containsKey c hc)
      (l/debug "no db-pool in thread-local, creating one.")
      (->> (merge pool-cfg options) (h/dbpool<> spec) (.put c hc)))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gconn
  "Connect to db." ^Connection [db cfg]
  (let [{:keys [isolation auto?]} cfg
        ^Connection c ((:open db) db)
        how (or isolation
                Connection/TRANSACTION_SERIALIZABLE)]
    (doto c
      (.setAutoCommit (c/!false? auto?))
      (.setTransactionIsolation (int how)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private undo
  [c] `(c/try! (.rollback ~(with-meta c {:tag 'Connection}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private commit
  [c] `(.commit ~(with-meta c {:tag 'Connection})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- tx-sqlr<> [db]
  (reify
    czlab.hoard.core.Transactable
    (tx-transact! [_ cb cfg]
      (c/wo* [c (gconn db (assoc cfg :auto? false))]
        (try (c/do-with [rc (cb (q/sqlr<> db #(% c)))]
                        (commit c))
             (catch Throwable _
               (undo c)
               (throw _)))))
    (tx-transact! [_ cb]
      (h/tx-transact! _ cb nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sim-sqlr<> [db]
  (let [cfg {:isolation
             Connection/TRANSACTION_SERIALIZABLE}]
    (q/sqlr<> db #(c/wo* [c2 (gconn db cfg)] (% c2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DbObjImpl []
  DbObj
  (db-composite [me] (:co me))
  (db-simple [me] (:si me))
  (db-finz [me] (c/if-fn? [f (:finz me)] (f me)))
  (db-open [me] (c/if-fn? [f (:open me)] (f me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbobj<> [dbm]
  (c/object<> DbObjImpl
              (assoc dbm
                     :co (tx-sqlr<> dbm)
                     :si (sim-sqlr<> dbm))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbio<>
  "Open/access to a datasource."
  ([jdbc schema] (dbio<> jdbc schema nil))
  ([jdbc schema options]
   (dbobj<> {:schema schema
             :jdbc jdbc
             :open #(h/conn<> (:jdbc %))
             :vendor (c/wo* [c (h/conn<> jdbc)] (h/db-vendor c))})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbio<+>
  "IO access to a datasource (pooled)."
  ([jdbc schema] (dbio<+> jdbc schema nil))
  ([jdbc schema options]
   (let [{:keys [vendor] :as pool}
         (h/dbpool<> jdbc
                     (merge pool-cfg options))]
     (dbobj<> {:schema schema
               :vendor vendor
               :jdbc jdbc
               :pool pool
               :open #(h/jp-next (:pool %))
               :finz #(h/jp-close (:pool %))}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

