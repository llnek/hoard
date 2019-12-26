;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.hoard.connect

  "Database connections."

  (:require [czlab.hoard.sql :as q]
            [czlab.hoard.core :as h]
            [czlab.basal.core :as c])

  (:import [java.sql
            Connection
            SQLException]
           [java.util Map]
           [java.io Closeable]
           [czlab.hoard TLocalMap]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol DbObj
  "Functions relating to a database."
  (simple [_] "Auto commit session")
  (composite [_] "Transaction enabled session"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;The calculation of pool size in order to avoid deadlock is a
;;fairly simple resource allocation formula:
;;pool size = Tn x (Cm - 1) + 1
;;Where Tn is the maximum number of threads,
;;and Cm is the maximum number of simultaneous connections
;;held by a single thread.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
(c/def- pool-cfg
  {;; idle time in pool
   :idleTimeout 600000
   :poolName ""
   :minimumIdle 10
   :maximumPoolSize 10
   ;; millis caller will wait
   :connectionTimeout 30000})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- register-jdbc-tl

  "Add a thread-local db pool."
  [spec options]

  (let [hc (:id spec)
        ^Map c (.get (TLocalMap/cache))]
    (when-not (.containsKey c hc)
      (c/debug "no db-pool in thread-local, creating one.")
      (->> (merge pool-cfg options)
           (h/dbpool<> spec) (.put c hc)))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- gconn

  "Connect to db."
  ^Connection [db cfg]

  (let [{:keys [isolation auto?]} cfg
        ^Connection c ((:open db) db)
        how (or isolation
                Connection/TRANSACTION_SERIALIZABLE)]
    (doto c
      (.setAutoCommit (c/!false? auto?))
      (.setTransactionIsolation (int how)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- undo

  [c] `(c/try! (.rollback ~(with-meta c {:tag 'Connection}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- commit

  [c] `(.commit ~(with-meta c {:tag 'Connection})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- tx-sqlr<>

  [db]

  (reify
    czlab.hoard.core.Transactable
    (transact! [_ cb]
      (h/transact! _ cb nil))
    (transact! [_ cb cfg]
      (c/wo* [c (gconn db (assoc cfg :auto? false))]
        (try (c/do-with [rc (cb (q/sqlr<> db #(% c)))]
               (commit c))
             (catch Throwable _ (undo c) (throw _)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sim-sqlr<>

  [db]

  (let [cfg {:isolation
             Connection/TRANSACTION_SERIALIZABLE}]
    (q/sqlr<> db #(c/wo* [c2 (gconn db cfg)] (% c2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DbObjImpl []
  DbObj
  (composite [me] (:co me))
  (simple [me] (:si me))
  c/Finzable
  (finz [me] (c/if-fn [f (:finz me)] (f me)))
  c/Openable
  (open [me] (c/if-fn [f (:open me)] (f me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbobj<>

  [dbm]

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
             :vendor (c/wo* [^Connection
                             c (h/conn<> jdbc)] (h/db-vendor c))})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbio<+>

  "IO access to a datasource (pooled)."

  ([in schema] (dbio<+> in schema nil))

  ([in schema options]
   (let [{:keys [jdbc vendor] :as pool}
         (if (c/sas? h/JdbcPool in)
           in (h/dbpool<> in
                          (merge pool-cfg options)))]
     (dbobj<> {:schema schema
               :vendor vendor
               :jdbc jdbc
               :pool pool
               :open #(h/next (:pool %))
               :finz #(.close ^Closeable (:pool %))}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

