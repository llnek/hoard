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
;; Copyright © 2013-2024, Kenneth Leung. All rights reserved.

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
           [czlab.hoard TLocalMap]
           [czlab.hoard.core JdbcPool]))

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
      (->> options (merge pool-cfg) (h/dbpool<> spec) (.put c hc)))
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
(defrecord DbObj []
  c/Finzable
  (finz [me] (c/if-fn [f (:finz me)] (f me)))
  c/Openable
  (open [me] (c/if-fn [f (:open me)] (f me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn composite

  "Get the Transactable component."
  {:arglists '([db])}
  [db]
  {:pre [(c/is? DbObj db)]}

  (:co db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn simple

  "Get the simple component."
  {:arglists '([db])}
  [db]
  {:pre [(c/is? DbObj db)]}

  (:si db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbobj<>

  [dbm]

  (c/object<> DbObj
              (assoc dbm
                     :co (tx-sqlr<> dbm)
                     :si (sim-sqlr<> dbm))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbio<>

  "Open/access to a datasource."
  {:arglists '([jdbc schema]
               [jdbc schema options])}

  ([jdbc schema]
   (dbio<> jdbc schema nil))

  ([jdbc schema options]
   (dbobj<> {:schema schema
             :jdbc jdbc
             :open #(h/conn<> (:jdbc %))
             :vendor (c/wo* [^Connection
                             c (h/conn<> jdbc)] (h/db-vendor c))})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn dbio<+>

  "IO access to a datasource (pooled)."
  {:arglists '([in schema]
               [in schema options])}

  ([in schema]
   (dbio<+> in schema nil))

  ([in schema options]
   (let [{:keys [jdbc vendor] :as pool}
         (if (c/is? JdbcPool in)
           in (h/dbpool<> in
                          (merge pool-cfg options)))]
     (dbobj<> {:schema schema
               :vendor vendor
               :jdbc jdbc
               :pool pool
               :open #(c/next (:pool %))
               :finz #(.close ^Closeable (:pool %))}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

