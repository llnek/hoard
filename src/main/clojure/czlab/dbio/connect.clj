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
;; Copyright (c) 2013-2016, Kenneth Leung. All rights reserved.

(ns ^{:doc ""
      :author "kenl" }

  czlab.dbio.connect

  (:import
    [czlab.dbio
     DBAPI
     Schema
     JDBCPool
     JDBCInfo
     DBIOLocal
     DBIOError
     OptLockError]
    [java.util Map])

  (:require
    [czlab.xlib.core :refer [try!]]
    [czlab.xlib.logging :as log])

  (:use [czlab.dbio.composite]
        [czlab.dbio.core]
        [czlab.dbio.simple]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn registerJdbcTL

  "Add a thread-local db pool"

  [^JDBCInfo jdbc options]

  (let [^Map
        c (-> (DBIOLocal/getCache)
              (.get))
        hc (.getId jdbc)]
    (when-not (.containsKey c hc)
      (log/debug "No db-pool in DBIO-thread-local, creating one")
      (->> {:max-conns 1
            :min-conns 1
            :partitions 1}
           (merge options )
           (mkDbPool jdbc )
           (.put c hc )))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioConnect

  "Connect to a datasource"

  ^DBAPI
  [^JDBCInfo jdbc schema options]

  ;;(log/debug "%s" (.metas schema))
  (reify

    DBAPI

    (newCompositeSQLr [this] (compositeSQLr this))

    (newSimpleSQLr [this] (simpleSQLr this))

    (getMetas [_] (.getModels ^Schema schema))

    (vendor [_] (resolveVendor jdbc))

    (open [_] (mkDbConnection jdbc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn dbioConnectViaPool

  "Connect to a datasource"

  ^DBAPI
  [^JDBCPool pool schema options]

  (reify

    DBAPI

    (newCompositeSQLr [this] (compositeSQLr this))

    (newSimpleSQLr [this] (simpleSQLr this))

    (getMetas [_] (.getModels ^Schema schema))

    (vendor [_] (.vendor pool))

    (open [_] (.nextFree pool))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


