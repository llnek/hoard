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

  czlab.xlib.dbio.connect

  (:require
    [czlab.xlib.util.core :refer [try!]]
    [czlab.xlib.util.logging :as log])

  (:use [czlab.xlib.dbio.core]
        [czlab.xlib.dbio.composite]
        [czlab.xlib.dbio.simple])

  (:import
    [java.util Map]
    [com.zotohlab.frwk.dbio DBAPI
    JDBCPool JDBCInfo
    DBIOLocal DBIOError OptLockError]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn RegisterJdbcTL

  "Add a thread-local db pool"

  [^JDBCInfo jdbc options]

  (let [tloc (DBIOLocal/getCache)
        hc (.getId jdbc)
        ^Map c (.get tloc) ]
    (when-not (.containsKey c hc)
      (log/debug "No db pool found in DBIO-thread-local, creating one")
      (->> {:partitions 1
            :max-conns 1 :min-conns 1 }
           (merge options )
           (DbPool* jdbc )
           (.put c hc )))
    (.get c hc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioConnect

  "Connect to a datasource"

  ^DBAPI
  [^JDBCInfo jdbc metaCache options]

  (let [hc (.getId jdbc) ]
    ;;(log/debug "%s" (.getMetas metaCache))
    (reify

      DBAPI

      (supportsLock [_] (not (false? (:opt-lock options))))

      (vendor [_] (ResolveVendor jdbc))

      (getMetaCache [_] metaCache)

      (finz [_] nil)

      (open [_] (DbConnection* jdbc))

      (newCompositeSQLr [this] (CompositeSQLr* this))
      (newSimpleSQLr [this] (SimpleSQLr* this)) )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn DbioConnectViaPool

  "Connect to a datasource"

  ^DBAPI
  [^JDBCPool pool metaCache options]

  (reify

    DBAPI

    (supportsLock [_] (not (false? (:opt-lock options))))
    (getMetaCache [_] metaCache)

    (vendor [_] (.vendor pool))
    (finz [_] nil)
    (open [_] (.nextFree pool))

    (newCompositeSQLr [this] (CompositeSQLr* this))
    (newSimpleSQLr [this] (SimpleSQLr* this)) ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

