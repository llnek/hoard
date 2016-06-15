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

(ns ^{:doc "Non transactional SQL client"
      :author "Kenneth Leung" }

  czlab.dbio.simple

  (:require
    [czlab.xlib.str :refer [hgl?]]
    [czlab.xlib.logging :as log])

  (:use [czlab.dbio.core]
        [czlab.dbio.sql])

  (:import
    [czlab.dbio
     DBAPI
     Schema
     SQLr]
    [java.sql Connection]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- openDB

  "Connect to a database"

  ^Connection
  [^DBAPI db]

  (doto (.open db)
    (.setAutoCommit true)
    (.setTransactionIsolation Connection/TRANSACTION_SERIALIZABLE)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn simpleSQLr

  "Non transactional SQL object"

  ^SQLr
  [db]

  (reifySQLr
    db
    #(with-open [c2 (openDB db)] (% c2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


