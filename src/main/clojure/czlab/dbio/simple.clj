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

  czlab.xlib.dbio.simple

  (:require
    [czlab.xlib.util.str :refer [hgl?]]
    [czlab.xlib.util.logging :as log])

  (:use [czlab.xlib.dbio.core]
        [czlab.xlib.dbio.sql])

  (:import
    [com.zotohlab.frwk.dbio DBAPI MetaCache SQLr]
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
    ;;(.setTransactionIsolation Connection/TRANSACTION_READ_COMMITTED)
    (.setTransactionIsolation Connection/TRANSACTION_SERIALIZABLE)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn SimpleSQLr*

  "Non transactional SQL object"

  ^SQLr
  [^DBAPI db]

  (ReifySQLr
    db
    #(openDB %)
    (fn [^Connection c f] (with-open [c c] (f c)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

