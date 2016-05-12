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

  czlab.dbio.composite

  (:import
    [czlab.dbio Transactable
     SQLr MetaCache DBAPI]
    [java.sql Connection])

  (:use [czlab.dbio.core]
        [czlab.dbio.sql])

  (:require
    [czlab.xlib.core :refer [test-nonil try!]]
    [czlab.xlib.str :refer [hgl?]]
    [czlab.xlib.logging :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- undo "" [conn]
  (try! (-> ^Connection
            conn
            (.rollback))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- commit "" [conn]
  (-> ^Connection
      conn
      (.commit)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- begin

  ""

  ^Connection
  [db]

  (let [conn (.open ^DBAPI db)]
    (.setAutoCommit conn false)
    (->> Connection/TRANSACTION_SERIALIZABLE
         (.setTransactionIsolation conn ))
    conn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn compositeSQLr

  "A composite supports transactions"

  ^Transactable
  [^DBAPI db]

  (reify

    Transactable

    (execWith [me func]
      (with-local-vars [rc nil]
      (with-open
        [conn (begin db)]
        (try
          (->> (reifySQLr
                 db
                 (fn [_] conn) #(%2 %1))
               (func )
               (var-set rc ))
          (commit conn)
          @rc
          (catch Throwable e#
            (undo conn)
            (log/warn e# "")
            (throw e#))) )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

