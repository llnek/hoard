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
    [czlab.dbio
     SQLr
     DBAPI
     Schema
     Transactable]
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
(defn- undo

  ""
  [^Connection conn]

  (try! (.rollback conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- commit

  ""
  [^Connection conn]

  (.commit conn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- begin

  ""

  ^Connection
  [^DBAPI db how]

  (doto (.open db)
    (.setTransactionIsolation how)
    (.setAutoCommit false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn compositeSQLr

  "A composite supports transactions"

  ^Transactable
  [^DBAPI db & [affinity]]

  (let [how (or affinity
                Connection/TRANSACTION_SERIALIZABLE)]
    (reify

      Transactable

      (execWith [me func]
        (with-local-vars [rc nil]
          (with-open
            [c (begin db how)]
              (try
                (let
                  [rc (->> (reifySQLr
                             db
                             (constantly c)
                             #(%2 %1))
                           (func ))]
                  (commit c)
                  rc)
                (catch Throwable e#
                  (undo c)
                  (log/warn e# "")
                  (throw e#)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


