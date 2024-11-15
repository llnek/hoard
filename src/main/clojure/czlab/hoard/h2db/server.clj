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

(ns czlab.hoard.h2db.server

  (:gen-class)

  (:require [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.io :as i]
            [czlab.basal.util :as u]
            [czlab.basal.core :as c])

  (:import [java.io File]
           [java.sql DriverManager]
           [org.h2.tools Server]
           [org.h2.server.web WebServer]
           [org.h2.server ShutdownHandler]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- make-db

  [dbdir dbid user pwd]

  (let [u (u/fpath (io/file dbdir dbid))
        pwd (c/stror pwd "")
        url (str "jdbc:h2:" u)]
    (c/prn!! "Creating H2: %s" url)
    (.mkdir (io/file dbdir))
    (c/wo* [c (DriverManager/getConnection url user pwd)]
      (.setAutoCommit c true)
      (c/wo* [s (.createStatement c)]
        ;;(.execute s (str "CREATE USER " user " PASSWORD \"" pwd "\" ADMIN"))
        (.execute s "SET DEFAULT_TABLE_TYPE CACHED"))
      (c/wo* [s (.createStatement c)]
        (.execute s "SHUTDOWN")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- ensure-db

  [base-dir db-id user passwd]

  (let [f (io/file base-dir
                   (str db-id ".mv.db"))]
    (if-not (i/file-read-write? f)
      (make-db base-dir db-id user passwd))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-web

  [& args]

  (let [web (Server/createWebServer (into-array String args))
        _ (.setShutdownHandler web
                               (reify ShutdownHandler
                                 (shutdown [_] (.stop web))))]
    (.start web)
    (u/pause 1500)
    (c/prn!! "%s" (.getStatus web))
    (Server/openBrowser (.getURL web))
    (while (.isRunning web true) (u/pause 3000))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-tcp

  [& args]

  (let [tcp (Server/createTcpServer (into-array String args))]
    (.start tcp)
    (u/pause 1500)
    (c/prn!! "%s" (.getStatus tcp))
    (while (.isRunning tcp true) (u/pause 3000))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- do-tcp-stop

  [options]

  (let [{:keys [tcpShutdown trace
                tcpShutdownForce tcpPassword]} options]
    (Server/shutdownTcpServer tcpShutdown
                              tcpPassword
                              (boolean tcpShutdownForce) true)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn -main

  [& cargs]

  (let [server (Server.)
        [opts args]
        (u/parse-options cargs)
        {:keys [user passwd
                tcpShutdown
                tcp web
                blocking baseDir dbid]} opts
        ARGS
        (loop [out [] [a & more] cargs]
          (if (nil? a)
            out
            (let [[d1 d2]
                  (cond (or (= "-dbid" a)
                            (= "-user" a)
                            (= "-passwd" a)) [true true]
                        (= "-blocking" a) [true false])]
              (recur (if d1 out (conj out a))
                     (if d2 (drop 1 more) more)))))]
    (when (and (c/hgl? dbid)
               (c/hgl? baseDir))
      (ensure-db baseDir dbid user passwd))
    ;(c/prn!! "%s" ARGS)
    (cond
      tcp (apply do-tcp ARGS)
      web (apply do-web ARGS)
      tcpShutdown (do-tcp-stop opts))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


