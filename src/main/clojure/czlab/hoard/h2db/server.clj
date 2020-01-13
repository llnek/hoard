;; Copyright © 2013-2020, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

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


