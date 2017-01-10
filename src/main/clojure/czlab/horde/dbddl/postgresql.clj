;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "DDL functions for Postgresql."
      :author "Kenneth Leung"}

  czlab.horde.dbddl.postgresql

  (:require [czlab.xlib.logging :as log])

  (:use [czlab.horde.dbddl.drivers]
        [czlab.horde.dbio.core :as dbcore]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def postgresql-url "jdbc:postgresql://{{host}}:{{port}}/{{db}}" )
(def postgresql-driver "org.postgresql.Driver")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Postgresql
(defmethod getTSKwd :postgresql [_] "timestamp with time zone")
(defmethod getBlobKwd :postgresql [_] "bytea")
(defmethod getDoubleKwd :postgresql [_] "double precision")
(defmethod getFloatKwd :postgresql [_] "real")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genCaldr
  Postgresql
  [dbtype field]
  (genTimestamp dbtype field))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoInteger
  Postgresql
  [dbtype model field]
  (genColDef dbtype (genCol field) "serial" false nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genAutoLong
  Postgresql
  [dbtype model field]
  (genColDef dbtype (genCol field) "bigserial" false nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod genDrop
  Postgresql
  [dbtype model]
  (str "drop table if exists "
       (gtable model)
       " cascade "
       (genExec dbtype) "\n\n"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


