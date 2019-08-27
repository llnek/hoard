;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc ""
      :author "Kenneth Leung"}

  czlab.hoard.rels

  (:require [czlab.basal.util :as u]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.str :as s]
            [czlab.basal.io :as i]
            [czlab.basal.log :as l]
            [czlab.basal.meta :as m]
            [czlab.basal.core :as c]
            [czlab.hoard.core :as h])

  (:import [clojure.lang Keyword APersistentMap APersistentVector]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- set-mxm-flds* [pojo & fvs]
  (reduce #(assoc %1 (first %2) (last %2)) pojo (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-get-relation
  "Get the relation definition."
  [model rid _kind]
  (if-some [{:keys [kind] :as r}
            (h/find-assoc model rid)] (if (= kind _kind) r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- select-side+ [mxm obj]
  (let [t (h/gtype obj)
        {rt :other :as rhs}
        (get-in mxm [:rels :rhs])
        {lf :other :as lhs}
        (get-in mxm [:rels :lhs])]
    (cond
      (= t rt)
      [:rhs-rowid :lhs-rowid lf]
      (= t lf)
      [:lhs-rowid :rhs-rowid rt]
      (nil? obj)
      [nil nil nil]
      :else
      (h/dberr! "Unknown many-to-many for: %s." t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro ^:private select-side
  [mxm obj] `(first (select-side+ ~mxm  ~obj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; handling assocs
(defn- dbio-get-o2x [ctx lhsObj kind]
  (let [{rid :as sqlr :with} ctx]
    (or (dbio-get-relation (h/gmodel lhsObj) rid kind)
        (h/dberr! "Unknown relation: %s." rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-set-o2x [ctx lhsObj rhsObj kind]
  (let [{rid :as sqlr :with} ctx]
    (if-some
      [{:keys [fkey] :as r}
       (dbio-get-relation (h/gmodel lhsObj) rid kind)]
      (let [fv (h/goid lhsObj)
            y (-> (h/mock-pojo<> rhsObj)
                  (h/db-set-fld fkey fv))
            cnt (h/sq-mod-obj sqlr y)]
        [lhsObj (merge rhsObj y)])
      (h/dberr! "Unknown relation: %s." rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-o2m
  "One to many assocs."
  [ctx lhsObj]
  {:pre [(map? ctx)(map? lhsObj)]}
  (let [{sqlr :with kast :cast} ctx]
    (if-some
      [{:keys [other fkey] :as r}
       (dbio-get-o2x ctx lhsObj :o2m)]
      (h/sq-find-some sqlr
                      (or kast other)
                      {fkey (h/goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-o2m
  "" [ctx lhsObj rhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)
         (map? rhsObj)]}
  (dbio-set-o2x ctx lhsObj rhsObj :o2m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-o2m*
  "" ^APersistentVector
  [ctx lhsObj & rhsObjs]
  (c/preduce<vec>
    #(conj! %1
            (last (dbio-set-o2x ctx lhsObj %2 :o2m))) rhsObjs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-o2o
  "One to one relation."
  ^APersistentMap
  [ctx lhsObj]
  {:pre [(map? ctx) (map? lhsObj)]}
  (let [{sqlr :with kast :cast} ctx]
    (if-some
      [{:keys [other fkey] :as r}
       (dbio-get-o2x ctx lhsObj :o2o)]
      (h/sq-find-one sqlr
                     (or kast other)
                     {fkey (h/goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-o2o
  "Set One to one relation."
  [ctx lhsObj rhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)
         (map? rhsObj)]}
  (dbio-set-o2x ctx lhsObj rhsObj :o2o))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-clr-o2x [ctx objA kind]
  (let [{sqlr :with rid :as kast :cast} ctx
        s (:schema sqlr)
        mA (h/gmodel objA)]
    (if-some
      [{:keys [cascade? other fkey] :as r}
       (dbio-get-relation mA rid kind)]
      (let [rt (or kast other)
            mB (h/find-model s rt)
            tn (h/find-table mB)
            cn (h/find-col (h/find-field mB fkey))]
        (h/sq-exec-sql
          sqlr
          (if-not cascade?
            (format
              "update %s set %s= null where %s=?"
              (h/sq-fmt-id sqlr tn)
              (h/sq-fmt-id sqlr cn)
              (h/sq-fmt-id sqlr cn))
            (format
              "delete from %s where %s=?"
              (h/sq-fmt-id sqlr tn)
              (h/sq-fmt-id sqlr cn)))
          [(h/goid objA)])
        objA)
      (h/dberr! "Unknown relation: %s." rid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-o2m
  "Clear one to many relation."
  [ctx lhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)]} (dbio-clr-o2x ctx lhsObj :o2m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-o2o
  "Clear one to one relation."
  [ctx lhsObj]
  {:pre [(map? ctx)
         (map? lhsObj)]} (dbio-clr-o2x ctx lhsObj :o2o))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-set-m2m
  "Set many to many relations."
  ^APersistentMap
  [ctx objA objB]
  {:pre [(map? ctx)
         (map? objA) (map? objB)]}
  (let [{sqlr :with jon :joined} ctx
        s (:schema sqlr)]
    (if-some
      [mm (h/find-model s jon)]
      (let [ka (select-side mm objA)
            kb (select-side mm objB)]
        (h/sq-add-obj sqlr
                      (-> (h/dbpojo<> mm)
                          (set-mxm-flds*
                            ka (h/goid objA)
                            kb (h/goid objB)))))
      (h/dberr! "Unkown relation: %s." jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-clr-m2m
  "Clear many to many relations."
  ([ctx obj] (db-clr-m2m ctx obj nil))
  ([ctx objA objB]
   {:pre [(some? objA)]}
    (let [{sqlr :with jon :joined} ctx
          s (:schema sqlr)]
      (if-some
        [{:keys [fields] :as mm} (h/find-model s jon)]
        (let [ka (select-side mm objA)
              kb (select-side mm objB)]
          (if (nil? objB)
            (h/sq-exec-sql sqlr
                           (format
                             "delete from %s where %s=?"
                             (h/sq-fmt-id sqlr (h/find-table mm))
                             (h/sq-fmt-id sqlr (h/find-col (fields ka))))
                           [(h/goid objA)])
            (h/sq-exec-sql sqlr
                           (format
                             "delete from %s where %s=? and %s=?"
                             (h/sq-fmt-id sqlr (h/find-table mm))
                             (h/sq-fmt-id sqlr (h/find-col (fields ka)))
                             (h/sq-fmt-id sqlr (h/find-col (fields kb))))
                           [(h/goid objA) (h/goid objB)])))
        (h/dberr! "Unkown relation: %s." jon)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn db-get-m2m
  "" [ctx pojo]
  {:pre [(map? ctx) (map? pojo)]}
  (let [{sqlr :with kast :cast jon :joined} ctx
        s (:schema sqlr)
        MM (h/sq-fmt-id sqlr "MM")
        RS (h/sq-fmt-id sqlr "RES")]
    (if-some
      [{:keys [fields] :as mm} (h/find-model s jon)]
      (let [{{:keys [col-rowid]} :____meta} @s
            [ka kb t] (select-side+ mm pojo)
            t2 (or kast t)
            tm (h/find-model s t2)]
        (if (nil? tm)
          (h/dberr! "Unknown model: %s." t2))
        (h/sq-select-sql sqlr
                         t2
                         (s/fmt
                           (str "select distinct %s.* from %s %s "
                                "join %s %s on "
                                "%s.%s=? and %s.%s=%s.%s")
                           RS
                           (h/sq-fmt-id sqlr (h/find-table tm))
                           RS
                           (h/sq-fmt-id sqlr (h/find-table mm))
                           MM
                           MM (h/sq-fmt-id sqlr (h/find-col (ka fields)))
                           MM (h/sq-fmt-id sqlr (h/find-col (kb fields)))
                           RS (h/sq-fmt-id sqlr col-rowid))
                         [(h/goid pojo)]))
      (h/dberr! "Unknown joined model: %s." jon))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


