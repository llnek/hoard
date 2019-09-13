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
            [czlab.basal.io :as i]
            [czlab.basal.log :as l]
            [czlab.basal.core :as c]
            [czlab.hoard.core :as h])

  (:import [clojure.lang Keyword APersistentMap APersistentVector]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol O2ORelAPI
  "Functions for one to one relations."
  (db-clr-o2o [_ ctx lhsObj] "")
  (db-get-o2o [_ ctx lhsObj] "")
  (db-set-o2o [_ ctx lhsObj rhsObj] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol O2MRelAPI
  "Functions for one to many relations."
  (db-set-o2m [_ ctx lhsObj rhsObj] "")
  (db-clr-o2m [_ ctx lhsObj] "")
  (db-get-o2m [_ ctx lhsObj] "")
  (db-set-o2m* [_ ctx lhsObj rhsObjs] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol M2MRelAPI
  "Functions for many to many relations."
  (db-set-m2m [_ ctx objA objB] "")
  (db-get-m2m [_ ctx pojo] "")
  (db-clr-m2m [_ ctx obj]
              [_ ctx objA objB] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- select-side
  "Find the right fkey based
  on the type of the object."
  [rel obj]
  (let [t (h/gtype obj)
        {[lhs lf] :lhs
         [rhs rt] :rhs} rel]
    (cond (= t rhs) [rt lf lhs]
          (= t lhs) [lf rt rhs]
          (nil? obj) [nil nil nil]
          :else (h/dberr! "Failed to select side for: %s." rel))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-set-o2x
  "Put id of lhsObj into fkey of rhsObj,
  hence binding the two objects."
  [rel ctx lhsObj rhsObj]
  (let [{:keys [fkey]} rel
        fv (h/goid lhsObj)
        ;fake a rhsObj<id,fkey>
        ;and update it
        y (-> (h/mock-pojo<> rhsObj)
              (h/db-set-fld fkey fv))
        cnt (h/sq-mod-obj ctx y)]
    ;give back the *modified* rhsObj
    [lhsObj (merge rhsObj y)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-clr-o2x
  "Undo the relation but cutting the binding."
  [rel ctx lhsObj]
  (let [{:keys [cascade? other fkey]} rel
        {:keys [cast schema]} ctx
        mA (h/gmodel lhsObj)
        ;caller can force cast to a different type
        rt (or cast other)
        mB (h/find-model schema rt)
        tn (h/find-table mB)
        cn (h/find-col (h/find-field mB fkey))]
    (h/sq-exec-sql ctx
                   (if-not cascade?
                     (c/fmt
                       "update %s set %s= null where %s=?"
                       (h/sq-fmt-id ctx tn)
                       (h/sq-fmt-id ctx cn)
                       (h/sq-fmt-id ctx cn))
                     (c/fmt
                       "delete from %s where %s=?"
                       (h/sq-fmt-id ctx tn)
                       (h/sq-fmt-id ctx cn)))
                   [(h/goid lhsObj)])
    lhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol O2ORelAPI
  czlab.hoard.core.DbioO2ORel

  (db-set-o2o [_ ctx lhsObj rhsObj]
    (dbio-set-o2x _ ctx lhsObj rhsObj))

  (db-clr-o2o [_ ctx lhsObj]
    (dbio-clr-o2x _ ctx lhsObj))

  (db-get-o2o [rel ctx lhsObj]
    (let [{:keys [cast]} ctx
          {:keys [other fkey]} rel]
      (h/sq-find-one ctx
                     (or cast other)
                     {fkey (h/goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol O2MRelAPI
  czlab.hoard.core.DbioO2MRel

  (db-set-o2m* [rel ctx lhsObj rhsObjs]
    {:pre [(sequential? rhsObjs)]}
    (loop [a lhsObj
           out (c/tvec*)
           [b & xs] rhsObjs]
      (if (nil? b)
        (c/cc+1 a (c/persist! out))
        (let [[x y]
              (dbio-set-o2x rel ctx a b)]
          (recur x (conj! out y) xs)))))

  (db-set-o2m [_ ctx lhsObj rhsObj]
    (dbio-set-o2x _ ctx lhsObj rhsObj))

  (db-clr-o2m [_ ctx lhsObj]
    (dbio-clr-o2x _ ctx lhsObj))

  (db-get-o2m [rel ctx lhsObj]
    (let [{:keys [cast]} ctx
          {:keys [other fkey]} rel]
      (h/sq-find-some ctx
                      (or cast other)
                      {fkey (h/goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol M2MRelAPI
  czlab.hoard.core.DbioM2MRel

  (db-set-m2m [rel ctx objA objB]
    (let [{:keys [cast schema]} ctx
          {:keys [owner]} rel]
      (if-some [mm (h/find-model schema owner)]
        (let [ka (c/_1 (select-side rel objA))
              kb (c/_1 (select-side rel objB))]
          (h/sq-add-obj ctx
                        (-> (h/dbpojo<> mm)
                            (h/db-set-fld ka (h/goid objA))
                            (h/db-set-fld kb (h/goid objB)))))
        (h/dberr! "Unknown relation: %s." rel))))

  (db-get-m2m [rel ctx pojo]
    (let [{:keys [cast schema]} ctx
          {:keys [owner]} rel
          MM (h/sq-fmt-id ctx "MM")
          RS (h/sq-fmt-id ctx "RES")]
      (if-some
        [{:keys [fields] :as mm}
         (h/find-model schema owner)]
        (let [{{:keys [col-rowid]} :____meta} @schema
              [ka kb t] (select-side rel pojo)
              t2 (or cast t)
              tm (h/find-model schema t2)]
          (if (nil? tm)
            (h/dberr! "Unknown model: %s." t2))
          (h/sq-select-sql ctx
                           t2
                           (c/fmt
                             (str "select distinct %s.* from %s %s "
                                  "join %s %s on "
                                  "%s.%s=? and %s.%s=%s.%s")
                             RS
                             (h/sq-fmt-id ctx (h/find-table tm))
                             RS
                             (h/sq-fmt-id ctx (h/find-table mm))
                             MM
                             MM (h/sq-fmt-id ctx (h/find-col (ka fields)))
                             MM (h/sq-fmt-id ctx (h/find-col (kb fields)))
                             RS (h/sq-fmt-id ctx col-rowid))
                           [(h/goid pojo)]))
        (h/dberr! "Unknown joined model: %s." rel))))

  (db-clr-m2m
    ([rel ctx obj]
     (db-clr-m2m rel ctx obj nil))
    ([rel ctx objA objB]
     (let [{:keys [schema]} ctx
           {:keys [owner]} rel]
       (if-some
         [{:keys [fields] :as mm}
          (h/find-model schema owner)]
         (let [ka (c/_1 (select-side rel objA))
               kb (c/_1 (select-side rel objB))]
           (if (nil? objB)
             (h/sq-exec-sql ctx
                            (c/fmt
                              "delete from %s where %s=?"
                              (h/sq-fmt-id ctx (h/find-table mm))
                              (h/sq-fmt-id ctx (h/find-col (fields ka))))
                            [(h/goid objA)])
             (h/sq-exec-sql ctx
                            (c/fmt
                              "delete from %s where %s=? and %s=?"
                              (h/sq-fmt-id ctx (h/find-table mm))
                              (h/sq-fmt-id ctx (h/find-col (fields ka)))
                              (h/sq-fmt-id ctx (h/find-col (fields kb))))
                            [(h/goid objA) (h/goid objB)])))
         (h/dberr! "Unkown relation: %s." rel))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

