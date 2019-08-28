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
(defprotocol O2ORelAPI
  (db-set-o2o [_ ctx lhsObj rhsObj] "")
  (db-clr-o2o [_ ctx lhsObj] "")
  (db-get-o2o [_ ctx lhsObj]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol O2MRelAPI
  (db-set-o2m* [_ ctx lhsObj rhsObjs] "")
  (db-set-o2m [_ ctx lhsObj rhsObj] "")
  (db-clr-o2m [_ ctx lhsObj] "")
  (db-get-o2m [_ ctx lhsObj] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol M2MRelAPI
  (db-set-m2m [_ ctx objA objB] "")
  (db-clr-m2m [_ ctx obj]
              [_ ctx objA objB] "")
  (db-get-m2m [_ ctx pojo] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- set-mxm-flds* [pojo & fvs]
  (reduce #(assoc %1 (first %2) (last %2)) pojo (partition 2 fvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- select-side [mxm obj]
  (let [t (h/gtype obj)
        {[lhs lf] :lhs
         [rhs rt] :rhs} mxm]
    (cond (= t rhs) [rt lf lhs]
          (= t lhs) [lf rt rhs]
          (nil? obj) [nil nil nil]
          :else (h/dberr! "Unknown many-to-many for: %s." t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-set-o2x [rel ctx lhsObj rhsObj]
  (let [{:keys [fkey]} rel
        fv (h/goid lhsObj)
        y (-> (h/mock-pojo<> rhsObj)
              (h/db-set-fld fkey fv))
        cnt (h/sq-mod-obj ctx y)]
    [lhsObj (merge rhsObj y)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dbio-clr-o2x [rel ctx objA]
  (let [{:keys [cast schema]} ctx
        {:keys [cascade? other fkey]} rel
        mA (h/gmodel objA)
        rt (or cast other)
        mB (h/find-model schema rt)
        tn (h/find-table mB)
        cn (h/find-col (h/find-field mB fkey))]
    (h/sq-exec-sql ctx
                   (if-not cascade?
                     (s/fmt
                       "update %s set %s= null where %s=?"
                       (h/sq-fmt-id ctx tn)
                       (h/sq-fmt-id ctx cn)
                       (h/sq-fmt-id ctx cn))
                     (s/fmt
                       "delete from %s where %s=?"
                       (h/sq-fmt-id ctx tn)
                       (h/sq-fmt-id ctx cn)))
                   [(h/goid objA)])
    objA))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(extend-protocol O2ORelAPI
  czlab.hoard.core.DbioO2ORel
  (db-set-o2o [rel ctx lhsObj rhsObj]
    (dbio-set-o2x rel ctx lhsObj rhsObj))
  (db-clr-o2o [rel ctx lhsObj]
    (dbio-clr-o2x rel ctx lhsObj))
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
    (c/preduce<vec>
      #(conj! %1
              (last (dbio-set-o2x rel ctx lhsObj %2))) rhsObjs))
  (db-set-o2m [rel ctx lhsObj rhsObj]
    (dbio-set-o2x rel ctx lhsObj rhsObj))
  (db-clr-o2m [rel ctx lhsObj]
    (dbio-clr-o2x rel ctx lhsObj))
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
                            (set-mxm-flds* ka (h/goid objA)
                                           kb (h/goid objB)))))
        (h/dberr! "Unknown relation: %s." rel))))
  (db-get-m2m [rel ctx pojo]
    (let [{:keys [cast schema]} ctx
          {:keys [owner]} rel
          MM (h/sq-fmt-id ctx "MM")
          RS (h/sq-fmt-id ctx "RES")]
      (if-some
        [{:keys [fields] :as mm} (h/find-model schema owner)]
        (let [{{:keys [col-rowid]} :____meta} @schema
              [ka kb t] (select-side rel pojo)
              t2 (or cast t)
              tm (h/find-model schema t2)]
          (if (nil? tm)
            (h/dberr! "Unknown model: %s." t2))
          (h/sq-select-sql ctx
                           t2
                           (s/fmt
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
  (db-clr-m2m ([rel ctx obj] (db-clr-m2m rel ctx obj nil))
    ([rel ctx objA objB]
    (let [{:keys [schema]} ctx
          {:keys [owner]} rel]
      (if-some
        [{:keys [fields] :as mm} (h/find-model schema owner)]
        (let [ka (c/_1 (select-side rel objA))
              kb (c/_1 (select-side rel objB))]
          (if (nil? objB)
            (h/sq-exec-sql ctx
                           (s/fmt
                             "delete from %s where %s=?"
                             (h/sq-fmt-id ctx (h/find-table mm))
                             (h/sq-fmt-id ctx (h/find-col (fields ka))))
                           [(h/goid objA)])
            (h/sq-exec-sql ctx
                           (s/fmt
                             "delete from %s where %s=? and %s=?"
                             (h/sq-fmt-id ctx (h/find-table mm))
                             (h/sq-fmt-id ctx (h/find-col (fields ka)))
                             (h/sq-fmt-id ctx (h/find-col (fields kb))))
                           [(h/goid objA) (h/goid objB)])))
        (h/dberr! "Unkown relation: %s." rel))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

