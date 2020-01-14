;; Copyright © 2013-2020, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.hoard.rels

  (:require [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.util :as u]
            [czlab.basal.io :as i]
            [czlab.basal.core :as c]
            [czlab.hoard.core :as h])

  (:import [czlab.hoard.core
            DbioO2ORel
            DbioM2MRel
            DbioO2MRel]
           [clojure.lang Keyword
            APersistentMap APersistentVector]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
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
              (h/set-fld fkey fv))
        cnt (h/mod-obj ctx y)]
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
    (h/exec-sql ctx
                (if-not cascade?
                  (c/fmt
                    "update %s set %s= null where %s=?"
                    (h/fmt-id ctx tn)
                    (h/fmt-id ctx cn)
                    (h/fmt-id ctx cn))
                  (c/fmt
                    "delete from %s where %s=?"
                    (h/fmt-id ctx tn)
                    (h/fmt-id ctx cn)))
                [(h/goid lhsObj)])
    lhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn set-o2o

  "Set the rhs of a one-one relation."
  {:arglists '([r ctx lhsObj rhsObj])}
  [r ctx lhsObj rhsObj]
  {:pre [(c/is? DbioO2ORel r)]}

  (dbio-set-o2x r ctx lhsObj rhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn clr-o2o

  "Clear the rhs of a one-one relation."
  {:arglists '([r ctx lhsObj])}
  [r ctx lhsObj]
  {:pre [(c/is? DbioO2ORel r)]}

  (dbio-clr-o2x r ctx lhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-o2o

  "Get the rhs of a one-one relation."
  {:arglists '([r ctx lhsObj])}
  [r ctx lhsObj]
  {:pre [(c/is? DbioO2ORel r)]}

  (let [{:keys [cast]} ctx
        {:keys [other fkey]} r]
    (first (h/find-some ctx
                        (or cast other)
                        {fkey (h/goid lhsObj)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn set-o2m*

  "Set the rhs of a one-many relation."
  {:arglists '([rel ctx lhsObj rhsObjs])}
  [rel ctx lhsObj rhsObjs]
  {:pre [(c/is? DbioO2MRel rel)
         (sequential? rhsObjs)]}

  (loop [a lhsObj
         out (c/tvec*)
         [b & xs] rhsObjs]
    (if (nil? b)
      (c/cc+1 a (c/persist! out))
      (let [[x y]
            (dbio-set-o2x rel ctx a b)]
        (recur x (conj! out y) xs)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn set-o2m

  "Set the rhs of a one-many relation."
  {:arglists '([rel ctx lhsObj rhsObj])}
  [rel ctx lhsObj rhsObj]
  {:pre [(c/is? DbioO2MRel rel)]}

  (dbio-set-o2x rel ctx lhsObj rhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn clr-o2m

  "Clear the rhs of a one-many relation."
  {:arglists '([r ctx lhsObj])}
  [r ctx lhsObj]
  {:pre [(c/is? DbioO2MRel r)]}

  (dbio-clr-o2x r ctx lhsObj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-o2m

  "Get the rhs of a one-many relation."
  {:arglists '([r ctx lhsObj])}
  [r ctx lhsObj]
  {:pre [(c/is? DbioO2MRel r)]}

  (let [{:keys [cast]} ctx
        {:keys [other fkey]} r]
    (h/find-some ctx
                 (or cast other)
                 {fkey (h/goid lhsObj)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn set-m2m

  "Set both lhs and rhs of a many-many relation."
  {:arglists '([rel ctx objA objB])}
  [rel ctx objA objB]
  {:pre [(c/is? DbioM2MRel rel)]}

  (let [{:keys [cast schema]} ctx
        {:keys [owner]} rel]
    (if-some [mm (h/find-model schema owner)]
      (let [ka (c/_1 (select-side rel objA))
            kb (c/_1 (select-side rel objB))]
        (h/add-obj ctx
                   (-> (h/dbpojo<> mm)
                       (h/set-fld ka (h/goid objA))
                       (h/set-fld kb (h/goid objB)))))
      (h/dberr! "Unknown relation: %s." rel))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-m2m

  "Get the other side a many-many relation."
  {:arglists '([rel ctx pojo])}
  [rel ctx pojo]
  {:pre [(c/is? DbioM2MRel rel)]}

  (let [{:keys [cast schema]} ctx
        {:keys [owner]} rel
        MM (h/fmt-id ctx "MM")
        RS (h/fmt-id ctx "RES")]
    (if-some
      [{:keys [fields] :as mm}
       (h/find-model schema owner)]
      (let [{{:keys [col-rowid]} :____meta} @schema
            [ka kb t] (select-side rel pojo)
            t2 (or cast t)
            tm (h/find-model schema t2)]
        (if (nil? tm)
          (h/dberr! "Unknown model: %s." t2))
        (h/select-sql ctx
                      t2
                      (c/fmt
                        (str "select distinct %s.* from %s %s "
                             "join %s %s on "
                             "%s.%s=? and %s.%s=%s.%s")
                        RS
                        (h/fmt-id ctx (h/find-table tm))
                        RS
                        (h/fmt-id ctx (h/find-table mm))
                        MM
                        MM (h/fmt-id ctx (h/find-col (ka fields)))
                        MM (h/fmt-id ctx (h/find-col (kb fields)))
                        RS (h/fmt-id ctx col-rowid))
                      [(h/goid pojo)]))
      (h/dberr! "Unknown joined model: %s." rel))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn clr-m2m

  "Clear a many-many relation."
  {:arglists '([rel ctx pojo]
               [rel ctx objA objB])}

  ([rel ctx obj]
   (clr-m2m rel ctx obj nil))

  ([rel ctx objA objB]
   {:pre [(c/is? DbioM2MRel rel)]}
   (let [{:keys [schema]} ctx
         {:keys [owner]} rel]
     (if-some
       [{:keys [fields] :as mm}
        (h/find-model schema owner)]
       (let [ka (c/_1 (select-side rel objA))
             kb (c/_1 (select-side rel objB))]
         (if (nil? objB)
           (h/exec-sql ctx
                       (c/fmt
                         "delete from %s where %s=?"
                         (h/fmt-id ctx (h/find-table mm))
                         (h/fmt-id ctx (h/find-col (fields ka))))
                       [(h/goid objA)])
           (h/exec-sql ctx
                       (c/fmt
                         "delete from %s where %s=? and %s=?"
                         (h/fmt-id ctx (h/find-table mm))
                         (h/fmt-id ctx (h/find-col (fields ka)))
                         (h/fmt-id ctx (h/find-col (fields kb))))
                       [(h/goid objA) (h/goid objB)])))
       (h/dberr! "Unkown relation: %s." rel)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

