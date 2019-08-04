;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc ""
      :author "Kenneth Leung"}

  czlab.test.horde.core

  (:require [czlab.horde.connect :as cn]
            [czlab.horde.drivers :as d]
            [clojure.java.io :as io]
            [czlab.horde.sql :as q]
            [czlab.horde.core :as h]
            [clojure.string :as cs]
            [clojure.test :as ct]
            [czlab.basal.io :as i]
            [czlab.basal.util :as u]
            [czlab.basal.core
             :refer [ensure?? ensure-thrown??] :as c])

  (:import [java.io File]
           [java.sql Connection]
           [java.util GregorianCalendar Calendar]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:private meta-cc
  (h/dbschema<>
    (h/dbmodel<> ::Address
      (h/dbfields
        {:addr1 {:size 200 :null? false}
         :addr2 {:size 64}
         :city {:null? false}
         :state {:null? false}
         :zip {:null? false}
         :country {:null? false}})
      (h/dbindexes
        {:i1 #{:city :state :country }
         :i2 #{:zip :country }
         :i3 #{:state }
         :i4 #{:zip } }))
    (h/dbmodel<> ::Person
      (h/dbfields
        {:first_name {:null? false }
         :last_name {:null? false }
         :iq {:domain :Int}
         :bday {:domain :Calendar :null? false}
         :sex {:null? false} })
      (h/dbindexes
        {:i1 #{ :first_name :last_name }
         :i2 #{ :bday } })
      (h/dbassocs
        {:addrs {:kind :o2m :other ::Address :cascade? true}
         :spouse {:kind :o2o :other ::Person } }))
    (h/dbmodel<> ::Employee
      (h/dbfields
        {:salary { :domain :Float :null? false }
         :passcode { :domain :Password }
         :pic { :domain :Bytes }
         :desc {}
         :login {:null? false} })
      (h/dbindexes {:i1 #{ :login } } )
      (h/dbassocs
        {:person {:kind :o2o :other ::Person } }))
    (h/dbmodel<> ::Department
      (h/dbfields
        {:dname { :null? false } })
      (h/dbuniques
        {:u1 #{ :dname }} ))
    (h/dbmodel<> ::Company
      (h/dbfields
        {:revenue { :domain :Double :null? false }
         :cname { :null? false }
         :logo { :domain :Bytes } })
      (h/dbassocs
        {:depts {:kind :o2m :other ::Department :cascade? true}
         :emps {:kind :o2m :other ::Employee :cascade? true}
         :hq {:kind :o2o :other ::Address :cascade? true}})
      (h/dbuniques
        {:u1 #{ :cname } } ))
    (h/dbjoined<> ::EmpDepts ::Department ::Employee)))
(def ^:private jdbc-spec nil)
(def ^:private DBID nil)
(def ^:private DB nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- init-test [dbid f]
  (let [url (d/h2db (u/sys-tmp-dir)
                    dbid "sa" "hello")
        jdbc (h/dbspec<> d/*h2-driver*
                         url "sa" "hello")
        ddl (d/get-ddl meta-cc :h2)
        db (cn/dbopen<+> jdbc meta-cc)]
    (when false
      (let [s (h/dbg-show-schema meta-cc)]
        (println "\n\n" ddl)
        (println "\n\n" s)
        (i/spit-utf8 (i/tmpfile "dbtest.out") s)))
    (alter-var-root #'jdbc-spec (constantly jdbc))
    (h/upload-ddl jdbc ddl)
    (alter-var-root #'DB (constantly db))
    (alter-var-root #'DBID (constantly dbid))
    (if (fn? f) (f))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- finz-test []
  (c/do#true (cn/db-finz DB)
             (d/close-h2db (u/sys-tmp-dir)
                           DBID "sa" "hello")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- person<>
  [fname lname sex]
  (-> (h/find-model meta-cc ::Person)
      h/dbpojo<>
      (h/db-set-flds* :first_name fname
                      :last_name  lname
                      :iq 100
                      :sex sex
                      :bday (GregorianCalendar.))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- emp<> [login]
  (-> (h/find-model meta-cc ::Employee)
      h/dbpojo<>
      (h/db-set-flds* :pic (i/x->bytes "poo")
                      :passcode "secret"
                      :desc "idiot"
                      :login login
                      :salary 1000000.00)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- company<> [cname]
  (-> (h/find-model meta-cc ::Company)
      h/dbpojo<>
      (h/db-set-flds* :cname cname
                      :revenue 100.00
                      :logo (i/x->bytes "hi"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- dept<> [dname]
  (-> (h/find-model meta-cc ::Department)
      h/dbpojo<>
      (h/db-set-fld :dname dname)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- create-emp [fname lname sex login]
  (let [tx (cn/db-composite DB)
        cb #(let [p (->> (person<> fname
                                   lname sex)
                         (h/add-obj %))
                  e (->> (emp<> login)
                         (h/add-obj %))]
              (h/db-set-o2o
                {:with % :as :person}
                e
                (h/find-one %
                            ::Person
                            {:first_name fname
                             :last_name lname})))]
    (c/_1 (h/transact! tx cb))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- fetch-person [fname lname]
  (-> (cn/db-simple DB)
      (h/find-one ::Person
                  {:first_name fname :last_name lname})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- fetch-emp [login]
  (-> (cn/db-simple DB)
      (h/find-one ::Employee {:login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- create-person [fname lname sex]
  (let [p (person<> fname lname sex)]
    (h/transact!
      (cn/db-composite DB) #(h/add-obj % p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/deftest test-core

  (ensure?? "iniz" (c/do#true (init-test (u/jid<>) nil)))

  (ensure?? "tstamp<>" (some? (h/tstamp<>)))

  (ensure?? "dbopen<+>"
            (let [db (cn/dbopen<+> jdbc-spec meta-cc)
                  url (:url jdbc-spec)
                  c (cn/db-composite db)
                  s (cn/db-simple db)
                  {:keys [schema vendor]} db
                  conn (cn/db-open db)
                  a (h/fmt-sqlid db "hello")
                  b (h/fmt-sqlid conn "hello")
                  id (h/dbtag ::Person schema)
                  t (h/dbtable ::Person schema)
                  m (h/find-model schema ::Person)
                  cn (h/dbcol :iq m)
                  ks1 (h/match-spec?? "h2")
                  ks2 (h/match-url?? url)]
              (try (and (some? c)
                        (some? s)
                        (map? vendor)
                        (some? conn)
                        (= a b)
                        (= id ::Person)
                        (= t "Person")
                        (= "iq" cn)
                        (= ks1 ks2))
                   (finally
                     (i/klose conn)
                     (cn/db-finz db)))))

  (ensure?? "dbopen<>"
            (let [db (cn/dbopen<> jdbc-spec meta-cc)
                  url (:url jdbc-spec)
                  c (cn/db-composite db)
                  s (cn/db-simple db)
                  {:keys [schema vendor]} db
                  conn (cn/db-open db)
                  a (h/fmt-sqlid db "hello")
                  b (h/fmt-sqlid conn "hello")
                  id (h/dbtag ::Person schema)
                  t (h/dbtable ::Person schema)
                  m (h/find-model schema ::Person)
                  cn (h/dbcol :iq m)
                  ks1 (h/match-spec?? "h2")
                  ks2 (h/match-url?? url)]
              (try (and (some? c)
                        (some? s)
                        (map? vendor)
                        (some? conn)
                        (= a b)
                        (= id ::Person)
                        (= t "Person")
                        (= "iq" cn)
                        (= ks1 ks2))
                   (finally
                     (i/klose conn)
                     (cn/db-finz db)))))

  (ensure?? "dbconnect<>"
            (let [c (h/dbconnect<> jdbc-spec)]
              (try
                (map? (h/load-table-meta c "Person"))
                (finally (i/klose c)))))

  (ensure?? "test-connect?"
            (h/test-connect? jdbc-spec))

  (ensure?? "resolve-vendor"
            (map? (h/resolve-vendor jdbc-spec)))

  (ensure?? "table-exist?"
            (h/table-exist? jdbc-spec "Person"))

  (ensure?? "dbpool<>"
            (let [p (h/dbpool<> jdbc-spec)]
              (try (h/table-exist? p "Person")
                   (finally
                     (h/p-close p)))))

  (ensure?? "add-obj"
            (pos? (:rowid
                    (create-person "joe"
                                   "blog" "male"))))

  (ensure?? "row-exists?"
            (h/row-exist? jdbc-spec "Person"))

  (ensure?? "add-obj"
            (pos? (:rowid (create-emp "joe" "blog"
                                      "male" "joeb"))))

  (ensure?? "find-all"
            (= 1 (count (h/find-all
                          (cn/db-simple DB) ::Employee))))

  (ensure?? "find-one"
            (some? (fetch-emp "joeb")))

  (ensure?? "mod-obj"
            (some?
              (h/transact!
                (cn/db-composite DB)
                #(let [o2 (-> (h/find-one %
                                          ::Employee
                                          {:login "joeb"})
                              (h/db-set-flds*
                                :salary 99.9234 :desc "yo!"))]
                   (if (pos? (h/mod-obj % o2)) o2)))))

  (ensure?? "del-obj"
            (= 0
               (h/transact!
                 (cn/db-composite DB)
                 #(let [o1 (h/find-one %
                                       ::Employee
                                       {:login "joeb"})]
                    (h/del-obj % o1)
                    (h/count-objs % ::Employee)))))

  (ensure?? "find-all"
            (= 0 (count (h/find-all
                          (cn/db-simple DB) ::Employee))))

  (ensure?? "db-set-o2o"
            (let [e (create-emp "joe" "blog" "male" "joeb")
                  w (create-person "mary" "lou" "female")]
              (h/transact!
                  (cn/db-composite DB)
                  #(let
                     [pm (h/db-get-o2o
                           {:with % :as :person} e)
                      [p1 w1]
                      (h/db-set-o2o
                        {:as :spouse :with %} pm w)
                      [w2 p2]
                      (h/db-set-o2o
                        {:as :spouse :with %} w1 p1)
                      w3 (h/db-get-o2o
                           {:as :spouse :with %} p2)
                      p3 (h/db-get-o2o
                           {:as :spouse :with %} w3)]
                     (and (some? w3)
                          (some? p3))))))

  (ensure?? "db-get-o2o,db-clr-o2o"
            (h/transact!
              (cn/db-composite DB)
              #(let
                 [e (fetch-emp "joeb")
                  pm (h/db-get-o2o
                       {:with % :as :person} e)
                  w (h/db-get-o2o
                      {:as :spouse :with %} pm)
                  p2 (h/db-clr-o2o
                       {:as :spouse :with %} pm)
                  w2 (h/db-clr-o2o
                       {:as :spouse :with %} w)
                  w3 (h/db-get-o2o
                       {:as :spouse :with %} p2)
                  p3 (h/db-get-o2o
                       {:as :spouse :with %} w2)]
                 (and (nil? w3)
                      (nil? p3)
                      (some? w)))))

  (ensure?? "db-set-o2m*"
            (h/transact!
              (cn/db-composite DB)
              #(let
                 [c (h/add-obj % (company<> "acme"))
                  _ (h/db-set-o2m
                      {:as :depts :with %}
                      c
                      (h/add-obj % (dept<> "d1")))
                  _ (h/db-set-o2m*
                      {:as :depts :with %}
                      c
                      (h/add-obj % (dept<> "d2"))
                      (h/add-obj % (dept<> "d3")))
                  _ (h/db-set-o2m*
                      {:as :emps :with %}
                      c
                      (h/add-obj % (emp<> "e1"))
                      (h/add-obj % (emp<> "e2"))
                      (h/add-obj % (emp<> "e3")))
                  ds (h/db-get-o2m
                       {:as :depts :with %} c)
                  es (h/db-get-o2m
                       {:as :emps :with %} c)]
                 (and (= (count ds) 3)
                      (= (count es) 3)))))

  (ensure?? "db-get-o2m"
            (h/transact!
              (cn/db-composite DB)
              #(let
                 [c (h/find-one %
                                ::Company
                                {:cname "acme"})
                  ds (h/db-get-o2m
                       {:as :depts :with %} c)
                  es (h/db-get-o2m
                       {:as :emps :with %} c)
                  _
                  (doseq [d ds
                          :when (= (:dname d) "d2")]
                    (doseq [e es]
                      (h/db-set-m2m
                        {:joined ::EmpDepts :with %} d e)))
                  _
                  (doseq [e es
                          :when (= (:login e) "e2")]
                    (doseq [d ds
                            :let [dn (:dname d)]
                            :when (not= dn "d2")]
                      (h/db-set-m2m
                        {:joined ::EmpDepts :with %} e d)))
                  s1 (h/db-get-m2m
                       {:joined ::EmpDepts :with %}
                       (some (fn [x]
                               (if (= (:dname x) "d2") x)) ds))
                  s2 (h/db-get-m2m
                       {:joined ::EmpDepts :with %}
                       (some (fn [x]
                               (if (= (:login x) "e2") x)) es))]
                 (and (== (count s1) 3)
                      (== (count s2) 3)))))

  (ensure?? "db-clr-m2m"
            (h/transact!
              (cn/db-composite DB)
              #(let
                 [d2 (h/find-one %
                                 ::Department
                                 {:dname "d2"})
                  e2 (h/find-one %
                                 ::Employee
                                 {:login "e2"})
                  _ (h/db-clr-m2m
                      {:joined ::EmpDepts :with %} d2)
                  _ (h/db-clr-m2m
                      {:joined ::EmpDepts :with %} e2)
                  s1 (h/db-get-m2m
                       {:joined ::EmpDepts :with %} d2)
                  s2 (h/db-get-m2m
                       {:joined ::EmpDepts :with %} e2)]
                 (and (== (count s1) 0)
                      (== (count s2) 0)))))

  (ensure?? "db-clr-o2m"
            (h/transact!
              (cn/db-composite DB)
              #(let [c (h/find-one %
                                   ::Company
                                   {:cname "acme"})
                     _ (h/db-clr-o2m
                         {:as :depts :with %} c)
                     _ (h/db-clr-o2m
                         {:as :emps :with %} c)
                     s1 (h/db-get-o2m
                          {:as :depts :with %} c)
                     s2 (h/db-get-o2m
                          {:as :emps :with %} c)]
                 (and (= (count s1) 0)
                      (= (count s2) 0)))))

  (ensure?? "finz" (finz-test))

  (ensure?? "test-end" (= 1 1)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(ct/deftest ^:test-core basal-test-core
  (ct/is (let [[ok? r]
               (c/runtest test-core "test-core")] (println r) ok?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


