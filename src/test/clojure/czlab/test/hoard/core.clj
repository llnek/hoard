;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc ""
      :author "Kenneth Leung"}

  czlab.test.hoard.core

  (:require [czlab.hoard.connect :as cn]
            [czlab.hoard.drivers :as d]
            [clojure.java.io :as io]
            [czlab.hoard.sql :as q]
            [czlab.hoard.rels :as r]
            [czlab.hoard.core :as h]
            [clojure.string :as cs]
            [clojure.test :as ct]
            [czlab.basal.log :as l]
            [czlab.basal.io :as i]
            [czlab.basal.util :as u]
            [czlab.basal.core
             :refer [ensure?? ensure-thrown??] :as c])

  (:import [java.io File Closeable]
           [java.sql Connection]
           [czlab.hoard.core JdbcSpec]
           [java.util GregorianCalendar Calendar]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(h/defschema

  ^{:private true
    ;:col-rowid "CZLAB_ROWID"
    ;:col-lhs-rowid "CZLAB_LHS_ROWID"
    ;:col-rhs-rowid "CZLAB_RHS_ROWID"
    }
  meta-cc

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
    (h/dbo2o :spouse :other ::Person)
    (h/dbo2m :addrs :other ::Address :cascade? true))
  (h/dbmodel<> ::Employee
    (h/dbfields
      {:salary { :domain :Float :null? false }
       :passcode { :domain :Password }
       :pic { :domain :Bytes }
       :desc {}
       :login {:null? false} })
    (h/dbindexes {:i1 #{ :login } } )
    (h/dbo2o :person :other ::Person))
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
    (h/dbo2m :depts :other ::Department :cascade? true)
    (h/dbo2m :emps :other ::Employee :cascade? true)
    (h/dbo2o :hq :other ::Address :cascade? true)
    (h/dbuniques {:u1 #{ :cname } } ))
  (h/dbjoined<> ::EmpDepts ::Department ::Employee))

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
        db (cn/dbio<+> jdbc meta-cc)]
    (when false
      (let [s (h/dbg-schema meta-cc true)]
        (println "\n\n" ddl)
        (println "\n\n" s)
        (i/spit-utf8 (i/tmpfile "dbtest.out") s)))
    (alter-var-root #'jdbc-spec (constantly jdbc))
    (c/wo* [^Connection
            c (h/conn<> jdbc)]
           (h/upload-ddl c ddl))
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
                         (h/sq-add-obj %))
                  e (->> (emp<> login)
                         (h/sq-add-obj %))
                  r (h/find-assoc (h/gmodel e) :person)]
              (r/db-set-o2o r % e
                            (h/sq-find-one %
                                           ::Person
                                           {:first_name fname
                                            :last_name lname})))]
    (c/_1 (h/tx-transact! tx cb))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- fetch-person [fname lname]
  (-> (cn/db-simple DB)
      (h/sq-find-one ::Person
                     {:first_name fname :last_name lname})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- fetch-emp [login]
  (-> (cn/db-simple DB)
      (h/sq-find-one ::Employee {:login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- create-person [fname lname sex]
  (let [p (person<> fname lname sex)]
    (h/tx-transact!
      (cn/db-composite DB) #(h/sq-add-obj % p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/deftest test-core

  (ensure?? "iniz" (c/do#true (init-test (u/jid<>) nil)))

  (ensure?? "tstamp<>" (some? (h/tstamp<>)))

  (ensure?? "dbio<+>"
            (let [db (cn/dbio<+> jdbc-spec meta-cc)
                  url (:url jdbc-spec)
                  c (cn/db-composite db)
                  s (cn/db-simple db)
                  {:keys [schema vendor]} db
                  conn (cn/db-open db)
                  a (h/fmt-sqlid vendor "hello")
                  b (h/fmt-sqlid conn "hello")
                  m (h/find-model schema ::Person)
                  id (h/find-id m)
                  t (h/find-table m)
                  cn (h/find-col (h/find-field m :iq ))
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

  (ensure?? "dbio<>"
            (let [db (cn/dbio<> jdbc-spec meta-cc)
                  url (:url jdbc-spec)
                  c (cn/db-composite db)
                  s (cn/db-simple db)
                  {:keys [schema vendor]} db
                  conn (cn/db-open db)
                  a (h/fmt-sqlid vendor "hello")
                  b (h/fmt-sqlid conn "hello")
                  m (h/find-model schema ::Person)
                  t (h/find-table m)
                  id (h/find-id m)
                  cn (h/find-col (h/find-field m :iq))
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

  (ensure?? "conn<>"
            (let [c (h/conn<> jdbc-spec)]
              (try
                (map? (h/table-meta c "Person"))
                (finally (i/klose c)))))

  (ensure?? "test-connect?"
            (h/testing? jdbc-spec))

  (ensure?? "db-vendor"
            (map? (c/wo* [^Connection
                          c (h/conn<> jdbc-spec)] (h/db-vendor c))))

  (ensure?? "table-exist?"
            (c/wo* [^Connection
                    c (h/conn<> jdbc-spec)] (h/table-exist? c "Person")))

  (ensure?? "dbpool<>"
            (c/wo* [^Closeable p (h/dbpool<> jdbc-spec)]
              (c/wo* [^Connection c (h/jp-next p)] (h/table-exist? c "Person"))))

  (ensure?? "add-obj"
            (pos? (:rowid
                    (create-person "joe"
                                   "blog" "male"))))

  (ensure?? "row-exists?"
            (c/wo* [^Connection
                    c (h/conn<> jdbc-spec)] (h/row-exist? c "Person")))

  (ensure?? "add-obj"
            (pos? (:rowid (create-emp "joe" "blog"
                                      "male" "joeb"))))

  (ensure?? "find-all"
            (= 1 (count (h/sq-find-all
                          (cn/db-simple DB) ::Employee))))

  (ensure?? "find-one"
            (some? (fetch-emp "joeb")))

  (ensure?? "mod-obj"
            (some?
              (h/tx-transact!
                (cn/db-composite DB)
                #(let [o2 (-> (h/sq-find-one %
                                             ::Employee
                                             {:login "joeb"})
                              (h/db-set-flds*
                                :salary 99.9234 :desc "yo!"))]
                   (if (pos? (h/sq-mod-obj % o2)) o2)))))

  (ensure?? "del-obj"
            (= 0
               (h/tx-transact!
                 (cn/db-composite DB)
                 #(let [o1 (h/sq-find-one %
                                          ::Employee
                                          {:login "joeb"})]
                    (h/sq-del-obj % o1)
                    (h/sq-count-objs % ::Employee)))))

  (ensure?? "find-all"
            (= 0 (count (h/sq-find-all
                          (cn/db-simple DB) ::Employee))))

  (ensure?? "db-set-o2o"
            (let [e (create-emp "joe" "blog" "male" "joeb")
                  w (create-person "mary" "lou" "female")]
              (h/tx-transact!
                  (cn/db-composite DB)
                  #(let
                     [pm (r/db-get-o2o
                           (h/find-assoc (h/gmodel e) :person) % e)
                      [p1 w1]
                      (r/db-set-o2o
                        (h/find-assoc (h/gmodel pm) :spouse) % pm w)
                      [w2 p2]
                      (r/db-set-o2o
                        (h/find-assoc (h/gmodel w1) :spouse) % w1 p1)
                      w3 (r/db-get-o2o
                           (h/find-assoc (h/gmodel p2) :spouse) % p2)
                      p3 (r/db-get-o2o
                           (h/find-assoc (h/gmodel w3) :spouse) % w3)]
                     (and (some? w3)
                          (some? p3))))))

  (ensure?? "db-(get|clr)-o2o"
            (h/tx-transact!
              (cn/db-composite DB)
              #(let
                 [e (fetch-emp "joeb")
                  pm (r/db-get-o2o
                       (h/find-assoc (h/gmodel e) :person) % e)
                  w (r/db-get-o2o
                      (h/find-assoc (h/gmodel pm) :spouse) % pm)
                  p2 (r/db-clr-o2o
                       (h/find-assoc (h/gmodel pm) :spouse) % pm)
                  w2 (r/db-clr-o2o
                       (h/find-assoc (h/gmodel w) :spouse) % w)
                  w3 (r/db-get-o2o
                       (h/find-assoc (h/gmodel p2) :spouse) % p2)
                  p3 (r/db-get-o2o
                       (h/find-assoc (h/gmodel w2) :spouse) % w2)]
                 (and (nil? w3)
                      (nil? p3)
                      (some? w)))))

  (ensure?? "db-set-o2m*"
            (h/tx-transact!
              (cn/db-composite DB)
              #(let
                 [c (h/sq-add-obj % (company<> "acme"))
                  _ (r/db-set-o2m
                      (h/find-assoc (h/gmodel c) :depts)
                      % c
                      (h/sq-add-obj % (dept<> "d1")))
                  _ (r/db-set-o2m*
                      (h/find-assoc (h/gmodel c) :depts)
                      %
                      c
                      [(h/sq-add-obj % (dept<> "d2"))
                       (h/sq-add-obj % (dept<> "d3"))])
                  _ (r/db-set-o2m*
                      (h/find-assoc (h/gmodel c) :emps)
                      %
                      c
                      [(h/sq-add-obj % (emp<> "e1"))
                       (h/sq-add-obj % (emp<> "e2"))
                       (h/sq-add-obj % (emp<> "e3"))])
                  ds (r/db-get-o2m
                       (h/find-assoc (h/gmodel c) :depts) % c)
                  es (r/db-get-o2m
                       (h/find-assoc (h/gmodel c) :emps) % c)]
                 (and (= (count ds) 3)
                      (= (count es) 3)))))

  (ensure?? "db-get-o2m"
            (h/tx-transact!
              (cn/db-composite DB)
              #(let
                 [c (h/sq-find-one %
                                   ::Company
                                   {:cname "acme"})
                  ds (r/db-get-o2m
                       (h/find-assoc (h/gmodel c) :depts) % c)
                  es (r/db-get-o2m
                       (h/find-assoc (h/gmodel c) :emps) % c)
                  _
                  (doseq [d ds
                          :when (= (:dname d) "d2")]
                    (doseq [e es]
                      (r/db-set-m2m
                        (h/gmxm (h/find-model
                                  meta-cc ::EmpDepts))
                        % d e)))
                  _
                  (doseq [e es
                          :when (= (:login e) "e2")]
                    (doseq [d ds
                            :let [dn (:dname d)]
                            :when (not= dn "d2")]
                      (r/db-set-m2m
                        (h/gmxm (h/find-model
                                  meta-cc ::EmpDepts))
                        % e d)))
                  s1 (r/db-get-m2m
                       (h/gmxm (h/find-model
                                 meta-cc ::EmpDepts))
                       %
                       (some (fn [x]
                               (if (= (:dname x) "d2") x)) ds))
                  s2 (r/db-get-m2m
                       (h/gmxm (h/find-model
                                 meta-cc ::EmpDepts))
                       %
                       (some (fn [x]
                               (if (= (:login x) "e2") x)) es))]
                 (and (== (count s1) 3)
                      (== (count s2) 3)))))

  (ensure?? "db-clr-m2m"
            (h/tx-transact!
              (cn/db-composite DB)
              #(let
                 [d2 (h/sq-find-one %
                                    ::Department
                                    {:dname "d2"})
                  e2 (h/sq-find-one %
                                    ::Employee
                                    {:login "e2"})
                  _ (r/db-clr-m2m
                      (h/gmxm (h/find-model
                                meta-cc ::EmpDepts))
                      % d2)
                  _ (r/db-clr-m2m
                      (h/gmxm (h/find-model
                                meta-cc ::EmpDepts))
                      % e2)
                  s1 (r/db-get-m2m
                       (h/gmxm (h/find-model
                                 meta-cc ::EmpDepts))
                       % d2)
                  s2 (r/db-get-m2m
                       (h/gmxm (h/find-model
                                 meta-cc ::EmpDepts))
                       % e2)]
                 (and (== (count s1) 0)
                      (== (count s2) 0)))))

  (ensure?? "db-clr-o2m"
            (h/tx-transact!
              (cn/db-composite DB)
              #(let [c (h/sq-find-one %
                                      ::Company
                                      {:cname "acme"})
                     _ (r/db-clr-o2m
                         (h/find-assoc (h/gmodel c) :depts) % c)
                     _ (r/db-clr-o2m
                         (h/find-assoc (h/gmodel c) :emps) % c)
                     s1 (r/db-get-o2m
                          (h/find-assoc (h/gmodel c) :depts) % c)
                     s2 (r/db-get-o2m
                          (h/find-assoc (h/gmodel c) :emps) % c)]
                 (and (= (count s1) 0)
                      (= (count s2) 0)))))

  (ensure?? "finz" (finz-test))

  (ensure?? "test-end" (= 1 1)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(ct/deftest
  ^:test-core basal-test-core
  (ct/is (c/clj-test?? test-core)))

;(println (u/sys-tmp-dir))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


