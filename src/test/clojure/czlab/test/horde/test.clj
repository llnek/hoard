;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.test.horde.test

  (:require [czlab.basal.io :as i :refer [writeFile]]
            [czlab.horde.connect :as hc]
            [czlab.horde.drivers :as d]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.horde.core :as h]
            [czlab.basal.core :as c])

  (:use [clojure.test])

  (:import [czlab.basal.core.GenericMutable]
           [czlab.jasal Disposable]
           [java.io File]
           [java.sql Connection]
           [java.util GregorianCalendar Calendar]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def
  ^:private
  meta-cc
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
         :descr {}
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
(def ^:private DB nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn initTest
  ""
  [f]
  (let [url (d/H2Db (c/sysTmpDir) (str (c/now<>)) "sa" "hello")
        jdbc (h/dbspec<>
               {:driver d/*h2-driver*
                :url url
                :user "sa"
                :passwd "hello"})
        ddl (d/getDdl meta-cc :h2)
        db (hc/dbopen<+> jdbc meta-cc)]
    (when false
      (i/writeFile (io/file (c/sysTmpDir)
                          "dbtest.out") (h/dbgShowSchema meta-cc))
      (println "\n\n" (h/dbgShowSchema meta-cc)))
    (if false (println "\n\n" ddl))
    (alter-var-root #'jdbc-spec (constantly jdbc))
    (h/uploadDdl jdbc ddl)
    (alter-var-root #'DB (constantly db))
  (if (fn? f) (f))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn finzTest "" [] (c/do->true (.dispose ^Disposable DB)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkPerson
  ""
  [fname lname sex]
  (-> (h/lookupModel meta-cc ::Person)
      h/dbpojo<>
      (h/dbSetFlds*
        {:first_name fname
         :last_name  lname
         :iq 100
         :bday (GregorianCalendar.)
         :sex sex})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkEmp
  ""
  [login]
  (-> (h/lookupModel meta-cc ::Employee)
      h/dbpojo<>
      (h/dbSetFlds*
        {:pic (c/bytesit "poo")
         :salary 1000000.00
         :passcode "secret"
         :desc "idiot"
         :login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkCompany
  ""
  [cname]
  (-> (h/lookupModel meta-cc ::Company)
      h/dbpojo<>
      (h/dbSetFlds*
        {:cname cname
         :revenue 100.00
         :logo (c/bytesit "hi")})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkDept
  ""
  [dname]
  (-> (h/lookupModel meta-cc ::Department)
      h/dbpojo<>
      (h/dbSetFld :dname dname)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createEmp
  ""
  [fname lname sex login]
  (let [tx (hc/composite-sqlr DB)]
    (->>
      (fn [s]
        (let [p (mkPerson fname
                          lname
                          sex)
              e (mkEmp login)
              e (h/add-obj s e)
              p (h/add-obj s p)]
          (h/dbSetO2O
            {:with s :as :person}
            e
            (h/find-one s
                      ::Person
                      {:first_name fname
                       :last_name lname}))))
      (h/transact! tx)
      first)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetchAllEmps
  ""
  []
  (-> (hc/simple-sqlr DB)
      (h/find-all ::Employee)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetch-person
  ""
  [fname lname]
  (-> (hc/simple-sqlr DB)
      (h/find-one ::Person
                  {:first_name fname :last_name lname})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetchEmp
  ""
  [login]
  (-> (hc/simple-sqlr DB)
      (h/find-one ::Employee {:login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- changeEmp
  ""
  [login]
  (->
      (hc/composite-sqlr DB)
      (h/transact!
        #(let [o2 (-> (h/find-one %
                                ::Employee {:login login})
                      (h/dbSetFlds* {:salary 99.9234 :iq 0}))]
           (if (> (h/mod-obj % o2) 0) o2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- deleteEmp
  ""
  [login]
  (->
      (hc/composite-sqlr DB)
      (h/transact!
        (fn [s]
          (let [o1 (h/find-one s ::Employee {:login login})]
            (h/del-obj s o1)
            (h/count-objs s ::Employee))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createPerson
  ""
  [fname lname sex]
  (let [p (mkPerson fname lname sex)]
    (->
        (hc/composite-sqlr DB)
        (h/transact! #(h/add-obj % p)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wedlock?
  ""
  []
  (let [sql (hc/composite-sqlr DB)
        e (createEmp "joe" "blog" "male" "joeb")
        w (createPerson "mary" "lou" "female")]
    (->>
      (fn [s]
        (let
          [pm (h/dbGetO2O {:with s :as :person} e)
           [p1 w1]
           (h/dbSetO2O {:as :spouse :with s} pm w)
           [w2 p2]
           (h/dbSetO2O {:as :spouse :with s} w1 p1)
           w3 (h/dbGetO2O {:as :spouse :with s} p2)
           p3 (h/dbGetO2O {:as :spouse :with s} w3)]
          (and (some? w3)(some? p3))))
      (h/transact! sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undoWedlock
  ""
  []
  (let [sql (hc/composite-sqlr DB)]
    (->>
      (fn [s]
        (let
          [e (fetchEmp "joeb")
           pm (h/dbGetO2O {:with s :as :person} e)
           w (h/dbGetO2O {:as :spouse :with s } pm)
           p2 (h/dbClrO2O {:as :spouse :with s } pm)
           w2 (h/dbClrO2O {:as :spouse :with s } w)
           w3 (h/dbGetO2O {:as :spouse :with s } p2)
           p3 (h/dbGetO2O {:as :spouse :with s } w2)]
          (and (nil? w3)(nil? p3)(some? w))))
      (h/transact! sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testCompany
  ""
  []
  (let [sql (hc/composite-sqlr DB)]
    (->>
      (fn [s]
        (let [c (h/add-obj s (mkCompany "acme"))
              _ (h/dbSetO2M
                  {:as :depts :with s }
                  c
                  (h/add-obj s (mkDept "d1")))
              _ (h/dbSetO2M*
                  {:as :depts :with s }
                  c
                  (h/add-obj s (mkDept "d2"))
                  (h/add-obj s (mkDept "d3")))
              _ (h/dbSetO2M*
                  {:as :emps :with s }
                  c
                  (h/add-obj s (mkEmp "e1" ))
                  (h/add-obj s (mkEmp "e2" ))
                  (h/add-obj s (mkEmp "e3" )))
              ds (h/dbGetO2M  {:as :depts :with s} c)
              es (h/dbGetO2M  {:as :emps :with s} c)]
          (and (= (count ds) 3)
               (= (count es) 3))))
      (h/transact! sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testM2M
  ""
  []
  (let [sql (hc/composite-sqlr DB)]
    (->>
      (fn [s]
        (let
          [c (h/find-one s ::Company {:cname "acme"})
           ds (h/dbGetO2M {:as :depts :with s} c)
           es (h/dbGetO2M {:as :emps :with s} c)
           _
           (doseq [d ds
                   :when (= (:dname d) "d2")]
             (doseq [e es]
               (h/dbSetM2M {:joined ::EmpDepts :with s} d e)))
           _
           (doseq [e es
                   :when (= (:login e) "e2")]
             (doseq [d ds
                     :let [dn (:dname d)]
                     :when (not= dn "d2")]
               (h/dbSetM2M {:joined ::EmpDepts :with s} e d)))
           s1 (h/dbGetM2M
                {:joined ::EmpDepts :with s}
                (some #(if (= (:dname %) "d2") %)  ds))
           s2 (h/dbGetM2M
                {:joined ::EmpDepts :with s}
                (some #(if (= (:login %) "e2") %)  es))]
          (and (== (count s1) 3)
               (== (count s2) 3))))
      (h/transact! sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undoM2M
  ""
  []
  (let [sql (hc/composite-sqlr DB)]
    (->>
      (fn [s]
        (let
          [d2 (h/find-one s ::Department {:dname "d2"})
           e2 (h/find-one s ::Employee {:login "e2"})
           _ (h/dbClrM2M {:joined ::EmpDepts :with s} d2)
           _ (h/dbClrM2M {:joined ::EmpDepts :with s} e2)
           s1 (h/dbGetM2M {:joined ::EmpDepts :with s} d2)
           s2 (h/dbGetM2M {:joined ::EmpDepts :with s} e2)]
          (and (== (count s1) 0)
               (== (count s2) 0))))
      (h/transact! sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undoCompany
  ""
  []
  (let [sql (hc/composite-sqlr DB)]
    (->>
      (fn [s]
        (let
          [c (h/find-one s ::Company {:cname "acme"})
           _ (h/dbClrO2M {:as :depts :with s} c)
           _ (h/dbClrO2M {:as :emps :with s} c)
           s1 (h/dbGetO2M {:as :depts :with s} c)
           s2 (h/dbGetO2M {:as :emps :with s} c)]
          (and (== (count s1) 0)
               (== (count s2) 0))))
      (h/transact! sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftest czlabtesthorde-test

  (is (c/do->true (initTest nil)))

  (is (some? (h/tstamp<>)))

  (testing
    "related to: db api"

    (is (let [db (hc/dbopen<+> jdbc-spec meta-cc)
              url (:url jdbc-spec)
              c (hc/composite-sqlr db)
              s (hc/simple-sqlr db)
              h (:schema db)
              v (:vendor db)
              ^Connection conn (hc/opendb db)
              a (h/fmtSqlId db "hello")
              b (h/fmtSqlId conn "hello")
              id (h/dbtag ::Person h)
              t (h/dbtable ::Person h)
              m (h/lookupModel h ::Person)
              cn (h/dbcol :iq m)
              ks1 (h/matchSpec "h2")
              ks2 (h/matchUrl url)]
          (try
            (and (some? c)
                 (some? s)
                 (c/ist? czlab.horde.core.Schema h)
                 (map? v)
                 (some? conn)
                 (= a b)
                 (= id ::Person)
                 (= t "Person")
                 (= "iq" cn)
                 (= ks1 ks2))
            (finally
              (.close conn)
              (.dispose ^Disposable db)))))

    (is (let [db (hc/dbopen<> jdbc-spec meta-cc)
              url (:url jdbc-spec)
              c (hc/composite-sqlr db)
              s (hc/simple-sqlr db)
              h (:schema db)
              v (:vendor db)
              ^Connection conn (hc/opendb db)
              a (h/fmtSqlId db "hello")
              b (h/fmtSqlId conn "hello")
              id (h/dbtag ::Person h)
              t (h/dbtable ::Person h)
              m (h/lookupModel h ::Person)
              cn (h/dbcol :iq m)
              ks1 (h/matchSpec "h2")
              ks2 (h/matchUrl url)]
          (try
            (and (some? c)
                 (some? s)
                 (c/ist? czlab.horde.core.Schema h)
                 (map? v)
                 (some? conn)
                 (= a b)
                 (= id ::Person)
                 (= t "Person")
                 (= "iq" cn)
                 (= ks1 ks2))
            (finally
              (.close conn)
              (.dispose ^Disposable db)))))

    (is (let [c (h/dbconnect<> jdbc-spec)]
          (try
            (map? (h/loadTableMeta c "Person"))
            (finally (.close c)))))

    (is (h/testConnect? jdbc-spec))

    (is (map? (h/resolveVendor jdbc-spec)))

    (is (h/tableExist? jdbc-spec "Person"))

    (is (let [p (h/dbpool<> jdbc-spec)]
          (try
            (h/tableExist? p "Person")
            (finally
              (h/shut-down p))))))

  (testing
    "related to: basic CRUD"

    (is (let [m (createPerson "joe" "blog" "male")
              r (:rowid m)]
          (> r 0)))

    (is (h/rowExist? jdbc-spec "Person"))

    (is (let [m (createEmp "joe" "blog" "male" "joeb")
              r (:rowid m)]
          (> r 0)))

    (is (let [a (fetchAllEmps)]
          (== (count a) 1)))

    (is (let [a (fetchEmp "joeb" )]
          (some? a)))

    (is (let [a (changeEmp "joeb" )]
          (some? a)))

    (is (let [rc (deleteEmp "joeb")]
          (== rc 0)))

    (is (let [a (fetchAllEmps)]
          (== (count a) 0))))

  (testing
    "related to: assocs"
    (is (wedlock?))
    (is (undoWedlock))
    (is (testCompany))
    (is (testM2M))
    (is (undoM2M)))

  (is (undoCompany))

  (is (finzTest))

  (is (string? "That's all folks!")))

;;(use-fixtures :each initTest)
;;(clojure.test/run-tests 'czlab.test.horde.test)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


