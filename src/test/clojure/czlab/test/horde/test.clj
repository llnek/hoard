;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.test.horde.test

  (:require [czlab.basal.io :refer [writeFile]]
            [clojure.java.io :as io]
            [clojure.string :as cs])

  (:use [czlab.horde.dbddl.drivers]
        [czlab.horde.dbio.connect]
        [czlab.horde.dbio.core]
        [czlab.basal.core]
        [czlab.horde.dbddl.h2]
        [clojure.test])

  (:import [czlab.horde Transactable JdbcInfo SQLr Schema DbApi]
           [java.io File]
           [java.util GregorianCalendar Calendar]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def
  ^:private
  meta-cc
  (atom
    (dbschema<>
      (dbmodel<> ::Address
        (dbfields
          {:addr1 {:size 200 :null? false}
           :addr2 {:size 64}
           :city {:null? false}
           :state {:null? false}
           :zip {:null? false}
           :country {:null? false}})
        (dbindexes
          {:i1 #{:city :state :country }
           :i2 #{:zip :country }
           :i3 #{:state }
           :i4 #{:zip } }))
      (dbmodel<> ::Person
        (dbfields
          {:first_name {:null? false }
           :last_name {:null? false }
           :iq {:domain :Int}
           :bday {:domain :Calendar :null? false}
           :sex {:null? false} })
        (dbindexes
          {:i1 #{ :first_name :last_name }
           :i2 #{ :bday } })
        (dbassocs
          {:addrs {:kind :O2M :other ::Address :cascade? true}
           :spouse {:kind :O2O :other ::Person } }))
      (dbmodel<> ::Employee
        (dbfields
          {:salary { :domain :Float :null? false }
           :passcode { :domain :Password }
           :pic { :domain :Bytes }
           :descr {}
           :login {:null? false} })
        (dbindexes {:i1 #{ :login } } )
        (dbassocs
          {:person {:kind :O2O :other ::Person } }))
      (dbmodel<> ::Department
        (dbfields
          {:dname { :null? false } })
        (dbuniques
          {:u1 #{ :dname }} ))
      (dbmodel<> ::Company
        (dbfields
          {:revenue { :domain :Double :null? false }
           :cname { :null? false }
           :logo { :domain :Bytes } })
        (dbassocs
          {:depts {:kind :O2M :other ::Department :cascade? true}
           :emps {:kind :O2M :other ::Employee :cascade? true}
           :hq {:kind :O2O :other ::Address :cascade? true}})
        (dbuniques
          {:u1 #{ :cname } } ))
      (dbjoined<> ::EmpDepts ::Department ::Employee))))
(def ^:private jdbc-spec (atom nil))
(def ^:private DB (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn initTest
  ""
  [f]
  (let [url (H2Db (sysTmpDir) (str (now<>)) "sa" "hello")
        jdbc (dbspec<>
               {:driver h2-driver
                :url url
                :user "sa"
                :passwd "hello"})
        ddl (getDDL @meta-cc :h2)]
    (if false
      (writeFile (io/file (sysTmpDir)
                          "dbtest.out") (dbgShowSchema @meta-cc)))
    (if false (println "\n\n" ddl))
    (reset! jdbc-spec jdbc)
    (uploadDdl jdbc ddl)
    (reset! DB (dbopen<+> jdbc @meta-cc ))
    (if false (println "\n\n" (dbgShowSchema @meta-cc))))
  (if (fn? f) (f)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn finzTest "" [] (do->true (. ^DbApi @DB finz)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkPerson
  ""
  [fname lname sex]
  (-> (. ^Schema @meta-cc get ::Person)
      dbpojo<>
      (dbSetFlds*
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
  (-> (. ^Schema @meta-cc get ::Employee)
      dbpojo<>
      (dbSetFlds*
        {:pic (bytesify "poo")
         :salary 1000000.00
         :passcode "secret"
         :desc "idiot"
         :login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkCompany
  ""
  [cname]
  (-> (. ^Schema @meta-cc get ::Company)
      dbpojo<>
      (dbSetFlds*
        {:cname cname
         :revenue 100.00
         :logo (bytesify "hi")})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkDept
  ""
  [dname]
  (-> (. ^Schema @meta-cc get ::Department)
      dbpojo<>
      (dbSetFld :dname dname)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createEmp
  ""
  [fname lname sex login]
  (let [sql (. ^DbApi @DB compositeSQLr)]
    (->>
      (fn [^SQLr s]
        (let [p (mkPerson fname
                          lname
                          sex)
              e (mkEmp login)
              e (.insert s e)
              p (.insert s p)]
          (dbSetO2O
            {:with s :as :person}
            e
            (.findOne s
                      ::Person
                      {:first_name fname
                       :last_name lname}))))
      (.execWith sql)
      first)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetchAllEmps
  ""
  []
  (-> (. ^DbApi @DB simpleSQLr)
      (.findAll ::Employee)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetch-person
  ""
  [fname lname]
  (-> (. ^DbApi @DB simpleSQLr)
      (.findOne ::Person
                {:first_name fname :last_name lname})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetchEmp
  ""
  [login]
  (-> (. ^DbApi @DB simpleSQLr)
      (.findOne ::Employee {:login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- changeEmp
  ""
  [login]
  (-> (. ^DbApi @DB compositeSQLr)
      (.execWith
        #(let [o2 (-> (. ^SQLr % findOne ::Employee {:login login})
                      (dbSetFlds* {:salary 99.9234 :iq 0}))]
           (if (> (. ^SQLr % update o2) 0) o2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- deleteEmp
  ""
  [login]
  (-> (.compositeSQLr ^DbApi @DB)
      (.execWith
        (fn [^SQLr s]
          (let [o1 (.findOne s ::Employee {:login login} )]
            (.delete s o1)
            (.countAll s ::Employee))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createPerson
  ""
  [fname lname sex]
  (let [p (mkPerson fname lname sex)]
    (-> (.compositeSQLr ^DbApi @DB)
        (.execWith #(.insert ^SQLr % p)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wedlock?
  ""
  []
  (let [sql (.compositeSQLr ^DbApi @DB)
        e (createEmp "joe" "blog" "male" "joeb")
        w (createPerson "mary" "lou" "female")]
    (->>
      (fn [^SQLr s]
        (let
          [pm (dbGetO2O {:with s :as :person} e)
           [p1 w1]
           (dbSetO2O {:as :spouse :with s} pm w)
           [w2 p2]
           (dbSetO2O {:as :spouse :with s} w1 p1)
           w3 (dbGetO2O {:as :spouse :with s} p2)
           p3 (dbGetO2O {:as :spouse :with s} w3)]
          (and (some? w3)(some? p3))))
      (.execWith sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undoWedlock
  ""
  []
  (let [sql (.compositeSQLr ^DbApi @DB)]
    (->>
      (fn [^SQLr s]
        (let
          [e (fetchEmp "joeb")
           pm (dbGetO2O {:with s :as :person} e)
           w (dbGetO2O {:as :spouse :with s } pm)
           p2 (dbClrO2O {:as :spouse :with s } pm)
           w2 (dbClrO2O {:as :spouse :with s } w)
           w3 (dbGetO2O {:as :spouse :with s } p2)
           p3 (dbGetO2O {:as :spouse :with s } w2)]
          (and (nil? w3)(nil? p3)(some? w))))
      (.execWith sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testCompany
  ""
  []
  (let [sql (.compositeSQLr ^DbApi @DB)]
    (->>
      (fn [^SQLr s]
        (let [c (.insert s (mkCompany "acme"))
              _ (dbSetO2M
                  {:as :depts :with s }
                  c
                  (.insert s (mkDept "d1")))
              _ (dbSetO2M*
                  {:as :depts :with s }
                  c
                  (.insert s (mkDept "d2"))
                  (.insert s (mkDept "d3")))
              _ (dbSetO2M*
                  {:as :emps :with s }
                  c
                  (.insert s (mkEmp "e1" ))
                  (.insert s (mkEmp "e2" ))
                  (.insert s (mkEmp "e3" )))
              ds (dbGetO2M  {:as :depts :with s} c)
              es (dbGetO2M  {:as :emps :with s} c)]
          (and (= (count ds) 3)
               (= (count es) 3))))
      (.execWith sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testM2M
  ""
  []
  (let [sql (.compositeSQLr ^DbApi @DB)]
    (->>
      (fn [^SQLr s]
        (let
          [c (.findOne s ::Company {:cname "acme"})
           ds (dbGetO2M {:as :depts :with s} c)
           es (dbGetO2M {:as :emps :with s} c)
           _
           (doseq [d ds
                   :when (= (:dname d) "d2")]
             (doseq [e es]
               (dbSetM2M {:joined ::EmpDepts :with s} d e)))
           _
           (doseq [e es
                   :when (= (:login e) "e2")]
             (doseq [d ds
                     :let [dn (:dname d)]
                     :when (not= dn "d2")]
               (dbSetM2M {:joined ::EmpDepts :with s} e d)))
           s1 (dbGetM2M
                {:joined ::EmpDepts :with s}
                (some #(if (= (:dname %) "d2") %)  ds))
           s2 (dbGetM2M
                {:joined ::EmpDepts :with s}
                (some #(if (= (:login %) "e2") %)  es))]
          (and (== (count s1) 3)
               (== (count s2) 3))))
      (.execWith sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undoM2M
  ""
  []
  (let [sql (.compositeSQLr ^DbApi @DB)]
    (->>
      (fn [^SQLr s]
        (let
          [d2 (.findOne s ::Department {:dname "d2"})
           e2 (.findOne s ::Employee {:login "e2"})
           _ (dbClrM2M {:joined ::EmpDepts :with s} d2)
           _ (dbClrM2M {:joined ::EmpDepts :with s} e2)
           s1 (dbGetM2M {:joined ::EmpDepts :with s} d2)
           s2 (dbGetM2M {:joined ::EmpDepts :with s} e2)]
          (and (== (count s1) 0)
               (== (count s2) 0))))
      (.execWith sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- undoCompany
  ""
  []
  (let [sql (.compositeSQLr ^DbApi @DB)]
    (->>
      (fn [^SQLr s]
        (let
          [c (.findOne s ::Company {:cname "acme"})
           _ (dbClrO2M {:as :depts :with s} c)
           _ (dbClrO2M {:as :emps :with s} c)
           s1 (dbGetO2M {:as :depts :with s} c)
           s2 (dbGetO2M {:as :emps :with s} c)]
          (and (== (count s1) 0)
               (== (count s2) 0))))
      (.execWith sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftest czlabtesthorde-test

  (is (do->true (initTest nil)))

  (is (some? (tstamp<>)))

  (testing
    "related to: db api"

    (is (let [db (dbopen<+> @jdbc-spec @meta-cc)
              url (.url ^JdbcInfo @jdbc-spec)
              c (.compositeSQLr db)
              s (.simpleSQLr db)
              h (.schema db)
              v (.vendor db)
              conn (.open db)
              a (fmtSQLId db "hello")
              b (fmtSQLId conn "hello")
              id (dbtag ::Person h)
              t (dbtable ::Person h)
              m (.get ^Schema h ::Person)
              cn (dbcol :iq m)
              ks1 (matchSpec "h2")
              ks2 (matchUrl url)]
          (try
            (and (some? c)
                 (some? s)
                 (ist? Schema h)
                 (map? v)
                 (some? conn)
                 (= a b)
                 (= id ::Person)
                 (= t "Person")
                 (= "iq" cn)
                 (= ks1 ks2))
            (finally
              (.close conn)
              (.finz db)))))

    (is (let [db (dbopen<> @jdbc-spec @meta-cc)
              url (.url ^JdbcInfo @jdbc-spec)
              c (.compositeSQLr db)
              s (.simpleSQLr db)
              h (.schema db)
              v (.vendor db)
              conn (.open db)
              a (fmtSQLId db "hello")
              b (fmtSQLId conn "hello")
              id (dbtag ::Person h)
              t (dbtable ::Person h)
              m (.get ^Schema h ::Person)
              cn (dbcol :iq m)
              ks1 (matchSpec "h2")
              ks2 (matchUrl url)]
          (try
            (and (some? c)
                 (some? s)
                 (ist? Schema h)
                 (map? v)
                 (some? conn)
                 (= a b)
                 (= id ::Person)
                 (= t "Person")
                 (= "iq" cn)
                 (= ks1 ks2))
            (finally
              (.close conn)
              (.finz db)))))

    (is (let [c (dbconnect<> @jdbc-spec)]
          (try
            (map? (loadTableMeta c "Person"))
            (finally (.close c)))))

    (is (testConnect? @jdbc-spec))

    (is (map? (resolveVendor @jdbc-spec)))

    (is (tableExist? @jdbc-spec "Person"))

    (is (let [p (dbpool<> @jdbc-spec)]
          (try
            (tableExist? p "Person")
            (finally
              (.shutdown p))))))

  (testing
    "related to: basic CRUD"

    (is (let [m (createPerson "joe" "blog" "male")
              r (:rowid m)]
          (> r 0)))

    (is (rowExist? @jdbc-spec "Person"))

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


