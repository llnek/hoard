;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
;; Copyright (c) 2013-2016, Kenneth Leung. All rights reserved.

(ns

  czlabtest.dbio.core

  (:require [czlab.xlib.io :refer [writeFile]]
            [clojure.java.io :as io]
            [clojure.string :as cs])

  (:use [czlab.dbddl.drivers]
        [czlab.dbio.connect]
        [czlab.xlib.core]
        [czlab.dbio.core]
        [czlab.dbddl.h2]
        [clojure.test])

  (:import [czlab.dbio Transactable JDBCInfo SQLr Schema DBAPI]
           [java.io File]
           [java.util GregorianCalendar Calendar]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def
  ^:private
  METAC
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
(def ^:private JDBC (atom nil))
(def ^:private DB (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn initTest
  ""
  [f]
  (let [url (H2Db (sysTmpDir) (str (now<>)) "sa" "hello")
        jdbc (dbspec<>
               {:driver H2-DRIVER
                :url url
                :user "sa"
                :passwd "hello"})
        ddl (getDDL @METAC :h2)]
    (if false
      (writeFile (io/file (sysTmpDir)
                          "dbtest.out") (dbgShowSchema @METAC)))
    (if false (println "\n\n" ddl))
    (reset! JDBC jdbc)
    (uploadDdl jdbc ddl)
    (reset! DB (dbopen<+> jdbc @METAC ))
    (if false (println "\n\n" (dbgShowSchema @METAC))))
  (if (fn? f) (f)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn finxTest "" [] (do->true (.finx ^DBAPI @DB)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkPerson
  ""
  [fname lname sex]
  (-> (.get ^Schema @METAC ::Person)
      (dbpojo<>)
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
  (-> (.get ^Schema @METAC ::Employee)
      (dbpojo<>)
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
  (-> (.get ^Schema @METAC ::Company)
      (dbpojo<>)
      (dbSetFlds*
        {:cname cname
         :revenue 100.00
         :logo (bytesify "hi")})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkDept
  ""
  [dname]
  (-> (.get ^Schema @METAC ::Department)
      (dbpojo<> )
      (dbSetFld :dname dname)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createEmp
  ""
  [fname lname sex login]
  (let [sql (.compositeSQLr ^DBAPI @DB)]
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
      (first))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetchAllEmps
  ""
  []
  (let [sql (.simpleSQLr ^DBAPI @DB)]
    (.findAll sql ::Employee)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetch-person
  ""
  [fname lname]
  (let [sql (.simpleSQLr ^DBAPI @DB)]
    (.findOne sql
              ::Person
              {:first_name fname :last_name lname})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fetchEmp
  ""
  [login]
  (let [sql (.simpleSQLr ^DBAPI @DB)]
    (.findOne sql
              ::Employee
              {:login login})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- changeEmp
  ""
  [login]
  (-> (.compositeSQLr ^DBAPI @DB)
      (.execWith
        (fn [^SQLr s]
          (let [o2 (-> (.findOne s ::Employee {:login login})
                       (dbSetFlds* {:salary 99.9234 :iq 0}))]
            (if (> (.update s o2) 0) o2 nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- deleteEmp
  ""
  [login]
  (-> (.compositeSQLr ^DBAPI @DB)
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
    (-> (.compositeSQLr ^DBAPI @DB)
        (.execWith #(.insert ^SQLr % p)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wedlock?
  ""
  []
  (let [sql (.compositeSQLr ^DBAPI @DB)
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
  (let [sql (.compositeSQLr ^DBAPI @DB)]
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
  (let [sql (.compositeSQLr ^DBAPI @DB)]
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
  (let [sql (.compositeSQLr ^DBAPI @DB)]
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
  (let [sql (.compositeSQLr ^DBAPI @DB)]
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
  (let [sql (.compositeSQLr ^DBAPI @DB)]
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
(deftest czlabtestdbio-core

  (is (do (initTest nil) true))

  (is (some? (now<ts>)))

  (is (let [db (dbopen<+> @JDBC @METAC)
            url (.url ^JDBCInfo @JDBC)
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
               (inst? Schema h)
               (map? v)
               (some? conn)
               (= a b)
               (= id ::Person)
               (= t "Person")
               (= "iq" cn)
               (= ks1 ks2))
          (finally
            (.close conn)
            (.finx db)))))

  (is (let [db (dbopen<> @JDBC @METAC)
            url (.url ^JDBCInfo @JDBC)
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
               (inst? Schema h)
               (map? v)
               (some? conn)
               (= a b)
               (= id ::Person)
               (= t "Person")
               (= "iq" cn)
               (= ks1 ks2))
          (finally
            (.close conn)
            (.finx db)))))

  (is (let [c (dbconnect<> @JDBC)]
        (try
          (map? (loadTableMeta c "Person"))
          (finally (.close c)))))

  (is (testConnect? @JDBC))

  (is (map? (resolveVendor @JDBC)))

  (is (tableExist? @JDBC "Person"))

  (is (let [p (dbpool<> @JDBC)]
        (try
          (tableExist? p "Person")
          (finally
            (.shutdown p)))))

  ;; basic CRUD
  ;;

  (is (let [m (createPerson "joe" "blog" "male")
            r (:rowid m)]
        (> r 0)))

  (is (rowExist? @JDBC "Person"))

  (is (let [m (createEmp "joe" "blog" "male" "joeb")
            r (:rowid m)]
        (> r 0)))

  (is (let [a (fetchAllEmps)]
        (== (count a) 1)))

  (is (let [a (fetchEmp "joeb" ) ]
        (some? a)))

  (is (let [a (changeEmp "joeb" ) ]
        (some? a)))

  (is (let [rc (deleteEmp "joeb") ]
        (== rc 0)))

  (is (let [a (fetchAllEmps) ]
        (== (count a) 0)))

  ;; one to one assoc
  (is (wedlock?))
  (is (undoWedlock))

  ;; one to many assocs
  (is (testCompany))

  (is (testM2M))

  ;; m to m assocs
  (is (undoM2M))

  (is (undoCompany))

  (is (finxTest))

  (is (string? "That's all folks!")))


;;(use-fixtures :each initTest)
;;(clojure.test/run-tests 'czlabtest.dbio.core)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


