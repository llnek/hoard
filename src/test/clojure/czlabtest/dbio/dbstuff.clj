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

  czlabtest.dbio.dbstuff

  (:use [czlab.xlib.files :only [writeOneFile]]
        [czlab.crypto.codec]
        [czlab.xlib.core]
        [czlab.dbddl.drivers]
        [czlab.dbio.connect]
        [czlab.dbio.core]
        [czlab.dbddl.h2]
        [clojure.test])

  (:import
    [czlab.crypto PasswordAPI]
    [java.io File]
    [java.util GregorianCalendar Calendar]
    [czlab.dbio Transactable SQLr Schema DBAPI]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defModel Address
  (withFields
    {:addr1 { :size 200 :null false }
     :addr2 { :size 64}
     :city { :null false}
     :state {:null false}
     :zip {:null false}
     :country {:null false} })
  (withIndexes
    {:i1 #{ :city :state :country }
     :i2 #{ :zip :country }
     :state #{ :state }
     :zip #{ :zip } } ))

(defModel Person
  (withFields
    {:first_name { :null false }
     :last_name { :null false }
     :iq { :domain :Int }
     :bday {:domain :Calendar :null false }
     :sex {:null false} })
  (withIndexes
    {:i1 #{ :first_name :last_name }
     :i2 #{ :bday } } )
  (withO2O :spouse ::Person))

(defO2O Spouse ::Person ::Person)

(defModel Employee
  (withParent ::Person)
  (withFields
    {:salary { :domain :Float :null false }
     :passcode { :domain :Password }
     :pic { :domain :Bytes }
     :descr {}
     :login {:null false} })
  (withIndexes { :i1 #{ :login } } ))

(defModel Department
  (withFields
    {:dname { :null false } })
  (withUniques
    {:u1 #{ :dname }} ))

(defModel Company
  (withFields
    {:revenue { :domain :Double :null false }
     :cname { :null false }
     :logo { :domain :Bytes } })
  (withO2M :depts ::Department)
  (withO2M :emps ::Employee)
  (withO2O :hq ::Address)
  (withUniques
    {:u1 #{ :cname } } ))

(defO2M Depts ::Company ::Department true)
(defO2M Emps ::Company ::Employee true)
(defO2O HQ ::Company ::Address true)

(defJoined EmpDepts ::Department ::Employee)

(def METAC (atom nil))
(def JDBC (atom nil))
(def DB (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn init-test [f]
  (reset! METAC
    (mkDbSchema Address Person EmpDepts Employee Department Company))
  (let [dir (File. (System/getProperty "java.io.tmpdir"))
        db (str "" (System/currentTimeMillis))
        url (H2Db dir db "sa" (pwdify "hello"))
        jdbc (mkJdbc
               {:driver H2-DRIVER
                :url url
                :user "sa"
                :passwd "hello" })]
    (writeOneFile (File. dir "dbstuff.out") (dbgShowSchema @METAC))
    (reset! JDBC jdbc)
    (uploadDdl jdbc (getDDL @METAC :h2))
    (reset! DB (dbioConnect jdbc @METAC {})))
  (when (fn? f) (f)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkt "" [t] (keyword (str "czlabtest.dbio.dbstuff/" t)))

(defn- mkEmp

  ""
  [fname lname login]

  (let [emp (-> (.get ^Schema @METAC (mkt "Employee"))
                 dbioCreateObj)]
    (dbioSetFlds*
      emp
      :first_name fname
      :last_name  lname
      :iq 100
      :bday (GregorianCalendar.)
      :sex "male"
      :salary 1000000.00
      :pic (.getBytes "poo")
      :passcode "secret"
      :desc "idiot"
      :login login)))

(defn- create-company

  ""
  [cname]

  (let [c (-> (.get ^Schema @METAC (mkt "Company"))
              (dbioCreateObj))]
    (dbioSetFlds*
      c
      :cname cname
      :revenue 100.00
      :logo (.getBytes "hi"))))

(defn- create-dept

  ""
  [dname]

  (-> (-> (.get ^Schema @METAC (mkt "Department"))
          (dbioCreateObj))
      (dbioSetFld :dname dname)))

(defn- create-emp

  ""
  [fname lname login]

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        obj (mkEmp fname lname login)]
    (.execWith sql
               #(.insert ^SQLr %1 obj))))

(defn- fetch-all-emps

  ""
  []

  (let [sql (.newSimpleSQLr ^DBAPI @DB)]
    (.findAll sql (mkt "Employee"))))

(defn- fetch-emp

  ""
  [login]

  (let [sql (.newSimpleSQLr ^DBAPI @DB)]
    (.findOne sql
              (mkt "Employee")
              {:login login})))

(defn- change-emp

  ""
  [login]

  (let [sql (.newCompositeSQLr ^DBAPI @DB)]
    (.execWith
      sql
      #(let [o1 (.findOne ^SQLr %1
                          (mkt "Employee") {:login login})]
         (.update ^SQLr %1
                  (dbioSetFlds* o1
                                :salary 99.9234 :iq 0))))))

(defn- delete-emp

  ""
  [login]

  (let [sql (.newCompositeSQLr ^DBAPI @DB)]
    (.execWith
      sql
      #(let [o1 (.findOne ^SQLr %1
                          (mkt "Employee") {:login login} )]
         (.delete ^SQLr %1 o1)))
    (.execWith
      sql
      #(.countAll ^SQLr %1 (mkt "Employee")))))

(defn- create-person

  ""
  [fname lname]

  (let [p (-> (-> (.get ^Schema @METAC (mkt "Person"))
                  (dbioCreateObj))
              (dbioSetFlds*
                :first_name fname
                :last_name  lname
                :iq 100
                :bday (GregorianCalendar.)
                :sex "female"))
        sql (.newCompositeSQLr ^DBAPI @DB)]
    (.execWith sql
               #(.insert ^SQLr %1 p))))

(defn- wedlock

  ""
  []

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        h (create-emp "joe" "blog" "joeb")
        w (create-person "mary" "lou")
        [h1 w1]
        (.execWith sql
                   #(dbioSetO2O {:as :spouse :with %1 } h w))
        w2
        (.execWith sql
                   #(dbioGetO2O {:as :spouse
                                 :cast (mkt "Person")
                                 :with %1 } h1))]
    (and (not (nil? h))
         (not (nil? w))
         (not (nil? w1))
         (not (nil? w2)))))

(defn- undo-wedlock

  ""
  []

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        h (fetch-emp "joeb")
        w (.execWith
            sql
            #(dbioGetO2O {:as :spouse
                          :with %1
                          :cast (mkt "Person")} h))
        h1 (.execWith
             sql
             #(dbioClrO2O {:as :spouse
                           :with %1
                           :cast (mkt "Person") } h))
        w1 (.execWith
             sql
             #(dbioGetO2O {:as :spouse
                           :with %1
                           :cast (mkt "Person")} h1))]
    (and (not (nil? h))
         (not (nil? w))
         (not (nil? h1))
         (nil? w1))))

(defn- test-company

  ""
  []

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        c (.execWith
            sql
            #(.insert ^SQLr %1 (create-company "acme")))]
    (.execWith
      sql
      #(dbioSetO2M {:as :depts :with %1 }
                   c
                   (.insert ^SQLr %1 (create-dept "d1"))))
    (.execWith
      sql
      #(dbioSetO2M* {:as :depts :with %1 }
                    c
                   (.insert ^SQLr %1 (create-dept "d2"))
                   (.insert ^SQLr %1 (create-dept "d3"))))
    (.execWith
      sql
      #(dbioSetO2M* {:as :emps :with %1 }
                    c
                    (.insert ^SQLr % (mkEmp "emp1" "ln1" "e1"))
                    (.insert ^SQLr % (mkEmp "emp2" "ln2" "e2"))
                    (.insert ^SQLr % (mkEmp "emp3" "ln3" "e3")) ))
    (let [ds (.execWith
               sql
               #(dbioGetO2M  {:as :depts :with %1} c))
          es (.execWith
               sql
               #(dbioGetO2M  {:as :emps :with %1} c))]
      (and (= (count ds) 3)
           (= (count es) 3))) ))

(defn- test-m2m

  ""
  []

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        c (.execWith
            sql
            #(.findOne ^SQLr %1
                       (mkt "Company") {:cname "acme"} ))
        ds (.execWith
             sql
             #(dbioGetO2M {:as :depts :with %1} c))
        es (.execWith
             sql
             #(dbioGetO2M {:as :emps :with %1} c))]
    (.execWith
      sql
      #(do
         (doseq [d ds]
           (if (= (:dname d) "d2")
             (doseq [e es]
               (dbioSetM2M {:joined (mkt "EmpDepts") :with %1} d e))))
         (doseq [e es]
           (if (= (:login e) "e2")
             (doseq [d ds
                     :let [dn (:dname d)]
                     :when (not= dn "d2")]
               (dbioSetM2M {:joined (mkt "EmpDepts") :with %1} e d))))))

    (let [s1 (.execWith
               sql
               (fn [s]
                 (dbioGetM2M
                   {:as :emps :with s}
                   (some #(if (= (:dname %) "d2") % nil)) ds)))
          s2 (.execWith
               sql
               (fn [s]
                 (dbioGetM2M
                   {:as :depts :with s}
                   (some #(if (= (:login %) "e2") % nil)) es)))]
      (and (== (count s1) 3)
           (== (count s2) 3)) )))

(defn- undo-m2m

  ""
  []

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        d2 (.execWith
             sql
             #(.findOne ^SQLr % (mkt "Department") {:dname "d2"} ))
        e2 (.execWith
             sql
             #(.findOne ^SQLr % (mkt "Employee") {:login "e2"} ))]
    (.execWith
      sql
      #(dbioClrM2M {:joined (mkt "EmpDepts") :with % } d2))
    (.execWith
      sql
      #(dbioClrM2M {:joined (mkt "EmpDepts") :with % } e2))
    (let [s1 (.execWith
               sql
               #(dbioGetM2M {:joined (mkt "EmpDepts") :with %} d2))
          s2 (.execWith
               sql
               #(dbioGetM2M {:joined (mkt "EmpDepts") :with %} e2))]
      (and (== (count s1) 0)
           (== (count s2) 0)) )))

(defn- undo-company

  ""
  []

  (let [sql (.newCompositeSQLr ^DBAPI @DB)
        c (.execWith
            sql
            #(.findOne ^SQLr % (mkt "Company") {:cname "acme"} ))]
    (.execWith
      sql
      #(dbioClrO2M {:as :depts :with %} c))
    (.execWith
      sql
      #(dbioClrO2M {:as :emps :with %} c))
    (let [s1 (.execWith
               sql
               #(dbioGetO2M {:as :depts :with %} c))
          s2 (.execWith
               sql
               #(dbioGetO2M {:as :emps :with %} c))]
      (and (== (count s1) 0)
           (== (count s2) 0)))))

(deftest czlabtestdbio-dbstuff

  (is (do (init-test nil) true))

  ;; basic CRUD
  ;;
  (is (let [m (create-emp "joe" "blog" "joeb")
            r (:rowid m)]
        (> r 0)))

  (is (let [a (fetch-all-emps)]
        (== (count a) 1)))

  (is (let [a (fetch-emp "joeb" ) ]
        (not (nil? a))))

  (is (let [a (change-emp "joeb" ) ]
        (not (nil? a))))

  (is (let [rc (delete-emp "joeb") ]
        (== rc 0)))

  (is (let [a (fetch-all-emps) ]
        (== (count a) 0)))

  ;; one to one assoc
  ;;
  (is (wedlock))
  (is (undo-wedlock))

  ;; one to many assocs
  (is (test-company))

  ;; m to m assocs
  (is (test-m2m))
  (is (undo-m2m))

  (is (undo-company))
)

;;(use-fixtures :each init-test)
;;(clojure.test/run-tests 'czlabtest.dbio.dbstuff)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


