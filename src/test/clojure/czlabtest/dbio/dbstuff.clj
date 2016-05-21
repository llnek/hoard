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
        [czlab.dbio.drivers]
        [czlab.dbio.connect]
        [czlab.dbio.core]
        [czlab.dbio.h2]
        [clojure.test])

  (:import
    [org.apache.commons.lang3 StringUtils]
    [czlab.crypto PasswordAPI]
    [java.io File]
    [java.util GregorianCalendar Calendar]
    [czlab.dbio Transactable SQLr MetaCache DBAPI]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defModel Address
  (withFields {
    :addr1 { :size 200 :null false }
    :addr2 { :size 64}
    :city { :null false}
    :state {:null false}
    :zip {:null false}
    :country {:null false}
                   })
  (withIndexes { :i1 [ :city :state :country ]
    :i2 [ :zip :country ]
    :state [ :state ]
    :zip [ :zip ] } ))

(defModel Person
  (withDbFields {
    :first_name { :null false }
    :last_name { :null false }
    :iq { :domain :Int }
    :bday {:domain :Calendar :null false }
    :sex {:null false}
                   })
  (withDbAssocs {
    :spouse { :kind :O2O :other (DbioScopeType "Person") }
                   })
  (withDbIndexes { :i1 [ :first_name :last_name ]
    :i2 [ :bday ]
    } ))

(DefJoined EmpDepts
           (DbioScopeType "Department")
           (DbioScopeType "Employee"))

(DefModel Employee
  (WithDbParentModel  (DbioScopeType "Person"))
  (WithDbFields {
    :salary { :domain :Float :null false }
    :pic { :domain :Bytes }
    :passcode { :domain :Password }
    :descr {}
    :login {:null false}
                   })
  (WithDbAssocs {
    :depts { :kind :M2M :joined (DbioScopeType "EmpDepts") }
                   })
  (WithDbIndexes { :i1 [ :login ]
    } ))

(DefModel Department
  (WithDbFields {
    :dname { :null false }
                   })
  (WithDbAssocs {
    :emps { :kind :M2M :joined (DbioScopeType "EmpDepts") }
                   })
  (WithDbUniques {
    :u1 [ :dname ]
    } ))

(DefModel Company
  (WithDbFields {
    :cname { :null false }
    :revenue { :domain :Double :null false }
    :logo { :domain :Bytes }
                   })
  (WithDbAssocs {
    :depts { :kind :O2M :other (DbioScopeType "Department") }
    :emps { :kind :O2M :other (DbioScopeType "Employee") }
    :hq { :kind :O2O :other (DbioScopeType "Address") }
                   })
  (WithDbUniques {
    :u1 [ :cname ]
    } ))

(def METAC (atom nil))
(def JDBC (atom nil))
(def DB (atom nil))

(defn init-test [f]
  (reset! METAC
    (MetaCache* (DbSchema*
                      [Address Person EmpDepts Employee Department Company])))
  (let [ dir (File. (System/getProperty "java.io.tmpdir"))
         db (str "" (System/currentTimeMillis))
         url (H2Db* dir db "sa" (Pwdify ""))
        jdbc (Jdbc* (juid)
               { :d H2-DRIVER :url url :user "sa" :passwd "" }
               (Pwdify "")) ]
    (WriteOneFile (File. dir "dbstuff.out") (DbgShowMetaCache @METAC))
    (reset! JDBC jdbc)
    (UploadDdl jdbc (GetDDL @METAC :h2))
    (reset! DB (DbioConnect jdbc @METAC {})))
  (if (nil? f) nil (f)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- mkt [t] (keyword (str "testcljc.dbio.dbstuff/" t)))


(defn- mkEmp [fname lname login]
  (let [ emp (DbioCreateObj (mkt "Employee")) ]
    (-> emp
      (DbioSetFld :first_name fname)
      (DbioSetFld :last_name  lname)
      (DbioSetFld :iq 100)
      (DbioSetFld :bday (GregorianCalendar.))
      (DbioSetFld :sex "male")
      (DbioSetFld :salary 1000000.00)
      (DbioSetFld :pic (.getBytes "poo"))
      (DbioSetFld :passcode "secret")
      (DbioSetFld :desc "idiot")
      (DbioSetFld :login login ))))

(defn- create-company [cname]
  (-> (DbioCreateObj :testcljc.dbio.dbstuff/Company)
    (DbioSetFld :cname cname)
    (DbioSetFld :revenue 100.00)
    (DbioSetFld :logo (.getBytes "hi"))))

(defn- create-dept [dname]
  (-> (DbioCreateObj :testcljc.dbio.dbstuff/Department)
    (DbioSetFld :dname dname)))

(defn- create-emp[fname lname login]
  (let [ obj (mkEmp fname lname login)
         ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
         o2 (.execWith sql
             (fn [^SQLr tx]
               (.insert tx obj))) ]
    o2))

(defn- fetch-all-emps []
  (let [ ^SQLr sql (.newSimpleSQLr ^DBAPI @DB)
         o1 (.findAll sql (mkt "Employee")) ]
    o1))

(defn- fetch-emp [login]
  (let [ ^SQLr sql (.newSimpleSQLr ^DBAPI @DB)
         o1 (.findOne sql (mkt "Employee") {:login login} ) ]
    o1))

(defn- change-emp [login]
  (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB) ]
    (.execWith
      sql
      (fn [^SQLr tx]
        (let [ o1 (.findOne tx (mkt "Employee") {:login login} )
               o2 (-> o1 (DbioSetFld :salary 99.9234)
                         (DbioSetFld :iq 0)) ]
          (.update tx o2))))))

(defn- delete-emp [login]
  (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB) ]
    (.execWith
      sql
      (fn [^SQLr tx]
        (let [ o1 (.findOne tx (mkt "Employee") {:login login} ) ]
          (.delete tx o1))))
    (.execWith
      sql
      (fn [^SQLr tx]
        (.countAll tx (mkt "Employee"))))))

(defn- create-person [fname lname]
  (let [ p (-> (DbioCreateObj (mkt "Person"))
                (DbioSetFld :first_name fname)
                (DbioSetFld :last_name  lname)
                (DbioSetFld :iq 100)
                (DbioSetFld :bday (GregorianCalendar.))
                (DbioSetFld :sex "female"))
         ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
         o2 (.execWith sql
             (fn [^SQLr tx]
               (.insert tx p))) ]
    o2))

(defn- wedlock []
    (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
           h (create-emp "joe" "blog" "joeb")
           w (create-person "mary" "lou")
           mc (.getMetaCache ^DBAPI @DB)
           [h1 w1] (.execWith
                     sql
                     (fn [^SQLr tx] (DbioSetO2O {:as :spouse :with tx :cache mc} h w)))
           w2 (.execWith
                sql
                (fn [^SQLr tx] (DbioGetO2O
                           {:as :spouse
                            :cast :testcljc.dbio.dbstuff/Person
                            :cache mc
                            :with tx } h1))) ]
      (and (not (nil? h))
           (not (nil? w))
           (not (nil? w2)))))


(defn- undo-wedlock []
    (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
           mc (.getMetaCache ^DBAPI @DB)
           h (fetch-emp "joeb")
           w (.execWith
               sql
               (fn [^SQLr tx] (DbioGetO2O
                          { :as :spouse
                            :cache mc
                            :with tx
                            :cast :testcljc.dbio.dbstuff/Person } h)))
           h1 (.execWith
                sql
                (fn [^SQLr tx] (DbioClrO2O
                           {:as :spouse
                             :with tx
                             :cache mc
                             :cast :testcljc.dbio.dbstuff/Person } h)))
           w1 (.execWith
                sql
                (fn [^SQLr tx] (DbioGetO2O
                           { :as :spouse
                             :with tx
                             :cache mc
                             :cast :testcljc.dbio.dbstuff/Person } h1))) ]
      (and
        (not (nil? h))
        (not (nil? w))
        (not (nil? h1))
        (nil? w1))))

(defn- test-company []
    (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
           mc (.getMetaCache ^DBAPI @DB)
           c (.execWith
               sql
               (fn [^SQLr tx]
                 (.insert tx (create-company "acme")))) ]
      (.execWith
         sql
         (fn [^SQLr tx]
           (DbioSetO2M {:as :depts :with tx :cache mc}
                             c (.insert tx (create-dept "d1")))))
      (.execWith
         sql
         (fn [^SQLr tx]
           (DbioAddO2M {:as :depts :with tx :cache mc}
                         c
                         [ (.insert tx (create-dept "d2"))
                           (.insert tx (create-dept "d3")) ] )))
      (.execWith
        sql
        (fn [^SQLr tx]
          (DbioAddO2M
            {:as :emps :with tx :cache mc}
            c
            [ (.insert tx (mkEmp "emp1" "ln1" "e1"))
              (.insert tx (mkEmp "emp2" "ln2" "e2"))
              (.insert tx (mkEmp "emp3" "ln3" "e3")) ])))

      (let [ ds (.execWith
                  sql
                  (fn [^SQLr tx] (DbioGetO2M  {:cache mc :as :depts :with tx} c)))
             es (.execWith
                  sql
                  (fn [^SQLr tx] (DbioGetO2M  {:cache mc :as :emps :with tx} c))) ]
        (and (= (count ds) 3)
             (= (count es) 3))) ))

(defn- test-m2m []
    (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
           mc (.getMetaCache ^DBAPI @DB)
           c (.execWith
               sql
               (fn [^SQLr tx]
                 (.findOne tx :testcljc.dbio.dbstuff/Company {:cname "acme"} )))
           ds (.execWith
                sql
                (fn [^SQLr tx] (DbioGetO2M {:cache mc :as :depts :with tx} c)))
           es (.execWith
                sql
                (fn [^SQLr tx] (DbioGetO2M {:cache mc :as :emps :with tx} c))) ]
      (.execWith
        sql
        (fn [^SQLr tx]
          (doseq [ d (seq ds) ]
            (if (= (:dname d) "d2")
              (doseq [ e (seq es) ]
                (DbioSetM2M {:cache mc :as :emps :with tx} d e))))
          (doseq [ e (seq es) ]
            (if (= (:login e) "e2")
              (doseq [ d (seq ds) ]
                (DbioSetM2M {:cache mc :as :depts :with tx} e d)))) ))

      (let [ s1 (.execWith
                  sql
                  (fn [^SQLr tx]
                    (DbioGetM2M
                    {:cache mc :as :emps :with tx}
                    (some (fn [d]
                              (if (= (:dname d) "d2") d nil)) ds) )))
             s2 (.execWith
                  sql
                  (fn [^SQLr tx]
                    (DbioGetM2M
                    {:cache mc :as :depts :with tx}
                    (some (fn [e]
                              (if (= (:login e) "e2") e nil)) es) ))) ]
        (and (== (count s1) 3)
             (== (count s2) 3)) )))

(defn- undo-m2m []
    (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
           mc (.getMetaCache ^DBAPI @DB)
           d2 (.execWith
               sql
               (fn [^SQLr tx]
                 (.findOne tx :testcljc.dbio.dbstuff/Department {:dname "d2"} )))
           e2 (.execWith
               sql
               (fn [^SQLr tx]
                 (.findOne tx :testcljc.dbio.dbstuff/Employee {:login "e2"} ))) ]

      (.execWith
        sql
        (fn [^SQLr tx]
          (DbioClrM2M {:cache mc :as :emps :with tx } d2)))

      (.execWith
        sql
        (fn [^SQLr tx]
          (DbioClrM2M {:cache mc :as :depts :with tx } e2)))

      (let [ s1 (.execWith
                  sql
                  (fn [^SQLr tx]
                    (DbioGetM2M {:cache mc :as :emps :with tx} d2)))

             s2 (.execWith
                  sql
                  (fn [^SQLr tx]
                    (DbioGetM2M {:cache mc :as :depts :with tx} e2))) ]

        (and (== (count s1) 0)
             (== (count s2) 0)) )))


(defn- undo-company []
    (let [ ^Transactable sql (.newCompositeSQLr ^DBAPI @DB)
           mc (.getMetaCache ^DBAPI @DB)
           c (.execWith
               sql
               (fn [^SQLr tx]
                 (.findOne tx :testcljc.dbio.dbstuff/Company {:cname "acme"} ))) ]
      (.execWith
        sql
        (fn [^SQLr tx]
          (DbioClrO2M {:cache mc :as :depts :with tx} c)))

      (.execWith
        sql
        (fn [^SQLr tx]
          (DbioClrO2M {:cache mc :as :emps :with tx} c)))

      (let [ s1 (.execWith
                  sql
                  (fn [^SQLr tx]
                    (DbioGetO2M {:cache mc :as :depts :with tx} c)))
             s2 (.execWith
                  sql
                  (fn [^SQLr tx]
                    (DbioGetO2M {:cache mc :as :emps :with tx} c))) ]

        (and (== (count s1) 0)
             (== (count s2) 0)))))


(deftest testdbio-dbstuff

  (is (do (init-test nil) true))

         ;; basic CRUD
         ;;
  (is (let [ m (meta (create-emp "joe" "blog" "joeb"))
             r (:rowid m)
             v (:verid m) ]
        (and (> r 0) (== v 0))))
  (is (let [ a (fetch-all-emps) ]
        (== (count a) 1)))
  (is (let [ a (fetch-emp "joeb" ) ]
        (not (nil? a))))
  (is (let [ a (change-emp "joeb" ) ]
        (not (nil? a))))
  (is (let [ rc (delete-emp "joeb") ]
        (== rc 0)))
  (is (let [ a (fetch-all-emps) ]
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
;;(clojure.test/run-tests 'testcljc.dbio.dbstuff)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF

