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
        [czlab.xlib.core]
        [czlab.dbddl.drivers]
        [czlab.dbio.connect]
        [czlab.dbio.core]
        [czlab.dbddl.h2]
        [clojure.test])

  (:import
    [java.io File]
    [java.util GregorianCalendar Calendar]
    [czlab.dbio Transactable SQLr Schema DBAPI]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declModel Address
  (declFields
    {:addr1 { :size 200 :null false }
     :addr2 { :size 64}
     :city { :null false}
     :state {:null false}
     :zip {:null false}
     :country {:null false} })
  (declIndexes
    {:i1 #{ :city :state :country }
     :i2 #{ :zip :country }
     :i3 #{ :state }
     :i4 #{ :zip } } ))

(declModel Person
  (declFields
    {:first_name { :null false }
     :last_name { :null false }
     :iq { :domain :Int }
     :bday {:domain :Calendar :null false }
     :sex {:null false} })
  (declIndexes
    {:i1 #{ :first_name :last_name }
     :i2 #{ :bday } } )
  (declRelations
    {:addrs {:kind :O2M :other ::Address :cascade true}
     :spouse {:kind :O2O :other ::Person } }))

(declModel Employee
  (declFields
    {:salary { :domain :Float :null false }
     :passcode { :domain :Password }
     :pic { :domain :Bytes }
     :descr {}
     :login {:null false} })
  (declIndexes {:i1 #{ :login } } )
  (declRelations
    {:person {:kind :O2O :other ::Person } }))

(declModel Department
  (declFields
    {:dname { :null false } })
  (declUniques
    {:u1 #{ :dname }} ))

(declModel Company
  (declFields
    {:revenue { :domain :Double :null false }
     :cname { :null false }
     :logo { :domain :Bytes } })
  (declRelations
    {:depts {:kind :O2M :other ::Department :cascade true}
     :emps {:kind :O2M :other ::Employee :cascade true}
     :hq {:kind :O2O :other ::Address :cascade true}})
  (declUniques
    {:u1 #{ :cname } } ))

(declJoined EmpDepts ::Department ::Employee)

(def METAC (atom nil))
(def JDBC (atom nil))
(def DB (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn init-test "" [f]
  (reset! METAC
    (mkDbSchema Address Person EmpDepts Employee Department Company))
  (let [dir (File. (System/getProperty "java.io.tmpdir"))
        db (str "" (System/currentTimeMillis))
        url (H2Db dir db "sa" "hello")
        jdbc (mkJdbc
               {:driver H2-DRIVER
                :url url
                :user "sa"
                :passwd "hello" })]
    ;;(writeOneFile (File. dir "dbstuff.out") (dbgShowSchema @METAC))
    (reset! JDBC jdbc)
    (uploadDdl jdbc (getDDL @METAC :h2))
    (reset! DB (dbioConnectViaPool jdbc @METAC )))
  ;;(println "\n\n" (dbgShowSchema @METAC))
  (when (fn? f) (f)))

(defn finz-test "" [] (.finz @DB) true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mkEmp

  ""
  [login]

  (let [emp (-> (.get ^Schema @METAC ::Employee)
                dbioCreateObj)]
    (dbioSetFlds*
      emp
      :salary 1000000.00
      :pic (.getBytes "poo")
      :passcode "secret"
      :desc "idiot"
      :login login)))

(defn- mkCompany

  ""
  [cname]

  (let [c (-> (.get ^Schema @METAC ::Company)
              dbioCreateObj)]
    (dbioSetFlds*
      c
      :cname cname
      :revenue 100.00
      :logo (.getBytes "hi"))))

(defn- mkDept

  ""
  [dname]

  (-> (-> (.get ^Schema @METAC ::Department)
          dbioCreateObj)
      (dbioSetFld :dname dname)))


(defn- create-emp

  ""
  [fname lname login]

  (let [sql (.compositeSQLr ^DBAPI @DB)
        obj (mkEmp login)
        e (.execWith
            sql
            #(.insert ^SQLr % obj))]
    (first
      (.execWith
        sql
        #(dbioSetO2O
            {:with %1 :as :person}
            e
            (.findOne ^SQLr %1
                      ::Person
                      {:first_name fname
                       :last_name lname}))))))

(defn- fetch-all-emps

  ""
  []

  (let [sql (.simpleSQLr ^DBAPI @DB)]
    (.findAll sql ::Employee)))


(defn- fetch-person

  ""
  [fname lname]

  (let [sql (.simpleSQLr ^DBAPI @DB)]
    (.findOne sql
              ::Person
              {:first_name fname :last_name lname})))


(defn- fetch-emp

  ""
  [login]

  (let [sql (.simpleSQLr ^DBAPI @DB)]
    (.findOne sql
              ::Employee
              {:login login})))

(defn- change-emp

  ""
  [login]

  (let [sql (.compositeSQLr ^DBAPI @DB)]
    (.execWith
      sql
      #(let [o2 (-> (.findOne ^SQLr %1
                          ::Employee {:login login})
                    (dbioSetFlds* :salary 99.9234 :iq 0))]
         (if (> (.update ^SQLr %1 o2) 0) o2 nil)))))

(defn- delete-emp

  ""
  [login]

  (let [sql (.compositeSQLr ^DBAPI @DB)]
    (.execWith
      sql
      #(let [o1 (.findOne ^SQLr %1
                          ::Employee {:login login} )]
         (.delete ^SQLr %1 o1)))
    (.execWith
      sql
      #(.countAll ^SQLr %1 ::Employee))))


(defn- create-person

  ""
  [fname lname sex]

  (let [p (-> (-> (.get ^Schema @METAC ::Person)
                  dbioCreateObj)
              (dbioSetFlds*
                :first_name fname
                :last_name  lname
                :iq 100
                :bday (GregorianCalendar.)
                :sex sex))
        sql (.compositeSQLr ^DBAPI @DB)]
    (.execWith sql
               #(.insert ^SQLr %1 p))))

(defn- wedlock

  ""
  []

  (let [sql (.compositeSQLr ^DBAPI @DB)
        e (create-emp "joe" "blog" "joeb")
        pm (.execWith sql
                      #(dbioGetO2O {:with %1 :as :person} e))
        w (create-person "mary" "lou" "female")
        [p1 w1]
        (.execWith sql
                   #(dbioSetO2O {:as :spouse :with %1 } pm w))
        [w2 p2]
        (.execWith sql
                   #(dbioSetO2O {:as :spouse :with %1 } w1 p1))
        w3
        (.execWith sql
                   #(dbioGetO2O {:as :spouse
                                 :with %1 } p2))
        p3
        (.execWith sql
                   #(dbioGetO2O {:as :spouse
                                 :with %1 } w3)) ]
    (and (some? w3)(some? p3))))


(defn- undo-wedlock

  ""
  []

  (let [sql (.compositeSQLr ^DBAPI @DB)
        e (fetch-emp "joeb")
        pm (.execWith
             sql
             #(dbioGetO2O {:with %1 :as :person} e))
        w (.execWith
            sql
            #(dbioGetO2O {:as :spouse
                          :with %1 } pm))
        p2 (.execWith
             sql
             #(dbioClrO2O {:as :spouse
                           :with %1 } pm))
        w2 (.execWith
             sql
             #(dbioClrO2O {:as :spouse
                           :with %1 } w))
        w3 (.execWith
             sql
             #(dbioGetO2O {:as :spouse
                           :with %1 } p2))
        p3 (.execWith
             sql
             #(dbioGetO2O {:as :spouse
                           :with %1 } w2))]
    (and (nil? w3)(nil? p3)(some? w))))

(defn- test-company

  ""
  []

  (let [sql (.compositeSQLr ^DBAPI @DB)
        c (.execWith
            sql
            #(.insert ^SQLr %1 (mkCompany "acme")))]
    (.execWith
      sql
      #(dbioSetO2M {:as :depts :with %1 }
                   c
                   (.insert ^SQLr %1 (mkDept "d1"))))
    (.execWith
      sql
      #(dbioSetO2M* {:as :depts :with %1 }
                    c
                   (.insert ^SQLr %1 (mkDept "d2"))
                   (.insert ^SQLr %1 (mkDept "d3"))))
    (.execWith
      sql
      #(dbioSetO2M* {:as :emps :with %1 }
                    c
                    (.insert ^SQLr % (mkEmp "e1" ))
                    (.insert ^SQLr % (mkEmp "e2" ))
                    (.insert ^SQLr % (mkEmp "e3" )) ))
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

  (let [sql (.compositeSQLr ^DBAPI @DB)
        c (.execWith
            sql
            #(.findOne ^SQLr %1
                       ::Company {:cname "acme"} ))
        ds (.execWith
             sql
             #(dbioGetO2M {:as :depts :with %1} c))
        es (.execWith
             sql
             #(dbioGetO2M {:as :emps :with %1} c))]
    (.execWith
      sql
      #(do
         (doseq [d ds
                 :when (= (:dname d) "d2")]
           (doseq [e es]
             (dbioSetM2M {:joined ::EmpDepts :with %1} d e)))
         (doseq [e es
                 :when (= (:login e) "e2")]
           (doseq [d ds
                   :let [dn (:dname d)]
                   :when (not= dn "d2")]
             (dbioSetM2M {:joined ::EmpDepts :with %1} e d)))))
    (let [s1 (.execWith
               sql
               (fn [s]
                 (dbioGetM2M
                   {:joined ::EmpDepts :with s}
                   (some #(if (= (:dname %) "d2") % nil)  ds))))
          s2 (.execWith
               sql
               (fn [s]
                 (dbioGetM2M
                   {:joined ::EmpDepts :with s}
                   (some #(if (= (:login %) "e2") % nil)  es))))]
      (and (== (count s1) 3)
           (== (count s2) 3)) )))

(defn- undo-m2m

  ""
  []

  (let [sql (.compositeSQLr ^DBAPI @DB)
        d2 (.execWith
             sql
             #(.findOne ^SQLr % ::Department {:dname "d2"} ))
        e2 (.execWith
             sql
             #(.findOne ^SQLr % ::Employee {:login "e2"} ))]
    (.execWith
      sql
      #(dbioClrM2M {:joined ::EmpDepts :with % } d2))
    (.execWith
      sql
      #(dbioClrM2M {:joined ::EmpDepts :with % } e2))
    (let [s1 (.execWith
               sql
               #(dbioGetM2M {:joined ::EmpDepts :with %} d2))
          s2 (.execWith
               sql
               #(dbioGetM2M {:joined ::EmpDepts :with %} e2))]
      (and (== (count s1) 0)
           (== (count s2) 0)) )))

(defn- undo-company

  ""
  []

  (let [sql (.compositeSQLr ^DBAPI @DB)
        c (.execWith
            sql
            #(.findOne ^SQLr % ::Company {:cname "acme"} ))]
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

  (is (let [m (create-person "joe" "blog" "male")
            r (:rowid m)]
        (> r 0)))

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
  (is (wedlock))
  (is (undo-wedlock))

  ;; one to many assocs
  (is (test-company))

  (is (test-m2m))

  ;; m to m assocs
  (is (undo-m2m))

  (is (undo-company))

  (is (finz-test))
)

;;(use-fixtures :each init-test)
;;(clojure.test/run-tests 'czlabtest.dbio.dbstuff)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


