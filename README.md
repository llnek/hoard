# horde

[![Build Status](https://travis-ci.org/llnek/horde.svg?branch=master)](https://travis-ci.org/llnek/horde)

`horde` is a light weight object-relationl mapping framework.

## Installation

Add the following dependency to your `project.clj` file:

    [io.czlab/horde "1.0.0"]

## Documentation

* [API Docs](https://llnek.github.io/horde/)

## Supported Databases

Currently supports

+ Postgresql
+ SQL Server
+ Oracle
+ H2

## Data Types

+ :Timestamp
+ :Calendar
+ :Date
+ :Boolean
+ :Password
+ :String
+ :Double
+ :Long
+ :Int
+ :Float
+ :Bytes


## Modeling

Defining a model in `horde` is very similar to defining a SQL table, with
the additional capability of defining relations.  Each model must have
an unique identifier, follow by a set of field definitions, indexes and
associations.  All models have a built-in primary key defined to be a
database autoincrement field of type long.

For example, to model a address object:

```clojure
(ns demo.app
  (:require [czlab.horde.dbio.core :as hc]))

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

```

The convention is to define models as a group.  To do that, use the
macro dbschema<>.

```clojure
(ns demo.app
  (:require [czlab.horde.dbio.core]))

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
         :bday {:domain :Calendar :null? true}
         :sex {:null? true} })
      (dbindexes
        {:i1 #{ :first_name :last_name }
         :i2 #{ :bday } })
      (dbassocs
        ;a person can have [0..n] Address objects
        ;that is, a one-to-many relation with Address
        {:spouse {:kind :o2o :other ::Person }
         :addrs {:kind :o2m :other ::Address :cascade? true} })))

```

## Connections

Connections to the underlying database can be done in 2 ways

+ pooled connections
+ raw connections

(dbopen<+> jdbc-info schema)
or
(dbopen<> jdbc-info schema)

## SQL DDL

To extract the ddl from your schema, use (getDdl db-flavor).  
For example, (getDDL :h2) will return the DDL as String.

To upload the ddl to the underlying database, use (uploadDdl ...)

## Basic CRUD operations

```clojure
(def schemaObj (dbschema<> ...))
(def db (dbopen<> ... schemaObj))

(let [person-model (. schemaObj get ::Person)

      ;create a person object
      joe (-> (dbpojo<> person-model)
              (dbSetFlds*
                {:first_name "Joe" :last_name  "Blogg" :age 21}))

      ;insert into db returning a fresh object
      ;joe2 is the persisted version, has primary field set
      joe2 (-> (. db simpleSQLr) (.insert joe))

      ;update joe
      joe3 (dbSetFld joe2 :sex "male")
      joe4 (-> (. db simpleSQLr) (.update joe3))

      ;query for joe
      joe5 (-> (. db simpleSQLr)
               (.findOne ::Person {:first_name "Joe" :last_name "Blogg"}))
      ;got joe from db
      ;delete joe
      _ (-> (. db simpleSQLr) (.delete joe5))]
  (println "no more joe"))

```

## Relations

`horde` supports the 3 classic relations:

+ many to many
  + e.g. a teacher can have many students and a student can have many teachers.
+ one to many
  + e.g. a person can own many cars.
+ one to one
  + e.g. a person can have one spouse.

### Many to Many

```clojure
(def schemaObj (dbschema<> (dbmodel<> ::Teacher ...)
                           (dbmodel<> ::Student ...)
                           (dbjoined<> ::t-and-s ::Teacher ::Student)))
(def db (dbopen<> ... schemaObj))

  ;let's assume we have some teachers T1, T2, T3 and 
  ;students S7, S8, S9 in the database

  (let [sql (. db compositeSQL)
        sim (. db simpleSQL)]

    (->> (fn [tx]
           (dbSetM2M {:joined ::t-and-s :with tx} T1 S7)
           (dbSetM2M {:joined ::t-and-s :with tx} S8 T1)
           (dbSetM2M {:joined ::t-and-s :with tx} S8 T2)
         (.execSQL sql ))

    ;let's check the db
    ;returns 2 teachers (T1, T2)
    (dbGetM2M {:joined ::t-and-s :with sim} S8)
    ;returns 2 students (S7, S8)
    (dbGetM2M {:joined ::t-and-s :with sim} T1)

    ;school closes, clear all
    (->> (fn [tx]
           (dbClrM2M {:joined ::t-and-s :with tx} T1)
           (dbClrM2M {:joined ::t-and-s :with tx} S8)
         (.execSQL sql )))

```

### One to Many

```clojure
(def schemaObj (dbschema<> ...))
(def db (dbopen<> ... schemaObj))

  ;following previous examples,
  ;we have joe in the database

  (let [sql (. db compositeSQL)]

    (->> (fn [tx]
           (dbSetO2M 
             {:as :addrs :with tx}
             joe
             (.insert tx (create-a-address-object)))
           (dbSetO2M 
             {:as :addrs :with tx}
             joe
             (.insert tx (create-another-address-object))))
         (.execSQL sql ))

    ;let's check the db
    ;returns all the addresses
    (dbGetO2M {:as :addrs :with sql} joe)

    ;joe moves and clears all addresses
    (->> (fn [tx]
           (dbClrO2M {:as :addrs :with tx} joe))
         (.execSQL sql )))

```

### One to One

```clojure
(def schemaObj (dbschema<> ...))
(def db (dbopen<> ... schemaObj))

  ;following previous examples,
  ;we have joe and another person mary in the db
  (let [sql (. db compositeSQL)]

    ;joe and mary gets married
    (->> (fn [tx]
           (dbSetO2O {:as :spouse :with tx} joe mary)
           (dbSetO2O {:as :spouse :with tx} mary joe))
         (.execSQL sql ))

    ;let's check the db
    ;returns joe
    (dbGetO2O {:as :spouse :with sql} mary)
    ;return mary
    (dbGetO2O {:as :spouse :with sql} joe)

    ;joe and mary separates
    (->> (fn [tx]
           (dbClrO2O {:as :spouse :with tx} joe)
           (dbClrO2O {:as :spouse :with tx} mary))
         (.execSQL sql ))

```



## Contacting me / contributions

Please use the project's [GitHub issues page] for all questions, ideas, etc. **Pull requests welcome**. See the project's [GitHub contributors page] for a list of contributors.

## License

Copyright © 2013-2017 Kenneth Leung

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

<!--- links (repos) -->
[CHANGELOG]: https://github.com/llnek/horde/releases
[GitHub issues page]: https://github.com/llnek/horde/issues
[GitHub contributors page]: https://github.com/llnek/horde/graphs/contributors




