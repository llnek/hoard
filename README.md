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

  (hc/dbmodel<> ::Address
    (hc/dbfields
      {:addr1 {:size 200 :null? false}
       :addr2 {:size 64}
       :city {:null? false}
       :state {:null? false}
       :zip {:null? false}
       :country {:null? false}})
    (hc/dbindexes
      {:i1 #{:city :state :country }
       :i2 #{:zip :country }
       :i3 #{:state }
       :i4 #{:zip } }))

```

The convention is to define models as a group.  To do that, use the
macro dbschema<>.

```clojure
(ns demo.app
  (:require [czlab.horde.dbio.core :as hc]))

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
        ;a person can have [0..n] Address objects
        ;that is, a one-to-many relation with Address
        {:addrs {:kind :o2m :other ::Address :cascade? true} })))

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


## Relations



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




