(set-env!

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :description ""
  :url "https://github.com/llnek/dbio"

  :dependencies '[

    [org.postgresql/postgresql "9.4.1208.jre7" ]
    [com.h2database/h2 "1.4.191" ]

    [commons-dbutils/commons-dbutils "1.6" ]
    [com.jolbox/bonecp "0.8.0.RELEASE" ]
    [com.google.guava/guava "19.0" ]

    [org.clojure/clojure "1.8.0" ]

    [czlab/czlab-crypto "1.0.0" ]
    [czlab/czlab-xlib "1.0.0" ]


    [codox/codox "0.9.5" :scope "provided"]
    ;; boot/clj stuff
    ;;[boot/base "2.6.0" :scope "provided"]
    ;;[boot/core "2.6.0" :scope "provided"]
    ;;[boot/pod "2.6.0" :scope "provided"]
    [boot/worker "2.6.0" :scope "provided"]
    ;; this is causing the RELEASE_6 warning
    ;;[boot/aether "2.6.0" :scope "provided"]


  ]

  :source-paths #{"src/main/clojure" "src/main/java"}
  :test-runner "czlabtest.dbio.ClojureJUnit"
  :version "1.0.0"
  :debug true
  :project 'czlab/czlab-dbio)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(require
  '[boot.task.built-in :refer [pom target]]
  '[czlab.tpcl.boot
    :as b
    :refer [artifactID fp! ge testjava testclj]]
  '[clojure.tools.logging :as log]
  '[clojure.java.io :as io]
  '[clojure.string :as cs]
  '[czlab.xlib.antlib :as a]
  '[boot.pom :as bp]
  '[boot.core :as bc])

(import '[org.apache.tools.ant Project Target Task]
        '[java.io File])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(b/bootEnv!)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;  task defs below !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask dev

  "for dev only"
  []

  (comp (b/initBuild)
        (b/libjars)
        (b/buildr)
        (b/pom!)
        (b/jar!)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask release

  ""
  [d doco bool "Generate doc"]

  (b/toggleDoco doco)
  (comp (dev)
        (b/localInstall)
        (b/packDistro)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


