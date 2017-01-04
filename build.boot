(set-env!

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :description ""
  :url "https://github.com/llnek/horde"

  :dependencies '[

    [org.postgresql/postgresql "9.4.1212.jre7" ]
    [com.zaxxer/HikariCP "2.5.1"]
    [com.h2database/h2 "1.4.193" ]

    [org.clojure/clojure "1.8.0"]
    [czlab/czlab-xlib "0.1.0" ]
    [czlab/czlab-pariah "0.1.0" :scope "provided"]
  ]

  :source-paths #{"src/main/clojure" "src/main/java"}
  :test-runner "czlabtest.horde.ClojureJUnit"
  :version "0.1.0"
  :debug true
  :project 'czlab/czlab-horde)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(require '[czlab.pariah.boot :as b :refer [artifactID fp! ge]]
         '[clojure.tools.logging :as log]
         '[clojure.java.io :as io]
         '[clojure.string :as cs]
         '[czlab.pariah.antlib :as a])

(import '[java.io File])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(b/bootEnv!)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;  task defs below !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
;;
(deftask tst

  "for test only"
  []

  (comp (b/testJava)
        (b/testClj)))

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
(deftask rel

  ""
  [d doco bool "Generate doc"]

  (b/toggleDoco doco)
  (comp (dev)
        (b/localInstall)
        (b/packDistro)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


