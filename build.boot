(set-env!

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :description ""
  :url "https://github.com/llnek/dbio"

  :dependencies '[

    [commons-dbutils/commons-dbutils "1.6" ]
    [com.jolbox/bonecp "0.8.0.RELEASE" ]
    [com.google.guava/guava "19.0" ]

    [org.clojure/clojure "1.8.0" ]

    [czlab/czlab-crypto "0.9.0-SNAPSHOT" ]
    [czlab/czlab-xlib "0.9.0-SNAPSHOT" ]
  ]

  :source-paths #{"src/main/clojure" "src/main/java"}
  :test-runner "czlabtest.dbio.ClojureJUnit"
  :version "0.9.0-SNAPSHOT"
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
  '[czlab.tpcl.antlib :as a]
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
(defn- cljDbio ""

  [& args]

  (a/cleanDir (fp! (ge :czzDir) "czlab/dbddl"))
  (a/cleanDir (fp! (ge :czzDir) "czlab/dbio"))
  (let [t1 (a/antJava
              (ge :CLJC_OPTS)
              (concat [[:argvalues (b/listCljNsps
                                     (fp! (ge :srcDir) "clojure")
                                     "czlab/dbio"
                                     "czlab/dbddl")]]
                      (ge :CJNESTED)))
        t2 (a/antCopy
             {:todir (fp! (ge :czzDir) "czlab")}
             [[:fileset {:dir (fp! (ge :srcDir) "clojure/czlab")
                         :excludes "**/*.clj"}]])]
    (->> [t1 t2]
         (a/runTarget "clj/dbio"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- distroInit ""

  [& args]

  (let [root (io/file (ge :packDir))]
    (a/cleanDir root)
    (doseq [d ["dist" "lib" "docs"]]
      (.mkdirs (io/file root d)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- packDocs ""

  [& args]

  (a/cleanDir (fp! (ge :packDir) "docs" "api"))

  (a/runTarget*
    "pack/docs"
    (a/antJavadoc
      {:destdir (fp! (ge :packDir) "docs/api")
       :access "protected"
       :author true
       :nodeprecated false
       :nodeprecatedlist false
       :noindex false
       :nonavbar false
       :notree false
       :source "1.8"
       :splitindex true
       :use true
       :version true}
       [[:fileset {:dir (fp! (ge :srcDir) "java")
                   :includes "**/*.java"}]
        [:classpath (ge :CPATH) ]])

    (a/antJava
      (ge :CLJC_OPTS)
      (concat [[:argvalues ["czlab.tpcl.codox"]]]
              (ge :CJNESTED_RAW)))

    (a/antJava
      {:classname "czlab.tpcl.codox"
       :fork true
       :failonerror true}
      [[:argvalues [(ge :basedir)
                    (fp! (ge :srcDir) "clojure")
                    (fp! (ge :packDir) "docs/api")]]
       [:classpath (ge :CJPATH) ]]) ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- packSrc ""

  [& args]

  (a/runTarget*
    "pack/src"
    (a/antCopy
      {:todir (fp! (ge :packDir) "src/main/clojure")}
      [[:fileset {:dir (fp! (ge :srcDir) "clojure")} ]])
    (a/antCopy
      {:todir (fp! (ge :packDir) "src/main/java")}
      [[:fileset {:dir (fp! (ge :srcDir) "java")} ]])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- packLics ""

  [& args]

  (a/runTarget*
    "pack/lics"
    (a/antCopy
      {:todir (ge :packDir)}
      [[:fileset {:dir (ge :basedir)
                  :includes "*.md,*.html,*.txt,LICENSE"}]])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- packDist ""

  [& args]

  (a/runTarget*
    "pack/dist"
    (a/antCopy
      {:todir (fp! (ge :packDir) "dist")}
      [[:fileset {:dir (ge :distDir)
                  :includes "*.jar"}]])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- packLibs ""

  [& args]

  (a/runTarget*
    "pack/lib"
    (a/antCopy
      {:todir (fp! (ge :packDir) "lib")}
      [[:fileset {:dir (ge :libDir)} ]])))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- packAll ""

  [& args]

  (a/runTarget*
    "pack/all"
    (a/antTar
      {:destFile (fp! (ge :distDir)
                      (str (artifactID)
                           "-"
                           (ge :version) ".tar.gz"))
       :compression "gzip"}
      [[:tarfileset {:dir (ge :packDir)
                     :excludes "bin/**"}]
       [:tarfileset {:dir (ge :packDir)
                     :mode "755"
                     :includes "bin/**"}]])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;  task defs below !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask prebuild

  "prepare build environment"
  []

  (bc/with-pre-wrap fileset
    ((comp b/preBuild
           (fn [& x]
             (a/cleanDir (ge :packDir)))
           b/clean4Build))
    fileset))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask javacmp

  "compile java files"
  []

  (bc/with-pre-wrap fileset
    (b/compileJava)
    fileset))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask cljcmp

  "compile clojure files"
  []

  (bc/with-pre-wrap fileset
    (cljDbio)
    fileset))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask jar!

  "jar all classes"
  []

  (bc/with-pre-wrap fileset
    (doseq [f (seq (output-files fileset))]
      (let [dir (:dir f)
            pn (:path f)
            tf (io/file (ge :jzzDir) pn)
            pd (.getParentFile tf)]
        (when (.startsWith pn "META-INF")
          (.mkdirs pd)
          (spit tf
                (slurp (fp! dir pn)
                       :encoding "utf-8")
                :encoding "utf-8"))))
    (b/replaceFile
      (fp! (ge :jzzDir)
           "czlab/dbio/version.properties")
      #(cs/replace % "@@pom.version@@" (ge :version)))
    (b/jarFiles)
    fileset))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask dev
  "for dev only"
  []

  (comp (prebuild)
        (b/libjars)
        (javacmp)
        (cljcmp)
        (pom :project (ge :project) :version (ge :version))
        (jar!)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask pack
  "internal use only"
  []

  (bc/with-pre-wrap fileset
    (distroInit)
    (packLics)
    (packSrc)
    (packDist)
    (packLibs)
    ;;(packDocs)
    (packAll)
    fileset))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftask release
  "release bundle"
  []

  (comp (dev)
        (pack)
        (install :file
                 (str (ge :distDir)
                      "/"
                      (artifactID)
                      "-"
                      (ge :version)
                      ".jar"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;(doseq [[k v] (get-env)] (println k "=" v))
;;(doseq [k (sort (keys (get-sys-env)))] (println k))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


