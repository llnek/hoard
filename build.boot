(set-env!

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :description ""
  :url "https://github.com/llnek/crypto"

  :dependencies '[

    [org.clojure/math.numeric-tower "0.0.4" ]
    [org.bouncycastle/bcprov-jdk15on "1.54"]
    [org.bouncycastle/bcmail-jdk15on "1.54"]
    [org.bouncycastle/bcpkix-jdk15on "1.54"]
    [org.jasypt/jasypt "1.9.2" ]
    ;;[org.mindrot/jbcrypt "0.3m" ]

    [org.apache.commons/commons-email "1.4" ]
    [com.sun.mail/javax.mail "1.5.5" ]
    [org.clojure/clojure "1.8.0" ]

    [czlab/czlab-xlib "0.9.0-SNAPSHOT" ]
  ]

  :source-paths #{"src/main/clojure" "src/main/java"}
  :test-runner "czlabtest.crypto.ClojureJUnit"
  :version "0.9.0-SNAPSHOT"
  :debug true
  :project 'czlab/czlab-crypto)

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
(defn- cljCrypto ""

  [& args]

  (a/cleanDir (fp! (ge :czzDir) "czlab/crypto"))
  (let [t1 (a/antJava
              (ge :CLJC_OPTS)
              (concat [[:argvalues (b/listCljNsps
                                     (fp! (ge :srcDir) "clojure")
                                     "czlab/crypto")]]
                      (ge :CJNESTED)))
        t2 (a/antCopy
             {:todir (fp! (ge :czzDir) "czlab/crypto")}
             [[:fileset {:dir (fp! (ge :srcDir) "clojure/czlab/crypto")
                         :excludes "**/*.clj"}]])]
    (->> [t1 t2]
         (a/runTarget "clj/crypto"))))

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
    (cljCrypto)
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
           "czlab/crypto/version.properties")
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


