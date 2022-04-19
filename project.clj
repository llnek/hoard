;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defproject io.czlab/hoard "2.1.0"

  :license {:url "https://www.apache.org/licenses/LICENSE-2.0.txt"
            :name "Apache License"}

  :description "SQL orm"
  :url "https://github.com/llnek/hoard"

  :dependencies [[org.postgresql/postgresql "42.3.4"]
                 [com.zaxxer/HikariCP "5.0.1"]
                 [com.h2database/h2 "2.1.212"]
                 [io.czlab/basal "2.1.0"]]

  :plugins [[cider/cider-nrepl "0.28.3"]
            [lein-codox "0.10.8"]
            [lein-cljsbuild "1.1.8"]]

  :profiles {:provided {:dependencies
                        [[org.clojure/clojure "1.11.1" :scope "provided"]]}
             :uberjar {:aot :all}}

  :aliases {"tcp-stop" ["trampoline" "run" "-m" "czlab.hoard.h2db.server"
                       "-tcpPassword" "admin123"
                       "-trace"
                       ;"-tcpShutdownForce"
                       "-tcpShutdown" "tcp://localhost:9092"]
            "web-run" ["trampoline" "run" "-m" "czlab.hoard.h2db.server"
                      "-blocking"
                      "-trace"
                      "-web"
                      "-browser"
                      ;"-webAllowOthers"
                      "-webDaemon"
                      ;"-webSSL"
                      "-webPort" "8082"
                      "-webAdminPassword" "admin123"]
            "tcp-run" ["trampoline" "run" "-m" "czlab.hoard.h2db.server"
                      "-blocking"
                      "-trace"
                      "-user" "sa"
                      "-passwd" "admin123"
                      "-dbid" "poop"
                      "-tcp"
                      "-tcpPassword" "admin123"
                      ;"-tcpAllowOthers"
                      "-tcpDaemon"
                      ;"-tcpSSL"
                      "-tcpPort" "9092"
                      "-baseDir" "/tmp"]}

  :test-selectors {:core :test-core}

  :global-vars {*warn-on-reflection* true}
  :target-path "out/%s"
  :aot :all

  :coordinate! "czlab"
  :omit-source true

  :java-source-paths ["src/main/java" "src/test/java"]
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]

  :jvm-opts ["-Dlog4j.configurationFile=file:attic/log4j2.xml"]
  :javac-options ["-source" "16"
                  "-target" "16"
                  "-Xlint:unchecked" "-Xlint:-options" "-Xlint:deprecation"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


