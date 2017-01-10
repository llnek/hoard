;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defproject io.czlab/horde "0.1.0"

  :description "sql orm"
  :url "https://github.com/llnek/horde"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.postgresql/postgresql "9.4.1212.jre7"]
                 [com.zaxxer/HikariCP "2.5.1"]
                 [com.h2database/h2 "1.4.193"]
                 [io.czlab/xlib "0.1.0"]]

  :plugins [[lein-codox "0.10.2"]
            [lein-czlab "0.1.0"]]
  :hooks [leiningen.lein-czlab]

  :profiles {:provided {:dependencies
                        [[net.mikera/cljunit "0.6.0" :scope "test"]
                         [junit/junit "4.12" :scope "test"]
                         [org.clojure/clojure "1.8.0" :scope "provided"]
                         [codox/codox "0.10.2" :scope "provided"]]}
             :uberjar {:aot :all}}

  :global-vars {*warn-on-reflection* true}
  :target-path "out/%s"
  :aot :all

  ;;:jar-exclusions [#"(?:^|/).svn/"]
  :root-package "czlab"
  :omit-source true

  :java-source-paths ["src/main/java" "src/test/java"]
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :resource-paths ["src/main/resources"]

  :jvm-opts ["-Dlog4j.configurationFile=file:attic/log4j2.xml"]
  :javac-options ["-source" "8"
                  "-Xlint:unchecked" "-Xlint:-options" "-Xlint:deprecation"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


