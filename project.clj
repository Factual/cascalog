(def shared
  '[[org.clojure/tools.macro "0.1.1"]
    [jgrapht "0.8.3"]
    [org.clojure/tools.macro "0.1.1"]
    [factual/cascading-hadoop "2.0.5-wip-dev"]
    [factual/cascading-core "2.0.5-wip-dev"]
    [factual/cascading-local "2.0.5-wip-dev"]
    [cascading.kryo "0.4.7"]
    [cascalog/carbonite "1.3.0"]
    [log4j/log4j "1.2.16"]
    [hadoop-util "0.2.8"]
    [com.twitter/maple "0.2.0"]
    [jackknife "0.1.2"]])

(defproject factual/cascalog "1.10.8-SNAPSHOT"
  :description "Hadoop without the Hassle."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "Cascalog user mailing list"
                 :archive "https://groups.google.com/d/forum/cascalog-user"
                 :post "cascalog-user@googlegroups.com"}
  :min-lein-version "2.0.0"
  :jvm-opts ["-Xmx768m" "-server" "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :codox {:include [cascalog.vars cascalog.ops cascalog.io cascalog.api]}
  :repositories #_{"conjars" "http://conjars.org/repo/"}
                {"releases" {:url "http://maven.corp.factual.com/nexus/content/repositories/releases"}
                 "snapshots" {:url "http://maven.corp.factual.com/nexus/content/repositories/snapshots"}
                 "public" {:url "http://maven.corp.factual.com/nexus/content/groups/public/"}}
  :aliases {"all" ["with-profile" "dev:1.2,dev:1.3"]}
  :dependencies ~(conj shared '[org.clojure/clojure "1.4.0"])
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]
  :profiles {:all {:dependencies ~shared}
             :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
             :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                    [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]}})
