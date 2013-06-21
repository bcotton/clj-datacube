(defproject clj-datacube "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [clj-time                                               "0.4.4"]
                 [com.urbanairship/datacube "1.2.4-SNAPSHOT" :classifier "hbase0.94.2-cdh4.2.0-hadoopCore2.0.0-mr1-cdh4.2.0-hadoop2.0.0-cdh4.2.0"]
                 [org.apache.hadoop/hadoop-core                          "2.0.0-mr1-cdh4.2.0"]
                 [org.apache.hadoop/hadoop-common                        "2.0.0-cdh4.2.0"]
                 [org.apache.hadoop/hadoop-hdfs                          "2.0.0-cdh4.2.0"]
                 [org.apache.hbase/hbase                                 "0.94.2-cdh4.2.0"]
                 [org.clojure/tools.logging                              "0.2.3"]
]
  :profiles {:dev
             {:dependencies [[midje               "1.5.1"]
                              [org.clojure/clojure "1.4.0"]]}}
  :repositories [["rally"     "http://alm-build.f4tech.com:8080/nexus/content/groups/public"]
                 ["releases"  {:url "http://alm-build:8080/nexus/content/repositories/releases"}]
                 ["snapshots" {:url "http://alm-build:8080/nexus/content/repositories/snapshots" :update :always :snapshots true}]
                 ["cloudera" "https://repository.cloudera.com/content/repositories/releases"]]
  :aot :all
  :main clj_datacube.core
)
