(ns clj-datacube.tweet_test
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clj-datacube.core :as cube]
            [clojure.string :as string :only [split]])
  (:import [org.joda.time DateTime])
  (:gen-class))

(defn- parse-tweet [line]
  (let [[_ time username text] (string/split line #"\t")
        date-time (tf/parse (:rfc822 tf/formatters) time)
        hash-tags (set (map last (re-seq #"#(\w+)" text)))
        retweeted-from (last (re-find #"RT @(\w+)" text))]
    {:time date-time :username username :hash-tags hash-tags :rt-from retweeted-from :text text}))


(defn -main []

  (cube/defcube tweets-cube :long
    (cube/time-dimension :time)
    (cube/string-dimension :retweeted-from)
    (cube/string-dimension :user)
    (cube/tags-dimension :tags)
    (cube/rollup)
    (cube/rollup :user)
    (cube/rollup :user :time cube/day-bucket)
    (cube/rollup :retweeted-from)
    (cube/rollup :user :retweeted-from)
    (cube/rollup :tags)
    (cube/rollup :tags :time cube/hour-bucket))


  (with-open [in-file (io/reader "dev-resources/tweets_25bahman.csv")]
    (doseq [line (line-seq in-file)]
      (let [tweet (parse-tweet line)]

        (cube/write-value tweets-cube 1
                          (cube/at :time (:time tweet))
                          (cube/at :user (:username tweet))
                          (cube/at :retweeted-from (or (:rt-from tweet) ""))
                          (cube/at :tags (:hash-tags tweet))))))

  (println "Total tweets: " 
           (cube/read-value tweets-cube))
  (println "Tweets by baraneshgh: "
           (cube/read-value tweets-cube 
                            (cube/at :user "baraneshgh")))
  (println "Tweets retweeting IranTube: "
           (cube/read-value tweets-cube 
                            (cube/at :retweeted-from "IranTube")))
  (println "Retweets of omidhabibinia by DominiqueRdr: " 
           (cube/read-value tweets-cube 
                            (cube/at :retweeted-from "omidhabibinia")
                            (cube/at :user "DominiqueRdr")))
  (println "Uses of hashtag #iran: " 
           (cube/read-value tweets-cube (cube/at :tags "iran")))
  (println "Uses of hashtag #iran during 2011-02-10T15:00Z: "
           (cube/read-value tweets-cube 
                            (cube/at :tags "iran") 
                            (cube/at :time cube/hour-bucket (t/date-time 2011, 2, 10, 15, 0, 0, 0))))
)
