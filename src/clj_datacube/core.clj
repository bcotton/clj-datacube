(ns clj-datacube.core
  (:import [java.util.concurrent ConcurrentHashMap]
           [com.urbanairship.datacube Dimension DimensionAndBucketType BucketType Rollup DataCube DataCubeIo SyncLevel DbHarness DbHarness$CommitType WriteBuilder ReadBuilder]
           [com.urbanairship.datacube.bucketers StringToBytesBucketer HourDayMonthBucketer BigEndianIntBucketer BigEndianLongBucketer BooleanBucketer MinutePeriodBucketer SecondPeriodBucketer]
           [com.urbanairship.datacube.idservices HBaseIdService MapIdService CachingIdService]
           [com.urbanairship.datacube.dbharnesses MapDbHarness HBaseDbHarness]
           [com.urbanairship.datacube.ops LongOp IntOp DoubleOp]
           [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.client HTablePool])
  (:require [clj-time.core :as time])
  (:use [clojure.string :only [join]])
  (:gen-class))

(def read-combine-cas DbHarness$CommitType/READ_COMBINE_CAS)

(def full-sync-level  SyncLevel/FULL_SYNC)
(def batch-sync-level SyncLevel/BATCH_SYNC)
(def batch-async-level SyncLevel/BATCH_ASYNC)

(def long-deserializer LongOp/DESERIALIZER)
(def int-deserializer IntOp/DESERIALIZER)
(def double-deserializer DoubleOp/DESERIALIZER)

(defn map-id-service 
  ([]
     (map-id-service "cache" 10))
  ([name size] 
     (CachingIdService. size (MapIdService.) name)))

(defn map-db-harness 
  ([commit-type deserializer]
     (MapDbHarness. (ConcurrentHashMap.) 
                    deserializer commit-type (map-id-service)))
  ([deserializer]
     (map-db-harness read-combine-cas deserializer)))

(defn hbase-configuration [zookeeper-connect]
  (doto (HBaseConfiguration/create) 
    (.set "hbase.zookeeper.quorum" zookeeper-connect)))

(defn hbase-id-service [zookeeper-connect]
  (CachingIdService. 500 (HBaseIdService.
                           (hbase-configuration zookeeper-connect)
                           (.getBytes "idlookup")
                           (.getBytes "idcounter")
                           (.getBytes "c")
                           (.getBytes "spans"))
                     "cache"))

(defn hbase-db-harness 
  ([commit-type deserializer zookeeper-connect]
     (println "Creating HBase DB Harness")
     (HBaseDbHarness. 
      (HTablePool. (hbase-configuration zookeeper-connect) 10) 
      (.getBytes "spans")
      (.getBytes "spans") 
      (.getBytes "spans") 
      deserializer 
      (hbase-id-service zookeeper-connect) 
      commit-type))
  ([deserializer zookeeper-connect]
     (hbase-db-harness read-combine-cas deserializer zookeeper-connect)))


;;
;; Dimensions
;;

;; (def string-dimension-2 [cube key] (dimension cube key (string-dimension (name key))) )
(defn string-dimension 
  "Create a dimension suitable for storing String values."
  ([name]
     (string-dimension name 5))
  ([name size]
     (Dimension. name (StringToBytesBucketer.) true size)))

(defn time-dimension 
  "Create a dimension that buckets Years, Months, Weeks Days and Hours."
  ([name]
     (time-dimension name 8))
  ([name size]
     (Dimension. name (HourDayMonthBucketer.) false size)))

(def year-bucket HourDayMonthBucketer/years)
(def month-bucket HourDayMonthBucketer/months)
(def day-bucket HourDayMonthBucketer/days)
(def week-bucker HourDayMonthBucketer/weeks)
(def hour-bucket HourDayMonthBucketer/hours)

(defn int-dimension 
  "Create a dimension for by Integers"
  [name] (Dimension. name (BigEndianIntBucketer.) false 8))

(defn long-dimension 
  "Create a dimension for Longs"
  [name] (Dimension. name (BigEndianLongBucketer.) false 8))

(defn boolean-dimension
  "Create a dimension for booleans"
  [name] (Dimension. name (BooleanBucketer.) false 1))

;; 
(defn- make-dim-and-bucket 
  ([dim bucket-type] 
     (DimensionAndBucketType. dim bucket-type))
  ([dim]
     (make-dim-and-bucket dim BucketType/IDENTITY)))

(defn- dim [cube name]
  (get (:dimensions cube) name))

(defn- parse-rollup-spec [cube rollup-spec]
  (let [dimensions (:dimensions cube)] 
    (loop [spec             rollup-spec
           dims-and-buckets #{}]
      (let [dimension (dim cube (first spec))
            bucket-type (second spec)]
        (cond
         (empty? spec) dims-and-buckets
         (nil? dimension) (throw (IllegalArgumentException. 
                                  (str "Problem creating Rollups. Dimension " (first spec) " not found in this cube. Known dimensions are " (join " " (keys dimensions)))))
         (instance? BucketType bucket-type) (recur (rest (rest spec)) 
                                                   (conj dims-and-buckets 
                                                         (make-dim-and-bucket dimension bucket-type)))
         :default (recur (rest spec) (conj dims-and-buckets
                                        (make-dim-and-bucket dimension))))))))

;;
;; Cube DSL
;; 
(defn dimension 
  [cube key dim]
  (-> cube 
      (assoc-in [:dimensions key] dim)
      (assoc :dimension-list  (conj (get cube :dimension-list []) key))))

(defn rollup 
  ([cube & spec] 
     (let [r (Rollup. (parse-rollup-spec cube spec))] 
       (assoc cube :rollups (conj (:rollups cube) r))))
  ([cube]
     (assoc cube :rollups (conj (:rollups cube) (Rollup. #{})))))

;;
;; Read and Write
;;
(defn- write-builder [cube dims-and-values]
  (let [builder (WriteBuilder. (:cube cube))]
    (doseq [[dim-name _ coordinate] dims-and-values]
      (.at builder (dim cube dim-name) coordinate))
    builder))

(defn- read-builder [cube dims-and-values]
  (let [builder (ReadBuilder. (:cube cube))]
    (doseq [[dim-name bucket-type coordinate] dims-and-values]
      (.at builder (dim cube dim-name) bucket-type coordinate))
    builder))

(defn at
  "Sugar for wrapping dimension and buckets."
  ([dim coordinate] (at dim BucketType/IDENTITY coordinate))
  ([dim ^BucketType bucket-type coordinate] (vector dim bucket-type coordinate)))

(defn- cube-op [cube value]
  (let [measure-type (:measure-type cube)]
    (case measure-type
      :long (LongOp. value)
      :int (IntOp. value)
      :double (DoubleOp. value)
      (throw (IllegalArgumentException. (str "Unknown measure type " measure-type))))))

(defn- write-io [cube value builder]
  (if (:sync-level cube full-sync-level)
    (.writeSync (:cubeio cube) (cube-op cube value) builder)    
    (.writeAsync (:cubeio cube) (cube-op cube value) builder)))

(defn- get-io [cube  builder]
  (let [value (.get (:cubeio cube) builder)
        measure-type (:measure-type cube)]
      (if (.isPresent value)
        (case measure-type
          :long (.getLong (.get value))
          :int (.getInt (.get value))
          :double (.getDouble (.get value)))
        0)))

(defn write-value [cube value & dims-and-values]
  (write-io cube value (write-builder cube dims-and-values)))

(defn read-value [cube & dims-and-values]
  (get-io cube (read-builder cube dims-and-values)))

(defmacro defcube 
  [cube-name measure-type db-harness batch-size flush-interval sync-level & body]
  `(let [cube# (-> {}
                   ~@body)
         data-cube# (DataCube. (for [dim# (:dimension-list cube#)] ;; Dimensions are ordered
                                 (dim# (:dimensions cube#)))
                               (:rollups cube#))
         cubeio# (DataCubeIo. data-cube# ~db-harness ~batch-size ~flush-interval ~sync-level)
         cube# (-> cube#
                   (assoc :cube data-cube#)
                   (assoc :cubeio cubeio#)
                   (assoc :sync-level ~sync-level)
                   (assoc :measure-type ~measure-type))]
     (def ~(symbol cube-name) cube#)))



(defn -main []
   (defcube alm-cube 
     :long (hbase-db-harness long-deserializer "bld-hadoop-06")
     10 1000 full-sync-level

     (dimension :host (string-dimension "host"))
     (dimension :measure (string-dimension "measure"))
     (dimension :five-minute (Dimension. "minute" (MinutePeriodBucketer. 5) false 8))
     (dimension :five-second (Dimension. "second" (SecondPeriodBucketer. 5) false 8))
     (rollup :measure :host)
     (rollup :measure :host :five-minute)
     (rollup :measure :host :five-second))
   
   (println (read-value alm-cube 
                (at :host "qs-app-01.rally.prod") 
                (at :measure "javaRequestSpan.heapAllocated"))))


(comment

(defn foo [& args]
  (let [aps (partition-all 2 args)
        [opts-and-vals ps] (split-with #(keyword? (first %)) aps)
        options (into {} (map vec opts-and-vals))
        positionals (reduce into [] ps)]
    [options positionals]))

)


