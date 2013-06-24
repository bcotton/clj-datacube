(ns clj-datacube.core
  (:import [java.util.concurrent ConcurrentHashMap]
           [com.urbanairship.datacube Dimension DimensionAndBucketType BucketType Rollup DataCube DataCubeIo SyncLevel DbHarness DbHarness$CommitType WriteBuilder ReadBuilder]
           [com.urbanairship.datacube.bucketers StringToBytesBucketer HourDayMonthBucketer BigEndianIntBucketer BigEndianLongBucketer BooleanBucketer TagsBucketer MinutePeriodBucketer SecondPeriodBucketer]
           [com.urbanairship.datacube.idservices HBaseIdService MapIdService CachingIdService]
           [com.urbanairship.datacube.dbharnesses MapDbHarness HBaseDbHarness]
           [com.urbanairship.datacube.ops LongOp IntOp DoubleOp]
           [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.client HTablePool])
  (:require [clj-time.core  :as time]
            [clojure.pprint :as pprint])
  (:use [clojure.string :only [join]])
  (:gen-class))

(declare dimension)

(def read-combine-cas-commit-type DbHarness$CommitType/READ_COMBINE_CAS)
(def increment-commit-type DbHarness$CommitType/INCREMENT)
(def overwirte-commit-type DbHarness$CommitType/OVERWRITE)

(def full-sync-level  SyncLevel/FULL_SYNC)
(def batch-sync-level SyncLevel/BATCH_SYNC)
(def batch-async-level SyncLevel/BATCH_ASYNC)

(def long-deserializer LongOp/DESERIALIZER)
(def int-deserializer IntOp/DESERIALIZER)
(def double-deserializer DoubleOp/DESERIALIZER)
(def deserializers {:long long-deserializer :int int-deserializer :double double-deserializer})

(defn map-id-service 
  "Create an in-memory Map-backed IdService. Useful for testing.
   By default will cache up to 10 items."
  ([]
     (map-id-service "cache" 10))
  ([name size] 
     (CachingIdService. size (MapIdService.) name)))

(defn map-db-harness 
  "Create an in-memory Map-backed DbHarness. Useful for testing.
   By default will use a read-combine-cas commit type."
  ([commit-type deserializer]
     (MapDbHarness. (ConcurrentHashMap.) 
                    deserializer commit-type (map-id-service)))
  ([deserializer]
     (map-db-harness read-combine-cas-commit-type deserializer)))

(defn hbase-configuration
  "Create an BHaseCOnfiguration with one value, the location of the zokeeper quorum."
  [zookeeper-connect]
  (doto (HBaseConfiguration/create) 
    (.set "hbase.zookeeper.quorum" zookeeper-connect)))

(defn hbase-id-service
  "Create a HBase backed IdService.

  Optional Parameters:
    :lookup-table s  - The name of the lookup table. Defaults to 'idlookup'
    :counter-table s - The name of the counter table. Defaults to 'idcounter'
    :cf s            - The name of the column family. Defaults to 'c'
    :cube-name c     - A uniqe cube name for the cube being used. Defaults to 'cube'
"
  [zookeeper-connect & { :keys [lookup-table counter-table cf cube-name]
                        :or {lookup-table "idlookup" counter-table "idcounter" cf "c" cube-name "cube"}}]
  (CachingIdService. 500 (HBaseIdService.
                           (hbase-configuration zookeeper-connect)
                           (.getBytes lookup-table)
                           (.getBytes counter-table)
                           (.getBytes cf)
                           (.getBytes cube-name))
                     "cache"))

(defn hbase-db-harness 
  ([commit-type deserializer zookeeper-connect id-service & {:keys [htable-pool-size
                                                                    cube-name 
                                                                    table-name 
                                                                    cf 
                                                                    flush-threads-count
                                                                    ioe-retry-count
                                                                    cas-retry-count
                                                                    metric-scope]
                                                             :or {htable-pool-size 10
                                                                  cube-name "cube"
                                                                  table-name "datacube"
                                                                  cf "c"
                                                                  flush-threads-count 5
                                                                  ioe-retry-count 5
                                                                  cas-retry-count 5
                                                                  metric-scope nil}}]
     (HBaseDbHarness. 
      (HTablePool. (hbase-configuration zookeeper-connect) htable-pool-size) 
      (.getBytes cube-name)
      (.getBytes table-name) 
      (.getBytes cf) 
      deserializer 
      id-service
      commit-type
      flush-threads-count
      ioe-retry-count
      cas-retry-count
      metric-scope))
  ([commit-type deserializer zookeeper-connect]
     (hbase-db-harness commit-type deserializer zookeeper-connect (hbase-id-service zookeeper-connect)))
  ([deserializer zookeeper-connect]
     (hbase-db-harness read-combine-cas-commit-type deserializer zookeeper-connect)))


;;
;; Dimensions
;;
(defn string-dimension
  "Create a dimension suitable for storing String values.
   Default to using 5 bytes to store all mapped values of this dimension."
  ([cube key]
     (string-dimension cube key 5))
  ([cube key size]
     (dimension cube key (Dimension. (name key) (StringToBytesBucketer.) true size))))

(defn time-dimension 
  "Create a dimension that buckets Years, Months, Weeks Days and Hours."
  ([cube key]
     (time-dimension cube key 8))
  ([cube key size]
     (dimension cube key (Dimension. (name key) (HourDayMonthBucketer.) false size))))

(def year-bucket HourDayMonthBucketer/years)
(def month-bucket HourDayMonthBucketer/months)
(def week-bucker HourDayMonthBucketer/weeks)
(def day-bucket HourDayMonthBucketer/days)
(def hour-bucket HourDayMonthBucketer/hours)

(defn int-dimension 
  "Create a dimension for Integers"
  [cube key] (dimension cube key (Dimension. (name key) (BigEndianIntBucketer.) false 4)))

(defn long-dimension 
  "Create a dimension for Longs"
  [cube key] (dimension cube key (Dimension. (name key) (BigEndianLongBucketer.) false 8)))

(defn boolean-dimension
  "Create a dimension for Booleans"
  [cube key] (dimension cube key (Dimension. (name key) (BooleanBucketer.) false 1)))

(defn tags-dimension
  "Create a dimension suitable for storing String tag values.
   Default to using 5 bytes to store all mapped values of this dimension."
  ([cube key]
     (tags-dimension cube key 5))
  ([cube key size]
     (dimension cube key (Dimension. (name key) (TagsBucketer.) true size))))

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
;; Cube Creation DSL
;; 
(defn dimension 
  "Pass a dimension to the cube. Use this function when creating
   dimensions that are not included in the pacage, or you have a 
   custom Bucketer.

   Designed for use within defcube.

  Arguments: 
     - a keyword to refer to the dimension in future calls.
     - an instance of com.urbanairship.datacube.Dimension."
  [cube key dim]
  (-> cube 
      (assoc-in [:dimensions key] dim)
      (assoc :dimension-list  (conj (get cube :dimension-list []) key))))

(defn rollup 
  "Define a rollup for this cube. A rollup is a combination of
   zero or more dimensions and buckets for which a value will be
   tracked.

   A rollup take a list of keywords that refer to dimensions already
   added to the cube. Each dimension reference may be followed by 
   an optional BucketType reference."
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

(defmulti cube-op (fn [cube _] (:measure-type cube)))
(defmethod cube-op :long [cube value] (LongOp. value))
(defmethod cube-op :int [cube value] (IntOp. value))
(defmethod cube-op :double [cube value] (DoubleOp. value))

(defn- write-io [cube value builder]
  (if (:sync-level cube full-sync-level)
    (.writeSync (:cubeio cube) (cube-op cube value) builder)    
    (.writeAsync (:cubeio cube) (cube-op cube value) builder)))

(defmulti unwrap-value (fn [cube _] (:measure-type cube)))
(defmethod unwrap-value :long [cube value] (.getLong (.get value)))
(defmethod unwrap-value :int [cube value] (.getInt (.get value)))
(defmethod unwrap-value :double [cube value] (.getDouble (.get value)))

(defn- get-io [cube  builder]
  (let [value (.get (:cubeio cube) builder)]
    (if (.isPresent value)
      (unwrap-value cube value)
      0)))

(defn at
  "Sugar for wrapping dimension and buckets.
  Pass a Dimension and the value, or a Dimsnion, the bucket-type and the value."
  ([dim coordinate] (at dim BucketType/IDENTITY coordinate))
  ([dim ^BucketType bucket-type coordinate] (vector dim bucket-type coordinate)))

(defn write-value 
  "Write the cube's value at a list of coordinates."
  [cube value & dims-and-values]
  (write-io cube value (write-builder cube dims-and-values)))

(defn read-value 
  "Read the cube's value at a list of coordinates."
  [cube & dims-and-values]
  (get-io cube (read-builder cube dims-and-values)))

(defn- extract-options [args]
  (let [pairs (partition-all 2 args)
        [options ps] (split-with #(keyword? (first %)) pairs)
        options (into {} (map vec options))
        body (reduce into [] ps)]
    [options body]))

(defmacro defcube
  "Define a DataCube with a set of dimensions and rollups.
  Cubes read and write values of a particular type. Supported types are
  :long, :int and :double.

  Cubes must have at least one rollup, which will accululate all the values.

  Example of a minimal cibe
    (defcube cube :long (rollup))
"
  [cube-name measure-type & options-and-body]
  (let [[options body] (extract-options options-and-body)
        db-harness     (or (:db-harness options) 
                           `(map-db-harness (~measure-type deserializers)))
        batch-size     (get options :batch-size 0)
        flush-interval (get options :flush-interval 1000)
        sync-level     (get options :sync-level 'clj-datacube.core/full-sync-level)]
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
       (def ~(symbol cube-name) cube#))))
