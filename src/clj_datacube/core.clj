(ns clj-datacube.core
  (:import [java.util.concurrent ConcurrentHashMap]
           [com.urbanairship.datacube Dimension DimensionAndBucketType BucketType Rollup DataCube DataCubeIo SyncLevel DbHarness DbHarness$CommitType WriteBuilder ReadBuilder]
           [com.urbanairship.datacube.bucketers StringToBytesBucketer HourDayMonthBucketer]
           [com.urbanairship.datacube.idservices HBaseIdService MapIdService CachingIdService]
           [com.urbanairship.datacube.dbharnesses MapDbHarness HBaseDbHarness]
           [com.urbanairship.datacube.ops LongOp]
           [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.client HTablePool])
  (:use [clojure.string :only [join]]))


(def read-combine-cas DbHarness$CommitType/READ_COMBINE_CAS)

(def full-sync  SyncLevel/FULL_SYNC)
(def batch-sync SyncLevel/BATCH_SYNC)
(def batch-async SyncLevel/BATCH_ASYNC)

(defn map-id-service 
  ([]
     (map-id-service "cache" 10))
  ([name size] 
     (CachingIdService. size (MapIdService.) name)))

(defn map-db-harness 
  ([commit-type]
     (MapDbHarness. (ConcurrentHashMap.) 
                    LongOp/DESERIALIZER commit-type (map-id-service)))
  ([]
     (map-db-harness read-combine-cas)))

(defn hbase-configuration [zookeeper-connect]
  (doto (HBaseConfiguration/create) 
    (.set "hbase.zookeeper.quorum" zookeeper-connect)))

(defn hbase-id-service []
  (CachingIdService. 500 (HBaseIdService.
                           (hbase-configuration)
                           (.getBytes "idlookup")
                           (.getBytes "idcounter")
                           (.getBytes "c")
                           (.getBytes "spans"))
                     "cache"))

(defn hbase-db-harness []
  (println "Creating HBase DB Harness")
  (HBaseDbHarness. 
   (HTablePool. (hbase-configuration) 10) 
   (.getBytes "spans")
   (.getBytes "spans") 
   (.getBytes "spans") 
   LongOp/DESERIALIZER 
   (hbase-id-service) 
   read-combine-cas))

(defn string-dimension 
  "Create a dimension suitable for storing String values."
  ([name]
     (string-dimension name 4))
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

(defn dimension 
  [cube key dim]
  (-> cube 
      (assoc-in [:dimensions key] dim)
      (assoc :dimension-list  (conj (get cube :dimension-list []) key))))

(defn- dim-and-bucket 
  ([dim bucket-type] 
     (DimensionAndBucketType. dim bucket-type))
  ([dim]
     (dim-and-bucket dim BucketType/IDENTITY)))

(defn- make-dims-and-buckets [cube rollup-spec]
  (let [dimensions (:dimensions cube)] 
    (loop [d                rollup-spec
           dims-and-buckets #{}]
      (let [dimension (get dimensions (first d))
            bucket-type (second d)]
        (cond
         (empty? d) dims-and-buckets
         (nil? dimension) (throw (IllegalArgumentException. 
                                  (str "Problem creating Rollups. Dimension " (first d) " not found in this cube. Known dimensions are " (join " " (keys dimensions)))))
         (instance? BucketType bucket-type) (recur (rest (rest d)) 
                                                   (conj dims-and-buckets 
                                                         (dim-and-bucket dimension bucket-type)))
         :default (recur (rest d) (conj dims-and-buckets
                                        (dim-and-bucket dimension))))))))

(defn rollup 
  ([cube dims] 
     (let [dimensions (:dimensions cube)
           r (Rollup. (make-dims-and-buckets cube dims))] 
       (assoc cube :rollups 
              (conj (:rollups cube) r))))
  ([cube]
     (assoc cube :rollups (conj (:rollups cube) (Rollup. #{})))))


(defn write-builder [cube dims-and-values]
  (let [builder (WriteBuilder. (:cube cube))]
    (doseq [[k v] dims-and-values]
      (.at builder k v))
    builder))

(defn read-builder [cube dims-and-values]
  (let [builder (ReadBuilder. (:cube cube))]
    (doseq [[k v] dims-and-values]
      (.at builder k v))
    builder))

(defn- dim [cube name]
  (get (:dimensions cube) name))

(defn write-io [io value builder]
  (.writeSync io (LongOp. value) builder))

(defn get-io [io builder]
  (let [value (.get io builder)]
      (when (.isPresent value)
        (-> (.get value) .getLong))))

(defn write-value [cube value & {:as dims}]
  (let [dims-and-values (reduce (fn [acc [k v]] (assoc acc (dim cube k) v)) {} dims)]
    (write-io (:cubeio cube) value (write-builder cube dims-and-values))))

(defn read-value [cube & {:as dims}]
  (let [dims-and-values (reduce (fn [acc [k v]] (assoc acc (dim cube k) v)) {} dims)] 
    (get-io (:cubeio cube) (read-builder cube dims-and-values))))

(defmacro defcube 
  [cube-name measure-type db-harness batch-size flush-interval sync-level & body]
  `(let [clj-cube# (-> {}
                       ~@body)
         cube# (DataCube. (for [dim# (:dimension-list clj-cube#)]
                            (dim# (:dimensions clj-cube#)))
                          (:rollups clj-cube#))
         cubeio# (DataCubeIo. cube# ~db-harness ~batch-size ~flush-interval ~sync-level)
         cube2# (-> clj-cube#
                   (assoc :cube cube#)
                   (assoc :cubeio cubeio#))]
    (def ~(symbol cube-name) cube2#)))







