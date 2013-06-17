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

(defn map-id-service [] 
  (CachingIdService. 5 (MapIdService.) "cache"))

(defn map-db-harness []
  (MapDbHarness. (ConcurrentHashMap.) 
                 LongOp/DESERIALIZER read-combine-cas (map-id-service)))

(defn hbase-configuration []
  (doto (HBaseConfiguration/create) 
    (.set "hbase.zookeeper.quorum" "bld-hadoop-06")))

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
  "Create a dimension that buckers the Hour, Day and Month"
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


;; (defmacro write-builder [cube method & ats]
;;   (list* 'doto `(new WriteBuilder  ~cube)
;;     (for [args (filter #(not (empty? %)) ats)]
;;       `(~method ~@args))))

;; (defmacro read-builder [cube method & ats]
;;   (list* 'doto `(new ReadBuilder  ~cube)
;;     (for [args (filter #(not (empty? %)) ats)]
;;       `(~method ~@args))))

;; (defn write-io [io value builder]
;;   (.writeSync io (LongOp. value) builder))

;; (defn get-io [io builder]
;;   (let [value (.get io builder)]
;;       (when (.isPresent value)
;;         (-> (.get value) .getLong))))

;; (defprotocol Cube
;;   (write-value [cube value & dims])
;;   (read-value [cube & dims]))

;; (defrecord CljDataCube [dimensions dimension-list rollups cube cubeio]
;;   Cube
;; (defn write-value [cube value builder]
;;              (let [dimensions (:dimensions cube)
;;                    dims-and-values (map #((vector (get dimensions (first %))) (last %)) 
;;                                         (partition 2 dims))] 
;;                (write-io (:cubeio cube) value (io-builder WriteBuilder (:cube cube) .at [dims-and-values]))))
;; (defn read-value [cube & dims]
;;   (let [dimensions (:dimensions cube)
;;         dims-and-values (map #((vector (get dimensions (first %))) (last %)) 
;;                                         (partition 2 dims))] 
;;     (get-io (:cubeio cube) (io-builder ReadBuilder (:cube cube) .at [dims-and-values]))))

(defmacro defcube 
  [cube-name measure-type db-harness batch-size flush-interval sync-level & body]
  `(let [clj-cube# (-> {}
                       ~@body)
         cube# (DataCube. (vals (:dimensions clj-cube#)) (:rollups clj-cube#))
         cubeio# (DataCubeIo. cube# ~db-harness ~batch-size ~flush-interval ~sync-level)
         cube2# (-> clj-cube#
                   (assoc :cube cube#)
                   (assoc :cubeio cubeio#))]
    (def ~(symbol cube-name) cube2#)))







