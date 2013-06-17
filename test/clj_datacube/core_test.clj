(ns clj-datacube.core_test
  (:use midje.sweet
        clj-datacube.core)
  (:import [com.urbanairship.datacube Dimension DimensionAndBucketType BucketType Rollup]
           [com.urbanairship.datacube.bucketers StringToBytesBucketer]))


(defcube my-cube :long (map-db-harness) 10 1000 full-sync
  (dimension :name (string-dimension "name"))
  (dimension :measure (string-dimension "test"))
  (dimension :time (time-dimension "time" 8))
  (rollup)
  (rollup [:name :time day-bucket])
  (rollup [:name])
  (rollup [:name :measure]))

(facts
 (fact "Cube contents"
  (count (:dimensions my-cube))                                  => 3
  (every? #(instance? Dimension %) (vals (:dimensions my-cube))) => true
  (:dimension-list my-cube)                                      => [:name :measure :time]

  (count (:rollups my-cube))                                     => 4
  (every? #(instance? Rollup %) (:rollups my-cube))              => true)

 ;; (fact "Writing to no dimensions"
 ;;       (write-value my-cube 101)
 ;;       (read-value my-cube) => 101)
)
