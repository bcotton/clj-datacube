(ns clj-datacube.core_test
  (:use midje.sweet
        clj-datacube.core)
  (:require [clj-time.core :as time])
  (:import [com.urbanairship.datacube Dimension Rollup]))

(facts "first cube"
  (defcube my-cube :long (map-db-harness long-deserializer) 10 1000 full-sync-level

    (dimension :name (string-dimension "name"))
    (dimension :measure (string-dimension "measure"))
    (dimension :time (time-dimension "time" 8))

    (rollup)
    (rollup :name)
    (rollup :name :time day-bucket)
    (rollup :name :measure))

  (write-value my-cube 102)
  (write-value my-cube 100 (at :name "name"))
  (write-value my-cube 100 (at :name "name"))
  (write-value my-cube 104 (at :name "other name") (at :measure "testing"))
  (write-value my-cube 105 (at :name "name2") (at :time (time/date-time 2013 06 02)))

  (fact "Cube contents"
    (count (:dimensions my-cube))                                  => 3
    (every? #(instance? Dimension %) (vals (:dimensions my-cube))) => true
    (:dimension-list my-cube)                                      => [:name :measure :time]
    (count (:rollups my-cube))                                     => 4
    (every? #(instance? Rollup %) (:rollups my-cube))              => true)

  (fact "Reading from no dimensions"
    (read-value my-cube) => 511)

  (fact "Multiple writes to a single dimension"
    (read-value my-cube (at :name "name")) => 200)

  (fact "Reading from multiple dimensions"
    (read-value my-cube (at :name "other name") (at :measure "testing")) => 104)

  (fact "Reading from day partitiond values"
     (read-value my-cube (at :name "name2") (at :time day-bucket (time/date-time 2013 06 01))) => 0
     (read-value my-cube (at :name "name2") (at :time day-bucket (time/date-time 2013 06 02))) => 105
     (read-value my-cube (at :name "name2") (at :time day-bucket (time/date-time 2013 06 03))) => 0
     ))

(facts "Type checking stored values"
  (defcube long-cube :long (map-db-harness long-deserializer) 10 1000 full-sync-level (rollup))
  (defcube int-cube :int (map-db-harness int-deserializer) 10 1000 full-sync-level (rollup))
  (defcube double-cube :double (map-db-harness double-deserializer) 10 1000 full-sync-level (rollup))

  (write-value long-cube 1000)
  (write-value int-cube 1000)
  (write-value double-cube 1000.0)

  (class (read-value long-cube))   => Long
  (class (read-value int-cube))    => Integer
  (class (read-value double-cube)) => Double
  )

(facts "cube rollups on missing dimensions"
  (defcube broken :long (map-db-harness long-deserializer) 10 1000 full-sync-level
    (dimension :name (string-dimension "name"))
    (rollup :unknown))  
  => (throws IllegalArgumentException #"Dimension :unknown not found")
)
