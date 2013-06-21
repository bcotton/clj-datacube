# clj-datacube

A Clojure wrapper around Urban Airship's DataCube.

## Artifacts

`clj-datacube` is release to clojars.

## Most Recent

With leiningen:

``` clj
  [clj-datacube 1.0.0]
```
  
With maven:

``` xml
  <dependency>
    <groupId>clj-datacube</groupId>
    <artifactId>clj-datacube</artifactId>
    <version>1.0.0</version>
  </dependency>
```

## Bugs and Enhancements

Bug reports and pull requests at [the official clj-datacube repo on Github](https://github.com/bcotton/clj-datacube).

## Usage

``` clj
(import 'clj-datacube)
```

### Defining a cube

``` clj
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
```
  

## License

Copyright © 2013 Bob Cotton

Distributed under the MIT License
