# clj-datacube

A Clojure wrapper for [Urban Airship's DataCube](https://github.com/urbanairship/datacube).

## Artifacts

For now, you should clone and build it.

```sh
lein package
```

## Most Recent

With leiningen:

``` clj
  [clj-datacube 0.1.0-SNAPSHOT]
```
  
With maven:

``` xml
  <dependency>
    <groupId>clj-datacube</groupId>
    <artifactId>clj-datacube</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>
```

## Bugs and Enhancements

Bug reports and pull requests at [the official clj-datacube repo on Github](https://github.com/bcotton/clj-datacube).

## Usage

``` clj
(use 'clj-datacube.core)
```

### Defining a cube

Cubes are declared using the defcube macro. A cube has a type (Intger, Long or Double) and a collection of dimensions. Rollups are declared to be some aspect of a combination of dimensions that needs to be counted.

``` clj
  (defcube my-cube :long 
    (string-dimension :name)
    (time-dimension :time)
    (rollup)
    (rollup :name)
    (rollup :name :time day-bucket))

  (write-value my-cube 102)
  (write-value my-cube 100 (at :name "name"))
  (write-value my-cube 100 (at :name "name"))
  (write-value my-cube 105 (at :name "name2") (at :time day-bucket (time/date-time 2013 06 02)))
```

### Built-in dimensions (and their Bucketers)

clj-datacube wrapps all the included Bucketers into a convient syntax for commonly used scenarios. The follwing Bucketers are supported through these functions:

 - `(string-dimension)` - StringToBytesBucketer
 - `(time-dimension)` - HourDayMonthBucketer. Buckets are provided as `year-bucket` `month-bucket` `day-bucket` `week-bucket` `hour-bucket`
 - `(int-dimension)` - BigEndianIntBucketer
 - `(long-dimension)` - BigEndianLongBucketer
 - `(boolean-dimension)` - BooleanBucketer
 - `(tags-dimension)` - Tags Bucketer
 
 Custom bucketers may be reify'ed and passed to the cube using the `(dimension)` function.
 
### Rollups

Rollups are how the cube decides which combination of Dimensions (and buckets) to track values for. You use the `(rollup)` function and pass in a list of dimension keywords and optionally a BucketType for each dimension.

### Reading and Writing

Use the `(write-value)` function to insert values to the cube. `read-value` takes a cube, and value to be applied and a list of coodinates, constructed with the `at` function. For example, to write a value for a user at a particular time you may say:

```clj
 (defcube cube ....)
 (write-value cube 10000 (at :user "bob") (:at :time hour-bucket (t/now)))
```

The rollups will automatically place the value in the correct location based on any bucket types that you pass to write-value.

Use the `(read-value)` function to extract values from the cube. The call is similar to write-value:

```clj
  (read-value cube (:at user "bob") (:at :time hour-bucket (t/now)))
```

The buckets used in the definition of the rollups must match those used for `(read-value)`

## Complete Example

```clj
  (defcube tweets-cube :long
    (time-dimension :time)
    (string-dimension :retweeted-from)
    (string-dimension :user)
    (tags-dimension :tags)
    (rollup)
    (rollup :user)
    (rollup :user :time day-bucket)
    (rollup :retweeted-from)
    (rollup :user :retweeted-from)
    (rollup :tags)
    (rollup :tags :time hour-bucket))


  (with-open [in-file (io/reader "dev-resources/tweets_25bahman.csv")]
    (doseq [line (line-seq in-file)]
      (let [tweet (parse-tweet line)]

        (write-value tweets-cube 1
                          (at :time (:time tweet))
                          (at :user (:username tweet))
                          (at :retweeted-from (or (:rt-from tweet) ""))
                          (at :tags (:hash-tags tweet))))))

  (println "Total tweets: " 
           (read-value tweets-cube))
  (println "Tweets by baraneshgh: "
           (read-value tweets-cube 
                            (at :user "baraneshgh")))
  (println "Tweets retweeting IranTube: "
           (read-value tweets-cube 
                            (at :retweeted-from "IranTube")))
  (println "Retweets of omidhabibinia by DominiqueRdr: " 
           (read-value tweets-cube 
                            (at :retweeted-from "omidhabibinia")
                            (at :user "DominiqueRdr")))
  (println "Uses of hashtag #iran: " 
           (read-value tweets-cube (at :tags "iran")))
  (println "Uses of hashtag #iran during 2011-02-10T15:00Z: "
           (read-value tweets-cube 
                            (at :tags "iran") 
                            (at :time hour-bucket (t/date-time 2011, 2, 10, 15, 0, 0, 0))))

```
  

## License

Copyright © 2013 Bob Cotton

Distributed under the MIT License
