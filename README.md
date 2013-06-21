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
(defcube tweet-cube :long (map-db-harness long-deserializer) 10 1000 full-sync-level
  (dimension 
```
  

## License

Copyright © 2013 Bob Cotton

Distributed under the Eclipse Public License, the same as Clojure.
