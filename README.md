# spark-lucenerdd-examples

Usage examples of [spark-lucenerdd](https://github.com/zouzias/spark-lucenerdd).

### Usage (spark-submit)

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd-examples.git
cd spark-lucenerdd-examples
sbt compile assembly
```

Download and extract apache spark under your home directory, update the `spark-submit.sh` script accordingly and run

```
./spark-linkage-*.sh
```

to run the record linkage examples and `./spark-search-capitalts.sh` to run a search example.

### Usage (docker)

Setup docker and  assuming that you have a docker machine named `default`, type

```
./startZeppelin.sh
```
To start an Apache Zeppelin with preloaded notebooks.


### Examples

#### Spatial linkage between countries and capitals

This example loads all countries from a parquet file containing fields "name" and "shape" (shape is mostly polygons in WKT)

```scala
val allCountries = spark.read.parquet("data/spatial/countries-poly.parquet")
```
then, it load all capitals from a parquet file containing fields "name" and "shape" (shape is mostly points in WKT)

```scala
val capitals = spark.read.parquet("data/spatial/capitals.parquet")
```

A ShapeLuceneRDD instance is created on the countries and a `linkageByRadius` is performed on the capitals. The output is presented in the logs.
