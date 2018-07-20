# spark-lucenerdd-examples

Usage examples of [spark-lucenerdd](https://github.com/zouzias/spark-lucenerdd).

### Record Linkage

Examples with "real-world" datasets are available:

* [DBLP vs ACM](https://github.com/zouzias/spark-lucenerdd-examples/blob/master/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageACMvsDBLP.scala) - DBLP academic articles versus ACM articles
* [DBLP vs Scholar](https://github.com/zouzias/spark-lucenerdd-examples/blob/master/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageScholarvsDBLP.scala) - DBLP academic articles versus Google Scholar articles
* [Amazon vs Google](https://github.com/zouzias/spark-lucenerdd-examples/blob/master/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageGooglevsAmazon.scala) - Amazon versus google product listings
* [Abt vs Buy](https://github.com/zouzias/spark-lucenerdd-examples/blob/master/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageAbtvsBuy.scala) - Abt versus buy product listings

The datasets used for record linkage are
available at [here](http://dbs.uni-leipzig.de/en/research/projects/object_matching/fever/benchmark_datasets_for_entity_resolution). A spark friendly version of the datasets (Parquet) is available at [parquet](https://github.com/zouzias/spark-lucenerdd-examples/tree/master/data). 

### Spatial linkage between countries and capitals

This example loads all countries from a parquet file containing fields "name" and "shape" (shape is mostly polygons in WKT)

```scala
val allCountries = spark.read.parquet("data/spatial/countries-poly.parquet")
```
then, it load all capitals from a parquet file containing fields "name" and "shape" (shape is mostly points in WKT)

```scala
val capitals = spark.read.parquet("data/spatial/capitals.parquet")
```

A ShapeLuceneRDD instance is created on the countries and a `linkageByRadius` is performed on the capitals. The output is presented in the logs.

#### Usage (spark-submit)

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

#### Usage (docker)

Setup docker and  assuming that you have a docker machine named `default`, type

```
./startZeppelin.sh
```
To start an Apache Zeppelin with preloaded notebooks.
