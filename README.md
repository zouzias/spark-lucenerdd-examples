# spark-lucenerdd-examples

Examples of [spark-lucenerdd](https://github.com/zouzias/spark-lucenerdd).


## Datasets and Entity Likage 
The following pairs of datasets are used here to demonstrate the accuracy/quality of the record linkage methods. Note 
that the goal here is to demonstrate the user-friendliness of the spark-lucenerdd library and no optimization is attempted.


|Dataset | Domain | Attributes  | Accuracy (top-1) | References |
|-------|---------|------------------|------|------|
| DBLP vs ACM article| Bibliographic| title, authors, venue, year | [0.98](https://github.com/zouzias/spark-lucenerdd-examples/blob/develop/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageACMvsDBLP.scala) | [Benchmark datasets for entity resolution](https://dbs.uni-leipzig.de/en/research/projects/object_matching/fever/benchmark_datasets_for_entity_resolution)|
| DBLP vs Scholar article| Bibliographic| title, authors, venue, year | [0.953](https://github.com/zouzias/spark-lucenerdd-examples/blob/develop/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageScholarvsDBLP.scala) | [Benchmark datasets for entity resolution](https://dbs.uni-leipzig.de/en/research/projects/object_matching/fever/benchmark_datasets_for_entity_resolution)|
| Amazon vs Google products| E-commerce| name, description, manufacturer, price | [0.58](https://github.com/zouzias/spark-lucenerdd-examples/blob/develop/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageGooglevsAmazon.scala) | [Benchmark datasets for entity resolution](https://dbs.uni-leipzig.de/en/research/projects/object_matching/fever/benchmark_datasets_for_entity_resolution)|
| Abt vs Buy products | E-commerce| name, description, manufacturer, price | [0.64](https://github.com/zouzias/spark-lucenerdd-examples/blob/develop/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/LinkageAbtvsBuy.scala) | [Benchmark datasets for entity resolution](https://dbs.uni-leipzig.de/en/research/projects/object_matching/fever/benchmark_datasets_for_entity_resolution)|

The reported accuracy above is by selecting as the linked entity: the first result from the top-K list of results.

All datasets are available in Spark friendly Parquet format [here](https://github.com/zouzias/spark-lucenerdd-examples/tree/master/data); original datasets are available [here](http://dbs.uni-leipzig.de/en/research/projects/object_matching/fever/benchmark_datasets_for_entity_resolution).

## Spatial linkage between countries and capitals

This [example](https://github.com/zouzias/spark-lucenerdd-examples/blob/develop/src/main/scala/org/zouzias/spark/lucenerdd/examples/linkage/shape/ShapeLuceneRDDLinkageCountriesvsCapitals.scala) loads all countries from a parquet file containing fields "name" and "shape" (shape is mostly polygons in WKT)

```scala
val allCountries = spark.read.parquet("data/spatial/countries-poly.parquet")
```
then, it load all capitals from a parquet file containing fields "name" and "shape" (shape is mostly points in WKT)

```scala
val capitals = spark.read.parquet("data/spatial/capitals.parquet")
```

A ShapeLuceneRDD instance is created on the countries and a `linkageByRadius` is performed on the capitals. The output is presented in the logs.

# Development

## Usage (spark-submit)

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




