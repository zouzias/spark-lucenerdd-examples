# spark-lucenerdd-examples

Usage examples of [spark-lucenerdd](https://github.com/zouzias/spark-lucenerdd).

## Usage

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd-examples.git
cd spark-lucenerdd-examples
sbt compile assembly
```

Download and extract apache spark under your home directory, update the `spark-submit.sh` script accordingly and run

```
./spark-submit.sh
```

to run the project.
