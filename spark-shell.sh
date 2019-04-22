#!/usr/bin/env bash

CURRENT_DIR=`pwd`

# Read the version from version.sbt
SPARK_LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`

# Spark version (requires uncompressed tar.gz under $HOME)
SPARK_VERSION="2.4.1"


echo "==============================================="
echo "Loading LuceneRDD with version ${SPARK_LUCENERDD_VERSION}"
echo "==============================================="


# Assumes that spark is installed under home directory
HOME_DIR=`echo ~`
export SPARK_LOCAL_IP=localhost
SPARK_HOME=${HOME_DIR}/spark-${SPARK_VERSION}-bin-hadoop2.7

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-examples-assembly-${SPARK_LUCENERDD_VERSION}.jar

echo "SPARK SHELL: $1"

# Run spark shell locally
${SPARK_HOME}/bin/spark-shell   \
        --conf "spark.executor.memory=1g" \
        --conf "spark.executor.cores=4" \
        --conf "spark.driver.memory=1g" \
        --conf "spark.rdd.compress=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.kryoserializer.buffer=24mb" \
        --conf "spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator" \
        --conf "spark.driver.extraJavaOptions=-Dlucenerdd.index.store.mode=disk" \
        --conf "spark.executor.extraJavaOptions=-Dlucenerdd.index.store.mode=disk" \
        --master local[*] \
        --jars ${MAIN_JAR}
