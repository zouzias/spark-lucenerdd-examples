#!/usr/bin/env bash

CURRENT_DIR=`pwd`

# Read the version from version.sbt
SPARK_LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`

echo "==============================================="
echo "Loading LuceneRDD with version ${SPARK_LUCENERDD_VERSION}"
echo "==============================================="


# Assumes that spark is installed under home directory
HOME_DIR=`echo ~`
export SPARK_LOCAL_IP=localhost
SPARK_HOME=${HOME_DIR}/spark-2.0.1-bin-hadoop2.7

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-examples-assembly-${SPARK_LUCENERDD_VERSION}.jar

echo "SPARK SUBMIT: $1"

# Run spark shell locally
${SPARK_HOME}/bin/spark-submit   \
        --conf "spark.executor.memory=1g" \
        --conf "spark.executor.cores=4" \
        --conf "spark.executor.instances=2" \
        --conf "spark.driver.memory=1g" \
        --conf "spark.rdd.compress=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.kryoserializer.buffer=24mb" \
        --conf "spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator" \
        --conf "spark.driver.extraJavaOptions=-Dlucenerdd.index.store.mode=disk" \
        --conf "spark.executor.extraJavaOptions=-Dlucenerdd.index.store.mode=disk" \
        --master local[*] \
        --class $1 \
        --jars ${MAIN_JAR} \
        "${MAIN_JAR}"
