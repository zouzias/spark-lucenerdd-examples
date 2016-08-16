#!/usr/bin/env bash

CURRENT_DIR=`pwd`

# Assumes that spark is installed under home directory
HOME_DIR=`echo ~`
#export SPARK_LOCAL_IP=localhost
SPARK_HOME=${HOME_DIR}/spark-1.6.2-bin-hadoop2.6

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.10/spark-lucenerdd-examples-assembly-0.0.1.jar

# Run spark shell locally
${SPARK_HOME}/bin/spark-submit   --conf "spark.executor.memory=512m" \
				--conf "spark.driver.memory=512m" \
				--master local[2] \
				--class org.zouzias.spark.lucenerdd.examples.shape.ShapeLinkageExample \
				--jars ${MAIN_JAR} \
				"${MAIN_JAR}"
