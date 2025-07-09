#!/bin/bash

# Define paths
SRC_FILE="../code/OnTimePerformance.java"
CLASS_DIR="otp_classes"
JAR_FILE="ontimeperf.jar"

# Compile
mkdir -p $CLASS_DIR
javac -classpath $(hadoop classpath) -d $CLASS_DIR $SRC_FILE
jar cf $JAR_FILE -C $CLASS_DIR/ .

# Run Hadoop job
INPUT_PATH="/user/hadoop/flightdata_small/flights_small_cleaned.csv"
OUTPUT_PATH="/user/hadoop/flightdata_small/ontime_output"

# Remove old output if exists
hdfs dfs -rm -r -f $OUTPUT_PATH

# Run
hadoop jar $JAR_FILE OnTimePerformance $INPUT_PATH $OUTPUT_PATH
