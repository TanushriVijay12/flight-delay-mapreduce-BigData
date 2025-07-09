#!/bin/bash

# Define paths
SRC_FILE="../code/TaxiTimeAnalysis.java"
CLASS_DIR="taxi_classes"
JAR_FILE="taxitime.jar"

# Compile
mkdir -p $CLASS_DIR
javac -classpath $(hadoop classpath) -d $CLASS_DIR $SRC_FILE
jar cf $JAR_FILE -C $CLASS_DIR/ .

# Run Hadoop job
INPUT_PATH="/user/hadoop/input/sample_flights.csv"
OUTPUT_PATH="/user/hadoop/output/taxi"

# Remove old output if exists
hdfs dfs -rm -r -f $OUTPUT_PATH

# Run
hadoop jar $JAR_FILE TaxiTimeAnalysis $INPUT_PATH $OUTPUT_PATH
