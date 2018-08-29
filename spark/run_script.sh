#!/bin/bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
spark-submit --master local[*] --driver-memory 4G $1 $2
export PYSPARK_DRIVER_PYTHON=jupyter