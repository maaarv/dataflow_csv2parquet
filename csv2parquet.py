#!/usr/bin/env python3

import os
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.types 


def main():


    spark = SparkContext(conf=SparkConf())
    sql_context = SQLContext(spark)


    df = (
        sql_context.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(sys.argv[1])
    )
    df.write.mode("overwrite").parquet(sys.argv[2])


if __name__ == "__main__":
    main()
