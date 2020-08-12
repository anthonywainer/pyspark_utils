# Utils for PySpark

It is often necessary to perform HDFS operations from a Spark application, 
either to list files in HDFS or to delete data. Because achieving this is not 
immediately obvious with the Python Spark API (PySpark), here are some ways 
to execute such commands.

## About HDFSUtils:

This class is used to connect to the HDFS and get the list of settings
Attributes:
     sc: SparkContext
     partition_name: The name of the partition
     date_format: The default date format uses '%Y-%m-%d

## About DataFrameUtils:
This utility is used to make readings.

Attributes:
     sc: SparkContext
     spark: SparkSession
     partition_name: The name of the partition
     date_format: The default date format uses '%Y-%m-%d

Allowed read formats: "avro", "parquet", txt", "ctl", "dat"

Importing
> import sys 
> sys.path.insert(0, '/path/workspace/utils/')
> 
> from HDFSUtils import HDFSUtils
> from DataFrameUtils import DataFrameUtils

For examples checking the utils.ipynb notebook 

https://colab.research.google.com/github/AnthonyWainer/pyspark_utils/blob/master/utils.ipynb