from __future__ import print_function
import os
import pandas as pd
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
from pyspark import SparkContext,SparkConf
import datetime
import pyspark.sql.functions as F
import  pyspark.sql.types
import numpy as np
import pandas as pd
from sklearn import preprocessing
import  utils
import warnings
warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName("DataFrame").getOrCreate()



def read_csv():
    path = 'r4.2_label'
    for file_name in os.listdir(path):
        df = spark.read.csv(path+'/'+file_name)
        df.printSchema()

read_csv()
