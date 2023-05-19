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

schema = StructType([ \
        StructField("id", StringType(), True), \
        ])

def wirte_csv(df,path):
    df.coalesce(1) \
        .write.mode("overwrite") \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .option("header", "true") \
        .csv(path)

def read_csv():
    path = 'r4.2_label'

    df_device = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    df_http = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    df_file = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    df_email = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    df_logon = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    for file_name in os.listdir(path):
        df_tmp = spark.read.csv(path+'/'+file_name)
        df_tmp = df_tmp.select("_c0","_c1")

        df_tmp_device = df_tmp.filter(df_tmp._c0 == 'device').select("_c1")
        df_device = df_device.union(df_tmp_device)

        df_tmp_http = df_tmp.filter(df_tmp._c0 == 'http').select("_c1")
        df_http = df_http.union(df_tmp_http)

        df_tmp_file = df_tmp.filter(df_tmp._c0 == 'file').select("_c1")
        df_file = df_file.union(df_tmp_file)

        df_tmp_email = df_tmp.filter(df_tmp._c0 == 'email').select("_c1")
        df_email = df_email.union(df_tmp_email)

        df_tmp_logon = df_tmp.filter(df_tmp._c0 == 'logon').select("_c1")
        df_logon = df_logon.union(df_tmp_logon)

    print('device count: ',df_device.count())
    print('http count: ',df_http.count())
    print('file count: ',df_file.count())
    print('email count: ',df_email.count())
    print('logon count: ',df_logon.count())

    wirte_csv(df_device, "E:\SDP\PolicyEngine\data\label_device")
    wirte_csv(df_http, "E:\SDP\PolicyEngine\data\label_http")
    wirte_csv(df_file, "E:\SDP\PolicyEngine\data\label_file")
    wirte_csv(df_email, "E:\SDP\PolicyEngine\data\label_email")
    wirte_csv(df_logon, "E:\SDP\PolicyEngine\data\label_logon")


#read_csv()

def add_label():

    types = ['device','http','email','file','logon']
    path_data = 'r4.2'
    path_label = 'data'
    for type in types:
        df_label = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
        file_path = path_label +'\label_' + type
        dir_list = os.listdir(file_path)
        for cur_file in dir_list:
            path = os.path.join(file_path , cur_file)
            # 判断是否存在.csv文件，如果存在则获取路径信息写入到list_csv列表中
            if os.path.splitext(path)[1] == '.csv':
                df_label = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path)

        df_data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path_data + '\\' + type +'.csv')

        df_label = df_label.withColumnRenamed("id",'ishave')

        df_data = df_data.join(df_label,df_data['id']==df_label['ishave'],'leftouter')

        df_data = df_data.withColumn('label', F.when(df_data['ishave'].isNull(), 0).otherwise(1))

        df_data = df_data.drop('ishave')




add_label()