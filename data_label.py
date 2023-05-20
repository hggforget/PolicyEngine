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
conf = SparkConf()
conf.set("spark.app.name", "es-hadoop")
conf.set("spark.executor.memory", "64g")
conf.set("spark.executor.cores", "4")
conf.set("spark.driver.memory", "5g")  # 这里是增加jvm的内存
conf.set("spark.driver.maxResultSize", "2g") # 这里是最大显示结果，这里是提示我改的。


spark = SparkSession.builder.appName("DataFrame").getOrCreate()

schema = StructType([ \
        StructField("id", StringType(), True), \
        StructField("date", StringType(), True), \
        StructField("user", StringType(), True), \
        StructField("activity", StringType(), True), \
        StructField("label", IntegerType(), True), \
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

    wirte_csv(df_device, "data\label_device")
    wirte_csv(df_http, "data\label_http")
    wirte_csv(df_file, "data\label_file")
    wirte_csv(df_email, "data\label_email")
    wirte_csv(df_logon, "data\label_logon")


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

def read_label(type):

    #path_data = 'r4.2'
    path_label = 'data'

    df_label = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    file_path = path_label + '\label_' + type
    dir_list = os.listdir(file_path)
    for cur_file in dir_list:
        path = os.path.join(file_path, cur_file)
        # 判断是否存在.csv文件，如果存在则获取路径信息写入到list_csv列表中
        if os.path.splitext(path)[1] == '.csv':
            df_label = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path)

   # df_data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path_data + '\\' + type + '.csv')

    return df_label

def add_label(df_label,df_data):


    df_label = df_label.withColumnRenamed("id", 'ishave')

    df_data = df_data.join(df_label, df_data['id'] == df_label['ishave'], 'leftouter')

    df_data = df_data.withColumn('label', F.when(df_data['ishave'].isNull(), 0).otherwise(1))

    df_data = df_data.drop('ishave')

    return df_data
def merge_csv():

    device = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('r4.2/device.csv')
    device = device.drop('pc')
    device = device.withColumn('activity', F.when(device.activity == 'Connect', '1').otherwise(device.activity))
    device = device.withColumn('activity', F.when(device.activity == 'Disconnect', '2').otherwise(device.activity))
    device = add_label(read_label('device'),device)

    email = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('r4.2/email.csv')
    email = email.drop('pc','to','from','cc','bcc','from','size','attachments','content')
    email = email.withColumn("activity",F.lit('3'))
    email = add_label(read_label('email'), email)

    file = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('r4.2/file.csv')
    file = file.drop('pc', 'content' , 'filename')
    file = file.withColumn("activity",F.lit('4'))
    file = add_label(read_label('file'), file)

    http = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('r4.2/http.csv')
    http = http.drop('pc', 'content', 'url')
    http = http.withColumn("activity",F.lit('5'))
    http = add_label(read_label('http'), http)

    logon = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('r4.2/logon.csv')
    logon = logon.drop('pc')
    logon = logon.withColumn('activity', F.when(logon.activity == 'Logon', '6').otherwise(logon.activity))
    logon = logon.withColumn('activity', F.when(logon.activity == 'Logoff', '7').otherwise(logon.activity))
    logon = add_label(read_label('logon'), logon)

    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    df = df.union(device)
    df = df.union(email)
    df = df.union(file)
    df = df.union(http)
    df = df.union(logon)
    df = df.drop('id')


    df = df.withColumn("day", F.substring(df.date, 1, 10))

    df = df.withColumn('hour', F.substring(df.date, 12, 2).cast('integer'))

    df = df.withColumn('feature', df.activity * 24 + df.hour)

    df = df.withColumn('feature', df.feature.cast(IntegerType()))
    #df = df.drop('activity','hour')
    wirte_csv(df, "data\merge_data")

#merge_csv()

