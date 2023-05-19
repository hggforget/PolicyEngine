from __future__ import print_function
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
def merge_csv():

    device = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/device.csv")
    device = device.drop('id','pc')
    device = device.withColumn('activity', F.when(device.activity == 'Connect', '1').otherwise(device.activity))
    device = device.withColumn('activity', F.when(device.activity == 'Disconnect', '2').otherwise(device.activity))


    email = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/email.csv")
    email = email.drop('id', 'pc','to','from','cc','bcc','from','size','attachments','content')
    email = email.withColumn("activity",F.lit('3'))

    file = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/file.csv")
    file = file.drop('id', 'pc', 'content' , 'filename')
    file = file.withColumn("activity",F.lit('4'))

    http = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/http.csv")
    http = http.drop('id', 'pc', 'content', 'url')
    http = http.withColumn("activity",F.lit('5'))

    logon = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/logon.csv")
    logon = logon.drop('id', 'pc')
    logon = logon.withColumn('activity', F.when(logon.activity == 'Logon', '6').otherwise(logon.activity))
    logon = logon.withColumn('activity', F.when(logon.activity == 'Logoff', '7').otherwise(logon.activity))

    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),device.schema)
    df = df.union(device)
    df = df.union(email)
    df = df.union(file)
    df = df.union(http)
    df = df.union(logon)
    df.printSchema()

    df = df.withColumn("day", F.substring(df.date, 1, 10))

    df.coalesce(1)  \
    .write.mode("overwrite")  \
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") \
    .option("header","true") \
    .csv("E:\SDP\PolicyEngine\data\merge_data")

def group_data():
    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(
        "data/merge_data/part-00000-921ca085-0f2a-4b32-a2a6-782e829ab0f4-c000.csv")

    user_group = Window.partitionBy("user", "day").orderBy("date")
    # user_group = user_group.withColumnRenamed('count', 'day_activity')

    # user_group.show()
    # user_group.select([F.mean("day_activity"), F.min("day_activity"), F.max("day_activity")]).show()

    grouped_df = df.withColumn("actions", F.collect_list("activity").over(user_group)
                               ).groupby("user", "day").agg(F.max("actions").alias("actions"))

    grouped_df = grouped_df.withColumn("actions",
                                       F.concat_ws(",", F.col("actions")))

    grouped_df.coalesce(1) \
        .write.mode("overwrite") \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .option("header", "true") \
        .csv("E:\SDP\PolicyEngine\data\group_data")


def seq_data():
    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(
        "data/group_data/part-00000-80d03d14-87b7-4abf-b4e5-ab534fb43fbf-c000.csv")

    df = df.withColumn("actions", F.split(F.col("actions"), ",").alias("actions"))

    df = df.withColumn("size", F.size(df.actions))
    df.show()
    df.printSchema()

    df.select([F.max("size"), F.min("size"), F.mean("size")]).show();

    schema = StructType([ \
        StructField("user", StringType(), True), \
        StructField("actions", ArrayType(StringType(), containsNull=True), True),
    ])
    '''
    
    rows = df.collect()
    
    seq_length = 5
    
    for row in rows:
        size = row.size
        if size < seq_length:
            tmp = ["0" for i in range(seq_length-size)]
            array = [tmp,row.actions]
            newRow = spark.createDataFrame([(row.user, array)], schema)
            seq_df = seq_df.union(newRow)
        else:
            for i in range(size-seq_length+1):
                array = row.actions[i:seq_length-1+i]
                newRow = spark.createDataFrame([(row.user, array)], schema)
                seq_df = seq_df.union(newRow)
    '''
    n = 10

    seq_df = df.withColumn("actions",F.expr(f"""
               filter(
         transform(actions,(x,i)-> if (i+{n}<size(actions) ,slice(actions,i+1,{n})
         , null)),x->
               x is not null)                               
    """))


    seq_df = seq_df.select("user","day",F.explode(seq_df.actions).alias("actions")).distinct()
    seq_df = seq_df.withColumn("actions",
                                       F.concat_ws(",", F.col("actions")))
    seq_df.coalesce(1) \
        .write.mode("overwrite") \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .option("header", "true") \
        .csv("E:\SDP\PolicyEngine\data\seq_data")

seq_data()
# 生成训练数据x并做标准化后，构造成dataframe格式，再转换为tensor格式
#df = pd.DataFrame(data=preprocessing.StandardScaler().fit_transform(np.random.randint(0, 10, size=(200, 5))))
#y = pd.Series(np.random.randint(0, 2, 200))