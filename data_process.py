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

conf = SparkConf()

conf.set("spark.driver.memory", "5g")  # 这里是增加jvm的内存


spark = SparkSession.builder.appName("DataFrame").config(conf=conf).getOrCreate()


schema = StructType([
    StructField("user", StringType(), True), \
    StructField("day", StringType(), True), \
    StructField("time_seq",ArrayType(
        StructType([
            StructField("feature",IntegerType(),True),
            StructField("label",IntegerType(),True)
        ])
    ),True)
])
def wirte_csv(df,path):
    df.coalesce(1) \
        .write.mode("overwrite") \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .option("header", "true") \
        .csv(path)
def group_data_action():
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

    wirte_csv(grouped_df,'data\group_data')

def group_data_feature():

    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(
        "data/merge_data/part-00000-7c491a38-d108-437d-8d76-5464d80b1754-c000.csv")

    #01/02/2010 07:21:06

    user_group = Window.partitionBy("user","day").orderBy("date")

    grouped_df = df.withColumn("time_seq", F.collect_list(F.struct("feature","label")).over(user_group)
                               ).groupby("user","day").agg(F.max("time_seq").alias("time_seq"),
                                                            )
    '''

    grouped_df = grouped_df.withColumn("time_seq",
                                       F.col("time_seq").cast('string'))\
        .orderBy("user")

   # wirte_csv(grouped_df,'data\group_data_today')
    
    '''
    return grouped_df
def seq_data_feature(df):
    '''

    :param df:
    :return:
    '''
    '''
    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(
        "data/group_data_today/part-00000-3bbd0d31-9766-4ec1-860f-6c3d273686d9-c000.csv")
    '''

    df = df.withColumnRenamed("time_seq","features")

    df.printSchema()

  #  df = df.withColumn("size",F.size(df.features))

  #  df.select(F.mean(df.size)).show()

    schema = StructType([ \
        StructField("user", StringType(), True),
        StructField("features", ArrayType(StringType(), containsNull=True), True),
        StructField("danger", IntegerType(), True),
        ])

    n = 32

    seq_df = df.withColumn("features", F.expr(f"""
               filter(
         transform(features,(x,i)-> if (i%{n}==0 ,slice(features,i+1,{n})
         , null)),x->
               x is not null)                               
    """))


    seq_df = seq_df.select("user", "day", F.explode(seq_df.features).alias("features")).distinct()

    print(seq_df.count())
    seq_df = seq_df.withColumn("label_sum", F.explode(seq_df.features))

    seq_df = seq_df.withColumn("l",seq_df.label_sum.getField("label"))

    seq_df = seq_df.groupby("user","day","features").agg(F.sum("l").alias('danger'))

    seq_df = seq_df.withColumn("features",
                                F.col("features").cast('string'))

    seq_df = seq_df.drop("user","day")


    seq_df.show()
    wirte_csv(seq_df, "data\seq_data_today")



seq_data_feature(group_data_feature())
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

    wirte_csv(seq_df,"data\seq_data")



# 生成训练数据x并做标准化后，构造成dataframe格式，再转换为tensor格式
#df = pd.DataFrame(data=preprocessing.StandardScaler().fit_transform(np.random.randint(0, 10, size=(200, 5))))
#y = pd.Series(np.random.randint(0, 2, 200))