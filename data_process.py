from __future__ import print_function
import pandas as pd
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import datetime
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
from sklearn import preprocessing
import  utils
import warnings
warnings.filterwarnings('ignore')

def load_csv():

    device = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/device.csv")
    device = device.drop('id','pc')

    email = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/email.csv")
    email = email.drop('id', 'pc','to','from','cc','bcc','from','size','attachments','content')
    email = email.withColumn("activity",F.lit('email'))

    file = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/file.csv")
    file = file.drop('id', 'pc', 'content' , 'filename')
    file = file.withColumn("activity",F.lit('file'))

    http = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/http.csv")
    http = http.drop('id', 'pc', 'content', 'url')
    http = http.withColumn("activity",F.lit('http'))

    logon = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("r4.2/logon.csv")
    logon = logon.drop('id', 'pc')

    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),device.schema)
    df = df.union(device)
    df = df.union(email)
    df = df.union(file)
    df = df.union(http)
    df = df.union(logon)
    df.printSchema()
    return df





spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = load_csv()

df = df.withColumn("day",F.substring(df.date, 1, 10))

user_group = df.groupBy(["user","day"]).count()
print('user number: ',user_group.select('user').distinct().count())
user_group = user_group.withColumnRenamed('count', 'day_activity')
user_group.show()
user_group.select([F.mean("day_activity"), F.min("day_activity"), F.max("day_activity")]).show()


print(user_group.count())

# 生成训练数据x并做标准化后，构造成dataframe格式，再转换为tensor格式
#df = pd.DataFrame(data=preprocessing.StandardScaler().fit_transform(np.random.randint(0, 10, size=(200, 5))))
#y = pd.Series(np.random.randint(0, 2, 200))