#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 10 12:38:55 2021

@author: keerag
"""

import findspark
findspark.find()
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

#create spark session with mysql configure
spark=SparkSession.builder.config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.25.jar") \
    .appName("Read AWS S3 & write MYSQL")\
        .master("local[*]")\
            .getOrCreate()

#hadoop configuration for acessing aws
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key","AKIAQKIUSS7F6JTCMYNL")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key","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")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint","s3.amazonaws.com")

#read Dataframe from aws s3
myDf=spark.read\
    .format("csv")\
        .option("header","true")\
            .option("inferSchema","true")\
                .csv("s3a://keer1993/data.csv")

#load Dataframe to mysql table
myDf.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/ragdb") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "s3tb") \
    .option("user", "keerag").option("password", "1Keerag@").mode('append').save()