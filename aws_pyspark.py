#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 12:35:30 2021

@author: keerag
"""
import findspark
findspark.find()
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

if __name__=='__main__':
    spark=SparkSession.builder\
        .appName("Read AWS S3 & write MYSQL")\
            .master("local[*]")\
                .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key","AKIAQKIUSS7F6JTCMYNL")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key","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")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint","s3.amazonaws.com")
    
    myDf=spark.read\
        .format("csv")\
            .option("header","true")\
                .option("inferSchema","true")\
                    .csv("s3a://keer1993/data.csv")
    myDf.printSchema()
    myDf.show(truncate=False)
# Data injection to aws s3    
myDf.write.format('csv').option('header','true').save('s3a://keer1993/dfpy.csv',mode='overwrite')
#myDf.write.csv("s3a://keer1993/newdf.csv")

df=spark.read.csv('s3a://keer1993/dfpy.csv/',header=True,inferSchema=True)
