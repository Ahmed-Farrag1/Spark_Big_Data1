import pyspark
from pyspark import SparkContext , SparkConf
import re

def extract(raw_rdd):
    field=raw_rdd.split(",")
    cus_id= int(field[0])
    amount = float(field[2])
    return (cus_id,amount)

conf = SparkConf().setMaster("local").setAppName("WordsCount")
sc=SparkContext()

book=sc.textFile(r"C:\Spark Course\TotalAmountBycustomer\customer-orders.csv")
rdd=book.map(extract).reduceByKey(lambda x,y:x+y)
sorting=rdd.map(lambda x:(x[1],x[0])).sortByKey()
final=sorting.map(lambda x:(x[1],x[0]))
result=final.collect()
for i in result:
    print(i)
