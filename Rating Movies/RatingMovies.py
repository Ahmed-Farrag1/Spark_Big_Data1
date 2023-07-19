import pyspark
from pyspark import SparkConf,SparkContext
import collections

conf =SparkConf().setMaster("local").setAppName("Ratingcount")
sc=SparkContext(conf=conf)
lines=sc.textFile(r'C:\Spark Course\ml-100k\ml-100k\u.data')

ratings=lines.map(lambda x:x.split()[2])
result= sorted(ratings.countByValue().items())
Ordereddict=collections.OrderedDict(result)
for key,value in Ordereddict.items():
    print(key , value)