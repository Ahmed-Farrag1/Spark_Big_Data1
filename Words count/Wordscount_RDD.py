import pyspark
from pyspark import SparkContext , SparkConf
import re

def normalizewords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordsCount")
sc=SparkContext()

book=sc.textFile(r"C:\Spark Course\Words count\Book")
words=book.flatMap(normalizewords)
wordcounts=words.map(lambda x:(x, 1)).reduceByKey(lambda x,y :x+y )

wordcountsSorted = wordcounts.map(lambda x :(x[1],x[0])).sortByKey() 
result=wordcountsSorted.collect()

for i in result:
    count = str(i[0])
    word =i[1].encode("ascii",'ignore')
    if(word):
        print(word , count)