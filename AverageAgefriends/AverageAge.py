import pyspark
from pyspark import SparkConf ,SparkContext

conf=SparkConf().setMaster("local").setAppName("Calculating_friends_Average")

sc=SparkContext(conf=conf)

def get_key_value(RDD_raw):
    fields=RDD_raw.split(",")
    return(int(fields[2]),int(fields[3]))

RDD=sc.textFile(r"C:\Spark Course\AverageAgefriends\fakefriends.csv")
key_value = RDD.map(get_key_value)
maping_values=key_value.mapValues(lambda x:(x,1))
reducing_bykey=maping_values.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
Average_age=reducing_bykey.mapValues(lambda x:(round(x[0]/x[1])))

result=Average_age.collect()
for i in result:
    print(i)
