import pyspark
from pyspark import SparkContext,SparkConf

conf=SparkConf().setMaster("local").setAppName("min_Temp")
sc=SparkContext(conf=conf)
file=sc.textFile(r"C:\Spark Course\min Tempreture\1800.csv")

def extract (rdd_raw):
    station=str(rdd_raw.split(",")[0]) 
    level= str(rdd_raw.split(",")[2])
    temp= float(rdd_raw.split(",")[3])*.1*(9/5)+32
    return(station,level,temp)
    
rdd=file.map(extract).filter(lambda x:x[1]=="TMIN")
newmap=rdd.map(lambda x:(x[0],x[2]))
final_min=newmap.reduceByKey(lambda x,y : min(x,y))
result=final_min.collect()
for i in result:
    print(i)

    
