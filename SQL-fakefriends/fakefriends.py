
#In this solution I will use rdd then converting it to a Data Frame
import pyspark
from pyspark.sql import SparkSession , Row



spark=SparkSession.builder.appName("SQL-1").getOrCreate()

def extract(raw_rdd):
    fields=raw_rdd.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))


rdd=spark.sparkContext.textFile(r"C:\Spark Course\SQL-fakefriends\fakefriends.csv")
people = rdd.map(extract)


df = spark.createDataFrame(people).cache()
df.createOrReplaceTempView("mapping")

result = spark.sql("SELECT  * FROM mapping WHERE age>=13 and age <=19")
for i in result.collect():
    print(i)
spark.stop()
 