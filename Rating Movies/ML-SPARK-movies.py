from pyspark.sql import SparkSession
from pyspark.sql.types import  IntegerType , StructType,StructField ,LongType
from pyspark.ml.recommendation import ALS
import codecs
import sys
import  os
import numpy
os.environ['PYSPARK_PYTHON'] = sys.executable



def movies_names():
    with codecs.open(r"C:\Spark Course\Rating Movies\ml-100k\ml-100k\u.item", errors="ignore",
                     encoding="ascii") as data:
        names_dict = {}
        for i in data:
            all_fiels = i.split("|")
            names_dict[all_fiels[0]] = all_fiels[1]
        return names_dict

# preparing the schema for 1 million movie rating df

schema= StructType( [StructField("userID", IntegerType(), True),
                     StructField("movieID", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])

spark= SparkSession.builder.master("local").appName("Spark_ML").getOrCreate()
df=spark.read.option("sep","\t").schema(schema).csv(r"C:\Spark Course\Rating Movies\ml-100k\ml-100k\u.data")
als=ALS().setRegParam(.01).setMaxIter(5).setItemCol("movieID").setRatingCol("rating").setUserCol("userID")
model=als.fit(df)

# I want to make to recomment by the user ID
userID = int(sys.argv[1])
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID,]], userSchema)

recommendations = model.recommendForUserSubset(users,10).collect()
names=movies_names()
for i in recommendations:
    recommend=i[1]
    for j in recommend:
        movie = j[0]
        rate =j[1]
        movie_name=names[movie]

        print(movie_name,str(rate))

