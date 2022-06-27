from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("createView").getOrCreate()

# Create a Dataframe
df = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
    ],
    ["id", "label"]  # add your column names here
)

# Create / Read a csv
csv_frame = spark.read.csv("src/main/resources/Lepetitfichier.csv")
csv_frame.printSchema()

# Writing iniside a file 
csv_frame.write.option("header",True) \
 .csv("src/main/resources/Lepetitfichier.csv")

df.write.csv('src/main/resources/Lepetitfichier.csv', header=True, mode='overwrite')




df.printSchema()

# Read JSON file and create temp table
df = spark.read.json("D:/Study_Materials/Spark/data-master/retail_db_json/departments.json")
df.createOrReplaceTempView("departments")

# Create a view using Spark SQL
spark.sql("CREATE VIEW departments_view AS SELECT * FROM departments")

# Query the data from the view
spark.sql("SELECT * FROM departments_view").show()

# Drop the view
spark.sql("DROP VIEW departments_view")

# Create a UDF with a decorator and asign the return type
@udf(returnType=StringType()) 
def upperCase(str):
    return str.upper()

# Apply the UDF to the dataframe
df.withColumn("Cureated Name", upperCase(col("Name"))) \
.show(truncate=False)



