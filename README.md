# Pyspark_Learnings

#this creates spark jobs\ means reading the data or creates sub jobs
df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('path of file')
df.show() or df.display() # to see data 

df.printschema() # show the schema of data
# We can change the schema of column like String column datatype to INT column datatype
my_ddl_schema='''
name STRING,
age INT,   # to change age datatype to str just change here only INT to STRING and again read the file
'''

df = spark.read.format('csv').schema(my_ddl_schema).option('header',True).load('path of file')
df.display() # u will see age datatype to STRING

# Change column datatype using StructType()
from pyspark.sql.types import *
from pyspark.functions import *
my_structType_schema = StructType([
Structfield("age",StringType(),True) , # True allowing null values in col
Structfield("col_name",StringType(),True),
Structfield("age",StringType(),True)
Structfield("age",StringType(),True)
])
 # After this again read the data and u will see datatype changed 
 df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('path of file')
df.show() or df.display() # to see data 

 # How to read JSON file
dbutils.fs.ls('file\tables\') # shows the available files in table
json_df = spark.read.format('json').option("inferSchema",True).option("multiline",False).load("path")
json_df.display() #Show data

 # Data Transformation

df_select  = df.select("Col_name1",Col_name2,Col_name3) or df.select(col("col_name1"),col("col_name2"),col("col_name3")) #this way select any col u want
df_select.display() # shows choosed cols

df.select(col("name")).alias("fullname").display() # alias on cols

# Applying filter on data
df.filter(col("name") == "Rohan").display() #Gives data of Rohan Only 
df.filter((col("name")=="Rohan") & (col("age") < 24)).display() #Applying multiple filter on columns
df.filter((col("name").isnull()) & (col("age").isin(23,24)) ).display() # Applying multiple condition isnull()->gives null values and isin()->similar to in() in sql

#withColumnRename() -> just rename the column
df.withColumnRename("name","fullname").display() change the name to fullname 

# withColumn()->can add new column and modified any column
df=df.withColumn("newColumn",lit("value")) #create a new col named as newColumn and put values in all using lit() funtn
df.withColumn("newcol",col(column1) * col(column2)).display()
df.withColumn("name",regex_replace(col("name"),"Rohan","Shokin").display() #replace Rohan to Shokin using regex_replace() funtns
or 
df.withColumn("name",regex_replace(col("name"),"Rohan","Shokin")\.
df.withColumn("name",regex_replace(col("name"),"Salman","Thanos").display() # replacing multiple values at onetime like this

# Changing column datatype cast() 
df.withColumn("name",col("name").cast(Stringtype())) # change the datatype of name column 
df.printSchema() 

# Sorting 
df.sort(col("age").desc()).display() or df.sort(col("age").asc()).display() #arrange data in ascending or descending 
df.sort(col("age"),col("roll"),ascending=[0,0]).display() #applying sorting on both columns both in decreasing 0->means false 1->means true 
df.sort(col("age"),col("roll"),ascending=[0,1]).display()#age col in desc and roll col in ascending 

#Limit apllying 
df.limit(10).displa() # display only 10 records 

# drop()->just drop the particular columns 
df.drop("name").display() 
df.frop("name","age").display() #drops multiple columns



