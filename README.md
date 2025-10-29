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

# How to read JSON file
dbutils.fs.ls('file\tables\') # shows the available files in table
json_df = spark.read.format('json').option("inferSchema",True).option("multiline",False).load("path")
json_df.display() #Show data

