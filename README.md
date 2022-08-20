# Pyspark
## Import Enviroments

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Important to be installed

pip install nbconvert
pip install seaborn
pip install pyppeteer
pip install pyspark
pip install findspark 
pip pyppeteer-install


import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Import Libraries 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession

spark = (
    SparkSession.builder
    .master('local')
    .appName('Project_01')
    .getOrCreate()
)

# Read File Received
# Always you need to put ## [ df = ]## to Save

df = spark.read.csv('C:/Users/IEUser/Documents/DeltaLake/Bronze/HumanResources.Department.csv',header=True, inferSchema=True)
df.show(truncate = False)

# Checking Schema

df.printSchema()

# Checking Datas null -- Pandas has limitations #Don't use - Only try

df.toPandas().isna().sum()

# Searching for Nulls

for column in df.columns:
    print(column,df.filter(df[column].isNull()).count())

# Rename Columns
# Always you need to put ## [ df = ]## to Save

df = df.withColumnRenamed('Modified Date','ModifiedDate')
df.show(truncate = False)

# Check all Columns

df.columns

# Select Columns

df.select(col('GroupName'),col('Name')).show(truncate = False)


# Create Alias
# Always you need to put ## [ df = ]## to Save

df.select(col('Name').alias('Names')).show(truncate = False)

# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save

df.select('DepartmentID Name GroupName ModifiedDate'.split()).show(truncate = False)

# Showing Columns as you want to see 

df.select('Name','GroupName').show(truncate = False)

# Filtring df  --Showing only the specific column and specific filtern and putting distinct function to not duplicate information

df.select(col('GroupName')).filter(col('GroupName') == "Inventory Management").distinct().show(truncate = False)

#Showing all df

df.show(truncate = False)

# Filtring df with more conditions and specific columns (AND / &)

df.select('Name','ModifiedDate').filter((col('Name') == "Finance")).show(truncate = False)

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentID','Name').filter((col('Name') == "Finance") & (col('DepartmentID') == 10)).show(truncate = False) 

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentId','Name').filter((col('DepartmentID') == 2)).show(truncate = False)

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentId','Name').filter((col('DepartmentID') != 12)).show(truncate = False)

# Filtring df with specific columns and more conditions  (AND / &)
## df.filter('Name = "Finance"').filter(col('DepartmentId') == 1).show()

df.select('DepartmentId','Name').filter((col('DepartmentID') >= 3)).show(truncate = False)

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentId','Name').filter((col('DepartmentID') <= 16)).show(truncate = False)

# Filtring df with more conditions (OR / |)

df.filter('DepartmentID = "1"').show(truncate = False)

# Filtring df with more conditions (OR / |)

df.filter((col('Name') == 'Finance') | (col('Name') == 'Sales') | (col('DepartmentID') == 12)).show(truncate = False)

# # Filtring df with more conditions (OR / |)

df.filter(col('GroupName') == 'Quality Assurance').show(truncate = False)

# # Filtring df combining & and | # And e OR #

df.filter((col('GroupName') == "Quality Assurance")  | (col('Name') == "Sales") | (col('DepartmentID') == 10)).show(truncate = False)

# Concatenate Columns without space

df.withColumn("DepartmentID + GroupName", concat('DepartmentID','GroupName')).show()

# Concatenate Columns with space

df.withColumn("DepartmentID + GroupName", concat_ws(' ', 'DepartmentID','GroupName')).show()

# Alter type of Column

df.show(truncate = False)

# Alter type of Column

df.printSchema()

# Alter type of Metada of the Column

#df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)

df.show()

# Alter type of Column
# Didn't change yet because we didn't put variable "" df = df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)"  bfore the execution of code,
# only when we put this everything will change

df.printSchema()

###Coming back to understanding better ###


##day = udf(lambda x: x.split(-)[18]):


##day = udf(lambda date: date.split('-')[5])

##df.withColumn('ModifiedDate', Year('ModifiedDate')).show(truncate=False)

#

#datediff()
df.select(col("input"), 
    datediff(current_date(),col("input")).alias("datediff")  
  ).show()

#Result
+----------+--------+
|     input|datediff|
+----------+--------+
|2020-02-01|     387|
|2019-03-01|     724|
|2021-03-01|      -7|
+----------+--------+

#

#The below example returns the difference between two dates using datediff().

df.select(col('ModifiedDate'),
         datediff(current_timestamp(), col('ModifiedDate')).alias('difference between two dates')).show(truncate=False)

#The below example returns the months between two dates 

df.select(col("ModifiedDate"), 
    months_between(current_timestamp(),col("ModifiedDate")).alias("months_between")  
  ).show()

#round(col("score")

#df.select(col("ModifiedDate"), 
 #   months_between(current_timestamp(),round(col("ModifiedDate")).alias("months_between"))  
  #).show()





#df.withColumn("ModifiedDate", round(col("ModifiedDate"))).show()

#Using round numbers ('Arredontar numeros')

df.select("*",round(col("DepartmentID")).alias("Teste")).show(truncate=False)

## test modifying dates##

#(df_date
#.withColumn("to_date", f.to_date("input_date"))

df.withColumn("year",year("ModifiedDate")).show(2)
df.withColumn("quarter", quarter("ModifiedDate")).show(2)
df.withColumn("month",month("ModifiedDate")).show(2)
df.withColumn("week",weekofyear("ModifiedDate")).show(2)
df.withColumn("dayofyear",dayofyear("ModifiedDate")).show(2)
df.withColumn("dayofmonth ",dayofmonth("ModifiedDate")).show(2)
df.withColumn("dayofweek" , dayofweek("ModifiedDate")).show(2)

# Extract Hour, Minutes and Seconds


#(df_date
#.withColumn("to_timestamp",f.to_timestamp("input_date"))
df.withColumn("hour", hour("ModifiedDate")).show(2)
df.withColumn("minute",minute("ModifiedDate")).show(2)
df.withColumn("second",second("ModifiedDate")).show(2)

#Days and Month in Words

df.withColumn("dayofweek" ,dayofweek("ModifiedDate")).show(2)
df.withColumn("dayinwords",date_format("ModifiedDate" , "EEEE")).show(2)
df.withColumn("monthinwords", date_format("ModifiedDate" , "LLLL")).show(2)


#Hadling Dates

df.withColumn("cur_date",current_date()).show(2)
df.withColumn("Days",datediff(current_date(),"ModifiedDate" )).show(2) 
df.withColumn("dateadd" ,date_add("ModifiedDate",5)).show(2) 
df.withColumn("datesub" ,date_sub("ModifiedDate",5)).show(2) 
df.withColumn("datetrnc",date_trunc('mm' , "ModifiedDate")).show(2) 

### Joins Dataframes ###

#Using Distinct #

df.select(col('GroupName')).distinct().show(truncate=False)


# Using Collect - show all the rows#

df.select(col('GroupName')).distinct().collect()

list = df.select(col('GroupName')).collect()

type(list[0][0])

list[5][0]

list[0][0]

# Generating a list GroupName = []

for GroupName in list:
    GroupName.asDict(GroupName[0])
GroupName

## Working with When () / Otherwise()##

##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")

df.withColumn("Correct", when(col("GroupName") == "Manufacturing" , lit("OK")).otherwise("")).distinct().show(truncate=False)

## Working with When () / Otherwise()##

##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")

df.withColumn("Correct", when(col("GroupName").isin("GroupName"),'Correct')
             

.otherwise("Ok")).distinct().show(truncate=False)


## Working with OrderBy desc

df.orderBy(col("GroupName").desc()).show(truncate=False)

## Working with OrderBy asc

df.orderBy(col("GroupName").asc()).show(truncate=False)

## Working with OrderBy, Distinct asc

df.orderBy(col("GroupName").asc()).distinct().show(truncate=False)
           

## Working with GroupBy, Count, Distinct and asc

df.groupBy("GroupName").count().distinct().show(truncate=False)

## Union and Union All

## Joins

df2 = spark.read.csv('C:/Users/IEUser/Documents/DeltaLake/Bronze/HumanResources.Employee.csv',header=True, inferSchema=True)
df2.show(1)

df2.printSchema()

# Searching for Nulls

for column in df2.columns:
    print(column,df2.filter(df2[column].isNull()).count())
    

# Check all Columns

df2.columns

# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save

df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(truncate=False)


#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     #.show(truncate=False)


#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")

dfnovo = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")
dfnovo.show(2)



#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     #.show(truncate=False)


#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")

dfHR = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")\
.select(col("JobTitle"))
dfHR.show(truncate=False)

df2.columns

df2.select(col("LoginID")).show(truncate=False)

# The applied options are for CSV files. For other file types, these will be ignored.


# File location and type
file_location = "C:/Users/IEUser/Documents/DeltaLake/Bronze/HumanResources.Employee.csv"
file_type = "csv"


# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","





df2 = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep" , delimiter) \
.load(file_location)
df2.show(truncate=False)
df2.select(col("LoginID"))
df2.show(truncate=False)
#df2.write.csv("C:/Users/IEUser/Documents/DeltaLake/Silver/Teste/HumanResources.Employee.csv")
df2.write.csv("C:/Users/IEUser/Documents/DeltaLake/Silver/Teste/HumanResources.Employee.csv")
#df2.write.mode("overwrite").csv("C:/Users/IEUser/Documents/DeltaLake/Silver/Teste/HumanResources.Employee.csv")
#df2.write.mode("overwrite").csv("C:/Users/IEUser/Documents/DeltaLake/Silver/Teste/HumanResources.Employee.csv")

# C:/Users/IEUser/Documents/DeltaLake/Silver/Teste/HumanResources.Employee.csv

df2.select(col("LoginID")).show(truncate=False)

