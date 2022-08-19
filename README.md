# Pyspark
Pyspark

# Import Enviroments

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

df = spark.read.csv('C:/Users/IEUser/Documents/Delta Lake/Bronze/HumanResources.Department.csv',header=True, inferSchema=True)
df.show(truncate = False)
+------------+--------------------------+------------------------------------+-------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate       |
+------------+--------------------------+------------------------------------+-------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00|
+------------+--------------------------+------------------------------------+-------------------+

# Checking Schema

df.printSchema()
root
 |-- DepartmentID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

# Checking Datas null -- Pandas has limitations #Don't use - Only try

df.toPandas().isna().sum()
DepartmentID    0
Name            0
GroupName       0
ModifiedDate    0
dtype: int64
# Searching for Nulls

for column in df.columns:
    print(column,df.filter(df[column].isNull()).count())
DepartmentID 0
Name 0
GroupName 0
ModifiedDate 0
# Rename Columns
# Always you need to put ## [ df = ]## to Save

df = df.withColumnRenamed('Modified Date','ModifiedDate')
df.show(truncate = False)
+------------+--------------------------+------------------------------------+-------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate       |
+------------+--------------------------+------------------------------------+-------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00|
+------------+--------------------------+------------------------------------+-------------------+

# Check all Columns

df.columns
['DepartmentID', 'Name', 'GroupName', 'ModifiedDate']
# Select Columns

df.select(col('GroupName'),col('Name')).show(truncate = False)
+------------------------------------+--------------------------+
|GroupName                           |Name                      |
+------------------------------------+--------------------------+
|Research and Development            |Engineering               |
|Research and Development            |Tool Design               |
|Sales and Marketing                 |Sales                     |
|Sales and Marketing                 |Marketing                 |
|Inventory Management                |Purchasing                |
|Research and Development            |Research and Development  |
|Manufacturing                       |Production                |
|Manufacturing                       |Production Control        |
|Executive General and Administration|Human Resources           |
|Executive General and Administration|Finance                   |
|Executive General and Administration|Information Services      |
|Quality Assurance                   |Document Control          |
|Quality Assurance                   |Quality Assurance         |
|Executive General and Administration|Facilities and Maintenance|
|Inventory Management                |Shipping and Receiving    |
|Executive General and Administration|Executive                 |
+------------------------------------+--------------------------+

# Create Alias
# Always you need to put ## [ df = ]## to Save

df.select(col('Name').alias('Names')).show(truncate = False)
+--------------------------+
|Names                     |
+--------------------------+
|Engineering               |
|Tool Design               |
|Sales                     |
|Marketing                 |
|Purchasing                |
|Research and Development  |
|Production                |
|Production Control        |
|Human Resources           |
|Finance                   |
|Information Services      |
|Document Control          |
|Quality Assurance         |
|Facilities and Maintenance|
|Shipping and Receiving    |
|Executive                 |
+--------------------------+

# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save

df.select('DepartmentID Name GroupName ModifiedDate'.split()).show(truncate = False)
+------------+--------------------------+------------------------------------+-------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate       |
+------------+--------------------------+------------------------------------+-------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00|
+------------+--------------------------+------------------------------------+-------------------+

# Showing Columns as you want to see 

df.select('Name','GroupName').show(truncate = False)
+--------------------------+------------------------------------+
|Name                      |GroupName                           |
+--------------------------+------------------------------------+
|Engineering               |Research and Development            |
|Tool Design               |Research and Development            |
|Sales                     |Sales and Marketing                 |
|Marketing                 |Sales and Marketing                 |
|Purchasing                |Inventory Management                |
|Research and Development  |Research and Development            |
|Production                |Manufacturing                       |
|Production Control        |Manufacturing                       |
|Human Resources           |Executive General and Administration|
|Finance                   |Executive General and Administration|
|Information Services      |Executive General and Administration|
|Document Control          |Quality Assurance                   |
|Quality Assurance         |Quality Assurance                   |
|Facilities and Maintenance|Executive General and Administration|
|Shipping and Receiving    |Inventory Management                |
|Executive                 |Executive General and Administration|
+--------------------------+------------------------------------+

# Filtring df  --Showing only the specific column and specific filtern and putting distinct function to not duplicate information

df.select(col('GroupName')).filter(col('GroupName') == "Inventory Management").distinct().show(truncate = False)
+--------------------+
|GroupName           |
+--------------------+
|Inventory Management|
+--------------------+

#Showing all df

df.show(truncate = False)
+------------+--------------------------+------------------------------------+-------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate       |
+------------+--------------------------+------------------------------------+-------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00|
+------------+--------------------------+------------------------------------+-------------------+

# Filtring df with more conditions and specific columns (AND / &)

df.select('Name','ModifiedDate').filter((col('Name') == "Finance")).show(truncate = False)
+-------+-------------------+
|Name   |ModifiedDate       |
+-------+-------------------+
|Finance|2008-04-30 00:00:00|
+-------+-------------------+

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentID','Name').filter((col('Name') == "Finance") & (col('DepartmentID') == 10)).show(truncate = False) 
+------------+-------+
|DepartmentID|Name   |
+------------+-------+
|10          |Finance|
+------------+-------+

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentId','Name').filter((col('DepartmentID') == 2)).show(truncate = False)
+------------+-----------+
|DepartmentId|Name       |
+------------+-----------+
|2           |Tool Design|
+------------+-----------+

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentId','Name').filter((col('DepartmentID') != 12)).show(truncate = False)
+------------+--------------------------+
|DepartmentId|Name                      |
+------------+--------------------------+
|1           |Engineering               |
|2           |Tool Design               |
|3           |Sales                     |
|4           |Marketing                 |
|5           |Purchasing                |
|6           |Research and Development  |
|7           |Production                |
|8           |Production Control        |
|9           |Human Resources           |
|10          |Finance                   |
|11          |Information Services      |
|13          |Quality Assurance         |
|14          |Facilities and Maintenance|
|15          |Shipping and Receiving    |
|16          |Executive                 |
+------------+--------------------------+

# Filtring df with specific columns and more conditions  (AND / &)
## df.filter('Name = "Finance"').filter(col('DepartmentId') == 1).show()

df.select('DepartmentId','Name').filter((col('DepartmentID') >= 3)).show(truncate = False)
+------------+--------------------------+
|DepartmentId|Name                      |
+------------+--------------------------+
|3           |Sales                     |
|4           |Marketing                 |
|5           |Purchasing                |
|6           |Research and Development  |
|7           |Production                |
|8           |Production Control        |
|9           |Human Resources           |
|10          |Finance                   |
|11          |Information Services      |
|12          |Document Control          |
|13          |Quality Assurance         |
|14          |Facilities and Maintenance|
|15          |Shipping and Receiving    |
|16          |Executive                 |
+------------+--------------------------+

# Filtring df with specific columns and more conditions  (AND / &)

df.select('DepartmentId','Name').filter((col('DepartmentID') <= 16)).show(truncate = False)
+------------+--------------------------+
|DepartmentId|Name                      |
+------------+--------------------------+
|1           |Engineering               |
|2           |Tool Design               |
|3           |Sales                     |
|4           |Marketing                 |
|5           |Purchasing                |
|6           |Research and Development  |
|7           |Production                |
|8           |Production Control        |
|9           |Human Resources           |
|10          |Finance                   |
|11          |Information Services      |
|12          |Document Control          |
|13          |Quality Assurance         |
|14          |Facilities and Maintenance|
|15          |Shipping and Receiving    |
|16          |Executive                 |
+------------+--------------------------+

# Filtring df with more conditions (OR / |)

df.filter('DepartmentID = "1"').show(truncate = False)
+------------+-----------+------------------------+-------------------+
|DepartmentID|Name       |GroupName               |ModifiedDate       |
+------------+-----------+------------------------+-------------------+
|1           |Engineering|Research and Development|2008-04-30 00:00:00|
+------------+-----------+------------------------+-------------------+

# Filtring df with more conditions (OR / |)

df.filter((col('Name') == 'Finance') | (col('Name') == 'Sales') | (col('DepartmentID') == 12)).show(truncate = False)
+------------+----------------+------------------------------------+-------------------+
|DepartmentID|Name            |GroupName                           |ModifiedDate       |
+------------+----------------+------------------------------------+-------------------+
|3           |Sales           |Sales and Marketing                 |2008-04-30 00:00:00|
|10          |Finance         |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control|Quality Assurance                   |2008-04-30 00:00:00|
+------------+----------------+------------------------------------+-------------------+

# # Filtring df with more conditions (OR / |)

df.filter(col('GroupName') == 'Quality Assurance').show(truncate = False)
+------------+-----------------+-----------------+-------------------+
|DepartmentID|Name             |GroupName        |ModifiedDate       |
+------------+-----------------+-----------------+-------------------+
|12          |Document Control |Quality Assurance|2008-04-30 00:00:00|
|13          |Quality Assurance|Quality Assurance|2008-04-30 00:00:00|
+------------+-----------------+-----------------+-------------------+

# # Filtring df combining & and | # And e OR #

df.filter((col('GroupName') == "Quality Assurance")  | (col('Name') == "Sales") | (col('DepartmentID') == 10)).show(truncate = False)
+------------+-----------------+------------------------------------+-------------------+
|DepartmentID|Name             |GroupName                           |ModifiedDate       |
+------------+-----------------+------------------------------------+-------------------+
|3           |Sales            |Sales and Marketing                 |2008-04-30 00:00:00|
|10          |Finance          |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control |Quality Assurance                   |2008-04-30 00:00:00|
|13          |Quality Assurance|Quality Assurance                   |2008-04-30 00:00:00|
+------------+-----------------+------------------------------------+-------------------+

# Concatenate Columns without space

df.withColumn("DepartmentID + GroupName", concat('DepartmentID','GroupName')).show()
+------------+--------------------+--------------------+-------------------+------------------------+
|DepartmentID|                Name|           GroupName|       ModifiedDate|DepartmentID + GroupName|
+------------+--------------------+--------------------+-------------------+------------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:00|    1Research and Dev...|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:00|    2Research and Dev...|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:00|    3Sales and Marketing|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:00|    4Sales and Marketing|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:00|    5Inventory Manage...|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:00|    6Research and Dev...|
|           7|          Production|       Manufacturing|2008-04-30 00:00:00|          7Manufacturing|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:00|          8Manufacturing|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:00|    9Executive Genera...|
|          10|             Finance|Executive General...|2008-04-30 00:00:00|    10Executive Gener...|
|          11|Information Services|Executive General...|2008-04-30 00:00:00|    11Executive Gener...|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:00|     12Quality Assurance|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:00|     13Quality Assurance|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:00|    14Executive Gener...|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:00|    15Inventory Manag...|
|          16|           Executive|Executive General...|2008-04-30 00:00:00|    16Executive Gener...|
+------------+--------------------+--------------------+-------------------+------------------------+

# Concatenate Columns with space

df.withColumn("DepartmentID + GroupName", concat_ws(' ', 'DepartmentID','GroupName')).show()
+------------+--------------------+--------------------+-------------------+------------------------+
|DepartmentID|                Name|           GroupName|       ModifiedDate|DepartmentID + GroupName|
+------------+--------------------+--------------------+-------------------+------------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:00|    1 Research and De...|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:00|    2 Research and De...|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:00|    3 Sales and Marke...|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:00|    4 Sales and Marke...|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:00|    5 Inventory Manag...|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:00|    6 Research and De...|
|           7|          Production|       Manufacturing|2008-04-30 00:00:00|         7 Manufacturing|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:00|         8 Manufacturing|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:00|    9 Executive Gener...|
|          10|             Finance|Executive General...|2008-04-30 00:00:00|    10 Executive Gene...|
|          11|Information Services|Executive General...|2008-04-30 00:00:00|    11 Executive Gene...|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:00|    12 Quality Assurance|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:00|    13 Quality Assurance|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:00|    14 Executive Gene...|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:00|    15 Inventory Mana...|
|          16|           Executive|Executive General...|2008-04-30 00:00:00|    16 Executive Gene...|
+------------+--------------------+--------------------+-------------------+------------------------+

# Alter type of Column

df.show(truncate = False)
+------------+--------------------------+------------------------------------+-------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate       |
+------------+--------------------------+------------------------------------+-------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00|
+------------+--------------------------+------------------------------------+-------------------+

# Alter type of Column

df.printSchema()
root
 |-- DepartmentID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

# Alter type of Metada of the Column

#df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)

df.show()
+------------+--------------------+--------------------+-------------------+
|DepartmentID|                Name|           GroupName|       ModifiedDate|
+------------+--------------------+--------------------+-------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:00|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:00|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:00|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:00|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:00|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:00|
|           7|          Production|       Manufacturing|2008-04-30 00:00:00|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:00|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:00|
|          10|             Finance|Executive General...|2008-04-30 00:00:00|
|          11|Information Services|Executive General...|2008-04-30 00:00:00|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:00|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:00|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:00|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:00|
|          16|           Executive|Executive General...|2008-04-30 00:00:00|
+------------+--------------------+--------------------+-------------------+

# Alter type of Column
# Didn't change yet because we didn't put variable "" df = df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)"  bfore the execution of code,
# only when we put this everything will change

df.printSchema()
root
 |-- DepartmentID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

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
+-------------------+----------------------------+
|ModifiedDate       |difference between two dates|
+-------------------+----------------------------+
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
|2008-04-30 00:00:00|5224                        |
+-------------------+----------------------------+

#The below example returns the months between two dates 

df.select(col("ModifiedDate"), 
    months_between(current_timestamp(),col("ModifiedDate")).alias("months_between")  
  ).show()

#round(col("score")
+-------------------+--------------+
|       ModifiedDate|months_between|
+-------------------+--------------+
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
|2008-04-30 00:00:00|  171.66045699|
+-------------------+--------------+

#df.select(col("ModifiedDate"), 
 #   months_between(current_timestamp(),round(col("ModifiedDate")).alias("months_between"))  
  #).show()





#df.withColumn("ModifiedDate", round(col("ModifiedDate"))).show()
#Using round numbers ('Arredontar numeros')

df.select("*",round(col("DepartmentID")).alias("Teste")).show(truncate=False)
+------------+--------------------------+------------------------------------+-------------------+-----+
|DepartmentID|Name                      |GroupName                           |ModifiedDate       |Teste|
+------------+--------------------------+------------------------------------+-------------------+-----+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00|1    |
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00|2    |
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00|3    |
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00|4    |
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00|5    |
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00|6    |
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00|7    |
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00|8    |
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00|9    |
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00|10   |
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00|11   |
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00|12   |
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00|13   |
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00|14   |
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00|15   |
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00|16   |
+------------+--------------------------+------------------------------------+-------------------+-----+

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
+------------+-----------+--------------------+-------------------+----+
|DepartmentID|       Name|           GroupName|       ModifiedDate|year|
+------------+-----------+--------------------+-------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|2008|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|2008|
+------------+-----------+--------------------+-------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+-------+
|DepartmentID|       Name|           GroupName|       ModifiedDate|quarter|
+------------+-----------+--------------------+-------------------+-------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|      2|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|      2|
+------------+-----------+--------------------+-------------------+-------+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+-----+
|DepartmentID|       Name|           GroupName|       ModifiedDate|month|
+------------+-----------+--------------------+-------------------+-----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|    4|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|    4|
+------------+-----------+--------------------+-------------------+-----+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+----+
|DepartmentID|       Name|           GroupName|       ModifiedDate|week|
+------------+-----------+--------------------+-------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|  18|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|  18|
+------------+-----------+--------------------+-------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+---------+
|DepartmentID|       Name|           GroupName|       ModifiedDate|dayofyear|
+------------+-----------+--------------------+-------------------+---------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|      121|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|      121|
+------------+-----------+--------------------+-------------------+---------+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+-----------+
|DepartmentID|       Name|           GroupName|       ModifiedDate|dayofmonth |
+------------+-----------+--------------------+-------------------+-----------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|         30|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|         30|
+------------+-----------+--------------------+-------------------+-----------+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+---------+
|DepartmentID|       Name|           GroupName|       ModifiedDate|dayofweek|
+------------+-----------+--------------------+-------------------+---------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|        4|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|        4|
+------------+-----------+--------------------+-------------------+---------+
only showing top 2 rows

# Extract Hour, Minutes and Seconds


#(df_date
#.withColumn("to_timestamp",f.to_timestamp("input_date"))
df.withColumn("hour", hour("ModifiedDate")).show(2)
df.withColumn("minute",minute("ModifiedDate")).show(2)
df.withColumn("second",second("ModifiedDate")).show(2)
+------------+-----------+--------------------+-------------------+----+
|DepartmentID|       Name|           GroupName|       ModifiedDate|hour|
+------------+-----------+--------------------+-------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|   0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|   0|
+------------+-----------+--------------------+-------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+------+
|DepartmentID|       Name|           GroupName|       ModifiedDate|minute|
+------------+-----------+--------------------+-------------------+------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|     0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|     0|
+------------+-----------+--------------------+-------------------+------+
only showing top 2 rows

+------------+-----------+--------------------+-------------------+------+
|DepartmentID|       Name|           GroupName|       ModifiedDate|second|
+------------+-----------+--------------------+-------------------+------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:00|     0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:00|     0|
+------------+-----------+--------------------+-------------------+------+
only showing top 2 rows

 
