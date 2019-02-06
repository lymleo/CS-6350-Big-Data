# Databricks notebook source
#dataset http://www.utdallas.edu/~axn112530/cs6350/data/flight-data/csv/2015-summary.csv
#Create a dataframe using the above file with following options:
#- "inferSchema" set to "true"
#- "header" set to "true"

formatPackage = "csv" if sc.version > '1.6' else "com.databricks.spark.csv"
flight_df = sqlContext.read.format(formatPackage).\
  option("header", "true").\
  option("inferSchema", "true").\
  load("/FileStore/tables/g0fun5zm1508743280505/2015_summary-ebaee.csv")


# COMMAND ----------

#Display the contents of the dataframe
flight_df.show()
flight_df.printSchema()

# COMMAND ----------

#Display the first 3 rows of the dataframe
flight_df.show(3)

# COMMAND ----------

#Sort the dataframe on the count field in a descending order
from pyspark.sql.functions import desc
flight_df1 = flight_df.sort(desc("count"))
flight_df.show()

# COMMAND ----------

#Display the summary statistics of each of the columns
flight_df1.describe(["DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count"]).show()

# COMMAND ----------

#Find out the maximum value of the counts field.
from pyspark.sql.functions import max
flight_df1.groupby().max("count").show()

# COMMAND ----------

#We would like to find the top 5 countries that have the largest number of incoming flights
#Hint: group by the DEST_COUNTRY_NAME field and generate a count of the flights by each group and then sort this count in a descending order and take top 5.
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
flight_df1.groupby("DEST_COUNTRY_NAME").agg(F.sum("count")).sort(desc("sum(count)")).show(5, truncate = False)

# COMMAND ----------

#more files 
#http://www.utdallas.edu/~axn112530/cs6350/data/orders/NW-Orders-01.csv
#http://www.utdallas.edu/~axn112530/cs6350/data/orders/NW-Order-Details.csv
#/FileStore/tables/kzh3ny8n1508748992846/NW_Orders_01-18e61.csv
#/FileStore/tables/kzh3ny8n1508748992846/NW_Order_Details-dac32.csv

od_df = sqlContext.read.format(formatPackage).\
  option("header", "true").\
  option("inferSchema", "true").\
  load("/FileStore/tables/kzh3ny8n1508748992846/NW_Orders_01-18e61.csv")
dt_df = sqlContext.read.format(formatPackage).\
  option("header", "true").\
  option("inferSchema", "true").\
  load("/FileStore/tables/kzh3ny8n1508748992846/NW_Order_Details-dac32.csv")

od_df.show()
dt_df.show()

# COMMAND ----------

#Find out the count of orders placed by each customer and then return the top 5 customers with the highest count of orders.
dt_df.registerTempTable("df")
tbl = sqlContext.sql("SELECT OrderID, SUM(Qty) FROM df GROUP by OrderID ORDER BY sum(Qty) desc ")
tbl.show(5, truncate= False)

# COMMAND ----------

#Another method for this question
dt_df.groupby("OrderID").agg(F.sum("Qty")).sort(desc("sum(Qty)")).show(5, truncate= False)

# COMMAND ----------

#Find out the count of orders placed by each customer and then return the top 5 customers with the highest count of orders.
dt1_df = dt_df.groupby("OrderID").agg(F.sum("Qty"))

joined_df = od_df.select("CustomerID", "OrderID").join(dt1_df, "OrderID", "inner").sort(desc("sum(Qty)"))
#get sorted count by CutomerID
data = joined_df.groupby("CustomerID").agg(F.sum("sum(Qty)")).sort(desc("sum(sum(Qty))"))
#rename columns
data = data.select(F.col("CustomerID").alias("CustomerID"), F.col("sum(sum(Qty))").alias("Qty"))
data.show(5)


# COMMAND ----------

#Join the orders and orderDetails data on the orderID column and display the results.
joined1_df =  od_df.join(dt_df, "OrderID", "inner")
joined1_df.show()


# COMMAND ----------

#Join the orders and orderDetails data on the orderID column and then group the data byShipCountry field 
#and find the sum of quantity purchased by each country. 
#Then sort by the sum of quantity field and list the top 10 countries.
od_df.join(dt_df, "OrderID").groupby("ShipCountry").agg(F.sum("Qty")).orderBy("sum(Qty)", ascending = False).show(10)
