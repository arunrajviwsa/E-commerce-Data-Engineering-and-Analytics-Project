# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------
#Load data
# File location and type
file_location = "/FileStore/tables/outputdata.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

df.write.format("parquet").saveAsTable("productTransactions")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# Query the table
result = spark.sql("SELECT * FROM productTransactions")
result.show()

# COMMAND ----------

df_grouped = df.groupBy("product_category","Product_Subcategory").sum("Total_Sales")

# Show the result
df_grouped.show()

# COMMAND ----------
#Calculateaggregates such as total sales per category and customer segmentation.
#Analyzethe distribution of sales across different product categories andsubcategories.
#Identify the top 5 best-selling products and the top 5 least-selling products.
#Visualize the monthly sales trends and identify any seasonal patterns.
#Analyze customer purchasing behavior by location and payment method.

from pyspark.sql import functions as F
df_grouped = df.groupBy("product_category","Product_Subcategory").agg(F.sum("Total_Sales").alias("TotalSales"))
df_groupedMonth = df.groupBy("Transaction_month").agg(F.sum("Total_Sales").alias("TotalSales"))
df_groupedLocation = df.groupBy("Customer_Location","payment_Method").agg(F.sum("Total_Sales").alias("TotalSales"))
df_ordered = df_grouped.orderBy(F.col("TotalSales").desc())
df_orderedLeast = df_grouped.orderBy(F.col("TotalSales").asc())
df_orderedmonth = df_groupedMonth.orderBy(F.col("Transaction_month").asc())
df_orderedLeast.show(5)
df_ordered.show(5)
df_orderedmonth.show()

df_groupedLocation.show()
df_ordered.show(1)

# COMMAND ----------

# Remove duplicate rows
df_cleaned = df.dropDuplicates()
df_cleaned = df.dropna()
# Fill missing values with a specific value
df_cleaned = df.fillna({"Transaction_Month": 0, "product_category": "Unknown"})

# Fill missing values with mean of a column (numeric)
mean_value = df.select(F.mean("Quantity_Sold")).first()[0]
df_cleaned = df.fillna({"Quantity_Sold": mean_value})

df_filtered = df.filter(F.col("Transaction_Month") > 10)

# Show the DataFrame after filtering outliers
df_filtered.show()
# Show the cleaned DataFrame
df_cleaned.show()

# COMMAND ----------
#Write queries to extract insights such as top-performing products, sales trends, and customer behavior.
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC select * from (SELECT 
# MAGIC     Product_category,
# MAGIC     Product_Subcategory,
# MAGIC     SUM(Total_Sales) AS total_sales,
# MAGIC     ROW_NUMBER() OVER ( ORDER BY SUM(Total_Sales) DESC) AS rank
# MAGIC FROM producttransactions
# MAGIC
# MAGIC GROUP BY Product_category, Product_Subcategory
# MAGIC
# MAGIC order by Total_Sales desc) where rank=1
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     Customer_Location,payment_Method,
# MAGIC     sum(Total_Sales) AS total_sales,
# MAGIC     ROW_NUMBER() OVER ( ORDER BY SUM(Total_Sales) DESC) AS salepatternrank
# MAGIC FROM producttransactions
# MAGIC
# MAGIC
# MAGIC GROUP BY Customer_Location,payment_Method
# MAGIC
# MAGIC
# MAGIC order by Total_Sales desc
