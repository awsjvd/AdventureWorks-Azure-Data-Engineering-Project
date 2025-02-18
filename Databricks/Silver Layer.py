# Databricks ETL Transformation Script for Silver Layer
# This script reads raw CSV data from the Bronze Layer in Azure Data Lake,
# applies transformations, and writes the processed data as Parquet files to the Silver Layer.

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------------------------------------------------------------------------------------
# Configure Spark to authenticate with Azure Data Lake Storage
# ------------------------------------------------------------------------------------
# Add the configuration code here

# ------------------------------------------------------------------------------------
# LOAD DATA FROM THE BRONZE LAYER
# ------------------------------------------------------------------------------------
# Each dataset is loaded from a specific container in the Bronze Layer.
# The data is stored in CSV format, with headers and automatic schema inference enabled.

# Load Calendar Data
df_cal = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Calendar")
# Load Customer Data
df_cust = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Customers")
# Load Product Categories Data
# Contains information about product categories.
df_prod_cat = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Product_Categories")
# Load Product Data
# Holds detailed product information
df_prod = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Products")
# Load Returns Data
# Contains records of returned products.
df_ret = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Returns")
# Load Sales Data
# Captures sales transactions.
df_sal = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Sales*")
# Load Product Territories Data
# Contains geographical information related to products.
df_prod_ter = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Territories")
# Load Product Subcategories Data
# Provides additional details on product subcategories.
df_prod_sub = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/Product_Subcategories")

# ------------------------------------------------------------------------------------
# DATA TRANSFORMATIONS AND WRITE TO SILVER LAYER
# ------------------------------------------------------------------------------------

# Transform Calendar Data:
# - Extract the month and year from the 'Date' column.
df_cal = df_cal.withColumn('Month', month(col('Date'))) \
               .withColumn('Year', year(col('Date')))
# Write the transformed Calendar Data to the Silver Layer in Parquet format.
df_cal.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Calendar") \
    .save()

# Transform Customer Data:
# - Concatenate 'prefix', 'FirstName', and 'LastName' into a single 'Name' column.
df_cust = df_cust.withColumn("Name", concat_ws(" ", col("prefix"), col("FirstName"), col("LastName")))
# Write the transformed Customer Data to the Silver Layer.
df_cust.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Customers") \
    .save()

# Write Product Subcategories Data directly to the Silver Layer.
df_prod_sub.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Product_Subcategories") \
    .save()

# Transform Product Data:
# - Split 'ProductSKU' on '-' and retain the first segment.
# - Split 'ProductName' on spaces and retain the first segment.
df_prod = df_prod.withColumn("ProductSKU", split(col("ProductSKU"), "-").getItem(0)) \
                 .withColumn("ProductName", split(col("ProductName"), " ").getItem(0))
# Write the transformed Product Data to the Silver Layer.
df_prod.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Products") \
    .save()

# Write Returns Data to the Silver Layer.
df_ret.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Returns") \
    .save()

# Write Product Territories Data to the Silver Layer.
df_prod_ter.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Product_Territories") \
    .save()

# Transform Sales Data:
# - Convert 'StockDate' to a timestamp.
# - Replace occurrences of 'S' with 'T' in 'OrderNumber'.
# - Calculate 'TotalQuantity' as the product of 'OrderLineItem' and 'OrderQuantity'.
df_sal = df_sal.withColumn('StockDate', to_timestamp('StockDate')) \
               .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T')) \
               .withColumn('TotalQuantity', col('OrderLineItem') * col('OrderQuantity'))
# Write the transformed Sales Data to the Silver Layer.
df_sal.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Sales") \
    .save()

