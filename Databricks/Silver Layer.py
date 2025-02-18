# Databricks ETL Transformation Script for Silver Layer
# Reads raw data from the Bronze Layer in Azure Data Lake, applies transformations, and writes the processed data to the Silver Layer.

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure Spark to authenticate with Azure Data Lake Storage
# Add the configuration code here

# Load data from the Bronze Layer
df_cal = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Calendar")
df_cust = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Customers")
df_prod_cat = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Product_Categories")
df_prod = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Products")
df_ret = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Returns")
df_sal = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Sales*")
df_prod_ter = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Territories")
df_prod_sub = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/Product_Subcategories")

# Transform Calendar Data
df_cal = df_cal.withColumn('Month', month(col('Date'))).withColumn('Year', year(col('Date')))
df_cal.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Calendar").save()

# Transform Customer Data
df_cust = df_cust.withColumn("Name", concat_ws(" ", col("prefix"), col("FirstName"), col("LastName")))
df_cust.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Customers").save()

# Transform Product Subcategories
df_prod_sub.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Product_Subcategories").save()

# Transform Product Data
df_prod = df_prod.withColumn("ProductSKU", split(col("ProductSKU"), "-").getItem(0)).withColumn("ProductName", split(col("ProductName"), " ").getItem(0))
df_prod.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Products").save()

# Load Returns Data to Silver Layer
df_ret.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Returns").save()

# Load Product Territories Data to Silver Layer
df_prod_ter.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Product_Territories").save()

# Transform Sales Data
df_sal = df_sal.withColumn('StockDate', to_timestamp('StockDate'))\
               .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))\
               .withColumn('TotalQuantity', col('OrderLineItem') * col('OrderQuantity'))
df_sal.write.format("parquet").mode("append").option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Sales").save()

