# SILVER LAYER SCRIPT
# This script extracts, transforms, and loads data into the Silver Layer in Azure Data Lake.

from pyspark.sql.functions import *
from pyspark.sql.types import *

# DATA ACCESS USING APPLICATION AUTHENTICATION
# Securely retrieving credentials using Databricks Secret Scope.
# This ensures that sensitive information is not exposed in the script.
service_credential = dbutils.secrets.get(scope="<your-secret-scope>", key="<your-service-credential-key>")

# Configure Spark to authenticate with Azure Data Lake Storage using OAuth
spark.conf.set("fs.azure.account.auth.type.<your-storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<your-storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<your-storage-account>.dfs.core.windows.net", "<your-app-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<your-storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<your-storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<your-directory-id>/oauth2/token")

# DATA LOADING
# Reading data from the Bronze layer in Azure Data Lake

df_cal = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Calendar")

df_cust = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Customers")

df_prod_cat = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Product_Categories")

df_prod = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Products")

df_ret = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Returns")

df_sal = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Sales*")

df_prod_ter = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/AdventureWorks_Territories")

df_prod_sub = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/Product_Subcategories")

# DATA TRANSFORMATIONS

# Extracting Month and Year from Date in Calendar Data
df_cal = df_cal.withColumn('Month', month(col('Date')))\
               .withColumn('Year', year(col('Date')))

# Writing transformed Calendar data to the Silver Layer
df_cal.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Calendar") \
    .save()

# Combining name fields in Customer Data
df_cust = df_cust.withColumn("Name", concat_ws(" ", col("prefix"), col("FirstName"), col("LastName")))

# Writing transformed Customer data to the Silver Layer
df_cust.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Customers") \
    .save()

# Writing Product Subcategories data to the Silver Layer
df_prod_sub.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Product_Subcategories") \
    .save()

# Transforming Product Data: Extracting SKU prefix and first word of product name
df_prod = df_prod.withColumn("ProductSKU", split(col("ProductSKU"), "-").getItem(0))\
                 .withColumn("ProductName", split(col("ProductName"), " ").getItem(0))

# Writing transformed Product data to the Silver Layer
df_prod.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Products") \
    .save()

# Writing Returns data to the Silver Layer
df_ret.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Returns") \
    .save()

# Writing Product Territories data to the Silver Layer
df_prod_ter.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Product_Territories") \
    .save()

# Transforming Sales Data
df_sal = df_sal.withColumn('StockDate', to_timestamp('StockDate'))\
               .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))\
               .withColumn('TotalQuantity', col('OrderLineItem') * col('OrderQuantity'))

# Writing transformed Sales data to the Silver Layer
df_sal.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<your-storage-account>.dfs.core.windows.net/Sales") \
    .save()

print("Data successfully processed and saved in the Silver Layer.")
