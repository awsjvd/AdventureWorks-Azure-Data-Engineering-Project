-- Creating a database-scoped credential to authenticate with the external storage
CREATE DATABASE SCOPED CREDENTIAL cred_aws
WITH 
   IDENTITY = 'Managed Identity';

-- Creating an external data source pointing to the Silver storage location
CREATE EXTERNAL DATA SOURCE source_silver
WITH 
(
    LOCATION = 'https://<your_storage_account>.dfs.core.windows.net/silver',
    CREDENTIAL = cred_aws
);

-- Creating an external data source pointing to the Gold storage location
CREATE EXTERNAL DATA SOURCE source_gold
WITH 
(
    LOCATION = 'https://<your_storage_account>.dfs.core.windows.net/gold',
    CREDENTIAL = cred_aws
);

-- Defining an external file format for Parquet files with Snappy compression
CREATE EXTERNAL FILE FORMAT file_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- Creating external tables that map to the data stored in the external data sources

CREATE External Table gold.tbl_sales
WITH 
(
   LOCATION = 'External_Sales_Table',
   DATA_SOURCE = source_gold,
   File_FORMAT = file_parquet
)
AS
SELECT * FROM gold.sales;

CREATE External Table gold.tbl_Customer
WITH 
(
   LOCATION = 'External_Customer_Table',
   DATA_SOURCE = source_gold,
   File_FORMAT = file_parquet
)
AS
SELECT * FROM gold.customer;

CREATE External Table gold.tbl_Products
WITH 
(
   LOCATION = 'External_Products_Table',
   DATA_SOURCE = source_gold,
   File_FORMAT = file_parquet
)
AS
SELECT * FROM gold.products;

CREATE External Table gold.tbl_productsubcategories
WITH 
(
   LOCATION = 'External_productsubcategories_Table',
   DATA_SOURCE = source_gold,
   File_FORMAT = file_parquet
)
AS
SELECT * FROM gold.productsubcategories;

CREATE External Table gold.tbl_ProductTerritories
WITH 
(
   LOCATION = 'External_ProductTerritories_Table',
   DATA_SOURCE = source_gold,
   File_FORMAT = file_parquet
)
AS
SELECT * FROM gold.ProductTerritories;

CREATE External Table gold.tbl_Returns
WITH 
(
   LOCATION = 'External_Returns_Table',
   DATA_SOURCE = source_gold,
   File_FORMAT = file_parquet
)
AS
SELECT * FROM gold.returns;

