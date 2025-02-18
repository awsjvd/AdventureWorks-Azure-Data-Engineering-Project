CREATE EXTERNAL DATA SOURCE silver_data_lake
WITH 
(
    LOCATION = 'https://<STORAGE_ALIAS>.dfs.core.windows.net/silver',
    CREDENTIAL = cred_aws
);

---------------- View Calendar ----------------
CREATE VIEW gold.calendar AS 
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_Calendar',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS Calendar_Query;

---------------- View Customer ----------------
CREATE VIEW gold.customer AS 
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_Customers',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS Customer_Query;

---------------- View Products ----------------
CREATE VIEW gold.products AS 
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_Products',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS Product_Query;

---------------- View Product Subcategories ----------------
CREATE VIEW gold.product_subcategories AS  
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_ProductSubcategories',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS ProductSubcategories_Query;

---------------- View Product Territories ----------------
CREATE VIEW gold.product_territories AS  
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_ProductTerritories',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS ProductTerritories_Query;

---------------- View Returns ----------------
CREATE VIEW gold.returns AS 
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_Returns',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS Returns_Query;

---------------- View Sales ----------------
CREATE VIEW gold.sales AS 
SELECT * 
FROM OPENROWSET(
    BULK 'AdventureWorks_Sales',
    DATA_SOURCE = silver_data_lake,
    FORMAT = 'PARQUET'
) AS Sales_Query;
