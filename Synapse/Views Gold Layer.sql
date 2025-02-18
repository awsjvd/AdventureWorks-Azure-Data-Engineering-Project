
---------------- View Calendar ----------------
CREATE VIEW gold.calendar AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Calendar',
    FORMAT = 'PARQUET'
) AS Calendar_Query;

---------------- View Customer ----------------
CREATE VIEW gold.customer AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Customers',
    FORMAT = 'PARQUET'
) AS Customer_Query;

---------------- View Products ----------------
CREATE VIEW gold.products AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Products',
    FORMAT = 'PARQUET'
) AS Product_Query;

---------------- View Product Subcategories ----------------
CREATE VIEW gold.product_subcategories AS  
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_ProductSubcategories',
    FORMAT = 'PARQUET'
) AS ProductSubcategories_Query;

---------------- View Product Territories ----------------
CREATE VIEW gold.product_territories AS  
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_ProductTerritories',
    FORMAT = 'PARQUET'
) AS ProductTerritories_Query;

---------------- View Returns ----------------
CREATE VIEW gold.returns AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Returns',
    FORMAT = 'PARQUET'
) AS Returns_Query;

---------------- View Sales ----------------
CREATE VIEW gold.sales AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Sales',
    FORMAT = 'PARQUET'
) AS Sales_Query;
