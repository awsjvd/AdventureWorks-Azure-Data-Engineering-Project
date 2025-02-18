=====These views will be created in the silver Layer-===========

----------------View calendar-------------

Create view gold.calendar
as 
Select 
      * from 
      OPENROWSET(
                BULK'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Calendar',
                FORMAT = 'Parquet'

      ) as Calendar_Query

----------------View Customer-------------

Create view gold.csutomer
as 
Select 
      * from 
      OPENROWSET(
                BULK'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Customers',
                FORMAT = 'Parquet'

      ) as Customer_Query

---------------View Products------------

Create view gold.products
as 
Select 
      * from 
      OPENROWSET(
                BULK'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Products',
                FORMAT = 'Parquet'

      ) as Product_Query

--------------Product SubCategories----------------

Create view gold.ProductSubcategories
as 
Select 
      * from 
      OPENROWSET(
                BULK'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_ProductSubcategories',
                FORMAT = 'Parquet'

      ) as ProductSubcategories_Query

------------------ Product Terrotries----------

CREATE VIEW gold.productterritories
AS 
SELECT * 
FROM 
    OPENROWSET(
        BULK 'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_ProductTerritories',
        FORMAT = 'PARQUET'
    ) AS ProductTerritories_Query;



      
-------view returns -----------------

Create view gold.returns
as 
Select 
      * from 
      OPENROWSET(
                BULK'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Returns',
                FORMAT = 'Parquet'

      ) as returns_Query

---------------view sales ---------------

Create view gold.sales
as 
Select 
      * from 
      OPENROWSET(
                BULK'https://azdedatalake.dfs.core.windows.net/silver/AdventureWorks_Sales',
                FORMAT = 'Parquet'

      ) as sales_Query


     
      
      
      
      
      
