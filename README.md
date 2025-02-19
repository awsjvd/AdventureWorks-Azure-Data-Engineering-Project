# Azure Data Engineering Project

## Project Overview  
This project demonstrates an **end-to-end Azure Data Engineering pipeline** using the **Adventure Works dataset**. The dataset was downloaded from **Kaggle** and stored in a **GitHub repository** before being processed through various Azure services to analyze and transform data efficiently.

## Tech Stack  
- **Azure Data Factory** (Data Ingestion)  
- **Azure Data Lake Gen2** (Storage for Bronze, Silver, and Gold layers)  
- **Azure Databricks** (Data Transformation)  
- **Azure Synapse Analytics** (Data Modeling and Querying)  

## Process  
1. **Data Ingestion**  
   - Data from Kaggle was stored in GitHub and then ingested using **Azure Data Factory** into the **Bronze Layer** of **Azure Data Lake Gen2**.  
2. **Data Transformation**  
   - The raw data from the Bronze Layer was processed in **Azure Databricks** to create structured datasets stored in the **Silver Layer**.  
3. **Data Serving & Analytics**  
   - **Azure Synapse Analytics** was used to create schemas, external tables, and views in the **Gold Layer** for data analytics.  

## Flow Diagram  
The data pipeline follows a structured **Bronze-Silver-Gold** architecture. The detailed **data flow diagram** can be viewed here:  

![Data Flow](https://github.com/awsjvd/Azure-Data-Engineering-Project/blob/main/Flow%20Diagram/Flow%20Diagram.png)  

## Insights  
- **Data Optimization**  
  - Structured processing from raw to refined data using a **layered approach**.  
- **Scalability**  
  - Azure services ensure **scalability and high performance** for large datasets.  
- **Integration with Synapse**  
  - Ready-to-use structured tables for seamless **BI & reporting**.  

## Future Enhancements  
- Automate pipeline execution using **Azure Data Factory triggers**.  
- Implement **CI/CD with Azure DevOps** for seamless deployment.  
- Leverage **Power BI** for interactive dashboards.  


