# Load data into a Microsoft Fabric data warehouse

## Introduction  

Microsoft Fabric Data Warehouse is a comprehensive platform for managing, analyzing, and storing large volumes of structured and semi-structured data. It integrates Synapse Analytics for enhanced query processing and supports full T-SQL capabilities like an enterprise data warehouse. Unlike Synapse’s dedicated SQL pool, Fabric warehouses store data in Microsoft OneLake using the Parquet format. The ETL process (Extract, Transform, Load) plays a crucial role in data preparation, involving extraction from sources, transformation for consistency, and loading into fact/dimension tables. Optimizations post-load enhance performance, ensuring efficient data processing. Future units will explore data-loading methods in detail.

## Explore Data Load Strategies  

Microsoft Fabric offers multiple data loading strategies to ensure efficient data integration and analytics. Data ingestion brings raw data into a central repository, while data loading transforms and stores it for reporting. Staging areas act as intermediate storage, reducing the impact on performance.  

There are two primary data loading methods: **full load**, which refreshes all data, and **incremental load**, which applies only new or changed data. Incremental loading is more efficient but requires tracking mechanisms.  

Data warehouses use **surrogate keys** for internal consistency and **business keys** for traceability with source systems. Dimension tables provide descriptive context (e.g., customer details), while fact tables store measurable events (e.g., sales transactions).  

**Slowly Changing Dimensions (SCDs)** manage historical changes in data, with **Type 2** retaining full history. Change tracking techniques like **Change Data Capture (CDC)** and **triggers** help detect and manage data updates efficiently, ensuring accuracy and consistency in data warehouse operations.

## Use Data Pipelines to Load a Warehouse  

Microsoft Fabric’s Warehouse integrates data ingestion tools for large-scale data loading through coding and no-code experiences. **Data pipelines**, built on Azure Data Factory, enable scalable data integration, supporting **ETL (Extract, Transform, Load) and ELT (Extract, Load, Transform) workflows**.  

Users can create pipelines via the workspace or warehouse asset, choosing from three options: **manual pipeline creation, the Copy Data assistant, or predefined templates**. The **Copy Data assistant** guides users through selecting sources, mapping data, and configuring settings.  

Pipelines can be **scheduled** for automated execution, making them ideal for scheduled data workflows and multi-source integration. Their low-code interface simplifies setup, transformation, and integration for business intelligence and analytics.

## Load Data Using T-SQL  

Microsoft Fabric’s Warehouse supports T-SQL for data manipulation, enabling complex queries like filtering, aggregating, and joining tables. The **COPY statement** is the primary method for importing data from Azure storage, supporting CSV and Parquet formats. It allows flexible configurations like skipping headers and storing rejected rows separately for quality control.  

Bulk data loading is optimized using **wildcards and multiple file imports**. Data can also be loaded from **other warehouses and lakehouses** using **CREATE TABLE AS SELECT (CTAS) and INSERT...SELECT**, allowing analysts to merge datasets across the same workspace. Cross-database queries within Fabric simplify data integration for large-scale analytics.

## Load and Transform Data with Dataflow Gen2  

**Dataflow Gen2** simplifies data ingestion and transformation using **Power Query**, reducing setup steps. It integrates with **data pipelines** to load data into a **lakehouse, warehouse, or Power BI dataset**.  

Users can **import various file types**, clean and reshape data, and apply transformations. **Copilot** assists by automating transformations based on natural language input.  

Dataflows separate **ETL logic** from storage, supporting multiple **destinations** like **Azure SQL Database, Warehouse, and Synapse Analytics**. Users can choose between **Append (adding rows)** or **Replace (overwriting data)** update methods.  

Once configured, **publishing a dataflow** enables execution manually or on a schedule, making transformations reusable and streamlining data workflows.

