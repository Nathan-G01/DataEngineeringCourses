# Query a data warehouse in Microsoft Fabric

## Introduction  

Microsoft Fabric Data Warehouse is a comprehensive platform for **data storage, analytics, and AI**, powered by **Synapse Analytics** for advanced query processing and full **T-SQL** support. It enables **business intelligence** by allowing users to extract insights from large datasets.  

A **star schema** is a common data warehouse design, organizing data into **fact tables** (quantitative data) and **dimension tables** (descriptive attributes). This structure improves query performance and simplifies analysis.  

Fact tables store raw transactional data, while dimension tables provide context, answering **who, what, where, when, and why** questions. Understanding schema design is crucial for effective **data querying and reporting** in Fabric.

## Query Data  

Microsoft Fabric’s **T-SQL** enables efficient querying of **fact and dimension tables** for data analysis, similar to SQL Server or Azure SQL Database. **Aggregations** are performed using **JOIN and GROUP BY**, linking fact tables with dimension tables to generate insights like **sales totals per quarter or city**.  

**Snowflake schemas** require multiple joins, such as linking **product categories** via an additional dimension table. **Ranking functions** (**ROW_NUMBER, RANK, DENSE_RANK, NTILE**) allow partitioning and ranking within categories, useful for analyzing sales performance.  

For **data exploration**, **COUNT and APPROX_COUNT_DISTINCT** retrieve record counts, with the latter providing a faster **approximate count** using **HyperLogLog** with a **2% error margin**. These functions optimize queries for large-scale data warehouses while balancing accuracy and performance.

## Use the SQL Query Editor  

Microsoft Fabric’s **SQL query editor** supports **T-SQL** for querying, managing, and manipulating data in a warehouse. It includes features like **IntelliSense** and **debugging** to assist with development.  

Users can launch the editor from **My workspace**, automatically connecting to the warehouse. Queries can be executed, with results displayed in the **Results section**, limited to **10,000 rows**.  

Results can be **exported as an Excel file** or saved as **a view or a table** for reuse. The query editor simplifies data management by enabling **table creation, row insertion, and permission handling** within Fabric.

## Explore the Visual Query Editor  

Microsoft Fabric’s **visual query editor** provides an intuitive **graphical interface** for building SQL queries, making it easier for users unfamiliar with SQL syntax. Users can **drag and drop tables**, visually define relationships, and automatically generate **T-SQL queries**.  

This tool democratizes data access, allowing **non-technical users** to extract insights without writing complex queries. Queries can be **saved as tables or views** for reuse, similar to the SQL query editor.  

By simplifying query creation, the visual editor speeds up **decision-making** and enables broader organizational use of data, making analytics more accessible.

## Use Client Tools to Query a Warehouse  

Microsoft Fabric **supports SQL Server Management Studio (SSMS)** for querying its data warehouse, providing a familiar environment for SQL users. To connect, users **copy the SQL connection string** from Fabric, paste it into **SSMS**, and authenticate using **Microsoft Entra ID (formerly Azure AD)**.  

Once connected, SSMS displays **tables and views** for querying. **SQL authentication is not supported**, but **third-party tools** can connect using **ODBC or OLE DB drivers** with Entra ID authentication. This allows flexible integration with various **BI and data analysis tools** beyond Fabric’s native environment.