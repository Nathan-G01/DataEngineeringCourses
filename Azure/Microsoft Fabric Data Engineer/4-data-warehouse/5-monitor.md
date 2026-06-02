# Monitor a Microoft Fabric data warehouse

## Monitor Capacity Metrics  

Microsoft Fabric’s **capacity-based licensing** determines resource availability, with costs measured in **Capacity Units (CUs)**. Data warehouse workloads **consume CUs** through **queries, data read/write operations, and OneLake storage interactions**, making monitoring crucial for cost management.  

The **Microsoft Fabric Capacity Metrics app** provides **real-time capacity monitoring**, allowing administrators to **filter warehouse-specific activity**, track **CU consumption trends**, and detect **throttling issues**. These insights help optimize **resource allocation and licensing**, ensuring efficient **performance and cost control**.

## Monitor Current Activity  

Microsoft Fabric **Dynamic Management Views (DMVs)** provide real-time insights into **data warehouse activity**, helping administrators monitor and optimize performance. Key DMVs include:  

- **sys.dm_exec_connections** – Tracks active connections.  
- **sys.dm_exec_sessions** – Monitors authenticated sessions.  
- **sys.dm_exec_requests** – Displays active queries and execution details.  

By querying these DMVs, users can **identify long-running queries**, detect **performance bottlenecks**, and optimize workloads. The sample query **joins these views** to show **active queries, session details, and execution time**, helping diagnose and improve query performance.

## Monitor Queries  

Microsoft Fabric **query insights** provides **historical and aggregated query data**, helping optimize **query performance**. Key views include:  

- **queryinsights.exec_requests_history** – Tracks completed SQL queries.  
- **queryinsights.long_running_queries** – Identifies slow-running queries.  
- **queryinsights.frequently_run_queries** – Lists commonly executed queries.  

Using these views, users can analyze **recent queries**, detect **performance bottlenecks**, and identify **high-frequency or slow queries**. Queries are **parameterized** for consistent tracking. Insights help refine **query optimization strategies** and improve **warehouse efficiency**.