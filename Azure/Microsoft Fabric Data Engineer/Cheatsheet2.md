# DP-700 Cheat Sheet: Designing and Implementing Microsoft Azure Data Solutions

## 1. Core Services Overview

| Component                   | Description                                                  |
|----------------------------|--------------------------------------------------------------|
| Azure Synapse Analytics     | Unified analytics service combining data warehouse + Spark   |
| Microsoft Fabric            | SaaS layer over Synapse, Power BI, Data Factory, and Lakehouse |
| Data Lake Gen2              | Hierarchical, scalable blob storage for big data             |
| Data Factory Pipelines      | ETL orchestration; supports mapping, wrangling, and control flow |
| Lakehouse (Fabric)          | Combines Delta format tables with analytics-ready semantics  |

---

## 2. Ingestion Patterns

| Pattern                         | When to Use                                    | Notes                                      |
|----------------------------------|------------------------------------------------|---------------------------------------------|
| COPY INTO (T-SQL)                | Efficient load from blob → Synapse/Fabric DW   | Supports wildcard + SAS + delimiter config |
| Dataflow Gen2                    | Low-code transformation for Lakehouse          | Fabric native                              |
| Pipeline with Parameters         | Ingest from varied sources dynamically         | Use dynamic content and expressions         |
| Event-based Trigger              | React to new files in Blob                     | Must use BlobCreated event trigger          |
| Logic Apps / Functions           | Advanced ingestion logic or API sources        | More complex but highly customizable        |

---

## 3. Security Models

| Type                      | Use Case                           | Key Feature                                   |
|---------------------------|------------------------------------|-----------------------------------------------|
| Row-Level Security (RLS)  | Restrict access by user attribute   | Implement via filter predicate in SQL         |
| Column-Level Security     | Restrict access to sensitive fields | Use column permissions in Synapse DW          |
| Workspace Permissions     | Control who sees what in Fabric    | Applies per user/group, not per row           |
| Deployment Rules          | Environment-specific settings      | Override connection strings, parameters       |

---

## 4. Deployment Pipelines (Fabric)

| Action                            | Microsoft's Expectation                                     |
|----------------------------------|-------------------------------------------------------------|
| Selective Deployment             | Deploy specific artifacts only                             |
| Backward Deployment              | Only allowed if target stage is empty and unassigned        |
| Rules                            | Used to override semantic model connections                |
| Pipeline Admin Role              | Only role that can change settings                         |

---

## 5. Spark & Data Engineering (Fabric)

| Feature                        | Key Consideration                                 |
|-------------------------------|----------------------------------------------------|
| Starter Pool                  | Good for dev/testing only; low compute limits     |
| Custom Spark Pool             | Required for large jobs or dynamic scaling        |
| Enable Dynamic Allocation     | Allows Spark to auto-scale executors              |
| Use Memory-Optimized Nodes    | Recommended for data-intensive transformations    |
| Avoid Hardcoded Paths         | Use parameters + string interpolation             |

---

## 6. Performance & Optimization

| Optimization Type       | Actions                                                     |
|-------------------------|-------------------------------------------------------------|
| Warehouse Ingestion     | Use COPY INTO, define FIELDTERMINATOR, use wildcard         |
| Semantic Models         | Tune partitions, refresh policies, RLS rules                |
| Pipelines               | Use parameterized paths, batch inserts, and staging         |
| Fabric Spark Jobs       | Use custom pools, autoscaling, dynamic allocation           |

---

## 7. Monitoring & Governance

| Tool/Feature             | Purpose                                 |
|--------------------------|------------------------------------------|
| Fabric Monitoring Hub    | Track jobs, pipelines, Spark sessions    |
| Activity Runs in ADF     | Pipeline run history and diagnostics     |
| Access Control Logs      | Audit access via Purview / workspace     |
| Deployment History       | See last deployed version per stage      |

---

## 8. Common Exam Traps & Gotchas

| Scenario                                      | DP-700 Expected Answer                     |
|----------------------------------------------|-------------------------------------------|
| Restrict pipeline editing                    | Assign pipeline admin role                |
| Trigger on file upload                       | Create Blob Created event trigger         |
| Dynamic pipeline across sources              | Use parameters + dynamic expressions      |
| Ensure production connects to prod DB        | Create deployment rule for connection     |
| Backward deployment                          | Target stage must be empty and unassigned |
| UDFs slow Spark jobs                         | Use traditional Spark engine              |
| Semantic model shows wrong DB                | Redeploy after setting rule               |

---

## 9. Study Resources

- Microsoft Learn: https://learn.microsoft.com/en-us/training/paths/design-implement-data-analytics-azure/
- Microsoft Fabric Docs: https://learn.microsoft.com/en-us/fabric/
- Azure Architecture Center – Data Patterns: https://learn.microsoft.com/en-us/azure/architecture/

