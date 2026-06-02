# Microsoft Fabric Data Engineer – Exam Day Pocket Guide

---

## 1. Architecture & Storage

- **OneLake** = default logical storage (unifies Lakehouse, Warehouse, KQL)
- **Lakehouse** = Delta tables + Spark + schema evolution
- **Warehouse** = Polaris engine + T-SQL + fixed schema
- **KQL DB** = Real-time analytics on streaming or logs

---

## 2. Data Ingestion

- **Dataflows Gen2** → best for SaaS/relational batch ingestion
- **Eventstreams** → best for low-latency streaming into Lakehouse/KQL
- **Notebooks (Spark)** → used for advanced or mixed batch/stream transformations
- **Pipelines** → orchestration of all components (schedule, chain, trigger)

---

## 3. Transformations & Modeling

- Use **Medallion Architecture**:
  - Bronze = raw
  - Silver = cleaned
  - Gold = modeled
- Use **Spark** for Lakehouse, **T-SQL** for Warehouse
- Use **semantic models** for Power BI with:
  - DirectLake (Lakehouse Delta)
  - DirectQuery (KQL or Warehouse)
  - Import (when needed)

---

## 4. Performance & Optimization

- Run `OPTIMIZE` + `VACUUM` on Delta Tables regularly
- Prefer **DirectLake** for large models without import
- Use **off-peak pipelines** for cost savings
- Avoid fallbacks: complex logic in DirectLake falls to **formula engine**

---

## 5. Security & Governance

- **RLS** → restrict rows via DAX in semantic model (`USERNAME()`)
- **OLS** → hide sensitive columns (e.g., salary)
- Use **workspace roles**: Viewer / Contributor / Admin
- **Tag items** (datasets, tables) with owner/sensitivity/project
- Monitor via **Fabric Admin Portal**

---

## 6. Real-Time Analytics

- Use **Eventstreams → KQL DB** for telemetry, real-time dashboards
- **KQL language** = best for log-style data
- Use **KQL Editor** for ad hoc query in Real-Time Analytics

---

## 7. Delta Lake Concepts

- Supports **ACID**, **schema evolution**, and **time travel**
- Used in **Lakehouse**, supports **Spark**
- Enables **append**, **update**, **merge**, and **streaming reads**

---

## 8. Development & Deployment

- Use **workspace per environment** (dev/test/prod)
- Use **Fabric Git integration** for version control
- Understand **lineage view** for end-to-end data tracing
- Trigger pipelines via **REST API** for automation

---

## 9. Licensing & Limits

- Fabric uses **F SKUs** (Capacity-based)
- Monitor workloads by **workspace**, not per user
- All compute engines are **isolated** (Spark ≠ Polaris ≠ KQL)

---

## 10. Pro Tips

- **DirectLake** = in-memory, zero import
- **KQL ≠ T-SQL** — different syntax
- Use **centralized semantic models** for scale
- Avoid **renaming fields used in Power BI measures**
