# Secure a Microsoft Fabric data warehouse

## Introduction  

Microsoft Fabric Data Warehouse is a **comprehensive platform for data, analytics, and AI**, offering **security features** to protect **sensitive information** without modifying applications.  

Administrators can implement **security mechanisms** through:  
- **Workspace roles** (Admin, Member, Contributor, Viewer) for access control.  
- **Item permissions** to manage data sharing.  
- **Data protection security** using **T-SQL** for **object, column, and row-level security**, as well as **dynamic data masking** to obscure sensitive data.  

These features ensure **secure access** across **Warehouse and SQL analytics endpoints**, safeguarding data integrity while enabling seamless analytics.

## Explore Dynamic Data Masking  

**Dynamic Data Masking (DDM)** enhances **data security** by **obscuring sensitive information** in query results for **nonprivileged users** without altering the actual data. It applies **real-time masking** at the **column level**, ensuring only authorized users can see unmasked data.  

### **Masking Types**:  
- **Default** – Fully hides data based on its type.  
- **Email** – Reveals the first letter and appends “.com”.  
- **Custom Text** – Partially exposes data while masking the middle.  
- **Random** – Replaces numeric/binary data with a random value.  

### **Implementation Example**:  
```sql
ALTER TABLE Customers  
ALTER COLUMN Email ADD MASKED WITH (FUNCTION = 'email()');  
ALTER TABLE Customers  
ALTER COLUMN PhoneNumber ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XXX-",4)');  
```
DDM protects **sensitive fields** like emails, phone numbers, and credit cards, but should be combined with **object-level security** and **minimal privilege access** for comprehensive protection.

## Implement Row-Level Security  

**Row-Level Security (RLS)** restricts access to specific rows in a **Microsoft Fabric Data Warehouse** based on user identity or group membership. It is enforced at the database level without requiring application changes.  

### **How RLS Works**  
- **Filter Predicates:** Define conditions (e.g., `WHERE CustomerId = USER_NAME()`) that determine row visibility.  
- **Security Policies:** Apply predicates at the table level to control access.  

### **Implementation Example:**  
1. **Create a security function:**  
```sql
CREATE FUNCTION sec.tvf_SecurityPredicatebyTenant(@TenantName NVARCHAR(50))  
RETURNS TABLE  
WITH SCHEMABINDING  
AS  
RETURN SELECT 1 AS result WHERE @TenantName = USER_NAME() OR USER_NAME() = 'tenantAdmin@contoso.com';  
GO
```
2. **Apply it via a security policy:**  
```sql
CREATE SECURITY POLICY sec.SalesPolicy  
ADD FILTER PREDICATE sec.tvf_SecurityPredicatebyTenant(TenantName) ON dbo.Sales  
WITH (STATE = ON);
```
**Use Cases:**  
- Isolating departmental access.  
- Restricting customer data visibility.  
- Enforcing **compliance** and **data protection** policies.  

### **Best Practices:**  
- Define **predicate functions and security policies** in a separate schema.  
- Minimize **type conversions, joins, and recursion** in predicate functions to **optimize performance**.

## Implement Column-Level Security  

**Column-Level Security (CLS)** restricts access to specific columns in a table, ensuring **sensitive data** is only available to **authorized users**. It is commonly used in **healthcare, finance, and compliance-sensitive industries**.  

### **Example Use Case: Healthcare**  
A **Patients** table contains **PatientID, Name, Address, DateOfBirth, and MedicalHistory**. The **MedicalHistory** column should only be accessible to **Doctors and Nurses**, while **Receptionists and Patients** should be restricted.  

### **Implementation:**  
```sql
-- Create roles  
CREATE ROLE Doctor AUTHORIZATION dbo;  
CREATE ROLE Nurse AUTHORIZATION dbo;  
CREATE ROLE Receptionist AUTHORIZATION dbo;  
CREATE ROLE Patient AUTHORIZATION dbo;  
GO  

-- Grant SELECT on all columns to all roles  
GRANT SELECT ON dbo.Patients TO Doctor, Nurse, Receptionist, Patient;  
GO  

-- Deny SELECT on the MedicalHistory column to restricted roles  
DENY SELECT ON dbo.Patients (MedicalHistory) TO Receptionist, Patient;  
GO  
```
### **CLS vs. Views:**  
| Aspect                 | Column-Level Security (CLS) | Views |
|------------------------|---------------------------|-------|
| **Granularity**        | Controls access per column | Requires multiple views |
| **Maintenance**        | Auto-adapts to schema changes | Needs manual updates |
| **Performance**        | More efficient | Can introduce overhead |
| **Transparency**       | Users query the table normally | Users must query views |
| **Flexibility**        | Less flexible | Supports row-level security & transformations |

**Best Practice:** Test **CLS policies** in a safe environment before applying them in **production**.

## Configure SQL Granular Permissions Using T-SQL  

SQL **granular permissions** control **Data Manipulation Language (DML) operations** (SELECT, INSERT, UPDATE, DELETE) on **tables, views, functions, and stored procedures**. These permissions can be **granted, revoked, or denied**, with **DENY overriding GRANT**.  

### **Example of Table-Level Permissions**  
```sql
-- Grant SELECT and INSERT on Patients table to Doctor role  
GRANT SELECT, INSERT ON dbo.Patients TO Doctor;  

-- Deny DELETE permission on Patients table to Receptionist role  
DENY DELETE ON dbo.Patients TO Receptionist;  
```
### **Function and Stored Procedure Permissions**  
| Permission | Definition |
|------------|------------|
| **ALTER** | Modify the object’s definition. |
| **CONTROL** | Full rights over the object. |

### **Principle of Least Privilege**  
Users and applications should only be granted **minimum required permissions**. Example:  
- Applications accessing data via **stored procedures** should only have **EXECUTE** permission on them, not direct table access.  

### **Dynamic SQL Example**  
```sql
CREATE PROCEDURE sp_TopTenRows @tableName NVARCHAR(128)
AS
BEGIN
    DECLARE @query NVARCHAR(MAX);
    SET @query = N'SELECT TOP 10 * FROM ' + QUOTENAME(@tableName);
    EXEC sp_executesql @query;
END;
```
Using **QUOTENAME** prevents **SQL injection attacks**, ensuring **secure query execution**.