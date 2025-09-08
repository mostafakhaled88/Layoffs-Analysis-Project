# 📘 Naming Conventions – Layoffs Data Warehouse

This document defines the **naming conventions** for the **LayoffsDWH project** to ensure consistency across SQL Server objects, GitHub repo, and documentation.

---

## 1. Database & Schemas

- **Database name:**  
  `LayoffsDWH`

- **Schemas:**  
  - `bronze` → raw ingested data (from source files)  
  - `silver` → cleaned & transformed data  
  - `gold` → business-ready star schema  

---

## 2. Tables

- **General format:**  
  `{layer}.{subject_area}_{object_type}`  

- **Examples:**  
  - Bronze: `bronze.layoffs_raw`  
  - Silver: `silver.layoffs_cleaned`  
  - Gold (Fact): `gold.fact_layoffs`  
  - Gold (Dimensions):  
    - `gold.dim_company`  
    - `gold.dim_industry`  
    - `gold.dim_location`  
    - `gold.dim_date`  

---

## 3. Columns

- Use **snake_case** (lowercase with underscores)  
- Keep names **short but descriptive**  
- Use consistent **_id suffix** for surrogate keys  

- **Examples:**  
  - `company_id`  
  - `industry_id`  
  - `location_id`  
  - `date_id`  
  - `total_laid_off`  
  - `percentage_laid_off`  

---

## 4. Keys & Indexes

- **Primary Key:**  
  `PK_<table>`  
  - Example: `PK_fact_layoffs`  

- **Foreign Key:**  
  `FK_<fact>_<dimension>`  
  - Example: `FK_fact_layoffs_dim_company`  

- **Index:**  
  `IX_<table>_<column>`  
  - Example: `IX_fact_layoffs_date_id`  

---

## 5. GitHub Repository & File Structure

- **Repository name:**  
  `layoffs-dwh-sqlserver`  

- **Folders:**  
