<img width="65" height="21" alt="image" src="https://github.com/user-attachments/assets/9470f31d-eaef-421c-8387-af552fb63c0a" /># 💼 Layoffs Analysis Project (Python + SQL + Power BI)

## 🧩 Project Overview
This end-to-end data analytics project analyzes global layoffs between **2020–2024** using the **Layoffs.fyi (Kaggle version)** dataset.  

The goal was to design a complete **data pipeline and dashboard** that turns raw layoff records into meaningful insights using:
- **Python** for data load and export  
- **SQL Server** for transformation, modeling, and aggregation  
- **Power BI** for visualization and storytelling  

---

## 🗂️ Project Structure

```
Layoffs-Analysis-Project/
│
├── layoffs_load.ipynb                # Python notebook for loading and exporting data
├── sql/
│   ├── 01_init_database.sql          # Create database, raw & clean tables
│   ├── 02_create_tables.sql           # Create raw & clean tables
│   ├── 03_load_clean_procedure.sql   # ETL procedure for cleaning and standardizing data
│   └── 04_vw_Layoffs_Dashboard.sql   # Analytics view for Power BI
│
├── data/
│   └── layoffs.csv                   # Raw dataset (Layoffs.fyi)
│
├── powerbi/
│   └── layoffs_dashboard.pbix        # Power BI dashboard (to be added)
│
└── README.md                         # Project documentation
```

---

## 🧮 Step 1 — Data Cleaning in Python

The dataset was first loaded and exported in **`layoffs_load.ipynb`**.

### Key Tasks
- Load layoffs_data.csv
- Exported cleaned data to CSV for SQL ingestion  

### Sample Code
```python
import pandas as pd

# Load dataset
import pandas as pd
import pyodbc

df = pd.read_csv(r"C:\SQLData\layoffs_data.csv")

conn = pyodbc.connect(
    "Driver=ODBC Driver 18 for SQL Server;"
    "Server=E4-1LNV5DM\\SQLEXPRESS;"
    "Database=LayoffsDB;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)
cursor = conn.cursor()


## 🧱 Step 2 — Data Modeling and ETL in SQL

All data modeling and transformations were handled in **SQL Server**.

### ⚙️ Database: `LayoffsDB`

#### 1️⃣ Tables
- **`dbo.LayoffsRaw`** — contains raw data  
- **`dbo.Layoffs_Clean`** — cleaned, structured table used for reporting  

#### 2️⃣ Stored Procedure: `load_Layoffs_Clean`
This ETL procedure:
- Truncates existing clean data  
- Maps “Other” industries to correct categories (e.g., Product, AI, Infrastructure)  
- Filters invalid records (e.g., missing layoffs count)  
- Tracks total inserted rows and execution time  

```sql
EXEC load_Layoffs_Clean;
```

#### 3️⃣ View: `vw_Layoffs_Dashboard`
An analytics-ready dataset used in Power BI.

```sql
CREATE VIEW vw_Layoffs_Dashboard AS
SELECT
    Company,
    Country,
    Location_HQ,
    Industry,
    Stage,
    CAST(Date AS DATE) AS Layoff_Date,
    SUM(Laid_Off_Count) AS Total_Laid_Off,
    AVG(Percentage) AS Avg_Percentage,
    SUM(Funds_Raised) AS Total_Funds_Raised
FROM Layoffs_Clean
GROUP BY
    Company, Country, Location_HQ, Industry, Stage, CAST(Date AS DATE);
```

---

## 📊 Step 3 — Power BI Dashboard

### 🎯 Dashboard Goals
Provide a visual overview of layoffs across companies, countries, and industries — highlighting workforce impact and financial context.

### Dashboard Pages

#### **Page : Global Overview**
- **KPIs**
  - 🧍 Total Laid Off  
  - 🏢 Total Companies Affected  
  - 📈 Average Workforce Laid Off  
  - 🌍 Countries Affected  
- **Visuals**
  - Layoffs Trend by Month/Year  
  - Top Countries Affected (bar chart)  
  - Top Industries Affected (bar chart)  
  - Top Companies Affected (pie charts)



 Filters & Slicers**
- Year, month, Country, Industry, and Stage for flexible exploration  

---

## ⚙️ Tools & Technologies

| Tool / Language | Purpose |
|-----------------|----------|
| **Python (Pandas)** | Load and expor the data |
| **SQL Server (T-SQL)** | ETL, data modeling, and view creation |
| **Power BI** | Dashboard visualization |
| **Excel / CSV** | Data exchange format |
| **Git & GitHub** | Version control & documentation |

---

## 🚀 How to Reproduce

1. **Clone this repository**
   ```bash
   git clone https://github.com/mostafakhaled88/Layoffs-Analysis-Project.git
   ```
2. **Run the Python notebook**
   - Open `layoffs_load.ipynb` load and export layoffs_data.csv`
3. **Run the SQL scripts**
   - Create the database and load cleaned data
4. **Open Power BI**
   - Connect to SQL Server (`vw_Layoffs_Dashboard`)
   - Refresh data and explore dashboard visuals

---

## 📈 Key Insights

- **USA** , **India** and **Germany** are the most affected countries  
- **Retail, Transportation,Infrastructure ,and Consumer** industries dominate layoffs  
- Layoff spikes occur during **early 2023** and **mid-2022**  
- Many **Post-IPO** and **Acquired** are Highest layoffs  

---

## 👤 Author

**Mostafa Khaled Farag**  
📍 Cairo, Egypt  
📧 [mosta.mk@gmail.com](mailto:mosta.mk@gmail.com)  
🔗 [LinkedIn](https://www.linkedin.com/in/mostafa-khaled-442b841b4/) | [GitHub](https://github.com/mostafakhaled88)

---

## 🏁 Next Steps
- Add `.pbix` Power BI file and screenshot folder  
- Include sample SQL query results in documentation  
- Extend dashboard with **trend forecasting** and **funds vs. layoffs correlation**
