
/* =======================================================================
   Silver Layer Quality Checks: layoffs_cleaned
   Purpose: Ensure transformed data meets expected quality standards
   ======================================================================= */

/* 1. Row Count Check (should not be zero after load) */
SELECT COUNT(*) AS total_rows
FROM silver.layoffs_cleaned;

/* 2. Null Value Checks in Critical Columns */
SELECT 
    SUM(CASE WHEN company IS NULL THEN 1 ELSE 0 END) AS null_company,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN industry IS NULL THEN 1 ELSE 0 END) AS null_industry,
    SUM(CASE WHEN [date] IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country
FROM silver.layoffs_cleaned;

/* 3. Duplicate Check (same company, location, date) */
SELECT company, location, [date], COUNT(*) AS dup_count
FROM silver.layoffs_cleaned
GROUP BY company, location, [date]
HAVING COUNT(*) > 1;

/* 4. Valid Range Checks */
SELECT 
    MIN(total_laid_off) AS min_laid_off,
    MAX(total_laid_off) AS max_laid_off,
    MIN(percentage_laid_off) AS min_pct,
    MAX(percentage_laid_off) AS max_pct,
    MIN(funds_raised_millions) AS min_funds,
    MAX(funds_raised_millions) AS max_funds
FROM silver.layoffs_cleaned;

/* 5. Percentage Laid Off Should Be Between 0 and 100 */
SELECT *
FROM silver.layoffs_cleaned
WHERE percentage_laid_off < 0 OR percentage_laid_off > 100;

/* 6. Invalid Country Names (sanity check for unexpected strings) */
SELECT DISTINCT country
FROM silver.layoffs_cleaned
WHERE country NOT LIKE '%a%' -- example simple heuristic
   OR country LIKE 'NULL';

/* 7. Date Consistency Check (no future dates) */
SELECT *
FROM silver.layoffs_cleaned
WHERE [date] > GETDATE();

/* 8. Industry Distribution Check (see if NULLs still exist after filling) */
SELECT industry, COUNT(*) AS cnt
FROM silver.layoffs_cleaned
GROUP BY industry
ORDER BY cnt DESC;

/* 9. Stage Values - Check for unexpected categories */
SELECT DISTINCT stage
FROM silver.layoffs_cleaned;

/* 10. Funds Raised - Negative Values Check */
SELECT *
FROM silver.layoffs_cleaned
WHERE funds_raised_millions < 0;
