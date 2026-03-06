-- Data Cleaning 
# more usable format to fix more a lot of issues in the raw data when you start creating visualization or starting in your product that data that actually useful.
#turning messy, raw data into clean, consistent, usable dataso you can analyze it, make charts, or build a product without wrong results.
#Common problems in raw data:
	# duplicate rows
	#different spellings of the same value
	#extra spaces
	#text dates instead of real dates
	#NULL or blank values
	#useless rows/columns
    
    
-- 1. Remove Duplicates
-- 2. Standardize the data
	# if theres an issue with spelling or things we just need to standardize it with all the same as it should be.
-- 3. Null Values or Blank Values
-- 4. Remove columns and rows that aren't necessary
   
   

#Step 0: Look at the raw data
SELECT *
FROM layoffs;

#Step 1: Create a STAGING table (safe copy)
#Why? We want to clean data without destroying the original.
CREATE TABLE layoffs_staging
LIKE layoffs;

#Copies all data from layoffs into layoffs_staging
#layoffs = raw data (untouched)
#layoffs_staging = working copy
#Best practice in real job
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;


#Step 2: Find Duplicates
#What is duplicate? Rows that are exactly the same in important columns.
#EXPLAINATION
#ROW_NUMBER() gives 1, 2, 3… to similar rows
#PARTITION BY groups rows that look the same
#First row = row_num = 1
#Duplicates = row_num > 1
#It just labels duplicates
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, `date`) AS row_num
FROM layoffs_staging;


#Find only duplicates
#CTE = temporary result (not a table)
#Groups rows that are exactly the same
#Shows only duplicates
#MySQL cannot delete directly from a CTE
WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date` ,stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_CTE
WHERE row_num >1;




#Step 2B: Create another staging table for deletion
#This table stores row numbers permanently
#Makes deleting duplicates possible
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


#duplicates = row_num > 1
#if greather than 1 it means it has duplicate
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, 
percentage_laid_off, `date` ,stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

#Duplicates removed
#Only one copy remains
DELETE
FROM layoffs_staging2
WHERE row_num >1
LIMIT 1000;

SELECT *
FROM layoffs_staging2;



#Step 3: Standardize the Data
#This mean: same spelling, same format, same values

#3.1 Remove extra spaces in company names
#TRIM() removes:
#spaces before
#spaces after
SELECT company, TRIM(company)
FROM layoffs_staging2;

#Fixes names like " Google " → "Google"
UPDATE layoffs_staging2
SET company = TRIM(company);

#3.2 Fix inconsistent industry names
#This finds:Crypto,Cryptocurrency,Crypto Currency,Crypto (Web3)
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

#Everything becomes Crypto
#Clean grouping in charts
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT * 
FROM layoffs_staging2;

#3.3 Fix country names
#Finds: United States, United States.
SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

#Removes trailing dot
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


#3.4 Convert date from TEXT → DATE
#Problem: Dates are TEXT, not real dates
#Converts text → date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

#Now values are correct, but column type is still TEXT
UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

#change the date(text) to date(date)
#Now the column is a real DATE
#sorting works
#filtering works
#time analysis works
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2;



#Step 4: Handle NULL and Blank Values

#4.1 Find useless rows
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company  LIKE 'Bally%';

#4.2 Fix blank industry values
#Blank ≠ NULL
# Standardizes missing data
#turn the blank into null
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

#4.3 Fill missing industry using company name
#make the industry null is equal to the same industry of the company
#Same company usually has the same industry
#If one row knows the industry → copy it
#Smart data filling
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2;


-- 4. Remove columns and rows that aren't necessary
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

#Step 5: Remove unnecessary rows & columns
#Deletes rows with no useful information
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

#remove the row_num column
#row_num was only for cleaning
#Remove it after finishing
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
