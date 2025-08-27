CREATE OR REPLACE TABLE labour_market.preprocessed_job_posting AS
SELECT
    r.job_title,
    r.posted_date,
    r.salary_min,
    r.salary_max,
    r.company_name,
    r.location,
    r.description,
    m.min_salary,
    r.ingestion_time
FROM `labour_market.raw_job_posting` r
LEFT JOIN `labour_market.min_wage` m
ON r.job_title = m.job_title
AND EXTRACT(MONTH FROM r.posted_date) = m.month;
