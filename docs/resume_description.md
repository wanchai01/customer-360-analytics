# Resume Description

**Customer 360 Analytics Platform**
*Python · PySpark · Apache Airflow · PostgreSQL · AWS (S3, RDS, IAM) · Power BI · Docker*

- Built an end-to-end Customer 360 data platform processing 9 synthetic
  e-commerce data sources through a medallion architecture (raw → staging
  → processed → warehouse → Gold), with a PySpark ETL layer handling up
  to 50M+ events using partitioning, broadcast joins, and window functions.
- Developed a 23-rule SQL-based Data Quality framework covering all six
  standard dimensions (completeness, uniqueness, validity, accuracy,
  referential integrity, consistency) that gates the Airflow pipeline —
  verified to genuinely halt and retry on sub-95% pass rates, not just log
  a warning.
- Delivered RFM customer segmentation (9 segments), Estimated CLV modeling,
  and cohort retention analysis into a Star Schema warehouse and 5-page
  Power BI dashboard, backed by 147 automated tests (99% coverage) and a
  documented, tested path to production AWS deployment.

---

### Shorter version (1-2 lines, for space-constrained formats)

Built an end-to-end Customer 360 analytics platform (Python, PySpark,
Airflow, PostgreSQL, AWS, Power BI) with a 23-rule Data Quality gate,
RFM/CLV/retention modeling, and 147 automated tests at 99% coverage.
