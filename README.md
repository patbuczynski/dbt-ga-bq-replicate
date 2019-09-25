### Replicate Google Analytics reports in BigQuery

Data build tool model for replicating 3 Google Analytics reports using BigQuery GA export data. Replicated reports:

- Source/Medium
- Product Performance
- All Pages

The model will query the table specified in dbt_project.yml file and save the tables with data in your BigQuery project and dataset specified in dbt profiles.yml file. Runing the model may result in additional costs in BigQuery, according to your project pricing.

Before you compile the model you need to adjust the variables (rangeStart, rangeEnd, table) in dbt_project.yml file.

---
- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [Installation](https://dbt.readme.io/docs/installation)

---
