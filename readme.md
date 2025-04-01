## Overview
Analysing NYC Yellow and Green Taxi Trip Records to identify key trends, routes, and patterns, and utilising ELT (Extract, Load, Transform) processes to extract meaningful insights.

### Architecture diagram
<img src="images/diagram.svg"/>

### Technologies
Data from the source website is extracted and stored in gcs, data is then loaded into BigQuery by creating external and partitioned tables. DBT then fetches data from these tables, applies transformations and loads it back into BigQuery.

- Used Terraform to set up the infrastructure
- Used a docker containers to run everything.
- Used Airflow to orchestrate the entire pipeline
- Used gcs as data lake and BigQuery as data warehouse
- Used DBT cloud to process, transform and clean the data
