# End-to-End Data Engineering Project: Smartphones Price Monitoring

## Business Case 

## Pipeline Architecture

![Pipeline Architecture](./images/Pipeline_Architecture.png)

### Tech Stacks

- **Docker:** Used for containerizing the entire data pipeline environment, ensuring consistent execution across different platforms and simplifying infrastructure management.

- **Apache Airflow:** The core orchestration engine used to schedule, automate, and monitor the end-to-end workflow, from data ingestion to the final transformation.

- **Google Cloud Storage (GCS):** Acts as the project's Data Lake, providing scalable object storage for landing and archiving raw data files before processing.

- **BigQuery:** A serverless, highly scalable cloud data warehouse used to store structured data and execute complex analytical queries at high speed.

- **dbt (Data Build Tool):** The transformation layer used to clean, model, and prepare data within BigQuery using SQL, implementing the Medallion Architecture (Raw, Staging, and Marts).

- **Power BI:** The visualization platform used to create interactive dashboards, turning processed data into actionable market insights.

### Data Pipeline Flow

- **Extract:** Automated data collection from SerpApi (Data Source) using Python scripts. These tasks are orchestrated by **Apache Airflow** and the extracted data is stored as raw files in **Google Cloud Storage (GCS)**.

- **Load:** Raw data files from GCS are transferred to **BigQuery** raw tables, serving as the landing zone for the data warehouse.

- **Transform:** Data is processed within BigQuery using **dbt**, following the Medallion Architecture to ensure data quality and integrity:

	- **Raw Data (Raw):** The initial ingestion layer where data is kept in its original format to maintain a source of truth.

	- **Cleaned Data (Staging):** Involves data cleaning, type casting, standardized naming conventions, and deduplication to create a reliable foundation.

	- **Business-level Data (Marts):** The final analytical layer where data is joined and aggregated. This includes complex logic, making it ready for **Power BI** visualization.



## Setup Instructions









