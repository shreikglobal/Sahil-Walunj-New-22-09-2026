
AWS DataOps Capstone Project

Overview

This project implements an end-to-end AWS DataOps and lakehouse pipeline for processing order data.

The solution ingests raw order data into Amazon S3, performs orchestration and data-quality validation using AWS Glue, stores curated data in Apache Iceberg format, exposes the data through Amazon Athena, and provides business analytics through Amazon QuickSight.

The project also includes governance, row-level security, automated refresh, monitoring, and freshness SLA validation.

---

Architecture

     text
Source Order Data
       |
       v
Amazon S3 - Raw Zone
       |
       v
AWS Glue Workflow
       |
       +--------------------+
       |                    |
       v                    v
Data Quality Gate      ETL Processing
       |                    |
       |                    v
       |              Apache Iceberg
       |              Curated Orders
       |                    |
       |                    v
       |              AWS Glue Catalog
       |                    |
       |                    v
       |              Amazon Athena
       |                    |
       |                    v
       |              Amazon QuickSight
       |
       v
Quarantine Zone
AWS Services Used
Amazon S3
AWS Glue
AWS Glue Data Catalog
Apache Iceberg
Amazon Athena
AWS Lake Formation
Amazon EventBridge
Amazon SNS
AWS Lambda
Amazon QuickSight
Amazon CloudWatch
AWS CloudShell
IAM
Project Components
1. S3 Data Lake
The S3 data lake is organized into logical zones:
Raw zone
Curated zone
Quarantine zone
The curated Iceberg table is stored under:
curated/orders/
2. AWS Glue ETL
The main ETL job is:
shreik-capstone-orders-etl
The ETL process:
Reads order data from the raw zone.
Performs transformations and standardization.
Removes duplicate records using order_id.
Writes the processed data to an Apache Iceberg table.
Uses the AWS Glue Data Catalog for metadata management.
Uses INSERT OVERWRITE to maintain the curated table.
Commits the Glue job after successful processing.
Curated table:
Database: shreik_capstone_db
Table: curated_orders
Format: Apache Iceberg
3. Data Quality
A data-quality gate is implemented using AWS Glue.
The project validates incoming data before allowing good data to proceed to the curated layer.
The pipeline supports:
Good-batch validation
Bad-batch detection
Quarantine of invalid records
Data-quality scoring
ETL failure handling
The tested bad batch was detected and quarantined.
The tested good batch passed validation with:
4,133 / 4,133 records passed
A separate bad-data test demonstrated the quarantine path.
4. Apache Iceberg Lakehouse
The curated dataset is stored as an Apache Iceberg table.
Table:
shreik_capstone_db.curated_orders
Important Iceberg features used in the project include:
Table metadata management
Partitioning by order date
Snapshot history
Time-travel capability
Efficient querying through Athena
Integration with AWS Glue Data Catalog
The curated dataset contains approximately:
162,164 orders
5. Amazon Athena
Amazon Athena is used as the SQL query layer over the curated Iceberg table.
Example query:
SELECT region, COUNT(*)
FROM shreik_capstone_db.curated_orders
GROUP BY region;
Athena is also used to reconcile business metrics with the QuickSight dashboard.
6. Amazon QuickSight Dashboard
The project includes a QuickSight business intelligence dashboard based on the curated_orders dataset.
The dashboard includes:
KPI 1 - Total Orders
162,164
KPI 2 - Total Net Revenue
62,441,156.12
Visual 1 - Revenue by Region
A bar chart compares net revenue across:
North
West
South
East
Central
Visual 2 - Revenue Trend Over Time
A time-series chart shows net amount by order date.
Visual 3 - Orders by Category
A category-level comparison is included to provide additional business insight.
The dashboard metrics were reconciled against Athena query results.
7. QuickSight Refresh Automation
The QuickSight dataset uses SPICE.
Dataset:
curated_orders
The dataset contains approximately:
162,164 rows
A scheduled refresh is configured.
In addition, an event-driven refresh mechanism was implemented so that a successful Glue ETL run triggers a QuickSight refresh.
EventBridge Rule
Capstone-Glue-Success-QuickSight-Refresh
The rule listens for:
AWS Glue Job State Change
and triggers when:
jobName = shreik-capstone-orders-etl
state = SUCCEEDED
Lambda Function
Capstone-QuickSight-Refresh
The Lambda function calls the QuickSight API:
quicksight:CreateIngestion
with:
FULL_REFRESH
This provides an automated path:
Glue ETL SUCCESS
       |
       v
EventBridge
       |
       v
Lambda
       |
       v
QuickSight SPICE Refresh
8. Governance and Row-Level Security
AWS Lake Formation is used to control access to the curated dataset.
Two analyst roles were configured:
Capstone-Analyst-North
Capstone-Analyst-West
Separate Lake Formation data filters restrict each analyst to the appropriate region.
North Analyst
The North analyst can see only:
North
Example validation result:
North = 37,515
West Analyst
The West analyst can see only:
West
Example validation result:
West = 34,049
This demonstrates row-level access control using AWS Lake Formation.
9. Failure Alerting
The pipeline includes failure alerting through Amazon SNS.
SNS topic:
Shreik-capstone-alerts
The alert mechanism is connected to the ETL workflow so that an ETL failure can generate an email notification.
A failure test was performed using an invalid input path to verify the failure-handling path.
10. Freshness SLA Monitoring
A freshness monitoring mechanism was implemented using AWS Lambda and Amazon EventBridge.
Lambda Function
Capstone-Freshness-Check
EventBridge Rule
Capstone-Freshness-24H-Check
Schedule:
rate(1 hour)
The freshness Lambda checks the latest successful Glue ETL execution against the required 24-hour freshness SLA.
Example successful response:
{
  "status": "OK",
  "last_success": "2026-10-05T09:33:13.986000+00:00",
  "age_hours": 0.66
}
The monitoring path is:
EventBridge
      |
      v
Freshness Lambda
      |
      v
Check latest Glue success
      |
      v
24-hour SLA validation
11. Lineage
The logical data lineage of the project is:
Raw S3 Order Data
       |
       v
AWS Glue ETL
       |
       +--> Data Cleaning
       |
       +--> Duplicate Removal
       |
       +--> Data Quality Validation
       |
       v
Apache Iceberg
curated_orders
       |
       v
AWS Glue Data Catalog
       |
       v
Amazon Athena
       |
       v
Amazon QuickSight
The main transformation relationship is:
Raw Orders
    ->
Glue Transformations
    ->
curated_orders
    ->
Athena
    ->
QuickSight
12. Data Quality and Governance Flow
The overall controlled pipeline is:
Raw Data
   |
   v
Glue Data Quality
   |
   +------ FAIL ------> Quarantine
   |
   v
PASS
   |
   v
Glue ETL
   |
   v
Iceberg Curated Table
   |
   +------> Lake Formation Row-Level Security
   |
   +------> Athena
   |
   +------> QuickSight
13. Monitoring and Observability
The project uses AWS monitoring components for operational visibility.
Monitoring includes:
Glue job state
Glue ETL success/failure
EventBridge rules
Lambda execution
QuickSight refresh status
Freshness SLA
SNS alerting
The successful ETL-to-QuickSight refresh flow was tested end-to-end.
14. Security
IAM roles and Lake Formation permissions are used to control access to AWS resources.
The project follows least-privilege principles where possible.
Examples of project-specific IAM roles include:
AWSGlueServiceRole-Capstone
Capstone-Analyst-North
Capstone-Analyst-West
Capstone-QuickSight-Refresh-Lambda-Role
Sensitive AWS account identifiers and ARNs should be masked in screenshots and public documentation.
No credentials, access keys, or secrets are stored in this repository.
15. Repository Contents
The repository contains project evidence such as:
QuickSight dashboard
AWS Glue ETL evidence
Data-quality evidence
Lake Formation governance evidence
EventBridge automation evidence
Freshness monitoring evidence
QuickSight refresh evidence
Project report
Additional source-code and configuration files can be added to make the repository fully reproducible.
16. How to Run the Pipeline
Step 1 - Upload Raw Data
Place the input order data in the configured S3 raw-zone location.
Step 2 - Run the AWS Glue Workflow
Start the configured Glue workflow:
shreik-capstone-orders-workflow
Step 3 - Run ETL
The main ETL job is:
shreik-capstone-orders-etl
Step 4 - Validate Data Quality
Check the data-quality gate and verify whether the batch passed or was quarantined.
Step 5 - Query Curated Data
Use Amazon Athena to query:
shreik_capstone_db.curated_orders
Example:
SELECT COUNT(*)
FROM shreik_capstone_db.curated_orders;
Step 6 - Validate Governance
Assume the appropriate analyst role and run:
SELECT region, COUNT(*)
FROM shreik_capstone_db.curated_orders
GROUP BY region;
Verify that the North analyst sees only North data and the West analyst sees only West data.
Step 7 - Validate QuickSight
Open the QuickSight dashboard and verify:
Total orders
Total revenue
Revenue by region
Revenue trend
Orders by category
Step 8 - Validate Refresh
After a successful Glue ETL execution, verify that the EventBridge rule invokes the QuickSight refresh Lambda and that the QuickSight dataset refresh completes successfully.
17. Key Results
Metric
Result
Curated orders
162,164
Total net revenue
62,441,156.12
North analyst rows
37,515
West analyst rows
34,049
Good batch validation
4,133 / 4,133
Freshness SLA
24 hours
Freshness check frequency
Every 1 hour
18. Project Outcomes
The project demonstrates:
End-to-end AWS DataOps pipeline
S3-based data lake architecture
AWS Glue orchestration
Data-quality validation
Bad-data quarantine
Apache Iceberg lakehouse
Athena SQL analytics
QuickSight business intelligence
Event-driven QuickSight refresh
Lake Formation row-level security
SNS failure alerting
EventBridge automation
Lambda-based freshness monitoring
Operational observability
Governance and access control
