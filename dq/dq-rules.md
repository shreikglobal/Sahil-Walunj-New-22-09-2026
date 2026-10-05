# Data Quality Rules

## Overview

This document defines the data-quality rules applied to the AWS DataOps pipeline before data is published to the curated Iceberg table.

The quality gate validates incoming order data and separates invalid records into the quarantine path while allowing valid records to continue to the curated layer.

## Data Quality Checks

### 1. Null Value Check

Required fields must not contain null or empty values.

Required fields include:

- order_id
- order_ts
- customer_id
- product_id
- category
- region
- quantity
- unit_price
- discount_pct

**Expected result:** No null values in required fields.

---

### 2. Duplicate Order Check

`order_id` must be unique for each order.

Duplicate records are identified using `order_id` and removed before writing to the curated Iceberg table.

**Expected result:** One record per unique order_id.

---

### 3. Quantity Validation

The `quantity` field must contain a valid positive value.

**Rule:**

```text
quantity > 0
Invalid or non-positive quantities are treated as data-quality failures.
4. Unit Price Validation
The unit_price field must contain a valid non-negative value.
Rule:
unit_price >= 0
5. Discount Validation
The discount percentage must be within the valid business range.
Rule:
discount_pct >= 0
AND
discount_pct <= 100
6. Region Validation
Region values must belong to the approved business regions.
Approved regions:
North
South
East
West
Central
Unexpected region values are treated as quality failures.
7. Order Date Validation
Order timestamps and dates must contain valid date/time values.
Invalid or unparseable order dates are rejected from the curated dataset.
8. Business Amount Validation
The pipeline validates calculated monetary fields such as:
gross_amount
discount_amount
net_amount
The calculated values must be numerically valid and consistent with the source order information.
Quality Gate Behaviour
The pipeline implements a quality gate before publishing data to the curated Iceberg table.
Bad Batch
If the incoming batch contains quality failures above the accepted threshold:
The batch is marked as failed.
Invalid records are quarantined.
The bad batch is prevented from being published as a valid curated batch.
Monitoring/alerting is triggered where configured.
A bad-batch test was successfully demonstrated during project validation.
Good Batch
A valid batch passes the quality gate and is allowed to continue to the curated Iceberg table.
The project validation demonstrated a successful good-batch result of:
4,133 / 4,133 records passed
Quarantine
Records failing data-quality validation are written to the quarantine area for investigation and remediation.
The quarantine process prevents invalid records from contaminating the curated analytical dataset.
Quality Status
The quality gate produces a pass/fail outcome for each processed batch.
The final curated dataset is intended to contain only records that satisfy the defined quality rules.
Validation Evidence
The project includes evidence for:
Null-value validation
Bad-data detection
Duplicate handling
Good-batch validation
Quarantined records
Data-quality gate execution
Curated Iceberg table validation
These checks support reliable downstream Athena queries and QuickSight analytics.
Tools Used
AWS Glue
AWS Glue Data Quality / validation logic
Amazon S3
Apache Iceberg
AWS Glue Data Catalog
Amazon Athena
Amazon QuickSight
Outcome
The data-quality framework ensures that only validated order data reaches the curated analytical layer while failed records are isolated in quarantine for further investigation.
