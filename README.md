# Real-Time Customer 360 Data Pipeline

A real-time data engineering project designed to build a unified and continuously updated Customer 360 view using Apache NiFi, AWS S3, and Snowflake. The pipeline automates ingestion, transformation, and change tracking of customer data to support real-time analytics and decision-making.

---

## Overview
This project implements a streaming data pipeline that ingests customer data in real time, lands it in AWS S3, and loads it into Snowflake using Snowpipe.  
It applies Slowly Changing Dimension (SCD Type 2) logic to maintain both the latest customer state and a complete historical record.

The system runs fully automatically — from ingestion to transformation — enabling up-to-date insights into customer activity and lifecycle.

---

## Architecture

### 1. Data Ingestion (Apache NiFi)
- Configured NiFi to fetch new customer data files from a local directory.
- Used processors like `GetFile` and `PutS3Object` to send data into an AWS S3 bucket.

### 2. Cloud Landing Zone (AWS S3)
- Created a dedicated S3 bucket with a `/stream_data/` folder to act as the raw data landing zone.
- This serves as the entry point for all incoming customer data.

### 3. Automated Loading (Snowpipe)
- Configured a Snowflake external stage pointing to the S3 bucket.
- Used Snowpipe to automatically load files into a staging table in near real time whenever new data arrives.

### 4. Change Data Capture (CDC) with Streams
- Created a Snowflake Stream on the staging table to capture incremental inserts, updates, and deletes.
- Built a view `v_customer_change_data` to:
  - Calculate `start_time` and `end_time`
  - Set `is_current` flags
  - Identify DML operations (`insert`, `update`, `delete`)

### 5. Transformation and Merge (Snowflake Tasks)
- A scheduled Snowflake Task runs every minute to execute a `MERGE` operation that:
  - Updates the Customer Table with the latest active records
  - Updates the Customer History Table using SCD Type 2 for full change tracking

---

## Key Features
- Near real-time ingestion and transformation  
- Automated Snowpipe data loading  
- Change Data Capture (CDC) using Streams  
- SCD Type 2 implementation for historical tracking  
- Fully automated with no manual intervention required

---

## Tech Stack

| Component | Purpose |
|------------|----------|
| Apache NiFi | Data ingestion and delivery to AWS S3 |
| AWS S3 | Cloud landing zone for raw data |
| Snowflake | Data warehouse with Snowpipe, Streams, and Tasks |
| SQL (Snowflake DDL/DML) | MERGE logic for SCD Type 2 |
| Python / Bash (optional) | For orchestration and automation scripts |

---

