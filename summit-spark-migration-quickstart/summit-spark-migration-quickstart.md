author: Snowflake
id: summit-spark-migration-quickstart
summary: Migrate a PySpark financial analytics pipeline to Snowflake-native Snowpark Python using Cortex Code and the Snowpark Migration Accelerator.
categories: snowflake-site:taxonomy/solution-center/certification/quickstart, snowflake-site:taxonomy/product/data-engineering
environments: web
status: Published
feedback link: https://github.com/Snowflake-Labs/sfguides/issues
tags: Spark Migration, Snowpark, PySpark, Cortex Code, SMA, Data Engineering, Python

# Migrating a PySpark Pipeline to Snowflake with Cortex Code
<!-- ------------------------ -->
## Overview

In this quickstart you will migrate a financial analytics pipeline from PySpark to **Snowflake-native Snowpark Python** using the `spark-migration` skill in Cortex Code and the Snowpark Migration Accelerator (SMA) CLI.

### Lab Architecture

```
+-----------------------------------------------------------------+
|  PART A - Cortex Code (local machine)                          |
|                                                                 |
|  transaction_pipeline.py  -->  SMA CLI  -->  Snowpark output  |
|  (PySpark source)              (converts)   (converted .py)   |
+--------------------------------+--------------------------------+
                                 | EWI Dashboard (opens in browser)
+--------------------------------v--------------------------------+
|  PART B - Snowflake Notebook                                    |
|                                                                 |
|  ACCOUNTS ---+                                                  |
|              +--> Snowpark Pipeline --> MONTHLY_SPEND_SUMMARY  |
|  TRANSACTIONS+                                                  |
+-----------------------------------------------------------------+
```

### What You'll Learn
- How to use Cortex Code's `spark-migration` skill to convert PySpark code to Snowpark
- How to interpret and fix EWI (Error, Warning, Issue) reports from the SMA tool
- Key API differences between PySpark and Snowpark (snake_case methods, UDF registration, Window functions)
- How to run a Snowpark pipeline natively inside Snowflake

### What You'll Need
- A [Snowflake account](https://signup.snowflake.com/) with `SYSADMIN` or equivalent role
- [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) — Snowflake CLI with AI assistance
- SMA CLI — Snowpark Migration Accelerator binary (contact your Snowflake account team if you do not have it)
- The `test_workload_for_demo/` source folder containing `transaction_pipeline.py`

### What You'll Build
- A fully converted Snowpark Python pipeline that reads from Snowflake tables, applies UDFs, performs aggregations with window functions, and writes results back to Snowflake — all without an external Spark cluster or connector.

<!-- ------------------------ -->
## Environment Setup

Run the SQL below to create the warehouse, database, and schema used throughout this lab.

### Create Warehouse

```sql
-- Create a dedicated warehouse for this lab
CREATE WAREHOUSE IF NOT EXISTS LAB_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Hands-on lab: PySpark to Snowflake migration';

USE WAREHOUSE LAB_WH;
```

### Create Database and Schema

```sql
-- Create the database and schema used by the pipeline
CREATE DATABASE IF NOT EXISTS DEMO_DB
    COMMENT = 'Migration lab database';

CREATE SCHEMA IF NOT EXISTS DEMO_DB.FINANCE
    COMMENT = 'Finance pipeline schema';

USE DATABASE DEMO_DB;
USE SCHEMA FINANCE;
```

<!-- ------------------------ -->
## Load Source Data

The synthetic finance dataset is embedded directly — no external files or stage uploads are required.

- **ACCOUNTS** (50 rows): account holders with type (CHECKING / SAVINGS / CREDIT), status (ACTIVE / INACTIVE / FROZEN), and credit limit
- **TRANSACTIONS** (300 rows): DEBIT, CREDIT, and TRANSFER transactions spanning January 2023 to June 2024

These tables represent the data that the original PySpark pipeline read from Snowflake using the legacy Spark-Snowflake connector.

### Load Accounts Table

```python
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import StructType, StructField, StringType, DoubleType

session = get_active_session()

# Schema for the ACCOUNTS table
accounts_schema = StructType([
    StructField('ACCOUNT_ID',    StringType(), False),
    StructField('CUSTOMER_NAME', StringType(), True),
    StructField('ACCOUNT_TYPE',  StringType(), True),
    StructField('OPENED_DATE',   StringType(), True),
    StructField('STATUS',        StringType(), True),
    StructField('CREDIT_LIMIT',  DoubleType(), True),
])

# 50 synthetic accounts (35 ACTIVE, 10 INACTIVE, 5 FROZEN)
accounts_data = [
    ('ACC001','James Mitchell','CHECKING','2018-03-15','ACTIVE',5000.0),
    ('ACC002','Sarah Chen','SAVINGS','2019-07-22','ACTIVE',10000.0),
    ('ACC003','Michael Torres','CREDIT','2020-01-10','ACTIVE',25000.0),
    ('ACC004','Emily Johnson','CHECKING','2018-11-05','ACTIVE',3000.0),
    ('ACC005','David Park','SAVINGS','2021-04-18','ACTIVE',8000.0),
    ('ACC006','Lisa Anderson','CREDIT','2019-09-30','ACTIVE',15000.0),
    ('ACC007','Robert Kim','CHECKING','2020-06-14','ACTIVE',4500.0),
    ('ACC008','Jennifer White','SAVINGS','2018-02-28','ACTIVE',12000.0),
    ('ACC009','William Brown','CREDIT','2021-08-07','ACTIVE',30000.0),
    ('ACC010','Amanda Davis','CHECKING','2022-03-19','ACTIVE',2500.0),
    ('ACC011','Christopher Wilson','SAVINGS','2019-12-01','ACTIVE',9000.0),
    ('ACC012','Stephanie Martinez','CREDIT','2020-05-23','ACTIVE',20000.0),
    ('ACC013','Kevin Thompson','CHECKING','2018-07-16','ACTIVE',6000.0),
    ('ACC014','Rachel Garcia','SAVINGS','2021-11-09','ACTIVE',7500.0),
    ('ACC015','Brian Lee','CREDIT','2019-04-25','ACTIVE',18000.0),
    ('ACC016','Melissa Clark','CHECKING','2022-01-30','ACTIVE',3500.0),
    ('ACC017','Andrew Rodriguez','SAVINGS','2020-09-12','ACTIVE',11000.0),
    ('ACC018','Nicole Lewis','CREDIT','2018-06-20','ACTIVE',22000.0),
    ('ACC019','Daniel Hall','CHECKING','2021-02-14','ACTIVE',4000.0),
    ('ACC020','Heather Young','SAVINGS','2019-08-03','ACTIVE',6500.0),
    ('ACC021','Joshua Hernandez','CREDIT','2020-11-27','ACTIVE',16000.0),
    ('ACC022','Samantha King','CHECKING','2022-05-08','ACTIVE',2000.0),
    ('ACC023','Ryan Wright','SAVINGS','2018-10-15','ACTIVE',14000.0),
    ('ACC024','Ashley Lopez','CREDIT','2021-06-29','ACTIVE',28000.0),
    ('ACC025','Matthew Hill','CHECKING','2019-03-07','ACTIVE',5500.0),
    ('ACC026','Megan Scott','SAVINGS','2020-07-21','ACTIVE',8500.0),
    ('ACC027','Justin Green','CREDIT','2018-12-13','ACTIVE',35000.0),
    ('ACC028','Brittany Adams','CHECKING','2022-09-24','ACTIVE',3000.0),
    ('ACC029','Nicholas Baker','SAVINGS','2021-01-06','ACTIVE',10500.0),
    ('ACC030','Amber Gonzalez','CREDIT','2019-05-18','ACTIVE',19000.0),
    ('ACC031','Tyler Nelson','CHECKING','2020-03-02','ACTIVE',4500.0),
    ('ACC032','Lauren Carter','SAVINGS','2018-08-29','ACTIVE',13000.0),
    ('ACC033','Brandon Mitchell','CREDIT','2021-10-11','ACTIVE',24000.0),
    ('ACC034','Kayla Perez','CHECKING','2022-07-17','ACTIVE',2800.0),
    ('ACC035','Austin Roberts','SAVINGS','2019-01-23','ACTIVE',9500.0),
    ('ACC036','Courtney Turner','CHECKING','2020-04-08','INACTIVE',5000.0),
    ('ACC037','Jordan Phillips','SAVINGS','2018-05-14','INACTIVE',7000.0),
    ('ACC038','Taylor Campbell','CREDIT','2021-09-26','INACTIVE',15000.0),
    ('ACC039','Morgan Parker','CHECKING','2019-11-30','INACTIVE',3200.0),
    ('ACC040','Alex Evans','SAVINGS','2022-02-15','INACTIVE',8000.0),
    ('ACC041','Casey Edwards','CHECKING','2020-08-19','INACTIVE',4000.0),
    ('ACC042','Riley Collins','SAVINGS','2018-04-03','INACTIVE',6000.0),
    ('ACC043','Quinn Stewart','CREDIT','2021-07-22','INACTIVE',12000.0),
    ('ACC044','Avery Morris','CHECKING','2019-10-07','INACTIVE',2500.0),
    ('ACC045','Jordan Rogers','SAVINGS','2022-06-13','INACTIVE',9000.0),
    ('ACC046','Blake Reed','CREDIT','2020-12-28','FROZEN',50000.0),
    ('ACC047','Drew Cook','CHECKING','2018-09-16','FROZEN',4800.0),
    ('ACC048','Skyler Morgan','SAVINGS','2021-03-05','FROZEN',11000.0),
    ('ACC049','River Bailey','CREDIT','2019-07-11','FROZEN',40000.0),
    ('ACC050','Sage Cooper','CHECKING','2022-04-20','FROZEN',3600.0),
]

accounts_df = session.create_dataframe(accounts_data, schema=accounts_schema)
accounts_df.write.mode('overwrite').save_as_table('ACCOUNTS')
row_count = session.table('ACCOUNTS').count()
print(f'ACCOUNTS loaded: {row_count} rows')
session.table('ACCOUNTS').show(5)
```

### Load Transactions Table

```python
# Schema for the TRANSACTIONS table
transactions_schema = StructType([
    StructField('TRANSACTION_ID',    StringType(), False),
    StructField('ACCOUNT_ID',        StringType(), True),
    StructField('AMOUNT',            DoubleType(), True),
    StructField('TRANSACTION_TYPE',  StringType(), True),
    StructField('TRANSACTION_DATE',  StringType(), True),
    StructField('MERCHANT_CATEGORY', StringType(), True),
    StructField('DESCRIPTION',       StringType(), True),
])

# 300 synthetic transactions spanning 2023-01 to 2024-06
transactions_data = [
    ('TXN0001','ACC001',45.5,'DEBIT','2023-01-05','GROCERIES','Grocery store purchase'),
    ('TXN0002','ACC001',28.75,'DEBIT','2023-01-12','DINING','Restaurant lunch'),
    ('TXN0003','ACC001',120.0,'DEBIT','2023-01-20','UTILITIES','Electric bill payment'),
    ('TXN0004','ACC001',65.25,'DEBIT','2023-02-03','GROCERIES','Weekly groceries'),
    ('TXN0005','ACC001',15.99,'DEBIT','2023-02-14','ENTERTAINMENT','Streaming subscription'),
    ('TXN0006','ACC001',500.0,'TRANSFER','2023-02-28','RETAIL','Online shopping transfer'),
    ('TXN0007','ACC001',89.5,'DEBIT','2023-03-10','DINING','Dinner with family'),
    ('TXN0008','ACC001',200.0,'DEBIT','2023-03-22','TRAVEL','Hotel booking'),
    ('TXN0009','ACC001',55.0,'DEBIT','2023-04-05','GROCERIES','Supermarket run'),
    ('TXN0010','ACC001',1200.0,'DEBIT','2023-04-18','TRAVEL','Flight tickets'),
    ('TXN0011','ACC001',35.5,'DEBIT','2023-05-02','DINING','Coffee shop'),
    ('TXN0012','ACC001',78.25,'DEBIT','2023-05-15','HEALTHCARE','Pharmacy purchase'),
    ('TXN0013','ACC002',2500.0,'CREDIT','2023-01-07','RETAIL','Salary deposit'),
    ('TXN0014','ACC002',150.0,'DEBIT','2023-01-15','GROCERIES','Bulk grocery shopping'),
    ('TXN0015','ACC002',2500.0,'CREDIT','2023-02-07','RETAIL','Salary deposit'),
    ('TXN0016','ACC002',85.0,'DEBIT','2023-02-20','DINING','Valentine dinner'),
    ('TXN0017','ACC002',2500.0,'CREDIT','2023-03-07','RETAIL','Salary deposit'),
    ('TXN0018','ACC002',320.0,'DEBIT','2023-03-15','UTILITIES','Quarterly insurance'),
    ('TXN0019','ACC003',4500.0,'DEBIT','2023-01-10','TRAVEL','International flight'),
    ('TXN0020','ACC003',250.0,'DEBIT','2023-01-22','DINING','Business dinner'),
    ('TXN0021','ACC003',1800.0,'DEBIT','2023-02-05','RETAIL','Electronics purchase'),
    ('TXN0022','ACC003',95.0,'DEBIT','2023-02-18','ENTERTAINMENT','Concert tickets'),
    ('TXN0023','ACC003',3200.0,'DEBIT','2023-03-01','TRAVEL','Resort booking'),
    ('TXN0024','ACC003',175.0,'DEBIT','2023-03-20','GROCERIES','Gourmet food delivery'),
    ('TXN0025','ACC004',42.0,'DEBIT','2023-01-08','GROCERIES','Local market'),
    ('TXN0026','ACC004',18.5,'DEBIT','2023-01-14','DINING','Fast food'),
    ('TXN0027','ACC004',95.0,'DEBIT','2023-01-28','UTILITIES','Water bill'),
    ('TXN0028','ACC004',55.75,'DEBIT','2023-02-10','GROCERIES','Supermarket'),
    ('TXN0029','ACC004',200.0,'TRANSFER','2023-02-25','RETAIL','Online order'),
    ('TXN0030','ACC004',30.0,'DEBIT','2023-03-05','ENTERTAINMENT','Movie tickets'),
    ('TXN0031','ACC005',1500.0,'CREDIT','2023-01-05','RETAIL','Investment return'),
    ('TXN0032','ACC005',75.0,'DEBIT','2023-01-18','DINING','Restaurant'),
    ('TXN0033','ACC005',1500.0,'CREDIT','2023-02-05','RETAIL','Investment return'),
    ('TXN0034','ACC005',220.0,'DEBIT','2023-02-22','HEALTHCARE','Doctor visit'),
    ('TXN0035','ACC005',1500.0,'CREDIT','2023-03-05','RETAIL','Investment return'),
    ('TXN0036','ACC005',180.0,'DEBIT','2023-03-19','UTILITIES','Electric + gas'),
    ('TXN0037','ACC006',850.0,'DEBIT','2023-01-12','RETAIL','Furniture purchase'),
    ('TXN0038','ACC006',125.0,'DEBIT','2023-01-25','DINING','Anniversary dinner'),
    ('TXN0039','ACC006',2200.0,'DEBIT','2023-02-08','TRAVEL','Weekend getaway'),
    ('TXN0040','ACC006',65.0,'DEBIT','2023-02-20','GROCERIES','Organic store'),
    ('TXN0041','ACC006',450.0,'DEBIT','2023-03-10','HEALTHCARE','Dental work'),
    ('TXN0042','ACC006',1100.0,'DEBIT','2023-03-28','RETAIL','Spring wardrobe'),
    ('TXN0043','ACC007',38.0,'DEBIT','2023-01-06','GROCERIES','Corner store'),
    ('TXN0044','ACC007',22.5,'DEBIT','2023-01-19','DINING','Lunch'),
    ('TXN0045','ACC007',150.0,'DEBIT','2023-02-01','UTILITIES','Internet + phone'),
    ('TXN0046','ACC007',67.0,'DEBIT','2023-02-15','ENTERTAINMENT','Gaming subscription'),
    ('TXN0047','ACC007',45.0,'DEBIT','2023-03-08','GROCERIES','Weekly shop'),
    ('TXN0048','ACC007',280.0,'TRANSFER','2023-03-22','RETAIL','Marketplace purchase'),
    ('TXN0049','ACC008',3000.0,'CREDIT','2023-01-10','RETAIL','Bonus deposit'),
    ('TXN0050','ACC008',195.0,'DEBIT','2023-01-20','GROCERIES','Costco run'),
    ('TXN0051','ACC008',3000.0,'CREDIT','2023-02-10','RETAIL','Bonus deposit'),
    ('TXN0052','ACC008',88.0,'DEBIT','2023-02-28','DINING','Brunch'),
    ('TXN0053','ACC008',3000.0,'CREDIT','2023-03-10','RETAIL','Bonus deposit'),
    ('TXN0054','ACC008',420.0,'DEBIT','2023-03-25','TRAVEL','Train tickets'),
    ('TXN0055','ACC009',5500.0,'DEBIT','2023-01-15','TRAVEL','Luxury vacation'),
    ('TXN0056','ACC009',350.0,'DEBIT','2023-01-28','DINING','Fine dining'),
    ('TXN0057','ACC009',2800.0,'DEBIT','2023-02-12','RETAIL','Jewelry purchase'),
    ('TXN0058','ACC009',150.0,'DEBIT','2023-02-25','ENTERTAINMENT','Theater tickets'),
    ('TXN0059','ACC009',4200.0,'DEBIT','2023-03-08','TRAVEL','Ski trip'),
    ('TXN0060','ACC009',275.0,'DEBIT','2023-03-22','GROCERIES','Wine collection'),
    ('TXN0061','ACC010',28.0,'DEBIT','2023-01-09','GROCERIES','Quick shop'),
    ('TXN0062','ACC010',15.0,'DEBIT','2023-01-22','DINING','Coffee'),
    ('TXN0063','ACC010',80.0,'DEBIT','2023-02-05','UTILITIES','Phone bill'),
    ('TXN0064','ACC010',42.5,'DEBIT','2023-02-18','GROCERIES','Market'),
    ('TXN0065','ACC010',120.0,'TRANSFER','2023-03-01','RETAIL','Online shopping'),
    ('TXN0066','ACC010',25.0,'DEBIT','2023-03-15','ENTERTAINMENT','App purchase'),
    ('TXN0067','ACC011',1800.0,'CREDIT','2023-01-05','RETAIL','Dividend payment'),
    ('TXN0068','ACC011',110.0,'DEBIT','2023-01-20','DINING','Family dinner'),
    ('TXN0069','ACC011',1800.0,'CREDIT','2023-02-05','RETAIL','Dividend payment'),
    ('TXN0070','ACC011',250.0,'DEBIT','2023-02-22','HEALTHCARE','Eye exam + glasses'),
    ('TXN0071','ACC011',1800.0,'CREDIT','2023-03-05','RETAIL','Dividend payment'),
    ('TXN0072','ACC011',95.0,'DEBIT','2023-03-18','UTILITIES','Streaming bundle'),
    ('TXN0073','ACC012',1500.0,'DEBIT','2023-01-11','RETAIL','Home improvement'),
    ('TXN0074','ACC012',200.0,'DEBIT','2023-01-24','DINING','Birthday celebration'),
    ('TXN0075','ACC012',3500.0,'DEBIT','2023-02-07','TRAVEL','Caribbean cruise deposit'),
    ('TXN0076','ACC012',85.0,'DEBIT','2023-02-20','GROCERIES','Specialty store'),
    ('TXN0077','ACC012',750.0,'DEBIT','2023-03-05','HEALTHCARE','Medical procedure'),
    ('TXN0078','ACC012',425.0,'DEBIT','2023-03-22','RETAIL','Spring shopping'),
    ('TXN0079','ACC013',52.0,'DEBIT','2023-01-07','GROCERIES','Weekly groceries'),
    ('TXN0080','ACC013',35.0,'DEBIT','2023-01-21','DINING','Takeout'),
    ('TXN0081','ACC013',130.0,'DEBIT','2023-02-04','UTILITIES','Gas + electric'),
    ('TXN0082','ACC013',48.0,'DEBIT','2023-02-17','ENTERTAINMENT','Books'),
    ('TXN0083','ACC013',72.0,'DEBIT','2023-03-03','GROCERIES','Fresh market'),
    ('TXN0084','ACC013',350.0,'TRANSFER','2023-03-20','RETAIL','Equipment purchase'),
    ('TXN0085','ACC014',2000.0,'CREDIT','2023-01-08','RETAIL','Freelance payment'),
    ('TXN0086','ACC014',165.0,'DEBIT','2023-01-22','GROCERIES','Whole Foods'),
    ('TXN0087','ACC014',2000.0,'CREDIT','2023-02-08','RETAIL','Freelance payment'),
    ('TXN0088','ACC014',95.0,'DEBIT','2023-02-25','DINING','Date night'),
    ('TXN0089','ACC014',2000.0,'CREDIT','2023-03-08','RETAIL','Freelance payment'),
    ('TXN0090','ACC014',300.0,'DEBIT','2023-03-22','TRAVEL','Weekend trip'),
    ('TXN0091','ACC015',2200.0,'DEBIT','2023-01-14','RETAIL','Designer purchase'),
    ('TXN0092','ACC015',180.0,'DEBIT','2023-01-28','DINING','Steakhouse'),
    ('TXN0093','ACC015',1500.0,'DEBIT','2023-02-10','TRAVEL','Ski lift passes'),
    ('TXN0094','ACC015',75.0,'DEBIT','2023-02-24','ENTERTAINMENT','Concert'),
    ('TXN0095','ACC015',3800.0,'DEBIT','2023-03-08','TRAVEL','Spring break trip'),
    ('TXN0096','ACC015',220.0,'DEBIT','2023-03-25','GROCERIES','Party supplies'),
    ('TXN0097','ACC016',32.0,'DEBIT','2023-02-05','GROCERIES','Quick stop'),
    ('TXN0098','ACC016',19.5,'DEBIT','2023-02-18','DINING','Cafe'),
    ('TXN0099','ACC016',110.0,'DEBIT','2023-03-02','UTILITIES','Bills'),
    ('TXN0100','ACC016',55.0,'DEBIT','2023-03-16','GROCERIES','Market run'),
    ('TXN0101','ACC017',1200.0,'CREDIT','2023-01-15','RETAIL','Side project income'),
    ('TXN0102','ACC017',145.0,'DEBIT','2023-01-28','DINING','Team lunch'),
    ('TXN0103','ACC017',1200.0,'CREDIT','2023-02-15','RETAIL','Side project income'),
    ('TXN0104','ACC017',280.0,'DEBIT','2023-03-01','HEALTHCARE','Annual checkup'),
    ('TXN0105','ACC018',1800.0,'DEBIT','2023-01-10','RETAIL','Luxury handbag'),
    ('TXN0106','ACC018',320.0,'DEBIT','2023-01-25','DINING','Fine dining'),
    ('TXN0107','ACC018',4500.0,'DEBIT','2023-02-08','TRAVEL','European vacation'),
    ('TXN0108','ACC018',150.0,'DEBIT','2023-02-22','GROCERIES','Gourmet delivery'),
    ('TXN0109','ACC018',2800.0,'DEBIT','2023-03-05','RETAIL','Watch purchase'),
    ('TXN0110','ACC018',95.0,'DEBIT','2023-03-20','ENTERTAINMENT','Spa day'),
    ('TXN0111','ACC019',40.0,'DEBIT','2023-01-12','GROCERIES','Convenience store'),
    ('TXN0112','ACC019',25.0,'DEBIT','2023-01-26','DINING','Pizza delivery'),
    ('TXN0113','ACC019',90.0,'DEBIT','2023-02-09','UTILITIES','Internet'),
    ('TXN0114','ACC019',60.0,'DEBIT','2023-02-23','ENTERTAINMENT','Video games'),
    ('TXN0115','ACC019',48.0,'DEBIT','2023-03-09','GROCERIES','Snack run'),
    ('TXN0116','ACC019',150.0,'TRANSFER','2023-03-23','RETAIL','Gadget purchase'),
    ('TXN0117','ACC020',1000.0,'CREDIT','2023-01-05','RETAIL','Interest payment'),
    ('TXN0118','ACC020',88.0,'DEBIT','2023-01-19','GROCERIES','Health food store'),
    ('TXN0119','ACC020',1000.0,'CREDIT','2023-02-05','RETAIL','Interest payment'),
    ('TXN0120','ACC020',55.0,'DEBIT','2023-02-20','DINING','Sushi'),
    ('TXN0121','ACC021',1400.0,'DEBIT','2023-01-08','RETAIL','Tech gadgets'),
    ('TXN0122','ACC021',95.0,'DEBIT','2023-01-22','DINING','Tapas bar'),
    ('TXN0123','ACC021',2600.0,'DEBIT','2023-02-05','TRAVEL','Beach vacation'),
    ('TXN0124','ACC021',180.0,'DEBIT','2023-02-19','GROCERIES','Organic market'),
    ('TXN0125','ACC021',800.0,'DEBIT','2023-03-05','HEALTHCARE','Therapy sessions'),
    ('TXN0126','ACC021',350.0,'DEBIT','2023-03-20','ENTERTAINMENT','Festival tickets'),
    ('TXN0127','ACC022',22.0,'DEBIT','2023-01-10','GROCERIES','Dollar store'),
    ('TXN0128','ACC022',12.5,'DEBIT','2023-01-24','DINING','Food truck'),
    ('TXN0129','ACC022',75.0,'DEBIT','2023-02-07','UTILITIES','Phone plan'),
    ('TXN0130','ACC022',35.0,'DEBIT','2023-02-21','ENTERTAINMENT','Movie night'),
    ('TXN0131','ACC023',2200.0,'CREDIT','2023-01-10','RETAIL','Rental income'),
    ('TXN0132','ACC023',175.0,'DEBIT','2023-01-24','GROCERIES','Trader Joes'),
    ('TXN0133','ACC023',2200.0,'CREDIT','2023-02-10','RETAIL','Rental income'),
    ('TXN0134','ACC023',310.0,'DEBIT','2023-02-24','TRAVEL','Road trip gas'),
    ('TXN0135','ACC024',3200.0,'DEBIT','2023-01-12','TRAVEL','Safari booking'),
    ('TXN0136','ACC024',450.0,'DEBIT','2023-01-26','DINING','Charity gala'),
    ('TXN0137','ACC024',5000.0,'DEBIT','2023-02-09','RETAIL','Art purchase'),
    ('TXN0138','ACC024',200.0,'DEBIT','2023-02-23','GROCERIES','Wine subscription'),
    ('TXN0139','ACC024',4800.0,'DEBIT','2023-03-09','TRAVEL','First class flights'),
    ('TXN0140','ACC024',150.0,'DEBIT','2023-03-23','ENTERTAINMENT','Opera tickets'),
    ('TXN0141','ACC025',58.0,'DEBIT','2023-01-09','GROCERIES','Weekly shop'),
    ('TXN0142','ACC025',42.0,'DEBIT','2023-01-23','DINING','Pub dinner'),
    ('TXN0143','ACC025',200.0,'DEBIT','2023-02-06','UTILITIES','Home insurance'),
    ('TXN0144','ACC025',75.0,'DEBIT','2023-02-20','ENTERTAINMENT','Sports event'),
    ('TXN0145','ACC025',62.0,'DEBIT','2023-03-06','GROCERIES','Fresh produce'),
    ('TXN0146','ACC025',400.0,'TRANSFER','2023-03-20','RETAIL','Home goods'),
    ('TXN0147','ACC026',1500.0,'CREDIT','2023-01-12','RETAIL','Consulting fee'),
    ('TXN0148','ACC026',130.0,'DEBIT','2023-01-26','DINING','Birthday dinner'),
    ('TXN0149','ACC026',1500.0,'CREDIT','2023-02-12','RETAIL','Consulting fee'),
    ('TXN0150','ACC026',95.0,'DEBIT','2023-02-26','HEALTHCARE','Supplements'),
    ('TXN0151','ACC027',6000.0,'DEBIT','2023-01-05','TRAVEL','Maldives resort'),
    ('TXN0152','ACC027',500.0,'DEBIT','2023-01-19','DINING','Michelin restaurant'),
    ('TXN0153','ACC027',3500.0,'DEBIT','2023-02-02','RETAIL','Designer clothes'),
    ('TXN0154','ACC027',250.0,'DEBIT','2023-02-16','ENTERTAINMENT','VIP box seats'),
    ('TXN0155','ACC027',8000.0,'DEBIT','2023-03-02','TRAVEL','Private villa rental'),
    ('TXN0156','ACC027',400.0,'DEBIT','2023-03-16','GROCERIES','Premium delivery'),
    ('TXN0157','ACC028',30.0,'DEBIT','2023-01-11','GROCERIES','Corner shop'),
    ('TXN0158','ACC028',18.0,'DEBIT','2023-01-25','DINING','Sandwich shop'),
    ('TXN0159','ACC028',85.0,'DEBIT','2023-02-08','UTILITIES','Electric'),
    ('TXN0160','ACC028',40.0,'DEBIT','2023-02-22','ENTERTAINMENT','Bowling'),
    ('TXN0161','ACC029',1800.0,'CREDIT','2023-01-08','RETAIL','Royalty payment'),
    ('TXN0162','ACC029',200.0,'DEBIT','2023-01-22','GROCERIES','Organic delivery'),
    ('TXN0163','ACC029',1800.0,'CREDIT','2023-02-08','RETAIL','Royalty payment'),
    ('TXN0164','ACC029',150.0,'DEBIT','2023-02-22','DINING','Wine tasting'),
    ('TXN0165','ACC030',2000.0,'DEBIT','2023-01-14','RETAIL','Home theater'),
    ('TXN0166','ACC030',175.0,'DEBIT','2023-01-28','DINING','Anniversary'),
    ('TXN0167','ACC030',3000.0,'DEBIT','2023-02-11','TRAVEL','Cruise deposit'),
    ('TXN0168','ACC030',120.0,'DEBIT','2023-02-25','GROCERIES','Specialty items'),
    ('TXN0169','ACC030',1500.0,'DEBIT','2023-03-11','HEALTHCARE','Cosmetic procedure'),
    ('TXN0170','ACC030',280.0,'DEBIT','2023-03-25','ENTERTAINMENT','Comedy show + dinner'),
    ('TXN0171','ACC031',45.0,'DEBIT','2023-01-07','GROCERIES','Neighborhood market'),
    ('TXN0172','ACC031',28.0,'DEBIT','2023-01-21','DINING','Deli lunch'),
    ('TXN0173','ACC031',110.0,'DEBIT','2023-02-04','UTILITIES','Water + sewer'),
    ('TXN0174','ACC031',65.0,'DEBIT','2023-02-18','GROCERIES','Pet supplies'),
    ('TXN0175','ACC031',35.0,'DEBIT','2023-03-04','ENTERTAINMENT','Streaming upgrade'),
    ('TXN0176','ACC031',250.0,'TRANSFER','2023-03-18','RETAIL','Furniture'),
    ('TXN0177','ACC032',2500.0,'CREDIT','2023-01-10','RETAIL','Teaching income'),
    ('TXN0178','ACC032',180.0,'DEBIT','2023-01-24','GROCERIES','Weekly shop'),
    ('TXN0179','ACC032',2500.0,'CREDIT','2023-02-10','RETAIL','Teaching income'),
    ('TXN0180','ACC032',95.0,'DEBIT','2023-02-24','DINING','Book club dinner'),
    ('TXN0181','ACC033',2800.0,'DEBIT','2023-01-13','RETAIL','Camera equipment'),
    ('TXN0182','ACC033',220.0,'DEBIT','2023-01-27','DINING','Photo studio rental'),
    ('TXN0183','ACC033',4000.0,'DEBIT','2023-02-10','TRAVEL','Photography trip'),
    ('TXN0184','ACC033',130.0,'DEBIT','2023-02-24','GROCERIES','Darkroom supplies'),
    ('TXN0185','ACC033',1500.0,'DEBIT','2023-03-10','RETAIL','Lens upgrade'),
    ('TXN0186','ACC033',85.0,'DEBIT','2023-03-24','ENTERTAINMENT','Gallery exhibition'),
    ('TXN0187','ACC034',25.0,'DEBIT','2023-01-08','GROCERIES','Quick mart'),
    ('TXN0188','ACC034',15.0,'DEBIT','2023-01-22','DINING','Coffee shop'),
    ('TXN0189','ACC034',70.0,'DEBIT','2023-02-05','UTILITIES','Trash + recycling'),
    ('TXN0190','ACC034',32.0,'DEBIT','2023-02-19','ENTERTAINMENT','Rental movie'),
    ('TXN0191','ACC035',1600.0,'CREDIT','2023-01-12','RETAIL','Stock dividend'),
    ('TXN0192','ACC035',140.0,'DEBIT','2023-01-26','GROCERIES','Farmers market'),
    ('TXN0193','ACC035',1600.0,'CREDIT','2023-02-12','RETAIL','Stock dividend'),
    ('TXN0194','ACC035',220.0,'DEBIT','2023-02-26','DINING','Wine dinner'),
    ('TXN0195','ACC001',72.0,'DEBIT','2023-06-05','GROCERIES','Summer produce'),
    ('TXN0196','ACC001',45.0,'DEBIT','2023-06-18','DINING','BBQ restaurant'),
    ('TXN0197','ACC001',350.0,'DEBIT','2023-07-02','TRAVEL','Beach rental'),
    ('TXN0198','ACC001',88.0,'DEBIT','2023-07-15','ENTERTAINMENT','Theme park'),
    ('TXN0199','ACC001',62.0,'DEBIT','2023-08-01','GROCERIES','Back to school'),
    ('TXN0200','ACC001',180.0,'DEBIT','2023-08-20','UTILITIES','Summer AC bills'),
    ('TXN0201','ACC003',3800.0,'DEBIT','2023-04-05','TRAVEL','Business class flights'),
    ('TXN0202','ACC003',280.0,'DEBIT','2023-04-20','DINING','Client entertainment'),
    ('TXN0203','ACC003',5200.0,'DEBIT','2023-05-10','RETAIL','Rolex purchase'),
    ('TXN0204','ACC003',150.0,'DEBIT','2023-05-25','ENTERTAINMENT','Golf club membership'),
    ('TXN0205','ACC003',6500.0,'DEBIT','2023-06-08','TRAVEL','Private yacht charter'),
    ('TXN0206','ACC003',420.0,'DEBIT','2023-06-22','GROCERIES','Catering order'),
    ('TXN0207','ACC009',7000.0,'DEBIT','2023-04-12','TRAVEL','Luxury cruise'),
    ('TXN0208','ACC009',550.0,'DEBIT','2023-04-26','DINING','Private chef'),
    ('TXN0209','ACC009',3500.0,'DEBIT','2023-05-10','RETAIL','Art collection'),
    ('TXN0210','ACC009',300.0,'DEBIT','2023-05-24','ENTERTAINMENT','Yacht party'),
    ('TXN0211','ACC009',9000.0,'DEBIT','2023-06-07','TRAVEL','Monaco GP trip'),
    ('TXN0212','ACC009',180.0,'DEBIT','2023-06-21','GROCERIES','Champagne order'),
    ('TXN0213','ACC012',2200.0,'DEBIT','2023-04-08','RETAIL','Kitchen renovation'),
    ('TXN0214','ACC012',160.0,'DEBIT','2023-04-22','DINING','Mothers day brunch'),
    ('TXN0215','ACC012',4500.0,'DEBIT','2023-05-06','TRAVEL','Hawaiian vacation'),
    ('TXN0216','ACC012',95.0,'DEBIT','2023-05-20','HEALTHCARE','Wellness retreat'),
    ('TXN0217','ACC012',1800.0,'DEBIT','2023-06-03','RETAIL','Patio furniture'),
    ('TXN0218','ACC012',250.0,'DEBIT','2023-06-17','ENTERTAINMENT','Music festival'),
    ('TXN0219','ACC015',1800.0,'DEBIT','2023-04-11','RETAIL','Golf clubs'),
    ('TXN0220','ACC015',320.0,'DEBIT','2023-04-25','DINING','Steakhouse'),
    ('TXN0221','ACC015',5500.0,'DEBIT','2023-05-09','TRAVEL','Summer house rental'),
    ('TXN0222','ACC015',120.0,'DEBIT','2023-05-23','ENTERTAINMENT','Boat rental'),
    ('TXN0223','ACC015',2500.0,'DEBIT','2023-06-06','RETAIL','Summer wardrobe'),
    ('TXN0224','ACC015',85.0,'DEBIT','2023-06-20','GROCERIES','Pool party supplies'),
    ('TXN0225','ACC018',3200.0,'DEBIT','2023-04-08','RETAIL','Handbag collection'),
    ('TXN0226','ACC018',450.0,'DEBIT','2023-04-22','DINING','Spa + dinner'),
    ('TXN0227','ACC018',6000.0,'DEBIT','2023-05-06','TRAVEL','Paris fashion week'),
    ('TXN0228','ACC018',200.0,'DEBIT','2023-05-20','GROCERIES','Organic beauty'),
    ('TXN0229','ACC018',4500.0,'DEBIT','2023-06-03','RETAIL','Summer collection'),
    ('TXN0230','ACC018',150.0,'DEBIT','2023-06-17','ENTERTAINMENT','Art gallery opening'),
    ('TXN0231','ACC021',1800.0,'DEBIT','2023-04-05','RETAIL','Smart home devices'),
    ('TXN0232','ACC021',140.0,'DEBIT','2023-04-19','DINING','Sushi omakase'),
    ('TXN0233','ACC021',3000.0,'DEBIT','2023-05-03','TRAVEL','Thailand trip'),
    ('TXN0234','ACC021',220.0,'DEBIT','2023-05-17','HEALTHCARE','Gym equipment'),
    ('TXN0235','ACC021',2200.0,'DEBIT','2023-06-01','RETAIL','Bike + accessories'),
    ('TXN0236','ACC021',95.0,'DEBIT','2023-06-15','ENTERTAINMENT','Escape room'),
    ('TXN0237','ACC024',5500.0,'DEBIT','2023-04-10','TRAVEL','Vineyard tour Italy'),
    ('TXN0238','ACC024',380.0,'DEBIT','2023-04-24','DINING','Private dining'),
    ('TXN0239','ACC024',7000.0,'DEBIT','2023-05-08','RETAIL','Jewelry commission'),
    ('TXN0240','ACC024',250.0,'DEBIT','2023-05-22','GROCERIES','Truffle delivery'),
    ('TXN0241','ACC024',6200.0,'DEBIT','2023-06-05','TRAVEL','Greek islands yacht'),
    ('TXN0242','ACC024',175.0,'DEBIT','2023-06-19','ENTERTAINMENT','Private screening'),
    ('TXN0243','ACC027',9500.0,'DEBIT','2023-04-02','TRAVEL','Dubai penthouse'),
    ('TXN0244','ACC027',600.0,'DEBIT','2023-04-16','DINING','Omakase experience'),
    ('TXN0245','ACC027',4500.0,'DEBIT','2023-04-30','RETAIL','Watch collection'),
    ('TXN0246','ACC027',350.0,'DEBIT','2023-05-14','ENTERTAINMENT','Formula 1 paddock'),
    ('TXN0247','ACC027',12000.0,'DEBIT','2023-05-28','TRAVEL','Safari lodge'),
    ('TXN0248','ACC027',500.0,'DEBIT','2023-06-11','GROCERIES','Wine cellar restock'),
    ('TXN0249','ACC030',2500.0,'DEBIT','2023-04-07','RETAIL','Bathroom renovation'),
    ('TXN0250','ACC030',195.0,'DEBIT','2023-04-21','DINING','Graduation dinner'),
    ('TXN0251','ACC033',3500.0,'DEBIT','2023-04-14','TRAVEL','Iceland photography'),
    ('TXN0252','ACC033',180.0,'DEBIT','2023-04-28','DINING','Studio party'),
    ('TXN0253','ACC033',2200.0,'DEBIT','2023-05-12','RETAIL','Drone + gimbal'),
    ('TXN0254','ACC033',110.0,'DEBIT','2023-05-26','ENTERTAINMENT','Film festival'),
    ('TXN0255','ACC001',95.0,'DEBIT','2023-09-05','GROCERIES','Fall harvest'),
    ('TXN0256','ACC001',55.0,'DEBIT','2023-09-20','DINING','Oktoberfest'),
    ('TXN0257','ACC001',400.0,'DEBIT','2023-10-05','RETAIL','Halloween costumes'),
    ('TXN0258','ACC001',120.0,'DEBIT','2023-10-20','UTILITIES','Heating startup'),
    ('TXN0259','ACC001',250.0,'DEBIT','2023-11-05','GROCERIES','Thanksgiving prep'),
    ('TXN0260','ACC001',800.0,'DEBIT','2023-11-25','RETAIL','Black Friday deals'),
    ('TXN0261','ACC001',350.0,'DEBIT','2023-12-10','RETAIL','Christmas gifts'),
    ('TXN0262','ACC001',150.0,'DEBIT','2023-12-24','DINING','Christmas Eve dinner'),
    ('TXN0263','ACC001',65.0,'DEBIT','2024-01-05','GROCERIES','New year groceries'),
    ('TXN0264','ACC001',180.0,'DEBIT','2024-01-20','UTILITIES','Winter heating'),
    ('TXN0265','ACC001',45.0,'DEBIT','2024-02-14','DINING','Valentines dinner'),
    ('TXN0266','ACC001',220.0,'DEBIT','2024-02-28','RETAIL','Winter clearance'),
    ('TXN0267','ACC001',75.0,'DEBIT','2024-03-15','GROCERIES','Spring refresh'),
    ('TXN0268','ACC001',500.0,'DEBIT','2024-03-30','TRAVEL','Spring break'),
    ('TXN0269','ACC001',88.0,'DEBIT','2024-04-15','DINING','Easter brunch'),
    ('TXN0270','ACC001',135.0,'DEBIT','2024-04-30','UTILITIES','Spring cleaning'),
    ('TXN0271','ACC001',92.0,'DEBIT','2024-05-15','GROCERIES','Mothers day'),
    ('TXN0272','ACC001',200.0,'DEBIT','2024-05-30','ENTERTAINMENT','Memorial day BBQ'),
    ('TXN0273','ACC001',110.0,'DEBIT','2024-06-15','DINING','Fathers day dinner'),
    ('TXN0274','ACC001',450.0,'DEBIT','2024-06-30','TRAVEL','July 4th trip'),
    ('TXN0275','ACC003',8500.0,'DEBIT','2023-07-10','TRAVEL','Yacht week Croatia'),
    ('TXN0276','ACC003',380.0,'DEBIT','2023-08-05','DINING','Wine country tour'),
    ('TXN0277','ACC003',5000.0,'DEBIT','2023-09-15','RETAIL','Fall fashion'),
    ('TXN0278','ACC003',7200.0,'DEBIT','2023-10-20','TRAVEL','Japan business trip'),
    ('TXN0279','ACC003',450.0,'DEBIT','2023-11-10','ENTERTAINMENT','Box seats playoffs'),
    ('TXN0280','ACC003',3800.0,'DEBIT','2023-12-05','RETAIL','Holiday gifts luxury'),
    ('TXN0281','ACC003',9000.0,'DEBIT','2024-01-15','TRAVEL','New Zealand adventure'),
    ('TXN0282','ACC003',600.0,'DEBIT','2024-02-10','DINING','Super Bowl party catering'),
    ('TXN0283','ACC003',4200.0,'DEBIT','2024-03-20','RETAIL','Spring collection designer'),
    ('TXN0284','ACC003',11000.0,'DEBIT','2024-04-25','TRAVEL','Maldives overwater villa'),
    ('TXN0285','ACC003',320.0,'DEBIT','2024-05-15','GROCERIES','Gala dinner prep'),
    ('TXN0286','ACC003',6800.0,'DEBIT','2024-06-10','TRAVEL','Mediterranean charter'),
    ('TXN0287','ACC009',8500.0,'DEBIT','2023-07-15','TRAVEL','South of France'),
    ('TXN0288','ACC009',420.0,'DEBIT','2023-08-10','DINING','Tasting menu'),
    ('TXN0289','ACC009',6000.0,'DEBIT','2023-09-20','RETAIL','Autumn wardrobe'),
    ('TXN0290','ACC009',500.0,'DEBIT','2023-10-25','ENTERTAINMENT','Halloween masquerade'),
    ('TXN0291','ACC009',10000.0,'DEBIT','2023-11-15','TRAVEL','Aspen ski chalet'),
    ('TXN0292','ACC009',750.0,'DEBIT','2023-12-20','GROCERIES','Holiday hampers'),
    ('TXN0293','ACC009',12000.0,'DEBIT','2024-01-10','TRAVEL','Aurora borealis trip'),
    ('TXN0294','ACC009',350.0,'DEBIT','2024-02-14','DINING','Valentines private chef'),
    ('TXN0295','ACC009',5500.0,'DEBIT','2024-03-25','RETAIL','Spring art acquisition'),
    ('TXN0296','ACC009',8000.0,'DEBIT','2024-04-20','TRAVEL','Cherry blossom Japan'),
    ('TXN0297','ACC009',280.0,'DEBIT','2024-05-10','ENTERTAINMENT','Derby day VIP'),
    ('TXN0298','ACC009',7500.0,'DEBIT','2024-06-15','TRAVEL','Wimbledon package'),
    ('TXN0299','ACC027',15000.0,'DEBIT','2024-01-05','TRAVEL','New Year Bora Bora'),
    ('TXN0300','ACC027',800.0,'DEBIT','2024-02-20','DINING','Charity auction dinner'),
]

transactions_df = session.create_dataframe(transactions_data, schema=transactions_schema)
transactions_df.write.mode('overwrite').save_as_table('TRANSACTIONS')
row_count = session.table('TRANSACTIONS').count()
print(f'TRANSACTIONS loaded: {row_count} rows')
session.table('TRANSACTIONS').show(5)
```

<!-- ------------------------ -->
## PySpark Source Code Patterns

The file `transaction_pipeline.py` is the PySpark pipeline we will migrate. Below are the key patterns the SMA tool converts, annotated with what changes.

### Pattern 1: Session Initialization

```python
# BEFORE — PySpark (does not run natively in Snowflake)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('FinancialTransactionPipeline') \
    .getOrCreate()
```

**What changes:** `SparkSession.builder` is replaced by Snowpark's `get_active_session()`. The `.appName()` call and any connector JAR configuration are removed entirely.

### Pattern 2: Reading via the Spark-Snowflake Connector

```python
# BEFORE — PySpark + Snowflake connector
accounts_df = spark.read \
    .format('net.snowflake.spark.snowflake') \
    .options(**SF_OPTIONS) \
    .option('dbtable', 'ACCOUNTS') \
    .load()
```

**What changes:** The connector format and connection options dictionary are replaced by `session.table('ACCOUNTS')`. Since Snowpark runs *inside* Snowflake, no external connector is needed.

### Pattern 3: User-Defined Functions (UDFs)

```python
# BEFORE — PySpark UDF
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

categorize_amount_udf = F.udf(categorize_amount, StringType())
df.withColumn('amount_category', categorize_amount_udf(F.col('amount')))
```

**What changes:**
- `F.udf(func, ReturnType)` becomes `udf(func, return_type=StringType(), input_types=[DoubleType()])`
- `.withColumn()` becomes `.with_column()` (snake_case)
- Import: `from snowflake.snowpark.functions import udf`

### Pattern 4: Window Functions

```python
# BEFORE — PySpark window
from pyspark.sql.window import Window

w = Window.partitionBy('account_id').orderBy('year_month')
df.withColumn('running_total', F.sum('amount').over(w))
```

**What changes:** `partitionBy` → `partition_by`, `orderBy` → `order_by`, `unboundedPreceding` → `UNBOUNDED_PRECEDING`, `currentRow` → `CURRENT_ROW`.

### Pattern 5: Writing Back to Snowflake

```python
# BEFORE — connector write
result_df.write \
    .format('net.snowflake.spark.snowflake') \
    .options(**SF_OPTIONS) \
    .option('dbtable', 'MONTHLY_SPEND_SUMMARY') \
    .mode('overwrite').save()
```

**What changes:** Replaced by `result_df.write.mode('overwrite').save_as_table('MONTHLY_SPEND_SUMMARY')` — far simpler because the DataFrame is already running inside Snowflake.

> Open `transaction_pipeline.py` in your editor to see the full source before running the migration skill.

<!-- ------------------------ -->
## Run the Migration Skill

This section runs **outside the Snowflake notebook** on your local machine using Cortex Code.

### Step 1: Trigger the Skill

Open Cortex Code and type any of the following:

```
migrate pyspark
convert spark to snowflake
run sma
sma conversion
```

The `snowflake-migration` skill activates automatically.

### Step 2: Provide Project Information

| Field | Value for this lab |
|-------|-------------------|
| Original Source Code Path | Full path to `test_workload_for_demo/` |
| Customer Email | your email address |
| Customer Company | your company name |
| Project Name | `finance-pipeline-lab` |

### Step 3: Choose Migration Status

Select **(b) I want to run the conversion now**.

### Step 4: Output Path

Enter a new empty folder, for example `~/sma-output/finance-lab/`.

### Step 5: Conversion Tool

Select **(c) Snowpark API**.

### Step 6: Configure SMA CLI (First Run Only)

Provide the path to your SMA binary, e.g. `/path/to/SMA-CLI/orchestrator/sma`.

Confirm these options:

| Option | Setting |
|--------|---------|
| Jupyter Conversion | Y |
| SQL Flavor | SparkSql |
| Generate Checkpoints | Y |

### Step 7: Confirm and Run

Type **yes** to start the conversion. Progress is printed as each SMA step completes:

```
[SMA] Step 1/20 - FileDiscovery: STARTED
[SMA] Step 4/20 - PythonConversion: STARTED
...
Conversion was successful.
```

### Step 8: Review the EWI Dashboard

Once conversion completes, the skill opens the EWI dashboard in your browser automatically.

<!-- ------------------------ -->
## Understanding the EWI Dashboard

The dashboard shows every flagged issue in the converted code.

### What is an EWI?

**EWI** = **Error, Warning, or Issue** — a marker left where the SMA tool could not perform a fully automatic conversion.

| Severity | Meaning |
|----------|---------|
| **Error** | Code will not run without a manual fix |
| **Warning** | Code may run but behavior could differ from original |
| **Info** | Informational note — usually safe to ignore |

### EWIs to Expect from This Workload

| PySpark pattern | Why flagged | Severity |
|-----------------|-------------|----------|
| `net.snowflake.spark.snowflake` read/write | Connector replaced by native Snowpark API | Warning |
| `SparkSession.builder` | Replaced by `get_active_session()` | Info |
| `F.udf(func, ReturnType)` | UDF registration syntax differs in Snowpark | Warning |
| `Window.partitionBy()` | Renamed `partition_by()` in Snowpark | Info |
| `.withColumn()` | Renamed `.with_column()` in Snowpark | Info |

### Running the EWI Fixer

When the skill asks *'Would you like to run the EWI Fixer?'*, select **Yes** with these settings:

- **EWI comment handling:** Mark (keeps comments prefixed with [FIXED] or [NOT-FIXED])
- **Which EWIs to process:** Only pending (first run)

The EWI Fixer scans all converted files and applies automatic fixes. The dashboard updates with resolved counts when it finishes.

> After the EWI Fixer completes, return to the Snowflake notebook and run the converted pipeline.

<!-- ------------------------ -->
## Execute the Converted Pipeline

The cells below implement the full Snowpark equivalent of `transaction_pipeline.py`. Run them in order.

### PySpark to Snowpark API Translation

| PySpark | Snowpark | Notes |
|---------|----------|-------|
| `SparkSession.builder.getOrCreate()` | `get_active_session()` | No setup needed inside Snowflake |
| `from pyspark.sql import functions as F` | `from snowflake.snowpark import functions as F` | Same alias, different import |
| `from pyspark.sql.window import Window` | `from snowflake.snowpark.window import Window` | Same class, different import |
| `F.udf(func, ReturnType)` | `udf(func, return_type=..., input_types=[...])` | Explicit input types required |
| `df.withColumn(name, col)` | `df.with_column(name, col)` | Snake_case |
| `df.groupBy(...)` | `df.group_by(...)` | Snake_case |
| `Window.partitionBy(...)` | `Window.partition_by(...)` | Snake_case |
| `Window.orderBy(...)` | `Window.order_by(...)` | Snake_case |
| `Window.unboundedPreceding` | `Window.UNBOUNDED_PRECEDING` | UPPER_CASE constant |
| `Window.currentRow` | `Window.CURRENT_ROW` | UPPER_CASE constant |
| `col.isNotNull()` | `col.is_not_null()` | Snake_case |
| `F.date_format(col, 'yyyy-MM')` | `F.to_char(col, 'YYYY-MM')` | Snowflake date format string |
| `connector.write(...).save()` | `df.write.save_as_table('TABLE')` | Native Snowpark write |
| `spark.read.format(connector).load()` | `session.table('TABLE')` | Native session read |

### Imports and UDFs

```python
# Imports
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import StringType, DoubleType
from snowflake.snowpark.functions import udf

session = get_active_session()
print('Session ready.')

# UDF: categorise transaction amount into size bands
# PySpark: F.udf(func, StringType())
# Snowpark: udf(func, return_type=StringType(), input_types=[DoubleType()])
def _categorize_amount(amount):
    if amount is None:
        return 'unknown'
    if amount < 50.0:
        return 'small'
    elif amount < 500.0:
        return 'medium'
    else:
        return 'large'

categorize_amount_udf = udf(_categorize_amount, return_type=StringType(), input_types=[DoubleType()])

# UDF: simple fraud-flag heuristic
def _fraud_flag(account_type, amount):
    if account_type == 'CREDIT' and amount is not None and amount > 3000.0:
        return 'REVIEW'
    return 'OK'

fraud_flag_udf = udf(_fraud_flag, return_type=StringType(), input_types=[StringType(), DoubleType()])

print('UDFs registered.')
```

### Read Source Tables

```python
# PySpark: spark.read.format('net.snowflake.spark.snowflake').option('dbtable', ...).load()
# Snowpark: session.table('TABLE_NAME')  — no connector needed

accounts_df     = session.table('ACCOUNTS')
transactions_df = session.table('TRANSACTIONS')

print(f'ACCOUNTS rows    : {accounts_df.count()}')
print(f'TRANSACTIONS rows: {transactions_df.count()}')
```

### Filter and Select

```python
# .filter() and .select() work identically in Snowpark
# .isNotNull()  ->  .is_not_null()  (snake_case)

active_accounts = (
    accounts_df
    .filter(F.col('STATUS') == 'ACTIVE')
    .select(
        'ACCOUNT_ID',
        'CUSTOMER_NAME',
        'ACCOUNT_TYPE',
        F.col('CREDIT_LIMIT').cast('double').alias('CREDIT_LIMIT'),
    )
)

filtered_txns = (
    transactions_df
    .filter(F.col('TRANSACTION_TYPE').isin('DEBIT', 'TRANSFER'))
    .filter(F.col('AMOUNT').is_not_null())
    .select(
        'TRANSACTION_ID',
        'ACCOUNT_ID',
        F.col('AMOUNT').cast('double').alias('AMOUNT'),
        'TRANSACTION_TYPE',
        F.to_date('TRANSACTION_DATE', 'YYYY-MM-DD').alias('TRANSACTION_DATE'),
        'MERCHANT_CATEGORY',
        'DESCRIPTION',
    )
)

print(f'Active accounts      : {active_accounts.count()}')
print(f'Filtered transactions: {filtered_txns.count()}')
```

### Apply UDFs and Join

```python
# PySpark: .withColumn()  ->  Snowpark: .with_column()  (snake_case)

enriched_txns = filtered_txns.with_column(
    'AMOUNT_CATEGORY',
    categorize_amount_udf(F.col('AMOUNT'))
)

# Join
joined_df = enriched_txns.join(active_accounts, on='ACCOUNT_ID', how='inner')

joined_df = joined_df.with_column(
    'FRAUD_FLAG',
    fraud_flag_udf(F.col('ACCOUNT_TYPE'), F.col('AMOUNT'))
)

print(f'Joined rows: {joined_df.count()}')
joined_df.select('ACCOUNT_ID', 'AMOUNT', 'AMOUNT_CATEGORY', 'FRAUD_FLAG').show(5)
```

### Aggregation

```python
# PySpark: .groupBy()    ->  Snowpark: .group_by()   (snake_case)
# F.date_format(col, 'yyyy-MM')  ->  F.to_char(col, 'YYYY-MM')
# All agg functions (sum, count, avg, max, when) are identical

monthly_spend = (
    joined_df
    .with_column('YEAR_MONTH', F.to_char(F.col('TRANSACTION_DATE'), 'YYYY-MM'))
    .group_by(
        'ACCOUNT_ID', 'CUSTOMER_NAME', 'ACCOUNT_TYPE',
        'CREDIT_LIMIT', 'YEAR_MONTH', 'MERCHANT_CATEGORY',
    )
    .agg(
        F.sum('AMOUNT').alias('TOTAL_SPEND'),
        F.count('TRANSACTION_ID').alias('TRANSACTION_COUNT'),
        F.avg('AMOUNT').alias('AVG_TRANSACTION_AMOUNT'),
        F.max('AMOUNT').alias('MAX_TRANSACTION_AMOUNT'),
        F.sum(
            F.when(F.col('FRAUD_FLAG') == 'REVIEW', F.lit(1)).otherwise(F.lit(0))
        ).alias('FLAGGED_TRANSACTIONS'),
    )
)

print(f'Monthly aggregation rows: {monthly_spend.count()}')
monthly_spend.show(5)
```

### Window Functions and Write

```python
# PySpark                          Snowpark
# Window.partitionBy(...)    ->    Window.partition_by(...)
# Window.orderBy(...)        ->    Window.order_by(...)
# Window.unboundedPreceding  ->    Window.UNBOUNDED_PRECEDING
# Window.currentRow          ->    Window.CURRENT_ROW

account_month_window = (
    Window.partition_by('ACCOUNT_ID')
    .order_by('YEAR_MONTH')
    .rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
)

global_rank_window = Window.order_by(F.col('TOTAL_SPEND').desc())

lag_window = (
    Window.partition_by('ACCOUNT_ID', 'MERCHANT_CATEGORY')
    .order_by('YEAR_MONTH')
)

result_df = (
    monthly_spend
    .with_column('CUMULATIVE_SPEND',        F.sum('TOTAL_SPEND').over(account_month_window))
    .with_column('SPEND_RANK',              F.rank().over(global_rank_window))
    .with_column('PREV_MONTH_SPEND',        F.lag('TOTAL_SPEND', 1).over(lag_window))
    .with_column('MONTH_OVER_MONTH_CHANGE', F.col('TOTAL_SPEND') - F.col('PREV_MONTH_SPEND'))
    .with_column(
        'CREDIT_UTILISATION_PCT',
        F.round((F.col('CUMULATIVE_SPEND') / F.col('CREDIT_LIMIT')) * 100, 2),
    )
)

# Write to Snowflake
# PySpark: df.write.format(connector).option('dbtable', ...).mode('overwrite').save()
# Snowpark: df.write.mode('overwrite').save_as_table('TABLE_NAME')

result_df.write.mode('overwrite').save_as_table('MONTHLY_SPEND_SUMMARY')

row_count = session.table('MONTHLY_SPEND_SUMMARY').count()
print(f'Pipeline complete. {row_count} rows written to MONTHLY_SPEND_SUMMARY.')
```

<!-- ------------------------ -->
## Validate the Results

Run the queries below to confirm the pipeline produced the expected output.

### Row Count and Column Check

```sql
SELECT
    COUNT(*)                                    AS total_rows,
    COUNT(DISTINCT account_id)                  AS distinct_accounts,
    COUNT(DISTINCT year_month)                  AS distinct_months,
    COUNT(DISTINCT merchant_category)           AS distinct_categories,
    SUM(flagged_transactions)                   AS total_flagged,
    ROUND(AVG(credit_utilisation_pct), 2)       AS avg_credit_utilisation_pct
FROM MONTHLY_SPEND_SUMMARY;
```

### Top 10 Accounts by Total Spend

```sql
SELECT
    account_id,
    customer_name,
    account_type,
    ROUND(SUM(total_spend), 2)   AS lifetime_spend,
    SUM(transaction_count)       AS total_transactions,
    MAX(credit_utilisation_pct)  AS peak_credit_utilisation_pct
FROM MONTHLY_SPEND_SUMMARY
GROUP BY account_id, customer_name, account_type
ORDER BY lifetime_spend DESC
LIMIT 10;
```

### Fraud-Flagged Rows

```sql
SELECT
    account_id,
    customer_name,
    year_month,
    merchant_category,
    total_spend,
    flagged_transactions
FROM MONTHLY_SPEND_SUMMARY
WHERE flagged_transactions > 0
ORDER BY total_spend DESC
LIMIT 15;
```

### Preview All Window Function Columns

```python
session.table('MONTHLY_SPEND_SUMMARY') \
    .select(
        'ACCOUNT_ID', 'CUSTOMER_NAME', 'YEAR_MONTH', 'MERCHANT_CATEGORY',
        'TOTAL_SPEND', 'CUMULATIVE_SPEND', 'SPEND_RANK',
        'PREV_MONTH_SPEND', 'MONTH_OVER_MONTH_CHANGE', 'CREDIT_UTILISATION_PCT'
    ) \
    .sort('SPEND_RANK') \
    .show(10)
```

<!-- ------------------------ -->
## Cleanup

Uncomment and run the SQL below to remove all objects created by this lab.

```sql
-- Uncomment to clean up all lab objects
-- DROP DATABASE IF EXISTS DEMO_DB;
-- DROP WAREHOUSE IF EXISTS LAB_WH;
```

<!-- ------------------------ -->
## Conclusion and Resources

Congratulations! You have successfully migrated a PySpark financial analytics pipeline to Snowflake-native Snowpark Python using Cortex Code and the Snowpark Migration Accelerator.

### What You Learned
- How to use Cortex Code's `spark-migration` skill to automatically convert PySpark code
- The key API differences between PySpark and Snowpark (snake_case methods, UDF registration, Window functions)
- How to interpret and resolve EWI (Error, Warning, Issue) reports
- How to run a complete Snowpark pipeline natively inside Snowflake without any external Spark cluster

### Related Resources
- [Snowpark Python Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
- [Snowpark Migration Accelerator](https://docs.snowflake.com/en/developer-guide/snowpark/migration/snowconvert)
- [Cortex Code Documentation](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code)
- [Snowpark Python API Reference](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index)
