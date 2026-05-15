# @ProjectName Conversion Assessment (**@lang**)
Below the assessment performed for @ProjectName (**@lang**) Migration and describes the technical considerations necessary for the code conversion to Snowflake for **SNOWSCRIPT** using SnowConvert for **@lang** tool.

Status: COMPLETED

Last update: @SessionExecution

## 1. INVENTORY

### 1.1 Source Objects Inventory 
| OBJECT TYPE        | QUANTITY   |
|:-------------------|-----------:|
| VIEW               |            |
| FUNCTION           |            |
| TABLE              |            |
| PROCEDURE          |            |
| SCHEMA             |            |
| INDEX              |            |
| SYNONYM            |            |
| TRIGGER            |            |
| **TOTAL**          | **    **   |

You can find the whole inventory [here]()


### 1.2 Critical Statements For Reviewing 
| ITEM                  | OCCURRENCES        | FILES        |
|:----------------------|:-------------------|-------------:|
| Missing Objects       |                    |              |
| Subqueries            |                    |              |
| Cursors In Functions  |                    |              |
| Cursors In Procedures |                    |              |
| Dynamic SQL           |                    |              |
| LOOP In Functions     |                    |              |
| LOOP In Procedures    |                    |              |
| OPENQUERY             |                    |              |

### 1.3 Inventory Notes
1. There are a total of @unitsTotal top level code units.


 ## 2. CODE ASSESSMENT USING SNOWCONVERT


### 2.1 Conversion Rates Summary
Snowflake SnowConvert for **@lang**
App Version **@appVersion** / Conversion Core **@coreVersion**

| CODE UNIT PARENT CATEGORY        | CODE UNIT               | FULLY CONVERTED CODE UNIT RATE  | LOC RATE | TOTAL CODE UNITS | LINES OF CODE | PARSING EWIS |
|:---------------------------------|:------------------------|:--------------------------------|:---------|:-----------------|:--------------|:-------------|

### 2.2 Automatic Conversion Estimate (SnowConvert for **@lang**)

Based on automatic metrics, this is the automatic conversion estimate:

| CODE UNIT            | TASK             | Quantity           | EFFORT (HH)     |
|:---------------------|:-----------------|:-------------------|----------------:|
| TOTAL                |                  | **@totalQuantity** | **@totalConv**  |




### 2.3 Conversion Notes
1. It is important to clarify that in the Object rate are all the objects that have 100% of conversion percentaje (fully converted), so if there is an object that has 99% of conversion percentaje the tool will not include it into this Object rate. That means that the most part of the object has may be a high conversion rate individually but they do not have the 100% to be included in the Object rate.
2. Indexes and table partitions are not required in Snowflake and this should not have impact on migration. If performance challenges are observed, existing indexes and table partition definitions may be used to define custom clustering keys.
3. Missing dependencies were not estimated, it's required to be reviewed with the customer, please refer to Missing Dependencies, [SSC-FDM-0007](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/functional-difference/generalFDM#ssc-fdm-0007) in the IssuesEstimationAggregate tab in the Estimation Report.
4. The Snowflake database service does not provide native support for triggers in the traditional sense, but this doesn't limit its capabilities, as the following can be performed:
   - <u>Triggers used for adding default values</u>: If the trigger is used to validate if there is no value coming, we can add that functionality at the table creation.
   - <u>Triggers used for adding incremental values</u>: If the trigger is used to validate if there is no value coming, we can add that functionality at the table creation. 
   - <u>Triggers used for running statements or DMLs</u>: This is not migratable to table creation, so, the way that can be recommended is using tasks that calls a stored procedure with the logic inside the trigger. 
   - <u>Triggers used for auditing tables</u>: In this case Streams and Tasks to replicate the logic of triggers in a more efficient and scalable way.
5. Synonyms are not supported objects in snowflake. The standard manual conversion is to convert synonyms into views, this solution only works for read only purposes. In case the synonym is used in other types of statements, a different approach should be done, these approaches are not included in this assessment and additional estimation should be applied.

### 2.4 Database Link Dependencies
Database links are not supported in Snowflake. Data used by database links will need to be made available as external tables or loaded into Snowflake and references in objects will need to be modified to reference the resulting database objects containing the source data. DDL can be provided and included in the conversion.

| DB LINK NAME | REFERENCING CODE UNIT TYPE  | REFERENCING CODE UNIT SCHEMA | REFERENCING CODE UNIT NAME   | REFERENCES<sup>1</sup> |
| :----------- | :-------------------------- | :--------------------------- | :--------------------------- | :--------------------- |

### 2.5 Assumptions and Restrictions
1. This assessment doesn't include the estimation of creating the UDFs for the functions not supported.
2. Collect stats and **@lang** database syntax wasn't completely commented out, just for the sake of assessment, the time for commenting out all of these on the code is considered on this assessment.
3. The estimate included in the assessment is the conversion effort, in addition, please consider:
    - **Meetings time**: Estimate additional time for every attendee from Snowflake (migration consultant, Consulting Manager, SME's). Additional analysis sessions or technical leader support meetings with the client are common during the early and mid stages.
    - **Code remediation**: Estimate additional time for common issues that are caused by missing object or execution order, missing parameters or missing data are not included.
    - **Status reporting**: This assessment includes updating the inventory report. Estimate additional time for more complex or future requested reports.
    - **Unit testing**: The estimation only includes basic unit testing. 
        * Additional testing will be performed by the customer.
        * Resolution of issues found during testing are not included in this estimation. 
    - **Contingency**: No contingency has been included in this estimation.
    - **Customer requirements**: This assessment assumes the code will be converted using standard best practices. Any custom customer requirement is not included.
    - **Support period**: Please consider the estimate of additional hours for the support phase.
4. All Datatypes will be converted as standard managed by SnowConvert. See for details [here](@langDataTypes)

 ## **3. MANUAL CONVERSION (without SnowConvert)**

In scenario of manual conversion, the estimate would be the following:

| CODE UNIT TYPE | QUANTITY  | MANUAL EFFORT (HH) |
|:---------------|:----------|-------------------:|
|                | **TOTAL** | **@totalManual**   |

If the conversion is automated, a reduction of **@effortSaved**% can be achieved compared to the manual conversion.

If you want to see more details on how a manual conversion is calculated click [here](/METRICS.md)