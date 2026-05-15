All the code in this folder was generated using SnowConvert

# Best practices
## Use recommended extraction scripts
It is highly encouraged that the input code for SnowConvert was extracted correctly from the database using the [recommended extraction scripts](https://github.com/Snowflake-Labs/SC.DDLExportScripts/). Doing this curates the input code, and a successful migration is far more likely.

## EWIs and Functional Differences Messages
SnowConvert generates a series of messages in the output code to indicate scenarios requiring additional effort. It is highly recommended to
review these messages before trying to deploy the code in Snowflake.

## Limit alter table statements for table conversion
It is best practice to have complete DDL table definitions including all metadata related to the table (columns, constraints, default column values, etc), rather than defining an incomplete table and then adding extra functionality via ALTER TABLE statements.

Some variants of ALTER TABLE statements are not supported in Snowflake, but the functionality can be replicated if the CREATE TABLE statement includes all this information.

Finally, it is more efficient to deploy complete TABLES rather than creating a version of them which will be altered later.

# Documentation references
For more information, please visit:
- SnowConvert [main documentation page](https://docs.snowconvert.com/snowconvert).
- [Extraction scripts for Teradata](https://github.com/Snowflake-Labs/SC.DDLExportScripts/tree/main/Teradata)
- [Extraction scripts for Oracle](https://github.com/Snowflake-Labs/SC.DDLExportScripts/tree/main/Oracle)
- [Extraction scripts for SqlServer](https://github.com/Snowflake-Labs/SC.DDLExportScripts/tree/main/SQLServer)
