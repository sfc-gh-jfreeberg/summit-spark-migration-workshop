-- <copyright file="XP_LOGININFO_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The XP_LOGININFO_UDF() function selects the information from the APPLICABLE_ROLES VIEW
-- and returns the same columns set that the XP_LOGININFO procedure in SQL Server
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.XP_LOGININFO_UDF(acctname TEXT)
RETURNS TABLE("account name" TEXT, type TEXT, privilege TEXT, "mapped login name" TEXT, "permission path" TEXT)
<SnowConvertVersionComment>
AS
$$
SELECT
  GRANTEE,
  ROLE_OWNER,
  ROLE_NAME,
  GRANTEE,
  NULL
FROM
    "ADVENTUREWORKS"."INFORMATION_SCHEMA"."APPLICABLE_ROLES"
WHERE
    GRANTEE = acctname
$$;

CREATE OR REPLACE FUNCTION PUBLIC.XP_LOGININFO_UDF()
RETURNS TABLE("account name" TEXT, type TEXT, privilege TEXT, "mapped login name" TEXT, "permission path" TEXT)
<SnowConvertVersionComment>
AS
$$
SELECT
  GRANTEE,
  ROLE_OWNER,
  ROLE_NAME,
  GRANTEE,
  NULL
FROM
    "ADVENTUREWORKS"."INFORMATION_SCHEMA"."APPLICABLE_ROLES"
$$;