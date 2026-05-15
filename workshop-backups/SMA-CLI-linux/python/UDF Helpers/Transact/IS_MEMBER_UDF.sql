-- <copyright file="IS_MEMBER.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: Determines Windows group membership by examining an access token
-- IS_MEMBER returns 1 when the current user is a member of role.; otherwise it returns 0.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.IS_MEMBER_UDF(ROLE STRING) 
RETURNS BOOLEAN
<SnowConvertVersionComment>
as
$$

SELECT ARRAY_CONTAINS(UPPER(ROLE)::VARIANT, PARSE_JSON(CURRENT_AVAILABLE_ROLES()))

$$;