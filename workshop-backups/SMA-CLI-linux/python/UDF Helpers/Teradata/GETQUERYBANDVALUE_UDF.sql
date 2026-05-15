-- <copyright file="GETQUERYBANDVALUE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ====================================================================
-- DESCRIPTION: RETRIEVES THE VALUE ASSOCIATED TO A NAME INSIDE THE NAME-VALUE PAIRS OF THE TRANSACTION, SESSION OR PROFILE QUERY BAND.
-- EQUIVALENT: GETQUERYBANDVALUE FUNCTION FROM TERADATA (3 PARAMETERS OVERLOAD).
-- PARAMETERS:
--  QUERYBAND VARCHAR - THE QUERY BAND, CAN BE A CONCATENATION OF TRANSACTION, SESSION AND PROFILE QUERY BANDS.
--  SEARCHTYPE FLOAT - THE LEVEL THE PAIRS WILL BE SEARCHED FOR A MATCH, 0 = ANY, 1 = TRANSACTION, 2 = SESSION, 3 = PROFILE.
--  SEARCHNAME VARCHAR - THE NAME KEY TO SEARCH IN THE PAIRS.
-- RETURNS: THE VALUE ASSOCIATED WITH THE NAME KEY AT THE SPECIFIED LEVEL OR NULL IF NOT FOUND.
-- EXAMPLE:
--  INPUT:
--      SELECT GETQUERYBANDVALUE_UDF('=T> account=Matt;user=Matt200; =S> account=SaraDB;user=Sara;role=DbAdmin;', 0, 'account');
--      SELECT GETQUERYBANDVALUE_UDF('=T> account=Matt;user=Matt200; =S> account=SaraDB;user=Sara;role=DbAdmin;', 2, 'account');
--      SELECT GETQUERYBANDVALUE_UDF('=T> account=Matt;user=Matt200; =S> account=SaraDB;user=Sara;role=DbAdmin;', 0, 'role');
--      SELECT GETQUERYBANDVALUE_UDF('=T> account=Matt;user=Matt200; =S> account=SaraDB;user=Sara;role=DbAdmin;', 1, 'role');
--  RESULT:
--      Matt
--      SaraDB
--      DbAdmin
--      NULL
-- ====================================================================
CREATE OR REPLACE FUNCTION PUBLIC.GETQUERYBANDVALUE_UDF(QUERYBAND VARCHAR, SEARCHTYPE FLOAT, SEARCHNAME VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    var KeyValuePairs = QUERYBAND;
    if (SEARCHTYPE != 0)
    {
        var queryTagDelimiterPrefixes = { 1 : "T", 2 : "S", 3 : "P"};
        var KeyValuePairsRegex = `=[${queryTagDelimiterPrefixes[SEARCHTYPE]}]>.+?((?==[TSP]>)|$)`;
        var matchedPairs = QUERYBAND.match(KeyValuePairsRegex);
        if (matchedPairs == null)
        {
            return null;
        }

        KeyValuePairs = matchedPairs[0];
    }

    var removedDelimiters = KeyValuePairs.replace(/=[TSP]>\s*/, "");
    KeyValuePairs = removedDelimiters.split(";");
    for (var pair of KeyValuePairs)
    { 
        if (pair.startsWith(SEARCHNAME))
        {
            return pair.split("=")[1];
        }
    }

    return null;
$$;

-- ====================================================================
-- DESCRIPTION: RETRIEVES THE VALUE ASSOCIATED TO A NAME INSIDE THE NAME-VALUE PAIRS OF THE SESSION QUERY BAND.
-- EQUIVALENT: GETQUERYBANDVALUE FUNCTION FROM TERADATA (2 PARAMETERS OVERLOAD).
--             PARTIAL EQUIVALENCE, CHECK SSC-FDM-TD0019 FOR MORE INFORMATION https://docs.snowconvert.com/snowconvert/for-teradata/issues/SSC-EWI-TD0084
-- PARAMETERS:
--  SEARCHNAME VARCHAR - THE NAME KEY TO SEARCH IN THE PAIRS.
-- RETURNS: THE VALUE ASSOCIATED WITH THE NAME KEY INSIDE THE SESSION QUERY BAND OR NULL IF NOT FOUND.
-- EXAMPLE:
--  INPUT:
--      ALTER SESSION SET QUERY_TAG = 'user=Tyrone;role=security';
--      SELECT GETQUERYBANDVALUE_UDF('role');
--  RESULT:
--      security
-- ====================================================================
CREATE OR REPLACE FUNCTION PUBLIC.GETQUERYBANDVALUE_UDF(SEARCHNAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    SELECT GETQUERYBANDVALUE_UDF(QUERY_TAG, 0, SEARCHNAME) FROM (SELECT QUERY_TAG FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 1)) WHERE LENGTH(QUERY_TAG) > 0)
$$;