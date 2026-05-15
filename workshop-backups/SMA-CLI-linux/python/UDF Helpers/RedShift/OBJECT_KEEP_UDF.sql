-- <copyright file="OBJECT_KEEP_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>
-- ====================================================================
CREATE OR REPLACE FUNCTION PUBLIC.OBJECT_KEEP_UDF(
    originalJson VARIANT,
    selectedProperties ARRAY
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    // Helper function to get value from nested path
    function getNestedValue(obj, path) {
      return path.split('.').reduce((current, key) => {
        return current?.[key];
      }, obj);
    }

    // Helper function to set value at nested path
    function setNestedValue(obj, path, value) {
      const keys = path.split('.');
      const lastKey = keys.pop();

      const target = keys.reduce((current, key) => {
        if (!(key in current)) {
          current[key] = {};
        }
        return current[key];
      }, obj);

      target[lastKey] = value;
    }

    const newJson = {};
    SELECTEDPROPERTIES.forEach(prop => {
      const value = getNestedValue(ORIGINALJSON, prop);
      if (value !== undefined) {
        setNestedValue(newJson, prop, value);
      }
    });

    return newJson;
$$;