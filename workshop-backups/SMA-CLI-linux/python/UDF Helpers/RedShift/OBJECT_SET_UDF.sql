-- <copyright file="OBJECT_SET_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>
-- ====================================================================

CREATE OR REPLACE FUNCTION PUBLIC.OBJECT_SET_UDF(
    input VARIANT,
    object_keys ARRAY,
    object_values ARRAY
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

    // Deep merge function that takes two arrays: paths and values
    function deepMerge(target, paths, values) {
      const result = JSON.parse(JSON.stringify(target)); // Deep copy of target

      // Validate arrays have same length
      if (paths.length !== values.length) {
        throw new Error('Paths and values arrays must have the same length');
      }

      // Set each path with its corresponding value
      paths.forEach((path, index) => {
        setNestedValue(result, path, values[index]);
      });

      return result;
    }

    return deepMerge(INPUT, OBJECT_KEYS, OBJECT_VALUES);
$$;