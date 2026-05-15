-- <copyright file="ASCIISTR_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.ASCIISTR_UDF(s STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  if (S === null) {
    return null;
  }

  const parts = [];
  for (let i = 0; i < S.length; i++) {
    const charCode = S.charCodeAt(i);
    if (charCode < 128) {
      // Character is already ASCII
      parts.push(S.charAt(i));
    } else {
      // Character is non-ASCII, so escape it
      const hexCode = charCode.toString(16).toUpperCase().padStart(4, '0');
      parts.push('\\' + hexCode);
    }
  }
  return parts.join('');
$$;