-- <copyright file="NET_IPV4_TO_INT64_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of NET.IPV4_TO_INT64 function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.NET_IPV4_TO_INT64_UDF(ip BINARY)
RETURNS NUMBER(38,0)
LANGUAGE JAVA
CALLED ON NULL INPUT
HANDLER = 'Net.ipv4ToInt64'
<SnowConvertVersionComment>
AS
  'import java.net.InetAddress;
   import java.net.UnknownHostException;

   public class Net {
    
        public static Long ipv4ToInt64(byte[] ipBytes) {
        if (ipBytes == null || ipBytes.length != 4) {
            return null;
        }
        
        long result = 0;
        for (int i = 0; i < 4; i++) {
            result = (result << 8) | (ipBytes[i] & 0xFF);
        }
        return result;
    }
}';