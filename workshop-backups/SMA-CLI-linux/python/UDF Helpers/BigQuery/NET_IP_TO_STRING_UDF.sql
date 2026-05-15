-- <copyright file="NET_IP_TO_STRING_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of NET.IP_TO_STRING function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.NET_IP_TO_STRING_UDF(IP_ADDRESS BINARY)
RETURNS STRING
LANGUAGE JAVA
CALLED ON NULL INPUT
HANDLER = 'Net.ipToString'
<SnowConvertVersionComment>
AS
'
public class Net {
    public static String ipToString(byte[] ipAddress) {
        if (ipAddress == null) {
            return null;
        }
        
        try {
            if (ipAddress.length == 4) {
                // IPv4: Convert to dotted decimal notation
                return String.format("%d.%d.%d.%d", 
                    (ipAddress[0] & 0xFF), 
                    (ipAddress[1] & 0xFF), 
                    (ipAddress[2] & 0xFF), 
                    (ipAddress[3] & 0xFF)
                );
            } else if (ipAddress.length == 16) {
                // IPv6: Convert to colon-separated hexadecimal
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 16; i += 2) {
                    if (i > 0) sb.append(":");
                    int group = ((ipAddress[i] & 0xFF) << 8) | (ipAddress[i + 1] & 0xFF);
                    sb.append(String.format("%04x", group));
                }
                return sb.toString();
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }
}
';