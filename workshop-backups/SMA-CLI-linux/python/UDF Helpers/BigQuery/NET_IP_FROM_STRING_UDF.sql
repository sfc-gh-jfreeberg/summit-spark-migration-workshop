-- <copyright file="NET_IP_FROM_STRING_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.NET_IP_FROM_STRING_UDF(IP VARCHAR)
RETURNS BINARY
LANGUAGE JAVA
CALLED ON NULL INPUT
HANDLER = 'Net.ipFromString'
<SnowConvertVersionComment>
AS
'
    import java.net.InetAddress;
    import java.net.UnknownHostException;
    
    public class Net {
        
        public static byte[] ipFromString(String ipString) {
            if (ipString == null || ipString.trim().isEmpty()) {
                return null;
            }
        
            try {
               InetAddress address = InetAddress.getByName(ipString.trim());
               return address.getAddress();
            } catch (UnknownHostException e) {
               return null;
            }
        }
    }
';