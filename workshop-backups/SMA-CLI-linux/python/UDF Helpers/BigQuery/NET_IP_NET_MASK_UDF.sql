-- <copyright file="NET_IP_NET_MASK_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of NET.IP_NET_MASK function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.NET_IP_NET_MASK_UDF(num_output_bytes INT, prefix_length INT)
RETURNS BINARY
LANGUAGE JAVA
CALLED ON NULL INPUT
HANDLER = 'Net.ipNetMask'
<SnowConvertVersionComment>
AS
'
    public class Net {
        public static byte[] ipNetMask(int numOutputBytes, int prefixLength) {
            if (numOutputBytes != 4 && numOutputBytes != 16) {
                throw new IllegalArgumentException("numOutputBytes must be 4 (IPv4) or 16 (IPv6)");
            }
            if (prefixLength < 0 || prefixLength > numOutputBytes * 8) {
                throw new IllegalArgumentException("prefixLength must be between 0 and " + (numOutputBytes * 8));
            }
            
            byte[] mask = new byte[numOutputBytes];
            
            for (int i = 0; i < numOutputBytes; i++) {
                int byteIndex = i;
                int bitsInThisByte = Math.min(8, Math.max(0, prefixLength - (byteIndex * 8)));
                
                if (bitsInThisByte == 8) {
                    mask[i] = (byte) 0xFF;
                } else if (bitsInThisByte > 0) {
                    mask[i] = (byte) (0xFF << (8 - bitsInThisByte));
                } else {
                    mask[i] = 0;
                }
            }
            
            return mask;
        }
    }
';