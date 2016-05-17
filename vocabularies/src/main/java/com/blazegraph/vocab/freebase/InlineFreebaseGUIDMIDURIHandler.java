/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package com.blazegraph.vocab.freebase;

import java.math.BigInteger;

import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * 
 * Utility IV to generate IVs for URIs in the form of http://rdf.freebase.com/ns/m.0_zdd15
 * where the localName of the URI is a Freebase MID.   
 * 
 * @author beebs
 */
public class InlineFreebaseGUIDMIDURIHandler extends InlineURIHandler{
	
	public InlineFreebaseGUIDMIDURIHandler(String namespace) {
		super(namespace);
	}

	final static String MID_PREFIX = "m.0";
	final static String GUID_PREFIX = "g.1";
	
	
	@Override
	@SuppressWarnings({ "rawtypes", "static-access" })
	protected AbstractLiteralIV createInlineIV(String localName) {
		// if (!localName.startsWith(this.MID_PREFIX) &&
		// !localName.startsWith(this.GUID_PREFIX)) {
		// return null;
		// }

		final BigInteger midLong;

		try {
			if (localName.startsWith(this.MID_PREFIX)) {
				// handle m.0 case
				// Use a pad of one for the mids m.0
				midLong = new BigInteger("1"
						+ Long.toString(midToLong(localName)));

			} else if (localName.startsWith(this.GUID_PREFIX)) { 
				// handle g.1  case
				// Use a pad of one for the guids g.1
				midLong = new BigInteger("2"
						+ Long.toString(midToLong(localName)));

			} else { // Handle just the MID without a prefix

				midLong = new BigInteger("3"
						+ Long.toString(midToLong(localName)));
			}
		} catch (NumberFormatException e) {
			// if (log.isDebugEnabled()) {
			// log.debug("Invalid integer", e);
			// }
			return null;
		}

		return new XSDIntegerIV(midLong);
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		final String strVal = delegate.getInlineValue().toString();
		final String type = strVal.substring(0,1);
		final String val = strVal.substring(1);
		final BigInteger longVal = new BigInteger(val);
		
		//Empty string used for un-prefixed MID, type "3"
		String prefix = "";
		
		if(type.equals("1")) {
			prefix = MID_PREFIX;
		} else if(type.equals("2")) {
			prefix = GUID_PREFIX;
		}

		return longToMid(prefix, longVal.longValue());
	}
	
	//Apache 2 License from https://github.com/paulhoule/infovore/blob/6672aed673fd5672b4ec7ce6bcf34913c14c9295/bakemono/src/main/java/com/ontology2/bakemono/util/StatelessIdFunctions.java
	private static String b32digits="0123456789bcdfghjklmnpqrstvwxyz_";

    public static long midToLong(String mid) {
        long value=0;
        int start = 0;
        
        if(mid.startsWith(MID_PREFIX) || mid.startsWith(GUID_PREFIX)) {
        	start = 3; //skip the prefix
        }

        for(int i=start;i<mid.length();i++) {
            String c=mid.substring(i,i+1);
            int digitValue= b32digits.indexOf(c);
            if (digitValue==-1) {
                throw new IllegalArgumentException("ill-formed mid ["+mid+"]");
            }
            value = value << 5;
            value = value | digitValue;
        }

        return value;
    }

    public static String longToGuid(long l) {
        return "#9202a8c04000641f"+Long.toHexString(l | 0x8000000000000000l);
    }

    public static String midToGuid(String mid) {
        return longToGuid(midToLong(mid));
    }

    public static long guidToLong(String guid) {
        String guidPrefix="#9202a8c04000641f8";
        if (!guid.startsWith(guidPrefix)) {
            throw new IllegalArgumentException("Guid ["+guid+"] does not start with valid prefix");
        }

        String guidDigits=guid.substring(guidPrefix.length());
        if (15!=guidDigits.length()) {
            throw new IllegalArgumentException("Guid ["+guid+"] has wrong number of digits");
        }

        return Long.parseLong(guidDigits,16);
    };

    public static String longToMid(final String prefix, long numericId) {
        final StringBuffer sb=new StringBuffer(16);
        final StringBuffer sbPrefix = new StringBuffer(prefix);
        while(numericId>0) {
            int digit=(int) (numericId % 32);
            sb.append(b32digits.charAt(digit));
            numericId=numericId/32;
        }
        sb.append(sbPrefix.reverse());
        return sb.reverse().toString();
    };

    public static String guidToMid(String guid) {
        return longToMid(MID_PREFIX,guidToLong(guid));
    }
    //End Apache 2 License
}
