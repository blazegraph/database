/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Jan 13, 2009
 */

package com.bigdata.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for working around some known issues.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated this was a red herring.
 */
public class InetAddressUtil {
    
    /** {@link Pattern} matching IPV4 addresses. */
    static private final Pattern ipv4 = Pattern
            .compile("^([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)$");
    
    /**
     * There is a bug with reverse DNS lookup in
     * {@link InetAddress#getByName(String)} when presented with a IP address
     * rather than a host name. It will always attempt reverse DNS lookup of the
     * IP address. Therefore if reverse DNS is NOT setup correctly, a
     * significant delay (4-5 seconds on Windows) will occur for each lookup and
     * resolution may fail with an {@link UnknownHostException}.
     * 
     * @throws UnknownHostException 
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5092063
     * 
     * @todo this does not handle IPV6
     */
    public static InetAddress getByName(String s) throws UnknownHostException {
     
        if (s == null)
            throw new NullPointerException();
        
        final Matcher m = ipv4.matcher(s);
        
        if(m.matches()) {
            
            byte a = encodeByte(Integer.parseInt(m.group(1)) - 128);
            byte b = encodeByte(Integer.parseInt(m.group(2)) - 128);
            byte c = encodeByte(Integer.parseInt(m.group(3)) - 128);
            byte d = encodeByte(Integer.parseInt(m.group(4)) - 128);

            return InetAddress.getByAddress(s, new byte[] { a, b, c, d });

        }

        return InetAddress.getByName(s);

    }

    /**
     * Converts an unsigned byte into a signed byte.
     * 
     * @param v
     *            The unsigned byte.
     *            
     * @return The corresponding signed value.
     */
    final static byte decodeByte(final int v) {

        int i = v;

        if (i < 0) {

            i = i + 0x80;

        } else {

            i = i - 0x80;

        }

        return (byte) (i & 0xff);

    }

    /**
     * Converts a signed byte into an unsigned byte.
     * 
     * @param v
     *            The signed byte.
     *            
     * @return The corresponding unsigned value.
     */
    final static byte encodeByte(final int v) {

        if (v > 127 || v < -128)
            throw new IllegalArgumentException("v=" + v);
        
        int i = v;

        if (i < 0) {

            i = i - 0x80;

        } else {

            i = i + 0x80;

        }

        return (byte) (i & 0xff);

    }

}
