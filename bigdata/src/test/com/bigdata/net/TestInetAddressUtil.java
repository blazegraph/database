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

import junit.framework.TestCase2;

/**
 * Test suite for {@link InetAddressUtil}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInetAddressUtil extends TestCase2 {

    /**
     * 
     */
    public TestInetAddressUtil() {
    }

    /**
     * @param arg0
     */
    public TestInetAddressUtil(String arg0) {
        super(arg0);
    }

    public void test01() {

        assertSameByte((byte) 0, "0");

        assertSameByte((byte) 0x7f, "127");
        
        assertSameByte((byte) 0x80, "128");
        
        assertSameByte((byte) 0xff, "255");

    }
    
    private void assertSameByte(final byte expected, final String str) {
        
        final int i = Integer.parseInt(str);
        
        final byte actual = (byte) (i - 128);

        final byte actual2 = InetAddressUtil.encodeByte(i - 128);

        final byte actual3 = InetAddressUtil.decodeByte(i);

        System.out.println("expected=" + expected + ", str=" + str + ", int=" + i
                    + ", actual=" + actual + ", actual2=" + actual2
                    + ", actual3=" + actual3);

        if (actual2 != expected)
            fail("expected=" + expected + ", str=" + str + ", int=" + i
                    + ", actual=" + actual + ", actual2=" + actual2
                    + ", actual3=" + actual3);

    }

    /**
     * Verifies that the same result is obtained when we interpret some IPV4
     * addresses using {@link InetAddress#getByName(String)} and using
     * {@link InetAddressUtil#getByName(String)}.
     * 
     * @throws UnknownHostException
     */
    public void test_getHostByName() throws UnknownHostException {
        
        assertEquals(
                InetAddress.getByName("0.127.128.255").getAddress(),
                InetAddressUtil.getByName("0.127.128.255").getAddress()
                );

        assertEquals(
              InetAddress.getByName("192.168.8.12").getAddress(),
              InetAddressUtil.getByName("192.168.8.12").getAddress()
              );

    }

}
