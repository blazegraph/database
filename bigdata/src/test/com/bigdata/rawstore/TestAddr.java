/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 14, 2006
 */

package com.bigdata.rawstore;

import java.util.Random;

import junit.framework.TestCase;

/**
 * @deprecated along with {@link Addr}.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAddr extends TestCase {

    public TestAddr() {
        
    }
    
    public TestAddr(String name) {
        super(name);
    }
    
    public void doRoundTrip( int nbytes, int offset ) {
        
        final long lval = Addr.toLong(nbytes,offset);

        assertEquals("nbytes(lval=" + Long.toHexString(lval) + ")", nbytes,
                Addr.getByteCount(lval));

        assertEquals("offset(lval=" + Long.toHexString(lval) + ")", offset,
                Addr.getOffset(lval));

    }

    /**
     * Spot tests round trip of some values.
     */
    public void test_toLong01() {

        doRoundTrip(1, 1);
        
        doRoundTrip(0xaa, 0xff);
        
    }

    /**
     * Test of {@link Addr#toLong(int nbytes, int offset)}.
     */
    public void test_toLong02() {
        
        Random r = new Random();
        
        final int limit = 100000;
        
        for( int i=0; i<limit; i++ ) {

            final int nbytes = r.nextInt(Integer.MAX_VALUE-1)+1;
            
            final int offset = r.nextInt(Integer.MAX_VALUE);

            doRoundTrip( nbytes, offset );
            
        }
        
    }

}
