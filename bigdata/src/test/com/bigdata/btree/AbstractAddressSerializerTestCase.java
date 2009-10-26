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
 * Created on Sep 6, 2007
 */

package com.bigdata.btree;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Abstract test case for {@link IAddressSerializer}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractAddressSerializerTestCase extends TestCase2 {

    Random r = new Random();

    SimpleMemoryRawStore store = new SimpleMemoryRawStore();
    
    IAddressSerializer ser;
    
    /**
     * 
     */
    public AbstractAddressSerializerTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractAddressSerializerTestCase(String arg0) {
        super(arg0);
    }

    public void test_roundTrip() throws IOException {

        final int limit = 10000;
        
        final int nchildren = r.nextInt(limit)+1;

        doRoundTripTest(limit,nchildren);
        
    }

    /**
     * Test helper.
     * 
     * @param limit
     *            The maximum #of children allowed in the {@link Node}.
     * @param nchildren
     *            The #of children that are defined (non-zero addresses).
     * @throws IOException
     */
    void doRoundTripTest(int limit, int nchildren) throws IOException {

        final long[] expectedChildAddrs = new long[limit];
        
        for(int i=0; i<nchildren; i++) {
            
            expectedChildAddrs[i] = nextNonZeroAddr();
            
        }

        doRoundTripTest(limit, nchildren, expectedChildAddrs);

    }
    
    void doRoundTripTest(int limit, int nchildren, long[] expectedChildAddrs) throws IOException {

        System.err.println("maxChildren="+limit+", nchildren="+nchildren);
        
        // serialize.
        final byte[] serialized;
        {
            
            final int estimatedCapacity = 0;// default - will grow as necessary; ser.getSize(nchildren);

            DataOutputBuffer os = new DataOutputBuffer(estimatedCapacity);

            ser.putChildAddresses(store.getAddressManager(), os, expectedChildAddrs, nchildren);

            serialized = os.toByteArray();

        }

        // de-serialize.
        {

            final long[] actualChildAddrs = new long[limit];
            
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);

            DataInput is = new DataInputStream(bais);

//            DataInputBuffer is = new DataInputBuffer(serialized);
            
            ser.getChildAddresses(store.getAddressManager(),is, actualChildAddrs, nchildren);

            assertEquals("Child addresses", expectedChildAddrs,
                    actualChildAddrs);
        }

    }

    /**
     * This tests a specific data set that was giving me grief in
     * {@link TestNodeSerializer}, but the data do not cause a problem when
     * isolated to the {@link IAddressSerializer} implementations.
     * 
     * @throws IOException
     */
    public void test_error01() throws IOException {

        long[] expectedChildAddrs = new long[] { 3229302690576073610L,
                3713339663679750631L, 3845286582340813772L, 2140934673277125062L,
                4080862695258063522L, 1645903168011239495L, 0, 0, 0      
        };
        
        doRoundTripTest(expectedChildAddrs.length, 6, expectedChildAddrs);

    }

    abstract long nextNonZeroAddr();
    
}
