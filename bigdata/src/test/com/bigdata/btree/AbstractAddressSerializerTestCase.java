/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
            final int estimatedCapacity = ser.getSize(nchildren);

            DataOutputBuffer os = new DataOutputBuffer(estimatedCapacity);

            ser.putChildAddresses(os, expectedChildAddrs, nchildren);

            serialized = os.toByteArray();

        }

        // de-serialize.
        {

            final long[] actualChildAddrs = new long[limit];
            
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);

            DataInput is = new DataInputStream(bais);

//            DataInputBuffer is = new DataInputBuffer(serialized);
            
            ser.getChildAddresses(is, actualChildAddrs, nchildren);

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
