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
 * Created on Dec 26, 2006
 */

package com.bigdata.objndx;

import com.bigdata.objndx.IndexSegment.CustomAddressSerializer;

/**
 * Tests logic to encode and decode the addresses of nodes and leaves in an {@link
 * IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IndexSegmentBuilder
 * @see IndexSegment
 * @see IndexSegment.CustomAddressSerializer
 */
public class TestIndexSegmentAddressSerializer extends AbstractBTreeTestCase {

//    CustomAddressSerializer addrSer = new CustomAddressSerializer();

    /**
     * 
     */
    public TestIndexSegmentAddressSerializer() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentAddressSerializer(String name) {
        super(name);
    }

    /**
     * Test works through the basic operations required to encode and decode
     * an address for a node and a leaf.
     */
    public void test_bitMath() {

        // addr:=1, shift left by one, result is (2).
        assertEquals(2,1<<1);
        
        // set low bit to true to indicate a node (vs a leaf).
        assertEquals(3,2|1);

        // test low bit - indicates a node.
        assertTrue((3&1)==1);

        // test low bit - indicates a leaf.
        assertTrue((2&1)==0);
        
        // shift right one to recover the address (low bit discarded).
        assertEquals(1,2>>1);
        
        // shift right one to recover the address (low bit discarded).
        assertEquals(1,3>>1);
        
    }
    
    public void test_encodeLeaf01() {
        
        final int nbytes = 12;
        final int offset = 44;
        
        final long addrLeaf = CustomAddressSerializer.encode(nbytes, offset, true);
        
        assertTrue((addrLeaf&1)==0);
        
        final long addrNode = CustomAddressSerializer.encode(nbytes, offset, false);

        assertTrue((addrNode&1)==1);

        assertEquals(nbytes,Addr.getByteCount(addrLeaf>>1));
        
        assertEquals(offset,Addr.getOffset(addrLeaf>>1));
        
        assertEquals(nbytes,Addr.getByteCount(addrNode>>1));
        
        assertEquals(offset,Addr.getOffset(addrNode>>1));
        
    }
    
}
