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
 * Created on Dec 26, 2006
 */

package com.bigdata.btree;

import com.bigdata.btree.IndexSegment.CustomAddressSerializer;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.WormAddressManager;

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
        
        int offsetBits = 48;
        
        IAddressManager am = new WormAddressManager(offsetBits);
        
        CustomAddressSerializer ser = new CustomAddressSerializer(am);
        
        final int nbytes = 12;
        final long offset = 44L;
        
        final long addrLeaf = ser.encode(nbytes, offset, true);
        
        assertTrue((addrLeaf&1)==0);
        
        final long addrNode = ser.encode(nbytes, offset, false);

        assertTrue((addrNode&1)==1);

        assertEquals(nbytes,am.getByteCount(addrLeaf>>1));
        
        assertEquals(offset,am.getOffset(addrLeaf>>1));
        
        assertEquals(nbytes,am.getByteCount(addrNode>>1));
        
        assertEquals(offset,am.getOffset(addrNode>>1));
        
    }
    
}
