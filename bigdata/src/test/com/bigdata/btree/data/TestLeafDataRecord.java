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
 * Created on Aug 5, 2009
 */

package com.bigdata.btree.data;


import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.ILeafData;
import com.bigdata.btree.raba.ReadOnlyRaba;
import com.bigdata.btree.raba.ReadOnlySearchRaba;
import com.bigdata.btree.raba.codec.CanonicalHuffmanDataCoder;
import com.bigdata.btree.raba.codec.FrontCodedDataCoder;
import com.bigdata.btree.raba.codec.IDataCoder;

/**
 * Test suite for the B+Tree {@link ILeafData} records (accessing coded data in
 * place).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test round trip for node, leaf, and linked-leaf with each of the
 *       {@link IDataCoder} implementations.
 * 
 *       FIXME Lot's more tests of various {@link ILeafData} states.
 */
public class TestLeafDataRecord extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestLeafDataRecord() {
    }

    /**
     * @param name
     */
    public TestLeafDataRecord(String name) {
        super(name);
    }

    protected IDataCoder keysCoder = null;
    protected IDataCoder valuesCoder = null;
    
    protected void setUp() throws Exception {
        
        super.setUp();

        /*
         * The implementation for the keys must not permit nulls and must
         * support search.
         */
        keysCoder = new FrontCodedDataCoder(8/*ratio*/);

        /*
         * @todo Since this is used for the values the instance must allow
         * nulls, should coded only the byte values that are actually used,
         * should not permit coding of new byte[]s and should not support
         * search.
         */
        valuesCoder = new CanonicalHuffmanDataCoder();
        
    }

    /**
     * Unit test for an empty leaf.
     */
    public void test_emptyLeaf() {

        final int m = 3;
        final int nkeys = 0;
        final byte[][] keys = new byte[m + 1][];
        final byte[][] vals = new byte[m + 1][];

        final ILeafData expected = new MockLeafData(new ReadOnlySearchRaba(
                nkeys, keys), new ReadOnlyRaba(nkeys, vals));

        final ILeafData actual = new ReadOnlyLeafData(expected, keysCoder,
                valuesCoder, false/* doubleLinked */);

        assertSameLeafData(expected, actual);

    }

    public void test_withData() {
        
        fail("test lots of cases");
        
    }
    
}
