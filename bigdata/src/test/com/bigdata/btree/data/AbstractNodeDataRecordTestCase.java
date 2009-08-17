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

import com.bigdata.btree.INodeData;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;

/**
 * Test suite for the B+Tree {@link INodeData} records (accessing coded data in
 * place).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractNodeDataRecordTestCase extends
        AbstractNodeOrLeafDataRecordTestCase {

    /**
     * 
     */
    public AbstractNodeDataRecordTestCase() {
    }

    /**
     * @param name
     */
    public AbstractNodeDataRecordTestCase(String name) {
        super(name);
    }

    @Override
    protected boolean mayGenerateLeaves() {
        return false;
    }

    @Override
    protected boolean mayGenerateNodes() {
        return true;
    }

   /**
     * Unit test for an empty node (this is not a legal instance since only the
     * root leaf may ever be empty).
     */
    public void test_emptyNode() {

        final int m = 3;
        final int nkeys = 0;
        final byte[][] keys = new byte[m + 1][];
        final int spannedTupleCount = 0;
        final long[] childAddr = new long[m + 1];
        final int[] childEntryCount = new int[m + 1];

        final INodeData expected = new MockNodeData(new ReadOnlyKeysRaba(
                nkeys, keys), spannedTupleCount, childAddr, childEntryCount);

        final INodeData actual = new ReadOnlyNodeData(expected, keysCoder);

        assertSameNodeData(expected, actual);

    }

    /**
     * This the minimum legal node for a branching factor of 3. It has one key
     * and two children.
     */
    public void test_tupleCount1() {
        
        final int m = 3;
        final int nkeys = 1;
        final byte[][] keys = new byte[m + 1][];
        final long[] childAddr = new long[m + 1];
        final int childEntryCount[] = new int[m + 1];

        keys[0] = new byte[]{1,2,3};
        
        int entryCount = 0;
        for (int i = 0; i < nkeys; i++) {
            entryCount += childEntryCount[i];
        }
        
        final INodeData expected = new MockNodeData(new ReadOnlyKeysRaba(
                nkeys, keys),entryCount,childAddr,childEntryCount);

        final INodeData actual = new ReadOnlyNodeData(expected, keysCoder);

        assertSameNodeData(expected, actual);

    }
    
}
