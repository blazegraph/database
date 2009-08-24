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


import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;

/**
 * Test suite for the B+Tree {@link ILeafData} records (accessing coded data in
 * place).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractLeafDataRecordTestCase extends
        AbstractNodeOrLeafDataRecordTestCase {

    /**
     * 
     */
    public AbstractLeafDataRecordTestCase() {
    }

    /**
     * @param name
     */
    public AbstractLeafDataRecordTestCase(String name) {
        super(name);
    }

    @Override
    protected boolean mayGenerateLeaves() {
        return true;
    }

    @Override
    protected boolean mayGenerateNodes() {
        return false;
    }

    /**
     * Unit test for an empty leaf.
     */
    public void test_emptyLeaf() {

        final int m = 3;
        final int nkeys = 0;
        final byte[][] keys = new byte[m + 1][];
        final byte[][] vals = new byte[m + 1][];

        final ILeafData expected = new MockLeafData(new ReadOnlyKeysRaba(
                nkeys, keys), new ReadOnlyValuesRaba(nkeys, vals));

        final ILeafData actual = new ReadOnlyLeafData(expected, keysCoder,
                valuesCoder, false/* doubleLinked */);

        assertSameLeafData(expected, actual);

    }

    public void test_tupleCount1_emptyKey_nullVal() {
        
        final int m = 3;
        final int nkeys = 1;
        final byte[][] keys = new byte[m + 1][];
        final byte[][] vals = new byte[m + 1][];

        keys[0] = new byte[0];
        
        final ILeafData expected = new MockLeafData(new ReadOnlyKeysRaba(
                nkeys, keys), new ReadOnlyValuesRaba(nkeys, vals));

        final ILeafData actual = new ReadOnlyLeafData(expected, keysCoder,
                valuesCoder, false/* doubleLinked */);

        assertSameLeafData(expected, actual);

    }

    public void test_tupleCount1_emptyKey_deleted() {
        
        final int m = 3;
        final int nkeys = 1;
        final byte[][] keys = new byte[m + 1][];
        final byte[][] vals = new byte[m + 1][];
        final boolean[] deleteMarkers = new boolean[m + 1];
        final long[] versionTimestamps = new long[m + 1];

        keys[0] = new byte[0];
        deleteMarkers[0]=true;
        versionTimestamps[0]=System.currentTimeMillis();
        
        final ILeafData expected = new MockLeafData(new ReadOnlyKeysRaba(
                nkeys, keys), new ReadOnlyValuesRaba(nkeys, vals),
                deleteMarkers,
                versionTimestamps
        );

        // spot check mock object impl.
        assertTrue(expected.getDeleteMarker(0));
        assertEquals(versionTimestamps[0], expected.getVersionTimestamp(0));

        final ILeafData actual = new ReadOnlyLeafData(expected, keysCoder,
                valuesCoder, false/* doubleLinked */);

        assertSameLeafData(expected, actual);

    }
    
}
