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
 * Created on Aug 15, 2009
 */

package com.bigdata.btree.data;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractNodeOrLeafDataRecordTestCase extends
        AbstractBTreeTestCase {

    /**
     * 
     */
    public AbstractNodeOrLeafDataRecordTestCase() {
    }

    /**
     * @param name
     */
    public AbstractNodeOrLeafDataRecordTestCase(String name) {
        super(name);
    }

    protected IRabaCoder keysCoder = null;
    protected IRabaCoder valuesCoder = null;

    /**
     * Small (de-)serialization stress test conducted for a variety of slot
     * sizes and branching factors.
     * 
     * @see #main(String[] args) for a large stress test.
     */
    public void testStress() {
     
        final int ntrials = 20;
        
        final int nnodes = 500;

        doStressTest(ntrials, nnodes);

    }

    /**
     * Run a stress test.
     * <p>
     * Note: You may run out of heap space during the test for large branching
     * factors when combined with a large #of nodes.
     * 
     * @param ntrials
     *            The #of trials. Each trial has a random slotSize and
     *            branchingFactor. 50% of the trials (on average) will use
     *            record compression.
     * @param nnodes
     *            The #of random nodes per trial.
     */
    public void doStressTest(final int ntrials, final int nnodes) {

        /*
         * Some branching factors to choose from.
         */
        final int[] branchingFactors = new int[] { 3, 4, 8, 16, 27, 32, 48, 64,
                96, 99, 112, 128, 256, 512, 1024, 4096};
//        int[] branchingFactors = new int[] {4096};
        
        for (int trial = 0; trial < ntrials; trial++) {

            // Choose the branching factor randomly.
            final int branchingFactor = branchingFactors[r
                    .nextInt(branchingFactors.length)];

            final boolean deleteMarkers = r.nextBoolean();

            final boolean versionTimestamps = r.nextBoolean();

            System.err.println("Trial "
                    + trial
                    + " of "
                    + ntrials
                    + " : testing "
                    + nnodes
                    + " random nodes:  branchingFactor="
                    + branchingFactor
                    + (mayGenerateLeaves() ? ", deleteMarkers=" + deleteMarkers
                            + ", versionTimestamps=" + versionTimestamps : ""));

            for (int i = 0; i < nnodes; i++) {

                final IAbstractNodeData expected = getRandomNodeOrLeaf(
                        branchingFactor, deleteMarkers, versionTimestamps);

                if (expected.isLeaf()) {

                    final ILeafData actual = new ReadOnlyLeafData(
                            (ILeafData) expected, keysCoder, valuesCoder, false/* doubleLinked */);

                    assertSameLeafData((ILeafData) expected, actual);

                } else {

                    final INodeData actual = new ReadOnlyNodeData(
                            (INodeData) expected, keysCoder);

                    assertSameNodeData((INodeData) expected, actual);

                }
                
            }
   
        }
        
    }
    
//    /**
//     * Run a large stress test.
//     * 
//     * @param args
//     *            unused.
//     */
//    public static void main(String[] args) {
//
//        final int NTRIALS = 1000;
//        final int NNODES = 10000;
//
//        new TestNodeSerializer().doStressTest(NTRIALS, NNODES);
//        
//    }
    
    /**
     * A random address that is only syntactically valid (do not dereference).
     */
    protected long nextAddr() {

        final int offset = r.nextInt(Integer.MAX_VALUE/2);

        final int nbytes = r.nextInt(1024);
        

        // return Addr.toLong(nbytes,offset);

        return ((long) offset) << 32 | nbytes;

    }

    /**
     * Generates a non-leaf node with random data.
     */
    public MockNodeData getRandomNode(final int m) {

        // #of keys per node.
        final int branchingFactor = m;

        // final long addr = nextAddr();

        final int nchildren = r.nextInt((branchingFactor + 1) / 2)
                + (branchingFactor + 1) / 2;

        assert nchildren >= (branchingFactor + 1) / 2;

        assert nchildren <= branchingFactor;

        final int nkeys = nchildren - 1;

        final byte[][] keys = getRandomKeys(branchingFactor, nkeys);
        
        final long[] children = new long[branchingFactor+1];

        final int[] childEntryCounts = new int[branchingFactor+1];
        
        // node with some valid keys and corresponding child refs.

        int nentries = 0;
        
        for (int i = 0; i < nchildren; i++) {

            children[i] = nextAddr();

            childEntryCounts[i] = r.nextInt(10)+1; // some non-zero count.  
            
            nentries += childEntryCounts[i];
            
        }
                
        /*
         * create the node and set it as the root to fake out the btree.
         */

        return new MockNodeData(new ReadOnlyKeysRaba(nkeys, keys), nentries,
                children, childEntryCounts);
        
    }

    /**
     * Generates a leaf node with random data.
     */
    public MockLeafData getRandomLeaf(final int m,
            final boolean isDeleteMarkers, final boolean isVersionTimestamps) {

        // #of keys per node.
        final int branchingFactor = m;

//        long addr = nextAddr();

        final int nkeys = r.nextInt((branchingFactor + 1) / 2)
                + (branchingFactor + 1) / 2;
        assert nkeys >= (branchingFactor + 1) / 2;
        assert nkeys <= branchingFactor;

        final byte[][] keys = getRandomKeys(branchingFactor + 1, nkeys);

        final byte[][] values = new byte[branchingFactor + 1][];

        final boolean[] deleteMarkers = isDeleteMarkers ? new boolean[branchingFactor + 1]
                : null;

        final long[] versionTimestamps = isVersionTimestamps ? new long[branchingFactor + 1]
                : null;

        for (int i = 0; i < nkeys; i++) {

            values[i] = new byte[r.nextInt(100)];

            r.nextBytes(values[i]);

            if (deleteMarkers != null) {

                deleteMarkers[i] = r.nextBoolean();

            }

            if (versionTimestamps != null) {

                versionTimestamps[i] = System.currentTimeMillis()
                        + r.nextInt(10000);

            }

        }

        return new MockLeafData(//
                new ReadOnlyKeysRaba(nkeys, keys),//
                new ReadOnlyValuesRaba(nkeys, values),//
                deleteMarkers,//
                versionTimestamps//
        );

    }

    /**
     * Generates a node or leaf (randomly) with random data.
     */
    public IAbstractNodeData getRandomNodeOrLeaf(final int m,
            final boolean deleteMarkers, final boolean versionTimestamps) {

        assert mayGenerateLeaves() || mayGenerateNodes();

        if (!mayGenerateLeaves()) {

            return getRandomNode(m);

        } else if (!mayGenerateNodes()) {

            return getRandomLeaf(m, deleteMarkers, versionTimestamps);

        } else {

            if (r.nextBoolean()) {

                return getRandomNode(m);

            } else {

                return getRandomLeaf(m, deleteMarkers, versionTimestamps);

            }

        }

    }

    abstract protected boolean mayGenerateNodes();
    abstract protected boolean mayGenerateLeaves();
    
}
