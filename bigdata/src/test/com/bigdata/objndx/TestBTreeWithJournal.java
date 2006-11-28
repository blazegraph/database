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
 * Created on Nov 17, 2006
 */

package com.bigdata.objndx;

import java.io.IOException;
import java.util.Properties;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Stress tests of the {@link BTree} writing on the {@link Journal}. This does
 * NOT include the use of the {@link BTree} to provide an object index of for
 * the journal - those tests require reading and write data on the journal using
 * its high-level API. Rather, this suite simply contains stress tests of the
 * btree operations at larger scale and including incremental writes against the
 * store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Add stress test with periodic re-loading of the btree, tracking its
 *       expected state, and verifying that state.
 */
public class TestBTreeWithJournal extends AbstractBTreeTestCase {

    public TestBTreeWithJournal() {
    }

    public TestBTreeWithJournal(String name) {
        super(name);
    }

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                    .toString());

        }

        return properties;

    }

    private Properties properties;
    
    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor) {

        try {
            
            Properties properties = getProperties();

            Journal journal = new Journal(properties);

            // A modest leaf queue capacity.
            final int leafQueueCapacity = 500;
            
            final int nscan = 10;

            BTree btree = new BTree(journal, branchingFactor,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            leafQueueCapacity, nscan),
                    new SimpleEntry.Serializer());

            return btree;

        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    /*
     * @todo try large branching factors, but limit the total #of keys inserted
     * or the running time will be too long (I am using an expontential #of keys
     * by default).
     * 
     * @todo For sequential keys and the simple split rule, m=128 causes the
     * journal to exceed its initial extent. Try this again with a modified
     * split rule that splits high for dense leaves.
     */
    int[] branchingFactors = new int[]{3,4,5,10,20};//,64};//,128};//,512};
    
    /**
     * A stress test for sequential key insertion that runs with a variety of
     * branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_increasingKeySequence() {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithIncreasingKeySequence( getBTree(m), m, m );
            
            doSplitWithIncreasingKeySequence( getBTree(m), m, m*m );

            doSplitWithIncreasingKeySequence( getBTree(m), m, m*m*m );

            doSplitWithIncreasingKeySequence( getBTree(m), m, m*m*m*m );

        }
        
    }

    /**
     * A stress test for sequential decreasing key insertions that runs with a
     * variety of branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_decreasingKeySequence() {
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithDecreasingKeySequence( getBTree(m), m, m );
            
            doSplitWithDecreasingKeySequence( getBTree(m), m, m*m );

            doSplitWithDecreasingKeySequence( getBTree(m), m, m*m*m );

            doSplitWithDecreasingKeySequence( getBTree(m), m, m*m*m*m );

        }
        
    }

    /**
     * A stress test for random key insertion using a that runs with a variety
     * of branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_randomKeySequence() {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithRandomKeySequence( getBTree(m), m, m );
            
            doSplitWithRandomKeySequence( getBTree(m), m, m*m );

            doSplitWithRandomKeySequence( getBTree(m), m, m*m*m );

            doSplitWithRandomKeySequence( getBTree(m), m, m*m*m*m );

        }
        
    }

    /**
     * Stress test inserts random permutations of keys into btrees of order m
     * for several different btrees, #of keys to be inserted, and permutations
     * of keys.
     */
    public void test_stress_split() {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
        
            doSplitTest( m, 0 );
        
        }
        
    }

    /**
     * Stress test of insert, removal and lookup of keys in the tree (allows
     * splitting of the root leaf).
     */
    public void test_insertLookupRemoveKeyTreeStressTest() {

        int nkeys = 5000;
        
        int ntrials = 50000;
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];

            if( m == 3 ) continue;  // FIXME fenceposts at m == 3.
            doInsertLookupRemoveStressTest(m, nkeys, ntrials);
            
        }

    }
    
    /**
     * Stress test for building up a tree and then removing all keys in a random
     * order.
     */
    public void test_stress_removeStructure() {
       
        int nkeys = 5000;
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            if( m == 3 ) continue;  // FIXME fenceposts at m == 3.
            doRemoveStructureStressTest(m,nkeys);
            
        }

    }

}
