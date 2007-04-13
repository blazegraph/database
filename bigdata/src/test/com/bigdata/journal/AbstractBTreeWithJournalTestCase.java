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

package com.bigdata.journal;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.SimpleEntry;

/**
 * Stress tests of the {@link BTree} writing on the {@link Journal}. This suite
 * simply contains stress tests of the btree operations at larger scale and
 * including incremental writes against the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeWithJournalTestCase extends AbstractBTreeTestCase {

    public AbstractBTreeWithJournalTestCase() {
    }

    public AbstractBTreeWithJournalTestCase(String name) {
        super(name);
    }

    abstract public BufferMode getBufferMode();
    
    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, getBufferMode().toString() );

            /*
             * Use a temporary file for the test. Such files are always deleted when
             * the journal is closed or the VM exits.
             * 
             * Note: Your unit test must close the store for delete to work.
             */
            properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//            properties.setProperty(Options.DELETE_ON_CLOSE,"true");
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

//            // Note: also deletes the file before it is used.
//            properties.setProperty(Options.FILE, AbstractTestCase
//                    .getTestJournalFile(getName(), properties));

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

        Journal journal = new Journal(getProperties());

        BTree btree = new BTree(journal, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);

        return btree;

    }
    
    /**
     * The branching factors that will be used in the stress tests. The larger
     * the branching factor, the longer the run for these tests. The very small
     * branching factors (3, 4) test the btree code more fully since they will
     * exercise the fence posts on the invariants for nodes and leaves on pretty
     * much each mutation.
     */
    int[] branchingFactors = new int[]{3,4};//,5,10,20,64};//,128};//,512};
    
    /**
     * A stress test for sequential key insertion that runs with a variety of
     * branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_increasingKeySequence() {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithIncreasingKeySequence( btree, m, m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithIncreasingKeySequence( btree, m, m*m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithIncreasingKeySequence( btree, m, m*m*m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithIncreasingKeySequence( btree, m, m*m*m*m );
                
                btree.getStore().closeAndDelete();
                
            }
            
        }
        
    }

    /**
     * A stress test for sequential decreasing key insertions that runs with a
     * variety of branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_decreasingKeySequence() {
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];

            {

                BTree btree = getBTree(m);
                
                doSplitWithDecreasingKeySequence( btree, m, m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithDecreasingKeySequence( btree, m, m*m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithDecreasingKeySequence( btree, m, m*m*m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {

                BTree btree = getBTree(m);
                
                doSplitWithDecreasingKeySequence( btree, m, m*m*m*m );
                
                btree.getStore().closeAndDelete();
                
            }

        }
        
    }

    /**
     * A stress test for random key insertion using a that runs with a variety
     * of branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_randomKeySequence() {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            {
            
                BTree btree = getBTree(m);
                
                doSplitWithRandomDenseKeySequence( btree, m, m );
                
                btree.getStore().closeAndDelete();
                
            }
            
            {
                
                BTree btree = getBTree(m);
                
                doSplitWithRandomDenseKeySequence( btree, m, m*m );
                
                btree.getStore().closeAndDelete();
                
            }

            {

                BTree btree = getBTree(m);

                doSplitWithRandomDenseKeySequence( btree, m, m * m * m);

                btree.getStore().closeAndDelete();

            }

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

        int nkeys = 2000;
        
        int ntrials = 25000;
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];

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
            
            doRemoveStructureStressTest(m,nkeys);
            
        }

    }

}
