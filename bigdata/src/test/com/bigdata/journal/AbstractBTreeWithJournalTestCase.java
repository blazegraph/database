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
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

        }

        return properties;

    }

    private Properties properties;
  
    private Journal journal;
    
    public void setUp() throws Exception {

        super.setUp();
        
        journal = new Journal(getProperties());

    }

    public void tearDown() throws Exception {
        
        if(journal!=null) {
            
            if(journal.isOpen()) {
                
                journal.closeAndDelete();
                
            }
            
        }

        super.tearDown();
        
    }
    
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
                
                try {

                    doSplitWithIncreasingKeySequence( btree, m, m );
                
                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {

                BTree btree = getBTree(m);

                try {

                    doSplitWithIncreasingKeySequence( btree, m, m*m );

                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {

                BTree btree = getBTree(m);
                
                try {
                    
                    doSplitWithIncreasingKeySequence( btree, m, m*m*m );

                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {

                BTree btree = getBTree(m);
                
                try {

                    doSplitWithIncreasingKeySequence( btree, m, m*m*m*m );
                
                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
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
                
                try {

                    doSplitWithDecreasingKeySequence( btree, m, m );
                
                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {

                BTree btree = getBTree(m);

                try {

                    doSplitWithDecreasingKeySequence( btree, m, m*m );
                
                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {

                BTree btree = getBTree(m);

                try {

                    doSplitWithDecreasingKeySequence( btree, m, m*m*m );

                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {

                BTree btree = getBTree(m);
                
                try {

                    doSplitWithDecreasingKeySequence( btree, m, m*m*m*m );

                } finally {

                    try {btree.getStore().closeAndDelete();}
                    catch(Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }

        }
        
    }

    /**
     * A stress test for random key insertion that runs with a variety of
     * branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_randomKeySequence() {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            {
            
                BTree btree = getBTree(m);
                
                try {

                    doSplitWithRandomDenseKeySequence( btree, m, m );
                    
                } finally {
                
                    try {
                        btree.getStore().closeAndDelete();
                    } catch (Throwable t) {
                        log.warn(t);
                    }
                    
                }
                
            }
            
            {
                
                BTree btree = getBTree(m);

                try {

                    doSplitWithRandomDenseKeySequence(btree, m, m * m);

                } finally {

                    try {
                        btree.getStore().closeAndDelete();
                    } catch (Throwable t) {
                        log.warn(t);
                    }

                }
                
            }

            {

                BTree btree = getBTree(m);
                
                try {

                    doSplitWithRandomDenseKeySequence( btree, m, m * m * m);

                } finally {
                    
                    try {
                        btree.getStore().closeAndDelete();
                    } catch (Throwable t) {
                        log.warn(t);
                    }
                    
                }

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
