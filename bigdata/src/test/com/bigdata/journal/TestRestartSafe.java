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
 * Created on Feb 3, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.SimpleEntry;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Test suite for restart-safety of {@link BTree}s backed by an
 * {@link IJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafe extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestRestartSafe() {
    }

    /**
     * @param name
     */
    public TestRestartSafe(String name) {
        super(name);
    }

//    public Properties getProperties() {
//
//        if (properties == null) {
//
//            properties = super.getProperties();
//
//            // we need to use a persistent mode of the journal (not transient).
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
//                    .toString());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//
//            properties.setProperty(Options.DELETE_ON_EXIT,"true");
//
//        }
//
//        return properties;
//
//    }
//
//    private Properties properties;
    
//    /**
//     * Re-open the same backing store.
//     * 
//     * @param store
//     *            the existing store.
//     * 
//     * @return A new store.
//     * 
//     * @exception Throwable
//     *                if the existing store is not closed, e.g., from failure to
//     *                obtain a file lock, etc.
//     */
//    protected Journal reopenStore(Journal store) {
//        
//        // close the store.
//        store.close();
//        
//        Properties properties = (Properties)getProperties().clone();
//        
//        // Turn this off now since we want to re-open the same store.
//        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//        
//        // The backing file that we need to re-open.
//        File file = store.getFile();
//        
//        assertNotNull(file);
//        
//        // Set the file property explictly.
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return new Journal( properties );
//        
//    }

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
    public BTree getBTree(int branchingFactor, Journal journal) {

        BTree btree;
        {
            
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor( branchingFactor );
            
            btree = BTree.create(journal, metadata);
            
        }
        
        return btree;
            
    }

    /**
     * Test basic btree is restart safe, including a test of
     * {@link BTree#removeAll()}
     * 
     * @throws IOException
     */
    public void test_restartSafe01() throws IOException {

        Journal journal = new Journal(getProperties());

        try {
        
        final int m = 3;

        final long addr1;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[]{v5,v6,v7,v8,v3,v4,v2,v1};

        {
            
            final BTree btree = getBTree(m,journal);
    
            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };
            
//            btree.insert(new BatchInsert(values.length, keys, values));

            for (int i = 0; i < values.length; i++) {
            
                btree.insert(keys[i], values[i]);
                
            }
            
            assertTrue(btree.dump(Level.DEBUG,System.err));
    
            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.rangeIterator());
    
            addr1 = btree.writeCheckpoint();
            
            journal.commit();
            
        }
        
        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            final long addr2;
            {
                journal = reopenStore(journal);

                final BTree btree = BTree.load(journal, addr1);

                assertTrue(btree.dump(Level.DEBUG, System.err));

                // @todo verify in more detail.
                assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7,
                        v8 }, btree.rangeIterator());

                // remove all entries by replacing the root node.

                btree.removeAll();

                assertTrue(btree.dump(Level.DEBUG, System.err));

                assertSameIterator(new Object[] {}, btree.rangeIterator());

                addr2 = btree.writeCheckpoint();

                journal.commit();
            }

            /*
             * restart, re-opening the same file.
             */
            {

                journal = reopenStore(journal);

                final BTree btree = BTree.load(journal, addr2);

                assertTrue(btree.dump(Level.DEBUG, System.err));

                assertSameIterator(new Object[] {}, btree.rangeIterator());

            }

        }
        
        }

        finally {

            journal.destroy();
            
        }

    }

    /**
     * Test verifies that the journal can be correctly reopened when using a
     * non-default value for <code>offsetBits</code>. The data on the
     * {@link BTree} is verified in order to force reads from the last commit
     * point on the journal after it is reopened.
     * 
     * @throws IOException
     */
    public void test_restartSafe_offsetBits() throws IOException {

        final int offsetBits = WormAddressManager.SCALE_OUT_OFFSET_BITS;
        Journal journal;
        {

            final Properties properties = getProperties();

            // override offset bits to a non-default value.
            properties.setProperty(Options.OFFSET_BITS, "" + offsetBits);

            journal = new Journal(properties);

            assertEquals("offsetBits", offsetBits, journal
                    .getRootBlockView().getOffsetBits());

        }

        try {
        
        final int m = 3;

        final long addr1;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[]{v5,v6,v7,v8,v3,v4,v2,v1};

        {
            
            final BTree btree = getBTree(m,journal);
    
            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };

            for (int i = 0; i < values.length; i++) {
            
                btree.insert(keys[i], values[i]);
                
            }
            
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.rangeIterator());
    
            addr1 = btree.writeCheckpoint();
            
            journal.commit();
            
        }
        
        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            final long addr2;
            {
                journal = reopenStore(journal);
                
                assertEquals("offsetBits", offsetBits, journal
                        .getRootBlockView().getOffsetBits());

                final BTree btree = BTree.load(journal, addr1);

                assertTrue(btree.dump(Level.DEBUG, System.err));

                // @todo verify in more detail.
                assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7,
                        v8 }, btree.rangeIterator());

                // remove all entries by replacing the root node.

                btree.removeAll();

                assertTrue(btree.dump(Level.DEBUG, System.err));

                assertSameIterator(new Object[] {}, btree.rangeIterator());

                addr2 = btree.writeCheckpoint();

                journal.commit();
            }

            /*
             * restart, re-opening the same file.
             */
            {

                journal = reopenStore(journal);

                final BTree btree = BTree.load(journal, addr2);

                assertTrue(btree.dump(Level.DEBUG, System.err));

                assertSameIterator(new Object[] {}, btree.rangeIterator());

            }

        }
        
        }

        finally {

            journal.destroy();
            
        }

    }
    
    /**
     * Test verifies that the {@link ICounter} is restart-safe.
     */
    public void test_restartSafeCounter() {
       
        Journal journal = new Journal(getProperties());

        try {
        
        final int m = 3;

        final long addr1;
        {
            
            final BTree btree = getBTree(m,journal);
            
            assertEquals(0,btree.getCounter().get());
            assertEquals(1,btree.getCounter().incrementAndGet());
            assertEquals(1,btree.getCounter().get());
            
            addr1 = btree.writeCheckpoint();
            
            journal.commit();
            
        }
        
        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            journal = reopenStore(journal);
            
            final BTree btree = BTree.load(journal, addr1);

            // verify the counter.
            assertEquals(1,btree.getCounter().get());
            
        }

        } finally {

            journal.destroy();
            
        }
        
    }

    /**
     * Test verifies that classes which extend {@link BTree} are correctly
     * restored by {@link BTree#load(com.bigdata.rawstore.IRawStore, long)}.
     */
    public void test_restartSafeSubclass() {

        Journal journal = new Journal(getProperties());

        try {
        
        final int m = 3;

        final long addr1;

        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[] { v5, v6, v7, v8, v3, v4, v2, v1 };

        {

            final MyBTree btree;
            {
                
                IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
                
                metadata.setBranchingFactor( m );
                
                // Note: override implementation class.
                metadata.setBTreeClassName(MyBTree.class.getName());
                
                btree = (MyBTree)BTree.create(journal, metadata);
                
            }

//            final BTree btree = new MyBTree(journal, m, UUID.randomUUID());

            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };

//            btree.insert(new BatchInsert(values.length, keys, values));
            
            for (int i = 0; i < values.length; i++) {
                
                btree.insert(keys[i], values[i]);
                
            }

            assertTrue(btree.dump(Level.DEBUG, System.err));

            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.rangeIterator());

            addr1 = btree.writeCheckpoint();

            journal.commit();

        }

        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            journal = reopenStore(journal);

            final MyBTree btree = (MyBTree) BTree.load(journal, addr1);

            assertTrue(btree.dump(Level.DEBUG, System.err));

            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.rangeIterator());

        }

        } finally {

            journal.destroy();
            
        }

    }

    public static class MyBTree extends BTree {

        /**
         * @param store
         * @param checkpoint
         * @param metadata
         */
        public MyBTree(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata, boolean readOnly) {
            
            super(store, checkpoint, metadata, readOnly);
            
        }

    }
    
}
