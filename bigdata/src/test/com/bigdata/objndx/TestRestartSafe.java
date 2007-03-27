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
 * Created on Feb 3, 2007
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rawstore.IRawStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafe extends AbstractBTreeTestCase {

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

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());

            properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//            properties.setProperty(Options.FILE, getName()+".jnl");
//            properties.setProperty(Options.DELETE_ON_CLOSE,"false");
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

        }

        return properties;

    }

    private Properties properties;
    
    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is not closed, e.g., from failure to
     *                obtain a file lock, etc.
     */
    protected Journal reopenStore(Journal store) {
        
        // close the store.
        store.close();
        
        Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        File file = store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new Journal( properties );
        
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
    public BTree getBTree(int branchingFactor, Journal journal) {

        BTree btree = new BTree(journal, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);

        return btree;
            
    }

    /**
     * 
     * @throws IOException
     */
    public void test_restartSafe01() throws IOException {

        Journal journal = new Journal(getProperties());

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
            
            btree.insert(new BatchInsert(values.length, keys, values));
            
            assertTrue(btree.dump(Level.DEBUG,System.err));
    
            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());
    
            addr1 = btree.write();
            
            journal.commit();
            
        }
        
        /*
         * restart, re-opening the same file.
         */
        {
        
            journal = reopenStore(journal);
            
            final BTree btree = BTree.load(journal, addr1);
            
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());
    
            journal.closeAndDelete();
            
        }

    }

    /**
     * Test verifies that classes which extend {@link BTree} are correctly
     * restored by {@link BTree#load(com.bigdata.rawstore.IRawStore, long)}.
     */
    public void test_restartSafeSubclass() {

        Journal journal = new Journal(getProperties());

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

            final BTree btree = new MyBTree(journal, 3, UUID.randomUUID(),
                    SimpleEntry.Serializer.INSTANCE);

            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };

            btree.insert(new BatchInsert(values.length, keys, values));

            assertTrue(btree.dump(Level.DEBUG, System.err));

            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());

            addr1 = btree.write();

            journal.commit();

        }

        /*
         * restart, re-opening the same file.
         */
        {

            journal = reopenStore(journal);

            final MyBTree btree = (MyBTree) BTree.load(journal, addr1);

            assertTrue(btree.dump(Level.DEBUG, System.err));

            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());

            journal.closeAndDelete();

        }

    }

    public static class MyBTree extends BTree {

        public MyBTree(IRawStore store, int branchingFactor, UUID indexUUID,
                IValueSerializer valSer) {
            
            super(store, branchingFactor, indexUUID, valSer);
            
        }
        
        /**
         * @param store
         * @param metadata
         */
        public MyBTree(IRawStore store, BTreeMetadata metadata) {
            super(store, metadata);
        }
        
    }
    
}
