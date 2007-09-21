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
package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for the close/reopen protocol designed to manage the resource
 * burden of indices without invalidating the index objects (indices opens can
 * be reopened as long as their backing store remains available).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReopen extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestReopen() {
    }

    /**
     * @param name
     */
    public TestReopen(String name) {
        super(name);
    }

    /**
     * Test close on a new tree - should force the root to the store since a new
     * root is dirty (if empty). reopen should then reload the empty root and on
     * life goes.
     */
    public void test_reopen01() {

        BTree btree = getBTree(3);

        assertTrue(btree.isOpen());

        btree.close();

        assertFalse(btree.isOpen());

        try {
            btree.close();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        assertNotNull(btree.getRoot());

        assertTrue(btree.isOpen());

    }

    /**
     * Stress test comparison with ground truth btree when {@link BTree#close()}
     * is randomly invoked during mutation operations.
     */
    public void test_reopen02() {

        IRawStore store = new SimpleMemoryRawStore();

        final UUID indexUUID = UUID.randomUUID();
        
        /*
         * The btree under test.
         * 
         * Note: the fixture factory is NOT used since this node evictions will
         * be forced when this tree is closed (node evictions are not permitted
         * by the default fixture factory).
         */
        BTree btree = new BTree(store, 3, indexUUID, SimpleEntry.Serializer.INSTANCE);

        /*
         * The btree used to maintain ground truth.
         * 
         * Note: the fixture factory is NOT used here since the stress test will
         * eventually overflow the hard reference queue and begin evicting nodes
         * and leaves onto the store.
         */
        BTree groundTruth = new BTree(store, 3, indexUUID, SimpleEntry.Serializer.INSTANCE);

        final int limit = 10000;
        final int keylen = 6;

        for (int i = 0; i < limit; i++) {

            int n = r.nextInt(100);

            if (n < 5) {
                if(btree.isOpen()) btree.close();
            } else if (n < 20) {
                byte[] key = new byte[keylen];
                r.nextBytes(key);
                btree.remove(key);
                groundTruth.remove(key);
            } else {
                byte[] key = new byte[keylen];
                r.nextBytes(key);
                SimpleEntry value = new SimpleEntry(i);
                btree.insert(key, value);
                groundTruth.insert(key, value);
            }

        }

        assertSameBTree(groundTruth, btree);

    }

}
