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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.UnisolatedBTree;

/**
 * Test suite for fully-isolated read-write transactions.
 * 
 * @todo verify with writes on multiple indices.
 * 
 * @todo Show that abort does not leave anything lying around, both that would
 *       break isolation (unlikely) or just junk that lies around unreclaimed on
 *       the slots (or in the index nodes themselves).
 * 
 * @todo Verify correct abort after 'prepare'.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTx extends ProxyTestCase {
    
    public TestTx() {
    }

    public TestTx(String name) {
        super(name);
    }

    /**
     * Test verifies that a transaction may start when there are (a) no commits
     * on the journal; and (b) no indices have been registered.
     * <P>
     * Note: The transaction will be unable to isolate an index if the index has
     * not been registered already by an unisolated operation.
     */
    public void test_noIndicesRegistered() {

        Journal journal = new Journal(getProperties());

        journal.commit();
        
        final long tx = journal.newTx(IsolationEnum.ReadWrite);
        
        /*
         * nothing written on this transaction.
         */
        
        // commit.
        assertTrue(journal.commit(tx)==0L);

        journal.closeAndDelete();
        
    }

    /**
     * Verify that an index is not visible in the tx unless the native
     * transaction in which it is registered has already committed before the tx
     * starts.
     */
    public void test_indexNotVisibleUnlessCommitted() {
       
        Journal journal = new Journal(getProperties());

        String name = "abc";
        
        // register index in unisolated scope, but do not commit yet.
        journal.registerIndex(name, new UnisolatedBTree(journal, 3, UUID
                .randomUUID()));
        
        // start tx1.
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        // the index is not visible in tx1.
        assertNull(journal.getIndex(name,tx1));
        
        // do unisolated commit.
        assertTrue(journal.commit()!=0L);
        
        // start tx2.
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        // the index still is not visible in tx1.
        assertNull(journal.getIndex(name,tx1));

        // the index is visible in tx2.
        assertNotNull(journal.getIndex(name,tx2));
        
        journal.abort(tx1);
        
        journal.abort(tx2);
        
        journal.closeAndDelete();
        
    }

    /**
     * Test verifies that you always get the same object back when you ask for
     * an isolated named index.  This is important both to conserve resources
     * and since the write set is in the isolated index -- you lose it and it
     * is gone.
     */
    public void test_sameIndexObject() {

        Journal journal = new Journal(getProperties());
        
        final String name = "abc";

        {
            
            journal.registerIndex(name, new UnisolatedBTree(journal,UUID.randomUUID()));
            
            journal.commit();
            
        }

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        final IIndex ndx1 = journal.getIndex(name,tx1);
        
        assertNotNull(ndx1);
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        final IIndex ndx2 = journal.getIndex(name,tx2);

        assertTrue(tx1 != tx2);

        assertTrue(ndx1 != ndx2);
        
        assertNotNull(ndx2);
        
        assertTrue( ndx1 == journal.getIndex(name,tx1));

        assertTrue( ndx2 == journal.getIndex(name,tx2));
        
        journal.closeAndDelete();
        
    }
    
    /**
     * Create a journal, setup an index, write an entry on that index, and
     * commit the store. Setup a transaction and verify that we can isolate that
     * index and read the written value. Write a value on the unisolated index
     * and verify that it is not visible within the transaction.
     */
    public void test_readIsolation() {
        
        Journal journal = new Journal(getProperties());
        
        final String name = "abc";
        
        final int branchingFactor = 3;
        
        final byte[] k1 = new byte[]{1};
        final byte[] k2 = new byte[]{2};

        final byte[] v1 = new byte[]{1};
        final byte[] v2 = new byte[]{2};
        
        {

            /*
             * register the index, write an entry on the unisolated index,
             * and commit the journal. 
             */
            
            IIndex index = journal.registerIndex(name, new UnisolatedBTree(
                    journal, branchingFactor, UUID.randomUUID()));
        
            assertNull(index.insert(k1, v1));
            
            assert(journal.commit()!=0L);
            
        }
        
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
    
        {

            /*
             * verify that the write is visible in a transaction that starts
             * after the commit.
             */
            
            IIndex index  = journal.getIndex(name,tx1);
            
            assertTrue(index.contains(k1));
            
            assertEquals(v1,(byte[])index.lookup(k1));
            
        }

        {
    
            /*
             * obtain the unisolated index and write another entry and commit
             * the journal. 
             */
            
            IIndex index  = journal.getIndex(name);
            
            assertNull(index.insert(k2, v2));
            
            assertTrue(journal.commit()!=0L);
            
        }
        
        {
            
            /*
             * verify that the entry written on the unisolated index is not
             * visible to the transaction that started before that write.
             */

            IIndex index = journal.getIndex(name,tx1);
            
            assertTrue(index.contains(k1));
            assertFalse(index.contains(k2));

        }
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);
        
        {

            /*
             * start another transaction and verify that the 2nd committed
             * write is now visible to that transaction.
             */
            
            IIndex index = journal.getIndex(name,tx2);
            
            assertTrue(index.contains(k1));
            assertTrue(index.contains(k2));

        }
        
        journal.abort(tx1);
        
        journal.abort(tx2);

        journal.closeAndDelete();
        
    }

    /**
     * Test verifies that an isolated write is visible inside of a transaction
     * (tx1) but not in a concurrent transaction (tx2) and not in the unisolated
     * index until the tx1 commits. Once the tx1 commits, the write is visible
     * in the unisolated index. The write never becomes visible in tx2.  If tx2
     * attempts to write a value under the same key then a write-write conflict
     * is reported and validation fails.
     */
    public void test_writeIsolation() {
        
        Journal journal = new Journal(getProperties());
        
        final String name = "abc";
        
        final int branchingFactor = 3;
        
        final byte[] k1 = new byte[]{1};

        final byte[] v1 = new byte[]{1};
        final byte[] v1a = new byte[]{1,1};
        
        {

            /*
             * register an index and commit the journal. 
             */
            
            journal.registerIndex(name, new UnisolatedBTree(journal,
                    branchingFactor, UUID.randomUUID()));
            
            assert(journal.commit()!=0L);
            
        }

        /*
         * create two transactions.
         */
        
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        {
            
            /*
             * write an entry in tx1.  verify that the entry is not visible
             * in the unisolated index or in the index as isolated by tx2.
             */
            
            IsolatedBTree ndx1 = (IsolatedBTree)journal.getIndex(name,tx1);
            
            assertFalse(ndx1.contains(k1));
            
            assertNull(ndx1.insert(k1,v1));
            
            // check the version counter in tx1.
            assertEquals("versionCounter", 0, ndx1.getValue(k1)
                    .getVersionCounter());
            
            // not visible in the other tx.
            assertFalse(journal.getIndex(name,tx2).contains(k1));

            // not visible in the unisolated index.
            assertFalse(journal.getIndex(name).contains(k1));

            /*
             * commit tx1. verify that the write is still not visible in tx2 but
             * that it is now visible in the unisolated index.
             */
            
            // commit tx1.
            assertTrue(journal.commit(tx1)!=0L);
            
            // still not visible in the other tx.
            assertFalse(journal.getIndex(name,tx2).contains(k1));

            // but now visible in the unisolated index.
            assertTrue(journal.getIndex(name).contains(k1));

            // check the version counter in the unisolated index.
            assertEquals("versionCounter", 1, ((UnisolatedBTree) journal
                    .getIndex(name)).getValue(k1).getVersionCounter());

            /*
             * write a conflicting entry in tx2 and verify that validation of
             * tx2 fails.
             */

            assertNull(journal.getIndex(name,tx2).insert(k1,v1a));

            // check the version counter in tx2.
            assertEquals("versionCounter", 0, ((IsolatedBTree) journal
                    .getIndex(name,tx2)).getValue(k1).getVersionCounter());

            ITx tmp = journal.getTx(tx2);
            
            try {

                journal.commit(tx2);
                
                fail("Expecting: "+ValidationError.class);
                
            } catch(ValidationError ex) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
            assertTrue(tmp.isAborted());
            
        }
        
        journal.closeAndDelete();
        
    }

    //
    // Delete object.
    //

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0 and then
     * another version (v1) is written on tx0 under the same key. Both
     * transactions prepare and commit. The end state is that (id0,v1) is
     * visible in the database after the commit.
     */
    public void test_delete001() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {
            /*
             * register an index and commit the journal.
             */
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID
                    .randomUUID()));
            
            journal.commit();

        }

        /*
         * create transactions.
         */

        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        /*
         * Write v0 on tx0.
         */
        final byte[] id0 = new byte[] { 1 };
        final byte[] v0 = getRandomData().array();
        journal.getIndex(name,tx0).insert(id0, v0);
        assertEquals(v0, journal.getIndex(name,tx0).lookup(id0));

        /*
         * Verify that the version does NOT show up in a concurrent transaction.
         */
         assertFalse(journal.getIndex(name,tx1).contains(id0));

         // delete the version.
         assertEquals(v0, (byte[]) journal.getIndex(name,tx0)
                .remove(id0));

         // no longer visible in that transaction.
         assertFalse(journal.getIndex(name,tx0).contains(id0));

         /*
          * Test delete after delete (succeeds, but returns null).
          */
         assertNull(journal.getIndex(name,tx0).remove(id0));

         /*
          * Test write after delete (succeeds, returning null).
          */
         final byte[] v1 = getRandomData().array();
         assertNull(journal.getIndex(name,tx0).insert(id0, v1));

         // Still not visible in concurrent transaction.
         assertFalse(journal.getIndex(name,tx1).contains(id0));

         // Still not visible in global scope.
         assertFalse(journal.getIndex(name).contains(id0));

         // Prepare and commit tx0.
         assertNotSame(0L,journal.commit(tx0));

         // Still not visible in concurrent transaction.
         assertFalse(journal.getIndex(name,tx1).contains(id0));

         // Now visible in global scope.
         assertTrue(journal.getIndex(name).contains(id0));

         // Prepare and commit tx1 (no writes).
         assertEquals(0L,journal.commit(tx1));

         // Still visible in global scope.
         assertTrue(journal.getIndex(name).contains(id0));

         journal.closeAndDelete();
    
    }

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0 and then
     * another version (v1) is written on tx0 under the same key and isolation
     * is re-verified. Finally, v1 is deleted from tx0. Both transactions
     * prepare and commit. The end state is that no entry for id0 is visible in
     * the database after the commit.
     */
    public void test_delete002() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {
            /*
             * register an index and commit the journal.
             */
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID.randomUUID()));
            
            journal.commit();

        }

        /*
         * create transactions.
         */

        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        /*
         * Write v0 on tx0.
         */
        final byte[] id0 = new byte[] { 1 };
        final byte[] v0 = getRandomData().array();
        journal.getIndex(name,tx0).insert(id0, v0);
        assertEquals(v0, journal.getIndex(name,tx0).lookup(id0));

        /*
         * Verify that the version does NOT show up in a concurrent transaction.
         */
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // delete the version.
        assertEquals(v0, (byte[]) journal.getIndex(name,tx0)
                .remove(id0));

        // no longer visible in that transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));

        /*
         * Test delete after delete (succeeds, but returns null).
         */
        assertNull(journal.getIndex(name,tx0).remove(id0));

        /*
         * Test write after delete (succeeds, returning null).
         */
        final byte[] v1 = getRandomData().array();
        assertNull(journal.getIndex(name,tx0).insert(id0, v1));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        /*
         * Delete v1.
         */
        assertEquals(v1, (byte[]) journal.getIndex(name,tx0).remove(id0));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        /*
         * Prepare and commit tx0.
         * 
         * Note: We MUST NOT propagate a delete marker onto the unisolated index
         * since no entry for that key is visible was visible when the tx0
         * began.
         */
        assertNotSame(0L, journal.commit(tx0));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        // Prepare and commit tx1 (no writes).
        assertEquals(0L,journal.commit(tx1));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        journal.closeAndDelete();

    }

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0. Another
     * version (v1) is written on tx1 under the same key and isolation is
     * re-verified. Both transactions prepare and commit. Since no entry for id0
     * was pre-existing in the global scope the delete in tx0 does not propagate
     * into the global scope and the end state is that the write (id0,v1) from
     * tx1 is visible in the database after the commit.
     */
    public void test_delete003() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {
            /*
             * register an index and commit the journal.
             */
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID.randomUUID()));
            
            journal.commit();

        }

        /*
         * create transactions.
         */

        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        /*
         * Write v0 on tx0.
         */
        final byte[] id0 = new byte[] { 1 };
        final byte[] v0 = getRandomData().array();
        journal.getIndex(name,tx0).insert(id0, v0);
        assertEquals(v0, journal.getIndex(name,tx0).lookup(id0));

        /*
         * Verify that the version does NOT show up in a concurrent transaction.
         */
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // delete the version.
        assertEquals(v0, (byte[]) journal.getIndex(name,tx0)
                .remove(id0));

        // no longer visible in that transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));
        
        /*
         * write(id0,v1) in tx1.
         */
        final byte[] v1 = getRandomData().array();
        assertNull(journal.getIndex(name,tx1).insert(id0, v1));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        /*
         * Prepare and commit tx0.
         * 
         * Note: We MUST NOT propagate a delete marker onto the unisolated index
         * since no entry for that key is visible was visible when the tx0
         * began.
         */
        assertNotSame(0L, journal.commit(tx0));

        // Prepare and commit tx1.
        assertNotSame(0L,journal.commit(tx1));

        // (id0,v1) is now visible in global scope.
        assertTrue(journal.getIndex(name).contains(id0));
        assertEquals(v1, (byte[])journal.getIndex(name).lookup(id0));

        journal.closeAndDelete();

    }

    /*
     * Transaction semantics tests.
     */
    
    /**
     * Simple test of commit semantics (no conflict). Four transactions are
     * started: tx0, which starts first. tx1 which starts next and on which we
     * will write one data version; tx2, which begins after tx1 but before tx1
     * commits - the change will NOT be visible in this transaction; and tx3,
     * which begins after tx1 commits - the change will be visible in this
     * transaction.
     */
    public void test_commit_noConflict01() {

        Journal journal = new Journal(getProperties());
        
        final String name = "abc";
        final long commitTime0;
        {
            
            journal.registerIndex(name,new UnisolatedBTree(journal,UUID.randomUUID()));
            
            commitTime0 = journal.commit();
            
            assertNotSame(0L,commitTime0);
            
        }

        /*
         * Transaction that starts before the transaction on which we write. The
         * change will not be visible in this scope.
         */
        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        // transaction on which we write and later commit.
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        // new transaction - commit will not be visible in this scope.
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        assertTrue(commitTime0<tx0);
        assertTrue(tx0<tx1);
        assertTrue(tx1<tx2);
        
        final byte[] id1 = new byte[]{1};

        final byte[] v0 = getRandomData().array();

        // write data version on tx1
        assertNull(journal.getIndex(name,tx1).insert(id1, v0));

        // data version visible in tx1.
        assertEquals(v0, (byte[])journal.getIndex(name,tx1).lookup(id1));

        // data version not visible in global scope.
        assertNull(journal.getIndex(name).lookup(id1));

        // data version not visible in tx0.
        assertNull(journal.getIndex(name,tx0).lookup(id1));

        // data version not visible in tx2.
        assertNull(journal.getIndex(name,tx2).lookup(id1));

        // commit.
        long tx1CommitTime = journal.commit(tx1);
        assertNotSame(0L,tx1CommitTime);

        // data version now visible in global scope.
        assertEquals(v0, (byte[])journal.getIndex(name).lookup(id1));

        // new transaction - commit is visible in this scope.
        final long tx3 = journal.newTx(IsolationEnum.ReadWrite);
        assertTrue(tx2<tx3);
        assertTrue(tx3>tx1CommitTime);
        
        // data version still not visible in tx0.
        assertNull(journal.getIndex(name,tx0).lookup(id1));

        // data version still not visible in tx2.
        assertNull(journal.getIndex(name,tx2).lookup(id1));

        // data version visible in the new tx (tx3).
        assertEquals(v0, (byte[])journal.getIndex(name,tx3).lookup(id1));

        /*
         * commit tx0 - nothing was written, no conflict should result.
         */
        journal.commit(tx0);

        /*
         * commit tx1 - nothing was written, no conflict should result.
         */
        journal.commit(tx2);

        // commit tx3 - nothing was written, no conflict should result.
        journal.commit(tx3);

        // data version in global scope was not changed by any other commit.
        assertEquals(v0, (byte[]) journal.getIndex(name).lookup(
                id1));

        journal.closeAndDelete();

    }

    /**
     * Test in which a transaction deletes a pre-existing version (that is, a
     * version that existed in global scope when the transaction was started).
     */
    public void test_deletePreExistingVersion_noConflict() {

        Journal journal = new Journal(getProperties());

        final String name = "abc";
        
        {
            
            journal.registerIndex(name, new UnisolatedBTree(journal,UUID.randomUUID()));
            
            journal.commit();
            
        }
        
        final byte[] id0 = new byte[]{1};

        final byte[] v0 = getRandomData().array();

        // data version not visible in global scope.
        assertNull(journal.getIndex(name).lookup(id0));

        // write data version in global scope.
        journal.getIndex(name).insert(id0, v0);

        // data version visible in global scope.
        assertEquals(v0, (byte[])journal.getIndex(name).lookup(id0));

        // commit the unisolated write.
        journal.commit();
        
        // start transaction.
        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        // data version visible in the transaction.
        assertEquals(v0, (byte[])journal.getIndex(name,tx0).lookup(id0));

        // delete version in transaction scope.
        assertEquals(v0,(byte[])journal.getIndex(name,tx0).remove(id0));

        // data version still visible in global scope.
        assertTrue(journal.getIndex(name).contains(id0));
        assertEquals(v0, (byte[])journal.getIndex(name).lookup(id0));

        // FIXME IsolatedBTree has remaining problems and needs a better
        // test suite!
        // data version not visible in transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));
        assertNull(journal.getIndex(name,tx0).lookup(id0));

        // commit.
        journal.commit(tx0);

        // data version now deleted in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        journal.closeAndDelete();

    }

}
