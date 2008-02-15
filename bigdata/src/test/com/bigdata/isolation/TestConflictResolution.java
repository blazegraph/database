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
 * Created on Feb 17, 2007
 */

package com.bigdata.isolation;

import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IsolationEnum;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.journal.ValidationError;

/**
 * Tests of write-write conflict resolution.
 * <p>
 * Write-write conflicts either result in successful reconcilation via
 * state-based conflict resolution or an abort of the transaction that is
 * validating. The tests in this suite verify that write-write conflicts can be
 * detected and provide versions of those tests where the conflict can and can
 * not be validated and verify the end state in each case.
 * <p>
 * State-based validation requires transparency at the object level, including
 * the ability to deserialize versions into objects, to compare objects for
 * consistency, to merge data into the most recent version where possible and
 * according to data type specific rules, and to destructively merge objects
 * when the conflict arises on <em>identity</em> rather than state.
 * <p>
 * An example of an identity based conflict is when two objects are created that
 * represent URIs in an RDF graph. Since the lexicon for an RDF graph generally
 * requires uniqueness those objects must be merged into a single object since
 * they have the same identity. For an RDFS store validation on the lexicon or
 * statements ALWAYS succeeds since they are always consistent.
 * 
 * @todo Verify that we can handle the bank account example (this is state-based
 *       conflict resolution altogether requiring that we carry a richer
 *       representation of state in the objects and then use that additional
 *       state to validate and resolve some kinds of data type specific
 *       conflicts).
 * 
 * @todo Do tests that verify that multiple conflicts are correctly detected and
 *       resolved.
 * 
 * @todo Verify that we can handle examples in which we have to traverse an
 *       object graph during conflict resolution. (Really, two object graphs: a
 *       readOnly view of the ground state for the transaction and the
 *       readWriteTx that we are trying to validate.) This last issue is by far
 *       the trickyest and may require support for concurrent modification of
 *       the transaction indices during traveral (or more simply of reading from
 *       a fused view of the resolved and unconflicting entries in the
 *       read-write tx index views).
 * 
 * @todo Destructive merging of objects in a graph can propagate changes other
 *       objects. Unless the data structures provide for encapsulation, e.g., by
 *       defining objects that serve as collectors for the link set members in a
 *       given segment, that change could propagate beyond the segment in which
 *       it is detected. If changes can propagate in that manner then care MUST
 *       be taken to ensure that validation terminates.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConflictResolution extends TestCase2 {

    /**
     * 
     */
    public TestConflictResolution() {
    }

    /**
     * @param name
     */
    public TestConflictResolution(String name) {
        super(name);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        
        return properties;
        
    }
    
    /**
     * Test correct detection of a write-write conflict. An index is registered
     * and the journal is committed. Two transactions (tx1, tx2) are then
     * started. Both transactions write a value under the same key. tx1 prepares
     * and commits.  tx2 attempts to prepare, and the test verifies that a
     * {@link ValidationError} is reported.
     */
    public void test_writeWriteConflict_correctDetection() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        final byte[] k1 = new byte[] { 1 };

        final byte[] v1a = new byte[] { 1 };
        final byte[] v1b = new byte[] { 2 };

        {

            /*
             * register an index and commit the journal.
             */
            
            IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());
            
            metadata.setIsolatable(true);
            
            // Note: No conflict resolver.
            
            journal.registerIndex(name, BTree.create(journal,metadata) );

            journal.commit();

        }

        /*
         * Create two transactions.
         */
        
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);
        
        /*
         * Write a value under the same key on the same index in both
         * transactions.
         */
        
        journal.getIndex(name,tx1).insert(k1, v1a);

        journal.getIndex(name,tx2).insert(k1, v1b);

        journal.commit(tx1);
        
        /*
         * verify that the value from tx1 is found under the key on the
         * unisolated index.
         */
        assertEquals(v1a,(byte[])journal.getIndex(name).lookup(k1));

        final ITx tmp = journal.getTx(tx2);

        try {
            journal.commit(tx2);
            fail("Expecting: "+ValidationError.class);
        } catch(ValidationError ex) {
            System.err.println("Ignoring expected exception: "+ex);
            assertTrue(tmp.isAborted());
        }
        
        journal.closeAndDelete();

    }
    
    /**
     * Test correct detection and resolution of a write-write conflict. An index
     * is registered with an {@link IConflictResolver} and the journal is
     * committed. Two transactions (tx1, tx2) are then started. Both
     * transactions write a value under the same key. tx1 prepares and commits.
     * tx2 attempts to prepare, and the test verifies that the conflict resolver
     * is invoked, that it may resolve the conflict causing validation to
     * succeed and that the value determined by conflict resolution is made
     * persistent when tx2 commits.
     */
    public void test_writeWriteConflict_conflictIsResolved() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        final byte[] k1 = new byte[] { 1 };

        final byte[] v1a = new byte[] { 1 };
        final byte[] v1b = new byte[] { 2 };
        final byte[] v1c = new byte[] { 3 };

        {

            /*
             * register an index with a conflict resolver and commit the
             * journal.
             */
            
            IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());
            
            metadata.setIsolatable(true);
            
            metadata.setConflictResolver(new SingleValueConflictResolver(k1,
                    v1c));
            
            journal.registerIndex(name, BTree.create(journal,metadata) );

            journal.commit();

        }

        /*
         * Create two transactions.
         */
        
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);
        
        /*
         * Write a value under the same key on the same index in both
         * transactions.
         */
        
        journal.getIndex(name,tx1).insert(k1, v1a);

        journal.getIndex(name,tx2).insert(k1, v1b);

        journal.commit(tx1);

        /*
         * verify that the value from tx1 is found under the key on the
         * unisolated index.
         */
        assertEquals(v1a,(byte[])journal.getIndex(name).lookup(k1));

        journal.commit(tx2);
        
        /*
         * verify that the resolved value is found under the key on the
         * unisolated index.
         */
        assertEquals(v1c,(byte[])journal.getIndex(name).lookup(k1));
        
        journal.closeAndDelete();

    }
    
//    /**
//     * The concurrency control algorithm must not permit two transactions to
//     * prepare at the same time since that violates the basic rules of
//     * serializability.
//     * 
//     * @todo javadoc and move into schedules test suite or its own test suite.
//     */
//    public void test_serializability() {
//
//        Properties properties = getProperties();
//
//        Journal journal = new Journal(properties);
//
//        String name = "abc";
//
//        final byte[] k1 = new byte[] { 1 };
//
//        final byte[] v1a = new byte[] { 1 };
//        final byte[] v1b = new byte[] { 2 };
//
//        {
//
//            /*
//             * register an index and commit the journal.
//             */
//            
//            journal.registerIndex(name, new UnisolatedBTree(journal));
//
//            journal.commit();
//
//        }
//
//        /*
//         * Create two transactions.
//         */
//        
//        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
//        
//        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);
//        
//        /*
//         * Write a value under the same key on the same index in both
//         * transactions.
//         */
//        
//        journal.getIndex(name,tx1).insert(k1, v1a);
//
//        journal.getIndex(name,tx2).insert(k1, v1b);
//
//        tx1.prepare();
//        
//        try {
//            tx2.prepare();
//            fail("Expecting: "+IllegalStateException.class);
//        } catch(IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: "+ex);
//        }
//        
//        journal.close();
//        
//    }
//
    /**
     * Helper class used to resolve a predicted conflict to a known value.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SingleValueConflictResolver implements IConflictResolver
    {

        private final byte[] expectedKey;
        private final byte[] resolvedValue;
        
        private static final long serialVersionUID = -1161201507073182976L;

        public SingleValueConflictResolver(byte[] expectedKey, byte[] resolvedValue) {
            
            this.expectedKey = expectedKey;
            
            this.resolvedValue = resolvedValue;
            
        }
        public boolean resolveConflict(IIndex writeSet, ITuple txTuple, ITuple currentTuple) throws Exception {

            // The key must be the same for both tuples.
            assertEquals(txTuple.getKey(),currentTuple.getKey());
            
            // the key for the conflicting writes.
            final byte[] key = txTuple.getKey();
            
            assertEquals("key", expectedKey, key );
            
            writeSet.insert(key, resolvedValue);
            
            return true;
            
        }

    }

}
