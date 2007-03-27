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
 * Created on Feb 17, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.isolation.IConflictResolver;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.isolation.Value;

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
public class TestConflictResolution extends ProxyTestCase {

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
            
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID
                    .randomUUID()));

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
            
            journal.registerIndex(name, new UnisolatedBTree(journal,
                    UUID.randomUUID(), new SingleValueConflictResolver(k1, v1c)));

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
        
        public byte[] resolveConflict(byte[] key, Value comittedValue,
                Value txEntry) throws RuntimeException {
 
            assertEquals("key",expectedKey,key);
            
            return resolvedValue;
            
        }
        
    }
    
}
