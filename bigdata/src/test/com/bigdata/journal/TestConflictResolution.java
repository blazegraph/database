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

import java.util.Properties;

import com.bigdata.isolation.IConflictResolver;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.isolation.Value;

/**
 * Tests of write-write conflict resolution.
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

        Properties properties = getProperties();

        Journal journal = new Journal(properties);

        String name = "abc";

        final byte[] k1 = new byte[] { 1 };

        final byte[] v1a = new byte[] { 1 };
        final byte[] v1b = new byte[] { 2 };

        {

            /*
             * register an index and commit the journal.
             */
            
            journal.registerIndex(name, new UnisolatedBTree(journal));

            journal.commit();

        }

        /*
         * Create two transactions.
         */
        
        ITx tx1 = journal.newTx();
        
        ITx tx2 = journal.newTx();
        
        /*
         * Write a value under the same key on the same index in both
         * transactions.
         */
        
        tx1.getIndex(name).insert(k1, v1a);

        tx2.getIndex(name).insert(k1, v1b);

        tx1.prepare();
        
        tx1.commit();
        
        /*
         * verify that the value from tx1 is found under the key on the
         * unisolated index.
         */
        assertEquals(v1a,(byte[])journal.getIndex(name).lookup(k1));

        try {
            tx2.prepare();
            fail("Expecting: "+ValidationError.class);
        } catch(ValidationError ex) {
            System.err.println("Ignoring expected exception: "+ex);
            assertTrue(tx2.isAborted());
        }
        
        journal.close();

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

        Properties properties = getProperties();

        Journal journal = new Journal(properties);

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
                    new SingleValueConflictResolver(k1, v1c)));

            journal.commit();

        }

        /*
         * Create two transactions.
         */
        
        ITx tx1 = journal.newTx();
        
        ITx tx2 = journal.newTx();
        
        /*
         * Write a value under the same key on the same index in both
         * transactions.
         */
        
        tx1.getIndex(name).insert(k1, v1a);

        tx2.getIndex(name).insert(k1, v1b);

        tx1.prepare();
        
        tx1.commit();

        /*
         * verify that the value from tx1 is found under the key on the
         * unisolated index.
         */
        assertEquals(v1a,(byte[])journal.getIndex(name).lookup(k1));

        tx2.prepare();
        
        // @todo the indices should probably become read only at this point.
        assertEquals(v1c,(byte[])tx2.getIndex(name).lookup(k1));
        
        tx2.commit();
        
        /*
         * verify that the resolved value is found under the key on the
         * unisolated index.
         */
        assertEquals(v1c,(byte[])journal.getIndex(name).lookup(k1));
        
        journal.close();

    }
    
    /**
     * The concurrency control algorithm must not permit two transactions to
     * prepare at the same time since that violates the basic rules of
     * serializability.
     * 
     * @todo javadoc and move into schedules test suite or its own test suite.
     */
    public void test_serializability() {

        Properties properties = getProperties();

        Journal journal = new Journal(properties);

        String name = "abc";

        final byte[] k1 = new byte[] { 1 };

        final byte[] v1a = new byte[] { 1 };
        final byte[] v1b = new byte[] { 2 };

        {

            /*
             * register an index and commit the journal.
             */
            
            journal.registerIndex(name, new UnisolatedBTree(journal));

            journal.commit();

        }

        /*
         * Create two transactions.
         */
        
        ITx tx1 = journal.newTx();
        
        ITx tx2 = journal.newTx();
        
        /*
         * Write a value under the same key on the same index in both
         * transactions.
         */
        
        tx1.getIndex(name).insert(k1, v1a);

        tx2.getIndex(name).insert(k1, v1b);

        tx1.prepare();
        
        try {
            tx2.prepare();
            fail("Expecting: "+IllegalStateException.class);
        } catch(IllegalStateException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        journal.close();
        
    }
 
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
