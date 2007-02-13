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
 * Created on Feb 13, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTxRunState extends ProxyTestCase {

    /**
     * 
     */
    public TestTxRunState() {
    }

    /**
     * @param name
     */
    public TestTxRunState(String name) {
        super(name);
    }

    /*
     * Transaction run state tests.
     */

    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activeAbort() throws IOException {
        
        final Properties properties = getProperties();

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            ITx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }

    }
    
    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareAbort() throws IOException {
        
        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            ITx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            tx0.prepare();

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.preparedTx.containsKey(ts0));

            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }

    }

    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareCommit() throws IOException {
        
        final Properties properties = getProperties();

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            ITx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            tx0.prepare();

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.preparedTx.containsKey(ts0));

            tx0.commit();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertTrue( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }

    }

    /**
     * Simple test of the transaction run state machine. verifies that a 2nd
     * attempt to abort the same transaction results in an exception that does
     * not change the transaction run state.
     */
    public void test_runStateMachine_activeAbortAbort_correctRejection() throws IOException {
        
        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            ITx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            try {
                tx0.abort();
                fail("Expecting: "+IllegalStateException.class);
            }
            catch( IllegalStateException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }

    }

    /**
     * Simple test of the transaction run state machine verifies that a 2nd
     * attempt to prepare the same transaction results in an exception that
     * changes the transaction run state to 'aborted'.
     */
    public void test_runStateMachine_activePreparePrepare_correctRejection() throws IOException {
        
        final Properties properties = getProperties();

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            ITx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            tx0.prepare();

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.preparedTx.containsKey(ts0));

            try {
                tx0.prepare();
                fail("Expecting: "+IllegalStateException.class);
            }
            catch( IllegalStateException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }

    }

    /**
     * Simple test of the transaction run state machine verifies that a commit
     * out of order results in an exception that changes the transaction run
     * state to 'aborted'.
     */
    public void test_runStateMachine_activeCommit_correctRejection() throws IOException {
        
        final Properties properties = getProperties();

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            ITx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            try {
                tx0.commit();
                fail("Expecting: "+IllegalStateException.class);
            }
            catch( IllegalStateException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }

    }

}
