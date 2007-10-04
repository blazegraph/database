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
 * Created on Feb 28, 2006
 */
package com.bigdata.concurrent.locking;

import junit.framework.TestCase;

import org.CognitiveWeb.concurrent.locking.DeadlockException;
import org.CognitiveWeb.concurrent.locking.TxDag;
import org.apache.log4j.Logger;

import com.bigdata.concurrent.TestConcurrencyControl.ResourceQueue;
import com.bigdata.concurrent.locking.action.LockAction;
import com.bigdata.concurrent.schedule.Condition;
import com.bigdata.concurrent.schedule.Schedule;
import com.bigdata.concurrent.schedule.Tx;

/**
 * Test deadlock scenarios for correct identification and prevention of
 * deadlocks. Deadlock prevention methods include operations such as selecting
 * one or more transactions to be rolled back or aborted. Such operations are
 * somewhat specific to a concurrency control solution, but this test suite
 * should verify that the basic mechanisms for deadlock prevention work
 * correctly.
 * 
 * @author thompsonbry
 * 
 * @see org.CognitiveWeb.concurrent.locking.TestDeadlockWithResourceDAG
 * 
 * @version $Id$
 */
public class TestDeadlock extends TestCase {

    public static final Logger log = Logger.getLogger
	( TestDeadlock.class
	  );

    /**
     * 
     */
    public TestDeadlock() {
        super();
    }

    /**
     * @param name
     */
    public TestDeadlock(String name) {
        super(name);
    }

    /**
     * Simple deadlock detection test. tx1 locks R1 in exclusive mode (X). tx2
     * locks R2 in exclusive mode (X). Both locks are granted. tx1 attempts to
     * acquire a share (S) lock on R2 - this blocks since S is not compatible
     * with X. Finally tx2 attempts to acquire a share lock (S) on R1 - this
     * would form a deadlock by creating cycles in the WAITS_FOR graph.
     * <p>
     * 
     * <pre>
     * 
     *    -&gt; means &quot;WAITS FOR&quot;
     *    
     *    tx1.lock( R1, X );
     *    tx2.lock( R2, X );
     *    tx1.lock( R2, X ); // tx1 -&gt; tx2 (blocks)
     *    tx2.lock( R1, X ); // tx2 -&gt; tx1 (deadlock)
     *  
     * </pre>
     *
     * @todo write more deadlock tests.
     */
    public void test_deadlock_001()
    {

        final TxDag waitsFor = new TxDag(2);
        
        final ResourceQueue<String, Tx> r1 = new ResourceQueue<String,Tx>( "R1", waitsFor );

        final ResourceQueue<String, Tx> r2 = new ResourceQueue<String,Tx>( "R2", waitsFor );
        
        Schedule s = new Schedule();

        final Tx tx1 = s.createTx("tx1");
        
        final Tx tx2 = s.createTx("tx2");
        
        s.add( new LockAction<String,Tx>(tx1,r1));
        
        s.add( new LockAction<String,Tx>(tx2,r2));

        /*
         * tx1 -> tx2 (blocks: tx1 is waiting for tx2).
         * 
         * Note: We use a pre-condition on the next action to test the outcome
         * since the expected behavior is that the tx1 blocks on this request.
         */

        s.add(new LockAction<String,Tx>(tx1, r2, true));

        /*
         * tx2 -> tx1 (deadlock: would form a cycle in the WAITS_FOR graph).
         */
   
        s.add(new LockAction<String,Tx>(tx2, r1)
                .addPreCondition(new Condition() {
                    /*
                     * Verify that the edge(s) were created in the WAITS_FOR DAG
                     * by the _previous_ lock attempt.
                     */
                    public void check(Tx _tx) {
                        System.err.println(waitsFor.toString());
                        assertTrue(waitsFor.hasEdge(tx1, tx2));
                        assertFalse(waitsFor.hasEdge(tx2, tx1));
                    }
                }));

        try {
            s.run();
            fail( "Expecting exception: "+DeadlockException.class );
        }
        catch( RuntimeException ex ) {
            log.info("Actual exception: "+ex, ex);
        	/*
        	 * This is a bit tricky since the deadlock exception is being
        	 * set as the cause on a RuntimeException by Schedule#run().
        	 */
        	Throwable cause = ex.getCause();
        	if( cause instanceof DeadlockException ) {
        		log.info("Expected cause: "+cause );
        	} else {
        		fail( "Expecting cause: "+DeadlockException.class+", not "+cause);
        	}
        }
        
    }

}
