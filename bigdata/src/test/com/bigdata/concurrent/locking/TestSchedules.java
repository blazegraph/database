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
 * Created on Feb 27, 2006
 */
package com.bigdata.concurrent.locking;

import junit.framework.TestCase;

import org.CognitiveWeb.concurrent.locking.TxDag;
import org.apache.log4j.Logger;

import com.bigdata.concurrent.TestConcurrencyControl.ResourceQueue;
import com.bigdata.concurrent.locking.action.LockAction;
import com.bigdata.concurrent.locking.action.UnlockAction;
import com.bigdata.concurrent.schedule.Action;
import com.bigdata.concurrent.schedule.Condition;
import com.bigdata.concurrent.schedule.Schedule;
import com.bigdata.concurrent.schedule.Tx;

/**
 * Tests for concurrent transactions designed to verify both correct concurrent
 * access to resources when the locks are compatible and correct deadlock
 * detection when the locks induce a WAITS_FOR graph with cycles.
 * 
 * @author thompsonbry
 */
public class TestSchedules extends TestCase {

    public static final Logger log = Logger.getLogger
	( TestSchedules.class
	  );

    /**
     * 
     */
    public TestSchedules() {
        super();
    }

    /**
     * @param name
     */
    public TestSchedules(String name) {
        super(name);
    }
    
    public void setUp() throws Exception
    {
        log.info( "\n"+getName() );
    }
    
    /**
     * Test verifies that we can run a schedule using a single transaction to
     * lock a resource, unlock the resource and then commit the transaction.
     */

    public void test_schedule_001()
    {
    
        TxDag waitsFor = new TxDag(2);
        
        Object resource = getName();
        
        ResourceQueue<Object, Tx> queue = new ResourceQueue<Object,Tx>( resource, waitsFor );
        
        Schedule s = new Schedule();

        Tx tx1 = s.createTx("tx1");
        
        s.add(new LockAction<Object, Tx>(tx1, queue));

        s.add(new UnlockAction<Object, Tx>(tx1, queue));

        s.add(new Action.Commit<Tx>(tx1));

        s.run();
        
    }
    
//    /**
//     * Test verifies that we can run a schedule with two concurrent transactions
//     * in which compatible lock modes are requested for the same resource, the
//     * locks are released, and the transactions are committed.
//     */
//
//    public void test_schedule_002()
//    {
//    
//        TxDag waitsFor = new TxDag(2);
//        
//        Object resource = getName();
//        
//        final Queue queue = new Queue( waitsFor, resource );
//        
//        Schedule s = new Schedule();
//
//        Tx tx1 = s.createTx("tx1");
//
//        Tx tx2 = s.createTx("tx2");
//        
//        s.add(new LockAction(tx1, queue, LockMode.S).addPostCondition(new Condition(){
//            public void check(Tx tx) {
////                assertEquals("groupSize", 1, queue.getGroupSize() );
//                assertEquals("groupMode", LockMode.S, queue.getGroupMode() );
////                assertEquals("pendingSize", 0, queue.getQueueSize() );
//        }}));
//
//        s.add(new LockAction(tx2, queue, LockMode.S));
//
//        s.add(new UnlockAction(tx1, queue));
//
//        s.add(new UnlockAction(tx2, queue));
//
//        s.add(new Action.Commit(tx1));
//
//        s.add(new Action.Commit(tx2));
//
//        s.run();
//        
//    }

//    /**
//     * Test verifies that we can run a schedule with two concurrent transactions
//     * in which compatible lock modes are requested for the same resource, the
//     * locks are released, and the transactions are committed.
//     */
//
//    public void test_schedule_003()
//    {
//    
//        TxDag waitsFor = new TxDag(2);
//        
//        Object resource = getName();
//        
//        final ResourceQueue<Object,Tx> queue = new ResourceQueue<Object,Tx>( resource, waitsFor);
//        
//        Schedule s = new Schedule();
//        
//        final Tx tx1 = s.createTx("tx1");
//
//        final Tx tx2 = s.createTx("tx2");
//
//        s.add(new LockAction(tx1, queue, LockMode.S)
//                .addPostCondition(new Condition() {
//                    public void check(Tx tx) {
//                        assertTrue(queue.isGranted(tx));
//                        assertEquals("grantedGroup", 1, queue.getGroupSize());
//                        assertEquals("groupMode", LockMode.S, queue
//                                .getGroupMode());
//                    }
//                }));
//
//        s.add(new LockAction(tx2, queue, LockMode.S)
//                .addPostCondition(new Condition() {
//                    public void check(Tx tx) {
//                        Lock tx1Lock = queue.getTxLock(tx1);
//                        Lock tx2Lock = queue.getTxLock(tx2);
//                        assertTrue(queue.isGranted(tx1Lock));
//                        assertTrue(queue.isGranted(tx2Lock));
//                        assertEquals("grantedGroup", 2, queue.getGroupSize());
//                        assertEquals("groupMode", LockMode.S, queue
//                                .getGroupMode());
//                    }
//                }));
//
//        s.add(new UnlockAction(tx1, queue).addPostCondition(new Condition() {
//            public void check(Tx tx) {
//                assertNull(queue.getTxLock(tx1));
//                Lock tx2Lock = queue.getTxLock(tx2);
//                assertNotNull(tx2Lock);
//                assertTrue("granted",queue.isGranted(tx2Lock));
//                assertEquals("grantedGroup", 1, queue.getGroupSize());
//                assertEquals("groupMode", LockMode.S, queue.getGroupMode());
//            }
//        }));
//
//        s.add(new UnlockAction(tx2, queue).addPostCondition(new Condition() {
//            public void check(Tx tx) {
//                assertNull(queue.getTxLock(tx1));
//                assertNull(queue.getTxLock(tx2));
//                assertEquals("grantedGroup", 0, queue.getGroupSize());
//                assertEquals("groupMode", LockMode.NL, queue.getGroupMode());
//            }
//        }));
//
//        s.add(new Action.Commit(tx1));
//
//        s.add(new Action.Commit(tx2));
//
//        s.run();
//    
//    }
        
    /**
     * Test verifies that we can run a schedule with two concurrent transactions
     * in which incompatible lock modes are requested for the same resource. The
     * transaction which obtains the first lock runs until it releases its lock
     * and then the other transaction enters the granted group and both
     * transactions complete without deadlock.
     */

    public void test_schedule_004()
    {
    
        TxDag waitsFor = new TxDag(2);
        
        Object resource = getName();
        
        final ResourceQueue<Object, Tx> queue = new ResourceQueue<Object,Tx>( resource, waitsFor);
        
        Schedule s = new Schedule();

        final Tx tx1 = s.createTx("tx1");

        final Tx tx2 = s.createTx("tx2");
        
        s.add(new LockAction<Object,Tx>(tx1, queue));

        s.add(new LockAction<Object,Tx>(tx2, queue, true)); // action MUST block.

        s.add(new UnlockAction<Object,Tx>(tx1, queue).addPreCondition(new Condition() {
            public void check(Tx tx) {
                assertTrue("locked", queue.isLocked());
                assertEquals("pendingSize", 1, queue.getQueueSize());
                assertTrue("lock(tx2)", queue.isGranted(tx2));
            }
        }).addPostCondition(new Condition() {
            public void check(Tx tx) {
                assertTrue("locked", queue.isLocked());
                assertEquals("pendingSize", 0, queue.getQueueSize());
            }
        }));

        s.add(new UnlockAction(tx2, queue));

        s.add(new Action.Commit(tx1));

        s.add(new Action.Commit(tx2));

        s.run();
        
    }
    
//    /**
//     * This tests that multiple transactions block may block a resource and than
//     * verifies that they all unblock when the resource lock is released by the
//     * owning transaction. tx1 obtains an exclusive (X) lock on the resource.
//     * tx2 and tx3 attempt to lock the resource in share (S) mode. The test
//     * verifies that tx2 and tx3 block. tx1 then releases its exclusive lock and
//     * the test verifies that both tx2 and tx3 enter the granted group.
//     */
//    
//    public void test_schedule_005()
//    {
//
//        TxDag waitsFor = new TxDag(3);
//        
//        Object resource = getName();
//        
//        final Queue queue = new Queue( waitsFor, resource );
//        
//        Schedule s = new Schedule();
//
//        final Tx tx1 = s.createTx("tx1");
//
//        final Tx tx2 = s.createTx("tx2");
//        
//        final Tx tx3 = s.createTx("tx3");
//        
//        s.add( new LockAction(tx1,queue,LockMode.X));
//        s.add( new LockAction(tx2,queue,LockMode.S,true));
//        s.add( new LockAction(tx3,queue,LockMode.S,true));
//
//        s.add( new UnlockAction(tx1,queue).addPreCondition(new Condition(){
//            public void check(Tx tx) {
//                assertEquals("groupMode", LockMode.X, queue.getGroupMode() );
//                assertEquals("groupSize", 1, queue.getGroupSize() );
//                assertEquals("pendingSize", 2, queue.getQueueSize() );
//            }}).addPostCondition(new Condition(){
//                public void check(Tx tx) {
//                    assertEquals("groupMode", LockMode.S, queue.getGroupMode() );
//                    assertEquals("groupSize", 2, queue.getGroupSize() );
//                    assertEquals("pendingSize", 0, queue.getQueueSize() );
//                }}));
//        
//        s.run();
//        
//    }

//    /**
//     * tx1 obtains an exclusive (X) lock on the resource. tx2 attempts to lock
//     * the resource in share (S) mode. tx3 attempts to lock the resource in
//     * share (X) mode. The test verifies that tx2 and tx3 block. tx1 then
//     * releases its exclusive lock and the test verifies that tx2 has its lock
//     * granted, but that tx3 is still blocked. tx2 then releases its share (S)
//     * lock and the test verifies that tx3 enters the granted group.
//     */
//    
//    public void test_schedule_006()
//    {
//
//        TxDag waitsFor = new TxDag(3);
//        
//        Object resource = getName();
//        
//        final Queue queue = new Queue( waitsFor, resource );
//        
//        Schedule s = new Schedule();
//
//        final Tx tx1 = s.createTx("tx1");
//
//        final Tx tx2 = s.createTx("tx2");
//        
//        final Tx tx3 = s.createTx("tx3");
//        
//        s.add( new LockAction(tx1,queue,LockMode.X));
//        s.add( new LockAction(tx2,queue,LockMode.S,true));
//        s.add( new LockAction(tx3,queue,LockMode.X,true));
//
//        s.add( new UnlockAction(tx1,queue).addPreCondition(new Condition(){
//            public void check(Tx tx) {
//                assertEquals("groupMode", LockMode.X, queue.getGroupMode() );
//                assertEquals("groupSize", 1, queue.getGroupSize() );
//                assertEquals("pendingSize", 2, queue.getQueueSize() );
//            }}).addPostCondition(new Condition(){
//                public void check(Tx tx) {
//                    assertEquals("groupMode", LockMode.S, queue.getGroupMode() );
//                    assertEquals("groupSize", 1, queue.getGroupSize() );
//                    assertEquals("pendingSize", 1, queue.getQueueSize() );
//                }}));
//        
//        s.add( new UnlockAction(tx2,queue).addPreCondition(new Condition(){
//            public void check(Tx tx) {
//                assertEquals("groupMode", LockMode.S, queue.getGroupMode() );
//                assertEquals("groupSize", 1, queue.getGroupSize() );
//                assertEquals("pendingSize", 1, queue.getQueueSize() );
//            }}).addPostCondition(new Condition(){
//                public void check(Tx tx) {
//                    assertEquals("groupMode", LockMode.X, queue.getGroupMode() );
//                    assertEquals("groupSize", 1, queue.getGroupSize() );
//                    assertEquals("pendingSize", 0, queue.getQueueSize() );
//                }}));
//
//        s.run();
//        
//    }

    /**
	 * Test verifies that a fair schedule is being used to grant locks. The test
	 * creates a number of transactions, each of which attempts to obtain an
	 * incompatible lock mode (X) for a common resource. Since the requested
	 * lock modes are incompatible only one lock at a time may be granted. The
	 * test verifies that the first transaction to request the lock in fact
	 * obtained the lock and that the correct #of requests are pending. Test
	 * test then causes the transaction with the lock to give it up and verifies
	 * that the next transaction entered into the queue for that resource is
	 * granted the lock. This process is repeated until each transaction has
	 * been granted the lock in its turn and the final transaction has released
	 * the lock.
	 * <p>
	 * Note: This test will sometimes fail, especially the first time it is run.
	 * This appears to be a side effect of the startup costs and the mechanism
	 * which is used to detect a blocked transaction. The test should pass if it
	 * is simple re-run.
	 */

    public void test_schedule_007()
    {

        final int N = 10;
        
        TxDag waitsFor = new TxDag( N );
        
        Object resource = getName();
        
        final ResourceQueue<Object,Tx> queue = new ResourceQueue<Object,Tx>( resource, waitsFor);
        
        Schedule s = new Schedule();

        final Tx[] tx = new Tx[ N ];
        
        for( int i=0; i<N; i++ ) {
            
            tx[ i ] = s.createTx("tx#"+i);
         
            s.add( new LockAction<Object,Tx>(tx[i], queue, (i>0?true:false) ) );
            
        }

        for( int i=1; i<N; i++ ) {
//            final int remainingAfter = N - 1 - 1;
            final Tx priorTx = tx[ i-1 ];
            final Tx currentTx = tx[ i ];
            s.add( new UnlockAction<Object,Tx>(priorTx,queue).addPreCondition(new Condition() {
                public void check(Tx tx2) {
                    assertTrue("locked", queue.isLocked() );
                    assertTrue("granted",queue.isGranted(priorTx));
                }
            }).addPostCondition(new Condition() {
                public void check(Tx tx2) {
                    assertTrue("locked", queue.isLocked() );
                    assertTrue("granted",queue.isGranted(currentTx));
//                    assertEquals("pendingSize",remainingAfter,queue.getQueueSize());
                }
            }) );
        }
        
        s.run();
        
    }

    /**
     * Simple test would cause a deadlock if the release of a lock was not
     * handled correctly. tx1 locks R1 in exclusive mode (X). tx2 locks R2 in
     * exclusive mode (X). Both locks are granted. tx1 attempts to acquire a
     * exclusive (X) lock on R2 - this blocks since nothing is compatible with
     * X. tx2 releases its locks.
     * <p>
     * 
     * <pre>
     *    
     *       -&gt; means &quot;WAITS FOR&quot;
     *       
     *       tx1.lock( R1, X );
     *       tx2.lock( R2, X );
     *       tx1.lock( R2, X ); // tx1 -&gt; tx2 (blocks)
     *       tx2.release( R1 ); // (unblocks tx1).
     *     
     * </pre>
     */

    public void test_schedule_008()
    {

        final TxDag waitsFor = new TxDag(2);
        
        final ResourceQueue r1 = new ResourceQueue( "R1",waitsFor );

        final ResourceQueue r2 = new ResourceQueue( "R2",waitsFor );
        
        Schedule s = new Schedule();

        final Tx tx1 = s.createTx("tx1");
        
        final Tx tx2 = s.createTx("tx2");
        
        s.add( new LockAction(tx1,r1));
        
        s.add( new LockAction(tx2,r2));

        /*
         * tx1 -> tx2 (blocks: tx1 is waiting for tx2).
         * 
         * Note: We use a pre-condition on the next action to test the outcome
         * since the expected behavior is that the tx1 blocks on this request.
         */

        s.add(new LockAction(tx1, r2, true));

        /*
		 * tx2 releases its X lock on r2. This should result in tx1 being
		 * unblocked and running to completion.
		 */
        s.add(new UnlockAction(tx2, r2));

        s.run();
        
    }

}
