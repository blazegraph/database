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
 * Created on Mar 3, 2006
 */
package com.bigdata.concurrent.schedule;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.locking.action.LockAction;

/**
 * A schedule of operations by concurrent transactions. The schedule is a
 * discrete timeline. Each point on the timeline is defined by an {@link Action}.
 * Each action is paired with the {@link Tx transaction} which will execute that
 * action. Actions execute in the thread associated with their transaction, so
 * actions never block the main thread. Actions may be associated with pre-
 * and/or post-{@link Condition}s. Actions are executed atomically unless the
 * action blocks. An action MUST declare whether or not it will
 * {@link Action#block}. If the action causes an exception to be thrown, then
 * the schedule will halt and the exception to be thrown out of the
 * {@link #run()} method.
 * <p>
 * Once the action has been successfully executed by the transaction, the thread
 * for that transaction will wait until the schedule sets the next action for
 * that transaction and notifies the transaction. If an action blocks, then the
 * next action in the schedule is executed and the duration of the blocked
 * action will extend until the transaction is unblocked. Normally blocking
 * occurs due to a specific combination of {@link LockAction}s and unblocking
 * results when appropriate {@link LockAction}s are released. If a transaction
 * is blocked when its next action is reached in the schedule, then an error is
 * reported (since it is impossible for the transaction to execute any further
 * actions while it is blocked). Once all actions have been executed, the
 * transactions are scanned to verify that none are blocked and that all
 * transactions are complete. An error message is logged if these conditions are
 * violated.
 * <p>
 * Aborts must be executed in the main thread so that blocked transactions may
 * be aborted.
 * <p>
 * An event log is recorded using {@link #log}.
 * <p>
 * 
 * @author thompsonbry
 * 
 * @see Tx
 * @see Action
 */
public class Schedule implements Runnable {

    public final Logger log = Logger.getLogger(Schedule.class);
    
    /**
     * The set of declared transactions.
     */
    Set<Tx> transactions = new HashSet<Tx>();

    /**
     * The sequence of actions. 
     */
    List<Action> actions = new LinkedList<Action>();

    /**
     * The thread in which the schedule is running. This is initially
     * <code>null</code> and is set when the schedule is {@link #run()}.
     */
    Thread thread = null;

    /**
     * Create a new schedule.
     */
    public Schedule() {
    }

    /**
     * Create a transaction for use with this {@link Schedule}.
     * 
     * @param name The name of the transaction.
     * 
     * @return The transaction.
     */
    public Tx createTx(String name) {
        Tx tx = new Tx(this, name);
        transactions.add(tx);
        return tx;
    }

    /**
     * Add an action to the schedule.
     * 
     * @param action
     *            The action.
     */
    public void add(Action action) {
        if (action == null) {
            throw new IllegalArgumentException();
        }
        Tx tx = action.getTx();
        if (!transactions.contains(tx)) {
            throw new IllegalArgumentException(
                    "action belongs to a transaction created by a different schedule.");
        }
        actions.add(action);
    }

    /**
     * Return iterator over the transactions known to the schedule.
     */
    public Iterator getTransactions() {
        return transactions.iterator();
    }

    /**
     * Return the #of transactions known to the schedule.
     */
    public int getTransactionCount() {
        return transactions.size();
    }

    /**
     * Return the #of actions scheduled for the specified transaction.
     * 
     * @param tx
     *            The transaction.
     * @return The #of actions scheduled for that transaction.
     */
    public int getActionCount(Tx tx) {
        if (tx == null) {
            throw new IllegalArgumentException();
        }
        int count = 0;
        Iterator itr = actions.iterator();
        while (itr.hasNext()) {
            Action action = (Action) itr.next();
            if (action.getTx() == tx) {
                count++;
            }
        }
        return count;
    }

    /**
     * Run all actions.
     * 
     * @exception RuntimeException
     *                If an error has occurred either in the scheduler state
     *                machine, in the execution of an action, or in the pre-
     *                or post-conditions for an action.
     */
    public void run() {

        thread = Thread.currentThread();

        log.info("Will run: " + actions.size()
                + " actions defined for " + transactions.size()
                + " transactions");

        Iterator itr = actions.iterator();

        int i = 0;

        while (itr.hasNext()) {

            Action action = (Action) itr.next();

            log.info("Action#=" + i + ": " + action);

            Tx tx = action.getTx();

            /*
             * Set the action to be executed and setup a timeout. If the
             * timeout is exceeded, then the action blocked. This should
             * only happen for actions which declare that they will block.
             * Blocking in any other case causes an exception to be thrown.
             * Failure to block when a the action was expected to block also
             * causes an exception to be thrown.
             * 
             * The schedule runs in the main thread. Synchronization is on
             * the individual transactions. Normally one transaction runs at
             * a time and only when the schedule is halted. The exception
             * occurs when a transaction which was blocked becomes
             * unblocked, e.g., by sending it an interrupt or notifying the
             * Tx object.
             * 
             * @todo schedule#run() Is there a way to detect a block without
             * relying on a long timeout? (Short timeouts are sometimes too
             * short and give a false positive.)
             */

//            synchronized (tx) {

                checkTx(tx);
                
                log.info("Setting action on tx: tx=" + tx
                        + ", action=" + action);
                
                tx.action = action; // set action to be executed.

                runAction(tx);

//            }

            i++;

        }

        log.info("Schedule done.");

        /*
         * Scan transactions and report any which are in an error condition
         * [exception != null], blocked [action != null ], or failed to
         * complete [done != true].
         */

        itr = transactions.iterator();

        while (itr.hasNext()) {

            Tx tx = (Tx) itr.next();

            if (tx.exception != null) {

                log
                        .error("error: tx=" + tx, tx.exception);

            } else if (tx.action != null) {

                log.error("blocked: tx=" + tx
                        + " on action=" + tx.action);

            } else if (!tx.done) {

                log.warn("active: tx=" + tx);

            }

        }

        //            /*
        //             * Transactions may still be executing at this point. Therefore we
        //             * scan through the transaction set repeatedly, removing any
        //             * transactions which have completed execution.
        //             *
        //             * If any transaction has set its [exception] field, then it has
        //             * failed and we will throw an exception out of the test case.
        //             * Otherwise if the [action] field is cleared then the transaction
        //             * has finished executing (since we are not scheduling more
        //             * actions). If the [done] flag was NOT set, then we log a warning
        //             * since the test harness did not "commit" that transaction.
        //             */
        //            
        //            while( true ) {
        //
        //                if( transactions.size() == 0 ) {
        //                    
        //                    // All transactions have finished.
        //                    
        //                    break;
        //                    
        //                }
        //                
        //                // Scan remaining transactions.
        //                
        //                itr = transactions.iterator();
        //            
        //                while( itr.hasNext() ) {
        //                    
        //                    Tx tx = (Tx) itr.next();
        //                    
        //                    if( tx.exception != null ) {
        //                    
        //                        /*
        //                         * This transaction failed, so thrown the exception out
        //                         * to the test harness.
        //                         */
        //                        
        //                        throw new RuntimeException("failure: tx=" + tx,
        //                                tx.exception);
        //                    
        //                    }
        //                    
        //                    if( tx.action == null ) {
        //                        
        //                        /*
        //                         * The transaction is not executing anything.
        //                         */
        //
        //                        if( ! tx.done ) {
        //                            
        //                            /*
        //                             * Log a warning since the test harness did not
        //                             * explicitly commit the transaction. (This is not
        //                             * an error since we are not really required to
        //                             * commit transactions during tests of the
        //                             * concurrency control mechanism.)
        //                             */
        //                            
        //                            log.warn( "transaction was not committed: tx="+tx );
        //                            
        //                        }
        //                        
        //                        // Remove from the set of remaining transactions.
        //                        
        //                        itr.remove();
        //
        //                    }
        //                    
        //                }
        //
        //            }

    }

    /**
     * Runs the current action for the transaction.
     * <p>
     * Note: You MUST NOT step through this code with the debugger since it will
     * cause a timeout to be reported due to the latency. Just step over or out!
     * 
     * @param tx
     *            The transaction.
     */
    private void runAction(Tx tx) {
        
        final long timeout = 1000L; // ms. @todo try 100ms for faster tests.
        long elapsed;
        //                    this.notifyAll(); // notify all transactions.
        //                    synchronized (this) {
        final long begin = System.currentTimeMillis();
        final Action action = tx.action;
        log.info("tx=" + tx + ", action=" + action);
        /*
         * Note: the timeout is "more or less" so we add 10% in an attempt to
         * convince wait(long) to wait for at least timeout ms.
         */
        final long requestedTimeout = (long) (timeout + timeout * .10);
        try {
            log.debug("Obtaining lock for schedule: tx=" + tx);
            lock.lock();
            try {
                log.debug("Have lock, sending signal to tx=" + tx);
                tx.run.signal();
                log.debug("Awaiting tx="+tx+" to signal schedule.");
                if(!run.await(requestedTimeout, TimeUnit.MILLISECONDS)) {
                    log.debug("Timed out waiting on tx: "+tx);
                }
            } finally {
                lock.unlock();
            }
//            log.debug("notify: tx=" + tx);
//            //                          tx.interrupt(); // wake up transaction.
//            tx.notifyAll(); // notify tx.
//            tx.wait(requestedTimeout); // transaction will attempt to run.
            elapsed = System.currentTimeMillis() - begin;
            log.debug("resuming: tx=" + tx+", timeout="+timeout
                    + ", elapsed=" + elapsed);
        } catch (InterruptedException ex) {
            elapsed = System.currentTimeMillis() - begin;
            log.debug("interrupted: elapsed="
                    + elapsed);
            //                        throw new RuntimeException("???", ex);
        }
        //                    }
        if (tx.exception != null) {
            /*
             * The action caused an exception to be thrown. This
             * halts the execution of the schedule and the exception
             * is reported back to the caller.
             */
            throw new RuntimeException("failure: tx=" + tx,
                    tx.exception);
        }
        if (action.blocks) {
            if (elapsed < timeout) {
                /*
                 * The transaction failed to blocked when it was
                 * supposed to block.
                 */
                throw new RuntimeException(
                        "action failed to block: tx=" + tx
                                + ", action=" + action + ", elapsed="
                                + elapsed);
            }
        } else {
            if (elapsed >= timeout) {
                /*
                 * The transaction blocked when it was not supposed
                 * to block.
                 */
                throw new RuntimeException("action blocked: tx=" + tx
                        + ", action=" + action + ", elapsed=" + elapsed);
            }
        }

    }
    
    /**
     * Check some pre-conditions before tasking a transaction to execute an
     * action.
     * 
     * @exception IllegalStateException
     *                If the transaction is complete [{@link Tx#done}==
     *                true].
     * 
     * @exception IllegalStateException
     *                If the transaction has thrown an exception [
     *                {@link Tx#exception}!= null]
     * 
     * @exception IllegalStateException
     *                If the transaction is blocked [{@link Tx#action}!=
     *                null].
     */
    private void checkTx(Tx tx) throws IllegalStateException {
        if (tx.done) { // @todo isInterrupted would work if tx is Thread.
            throw new IllegalStateException("transaction is done: tx=" + tx);
        }
        if (tx.action != null) {
            /*
             * This case arises when a lock was not granted for a
             * transaction and the schedule reaches the next action for that
             * transaction. This is either a problem with the test case or a
             * failure in the locking system.
             */
            if (tx.exception != null) {
                // Special case when a block transaction throws an
                // exception.
                IllegalStateException ex = new IllegalStateException(
                        "transaction is blocked: tx=" + tx + " by action="
                                + tx.action);
                ex.initCause(tx.exception);
                throw ex;
            } else {
                throw new IllegalStateException("transaction is blocked: tx="
                        + tx + " by action=" + tx.action);
            }
        }
    }

    /**
     * Lock used to coordinate running {@link Tx}s and the {@link Schedule}.
     */
    Lock lock = new ReentrantLock();

    /**
     * Condition to notify the {@link Schedule} that it should run.
     */
    java.util.concurrent.locks.Condition run = lock.newCondition();

}
