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

import org.apache.log4j.Logger;

/**
 * Test helper conflates a transactions with a thread. The {@link Runnable}
 * target executions the operations of the transaction as defined by a
 * {@link Schedule}.
 * 
 * @author thompsonbry
 * 
 * @see Schedule
 */
public class Tx extends Thread {

    public final Logger log = Logger.getLogger(Tx.class);

    /**
     * Initially false and set to [true] when the transaction completes.
     * This field is checked by the {@link Schedule}.
     */

    volatile boolean done = false;

    /**
     * Initially <code>null</code>. If a pre-condition, post-condition or
     * action fails, then this is set to the thrown exception. This field is
     * checked by the {@link Schedule}.
     */

    volatile Throwable exception = null;

    /**
     * The next/current action to be run by the transaction. This field is
     * set by the {@link Schedule}and cleared by the {@link Tx}as each
     * action is successfully executed. The field is NOT cleared if an
     * action is blocked or if an exception is thrown.
     */

    volatile Action action = null;

    /**
     * The schedule responsible for assigning actions to this transaction.
     */

    final Schedule schedule;

    /**
     * Counter of the #of actions executed.
     */
    private int nactions = 0;

    /**
     * Create a new transaction.
     * 
     * @param schedule
     *            The schedule which will run this transaction.
     * 
     * @param name
     *            The name of the transaction.
     */

    Tx(Schedule schedule, String name) {
        super(name);
        if (schedule == null) {
            throw new IllegalArgumentException();
        }
        this.schedule = schedule;
        run = schedule.lock.newCondition();
        setDaemon(true); // can exit while this is running.
        start(); // start transaction -- it will wait on the Schedule.
    }

    public String toString() {
        return getName();
    }

    /**
     * Run the transaction.  Actions are executed as they set set on
     * the transaction by the schedule.
     * 
     * @see Schedule#run()
     */

//    synchronized 
    public void run() {
        while (true) {
            waitOnSchedule();
            if (action != null) {
                schedule.lock.lock();
                runAction();
            }
        } // while(true)
    } // run()

    /**
     * If {@link #action}!= null, then executes the pre-conditions, the
     * action, and the post-conditions defined for that action and clears
     * the {@link #action}so that another action may be tasked to this
     * transaction.
     * 
     * @exception IllegalStateException
     *                If the transaction is complete [done == true].
     * 
     * @exception IllegalStateException
     *                If the transaction has thrown an exception [exception !=
     *                null].
     */

//    synchronized 
    private void runAction() {
        final long begin = System.currentTimeMillis();
        try {
            // Make sure that the transaction is still valid.
            if (done || exception != null) {
                // Transaction may not execute more actions.
                throw new IllegalStateException("done/error: tx=" + this
                        + ", done=" + done + ", ex=" + exception);
            }
            // pre-conditions.
            log.debug("preConditions: tx=" + this
                    + ", action=" + action);
            action.runPreConditions();
            // action.
            log.info("action: tx=" + this + ", action="
                    + action);
            action.run();
            // post-condition.
            log.debug("postConditions: tx=" + this
                    + ", action=" + action);
            action.runPostConditions();
            // success: Clear [action].
            log.debug("success: tx=" + this + ", action="
                    + action);
        } catch (Throwable t) {
            exception = t;
            log.error("tx=" + this, t);
        } finally {
            action = null;
            nactions++;
            final long elapsed = System.currentTimeMillis() - begin;
            log.info("action: elapsed=" + elapsed);
        }
    }

    /**
     * Hand off execution to the schedule.
     */

//    synchronized 
    private void waitOnSchedule() {
        if (action != null) {
            /*
             * Do NOT wait if an action is assigned. This can happen on the
             * first action if {@link Schedule#run()} begins executing
             * actions before {@link Tx#run()} starts to execute. In this
             * case the Schedule will hold the monitor on the Tx and set the
             * action before the Tx enters this method for the first time.
             * After that this SHOULD NOT happen if things are synchronizing
             * properly.
             */
            if (nactions > 0) {
                log.warn("Action already assigned: tx="
                        + this + ", action=" + action);
            }
            return;
        }
        // Wait.
        log.info("waiting: tx=" + this
                + (done ? ", done" : "")
                + (exception == null ? "" : ", ex=" + exception.getMessage()));
        final long begin = System.currentTimeMillis();
        try {
//            notifyAll(); // notify - will notify schedule waiting on tx.
//            //              schedule.thread.interrupt();
//            wait(); // hand off execution to the schedule.
            schedule.lock.lock();
            try {
                schedule.run.signal();
                run.await();
            } finally {
                schedule.lock.unlock();
            }
            long elapsed = System.currentTimeMillis() - begin;
            log.info("resume: tx=" + this + ", elapsed="
                    + elapsed);
        } catch (InterruptedException ex) {
            long elapsed = System.currentTimeMillis() - begin;
            log.info("interrupted: tx=" + this
                    + ", elapsed=" + elapsed);
        }
    }

//    /**
//     * Lock used to coordinate running {@link Tx}s.
//     */
//    Lock lock = new ReentrantLock();
    
    /**
     * The {@link Schedule} will notify the {@link Tx} using this condition when
     * it should run.
     */
    final java.util.concurrent.locks.Condition run;

}