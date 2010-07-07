/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum.MockQuorumActor;

/**
 * Abstract base class for testing using a {@link MockQuorumFixture}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorumTestCase extends TestCase2 {

    public AbstractQuorumTestCase() {
        
    }

    public AbstractQuorumTestCase(String name) {
        super(name);
    }

    /** The service replication factor (this must be set in {@link #setupUp()}). */
    protected int k;

    /** The per-client quorum objects. */
    protected MockQuorum[] quorums;

    /** The clients. */
    protected MockQuorumMember[] clients;

    /**
     * The per-client quorum actor objects. The unit tests send events on the
     * behalf of the clients using these actor objects.
     */
    protected MockQuorumActor[] actors;

    /** The mock shared quorum state object. */
    protected MockQuorumFixture fixture;
    
    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        if(log.isInfoEnabled())
            log.info(getName());

        if (k == 0)
            throw new AssertionError("k is not set");
        
        quorums = new MockQuorum[k];
        
        clients = new MockQuorumMember[k];

        actors  = new MockQuorumActor[k];

        fixture = new MockQuorumFixture();

        fixture.start(); 

        /*
         * Setup the client quorums.
         */
        final String logicalServiceId = "testLogicalService1";
        for (int i = 0; i < k; i++) {
            quorums[i] = new MockQuorum(k,fixture);
            clients[i] = new MockQuorumMember(logicalServiceId,fixture);
            quorums[i].start(clients[i]);
            actors [i] = quorums[i].getActor();
        }
    
    }

    protected void tearDown() throws Exception {
        if(log.isInfoEnabled())
            log.info(getName());
        if (quorums != null) {
            for (int i = 0; i < k; i++) {
                if (quorums[i] != null) {
                    quorums[i].terminate();
                    quorums[i] = null;
                }
            }
        }
        if (fixture != null) {
            fixture.terminate();
        }
        quorums = null;
        clients = null;
        actors  = null;
        fixture = null;
    }

    /**
     * Wait up to a timeout until some condition succeeds.
     * <p>
     * Whenever more than one {@link AbstractQuorum} is under test there will be
     * concurrent indeterminism concerning the precise ordering and timing as
     * updates propagate from the {@link AbstractQuorum} which takes some action
     * (castVote(), pipelineAdd(), etc.) to the other quorums attached to the
     * same {@link MockQuorumFixture}. This uncertainty about the ordering and
     * timing state changes is not dissimilar from the uncertainty we face in a
     * real distributed system.
     * <p>
     * While there are times when this uncertainty does not affect the behavior
     * of the tests, there are other times when we must have a guarantee that a
     * specific vote order or pipeline order was established. For those cases,
     * this method may be used to await an arbitrary condition. This method
     * simply retries until the condition becomes true, sleeping a little after
     * each failure.
     * <p>
     * Actions executed in the main thread of the unit test will directly update
     * the internal state of the {@link MockQuorumFixture}, which is shared
     * across the {@link MockQuorum}s. However, uncertainty about ordering can
     * arise as a result of the interleaving of the actions taken by the
     * {@link QuorumWatcher}s in response to both top-level actions and actions
     * taken by other {@link QuorumWatcher}s. For example, the vote order or the
     * pipeline order are fully determined based on sequence such as the
     * following:
     * 
     * <pre>
     * actor0.pipelineAdd();
     * actor2.pipelineAdd();
     * actor1.pipelineAdd();
     * </pre>
     * 
     * When in doubt, or when a unit test displays stochastic behavior, you can
     * use this method to wait until the quorum state has been correctly
     * replicated to the {@link Quorum}s under test.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * @param timeout
     *            The timeout.
     * @param unit
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     */
    static public void assertCondition(final Runnable cond,
            final long timeout, final TimeUnit units) {
        final long begin = System.nanoTime();
        long nanos = units.toNanos(timeout);
        // remaining -= (now - begin) [aka elapsed]
        nanos -= System.nanoTime() - begin;
        while (true) {
            AssertionFailedError cause = null;
            try {
                // try the condition
                cond.run();
                // success.
                return;
            } catch (AssertionFailedError e) {
                nanos -= System.nanoTime() - begin;
                if (nanos < 0) {
                    // Timeout - rethrow the failed assertion.
                    throw e;
                }
                cause = e;
            }
            // Sleep up to 10ms or the remaining nanos, which ever is less.
            final int millis = (int) Math.min(TimeUnit.NANOSECONDS
                    .toMillis(nanos), 10);
            if (log.isInfoEnabled())
                log.info("Will retry: millis=" + millis + ", cause=" + cause);
            // sleep and retry.
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e1) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Waits up to 5 seconds for the condition to succeed.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     * 
     * @see #assertCondition(Runnable, long, TimeUnit)
     */
    static public void assertCondition(final Runnable cond) {
        
        assertCondition(cond, 5, TimeUnit.SECONDS);
        
    }
    
}
