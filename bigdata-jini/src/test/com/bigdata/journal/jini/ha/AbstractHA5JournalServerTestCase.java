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
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.quorum.AsynchronousQuorumCloseException;

/**
 * Test suite for HA5.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class AbstractHA5JournalServerTestCase extends
		AbstractHA3JournalServerTestCase {

    /**
     * The {@link Remote} interfaces for these services (if started and
     * successfully discovered).
     */
    protected HAGlue serverD = null;

	protected HAGlue serverE = null;

    /**
     * {@link UUID}s for the {@link HAJournalServer}s.
     */
    private UUID serverDId = UUID.randomUUID();
    private UUID serverEId = UUID.randomUUID();

    /**
     * The HTTP ports at which the services will respond.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/730" > Allow configuration
     *      of embedded NSS jetty server using jetty-web.xml </a>
     */
	protected final int D_JETTY_PORT = C_JETTY_PORT + 1;
	protected final int E_JETTY_PORT = D_JETTY_PORT + 1;

    /**
     * These {@link IServiceListener}s are used to reliably detect that the
     * corresponding process starts and (most importantly) that it is really
     * dies once it has been shutdown or destroyed.
     */
    protected ServiceListener serviceListenerD = null, serviceListenerE = null;

    protected File getServiceDirD() {
        return new File(getTestDir(), "D");
    }
    
    protected File getServiceDirE() {
        return new File(getTestDir(), "E");
    }
    
    protected File getHAJournalFileD() {
        return new File(getServiceDirD(), "bigdata-ha.jnl");
    }

    protected File getHAJournalFileE() {
        return new File(getServiceDirE(), "bigdata-ha.jnl");
    }

    protected File getHALogDirD() {
        return new File(getServiceDirD(), "HALog");
    }

    protected File getHALogDirE() {
        return new File(getServiceDirE(), "HALog");
    }

    @Override
    protected int replicationFactor() {

        return 5;
        
    }

    /**
     * Start A then B then C. As each service starts, this method waits for that
     * service to appear in the pipeline in the proper position.
     * 
     * @return The ordered array of services <code>[A, B, C]</code>
     */
    protected HAGlue[] startSequenceABCDE() throws Exception {

        startA();
        awaitPipeline(new HAGlue[] { serverA });
        
        startB();
        awaitPipeline(new HAGlue[] { serverA, serverB });
        
        startC();
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC });

        startD();
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC, serverD });

        startE();
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        return new HAGlue[] { serverA, serverB, serverC, serverD, serverE };
        
    }
    
    /**
     * Helper class for simultaneous/seqeunced start of 3 HA services.
     */
    protected class ABCDE {
        
        /**
         * The services.
         */
        final HAGlue serverA, serverB, serverC, serverD, serverE;

        /**
         * Start of 3 HA services (this happens in the ctor).
         * 
         * @param sequential
         *           True if the startup should be sequential or false
         *           if services should start concurrently.
         * @throws Exception
         */
        public ABCDE(final boolean sequential)
                throws Exception {

            this(true/* sequential */, true/* newServiceStarts */);

        }

        /**
         * Start of 5 HA services (this happens in the ctor).
         * 
         * @param sequential
         *            True if the startup should be sequential or false if
         *            services should start concurrently.
         * @param newServiceStarts
         *            When <code>true</code> the services are new, the database
         *            should be at <code>commitCounter:=0</code> and the
         *            constructor will check for the implicit create of the
         *            default KB.
         * @throws Exception
         */
        public ABCDE(final boolean sequential, final boolean newServiceStarts)
                throws Exception {

            if (sequential) {
            
                final HAGlue[] services = startSequenceABCDE();

                serverA = services[0];

                serverB = services[1];

                serverC = services[2];

                serverD = services[3];

                serverE = services[4];

            } else {
                
                final List<Callable<HAGlue>> tasks = new LinkedList<Callable<HAGlue>>();

                tasks.add(new StartATask(false/* restart */));
                tasks.add(new StartBTask(false/* restart */));
                tasks.add(new StartCTask(false/* restart */));
                tasks.add(new StartDTask(false/* restart */));
                tasks.add(new StartETask(false/* restart */));

                // Start all servers in parallel. Wait up to a timeout.
                final List<Future<HAGlue>> futures = executorService.invokeAll(
                        tasks, 30/* timeout */, TimeUnit.SECONDS);

                serverA = futures.get(0).get();

                serverB = futures.get(1).get();

                serverC = futures.get(2).get();

                serverD = futures.get(3).get();

                serverE = futures.get(4).get();

            }

            // wait for the quorum to fully meet.
            awaitFullyMetQuorum();

            if(newServiceStarts) {
                // wait for the initial commit point (KB create).
                awaitCommitCounter(1L, serverA, serverB, serverC, serverD, serverE);
            }
            
        }

        public void shutdownAll() throws InterruptedException,
                ExecutionException {

            shutdownAll(false/* now */);
            
        }

        public void shutdownAll(final boolean now) throws InterruptedException,
                ExecutionException {
            
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

            tasks.add(new SafeShutdownATask());
            tasks.add(new SafeShutdownBTask());
            tasks.add(new SafeShutdownCTask());
            tasks.add(new SafeShutdownDTask());
            tasks.add(new SafeShutdownETask());

            // Start all servers in parallel. Wait up to a timeout.
            final List<Future<Void>> futures = executorService.invokeAll(
                    tasks, 30/* timeout */, TimeUnit.SECONDS);

            futures.get(0).get();
            futures.get(1).get();
            futures.get(2).get();
            futures.get(3).get();
            futures.get(4).get();

        }

		public void assertDigestsEqual() throws NoSuchAlgorithmException, DigestException, IOException {
	        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });
		}

    }

    protected HAGlue startD() throws Exception {

        return new StartDTask(false/* restart */).call();

    }

    protected HAGlue startE() throws Exception {

        return new StartETask(false/* restart */).call();

    }

    protected HAGlue restartD() throws Exception {

        return new StartDTask(true/* restart */).call();

    }

    protected HAGlue restartE() throws Exception {

        return new StartETask(true/* restart */).call();

    }

    protected void shutdownD() throws IOException {
        safeShutdown(serverD, getServiceDirD(), serviceListenerD, true);

        serverD = null;
        serviceListenerD = null;
    }

    protected void shutdownE() throws IOException {
        safeShutdown(serverE, getServiceDirE(), serviceListenerE, true);

        serverE = null;
        serviceListenerE = null;
    }

    protected class StartDTask extends StartServerTask {

        public StartDTask(final boolean restart) {

            super("D", "HAJournal-D.config", serverDId, D_JETTY_PORT,
                    serviceListenerD = new ServiceListener(), restart);

        }

        @Override
        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverD, getServiceDirD(), serviceListenerD);
                
                serverD = null;
                
            }
            
            return serverD = start();

        }

    }

    protected class StartETask extends StartServerTask {

        public StartETask(final boolean restart) {

            super("E", "HAJournal-E.config", serverEId, E_JETTY_PORT,
                    serviceListenerE = new ServiceListener(), restart);

        }

        @Override
        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverE, getServiceDirE(), serviceListenerE);
                
                serverE = null;
                
            }
            
            return serverE = start();

        }

    }
    
    protected class SafeShutdownDTask extends SafeShutdownTask {

        public SafeShutdownDTask() {
            this(false/* now */);
        }

        public SafeShutdownDTask(final boolean now) {
            super(serverD, getServiceDirC(), serviceListenerD, now);
        }

    }

    protected class SafeShutdownETask extends SafeShutdownTask {

        public SafeShutdownETask() {
            this(false/* now */);
        }

        public SafeShutdownETask(final boolean now) {
            super(serverE, getServiceDirC(), serviceListenerE, now);
        }

    }
    public AbstractHA5JournalServerTestCase() {
    }

    public AbstractHA5JournalServerTestCase(final String name) {
        super(name);
    }
    
    @Override
	protected void destroyAll() throws AsynchronousQuorumCloseException,
			InterruptedException, TimeoutException {
		/**
		 * The most reliable tear down is in reverse pipeline order.
		 * 
		 * This may not be necessary long term but for now we want to avoid
		 * destroying the leader first since it can lead to problems as
		 * followers attempt to reform
		 */
		final HAGlue leader;
		final File leaderServiceDir;
		final ServiceListener leaderListener;
		if (quorum.isQuorumMet()) {
			final long token = quorum.awaitQuorum(awaitQuorumTimeout,
					TimeUnit.MILLISECONDS);
			/*
			 * Note: It is possible to resolve a proxy for a service that has
			 * been recently shutdown or destroyed. This is effectively a data
			 * race.
			 */
			final HAGlue t = quorum.getClient().getLeader(token);
			if (t.equals(serverA)) {
				leader = t;
				leaderServiceDir = getServiceDirA();
				leaderListener = serviceListenerA;
			} else if (t.equals(serverB)) {
				leader = t;
				leaderServiceDir = getServiceDirB();
				leaderListener = serviceListenerB;
			} else if (t.equals(serverC)) {
				leader = t;
				leaderServiceDir = getServiceDirC();
				leaderListener = serviceListenerC;
			} else if (t.equals(serverD)) {
				leader = t;
				leaderServiceDir = getServiceDirD();
				leaderListener = serviceListenerD;
			} else if (t.equals(serverE)) {
				leader = t;
				leaderServiceDir = getServiceDirE();
				leaderListener = serviceListenerE;
			} else {
				if (serverA == null && serverB == null && serverC == null && serverD == null && serverE == null) {
					/*
					 * There are no services running and nothing to shutdown. We
					 * probably resolved a stale proxy to the leader above.
					 */
					return;
				}
				throw new IllegalStateException(
						"Leader is none of A, B, or C: leader=" + t + ", A="
								+ serverA + ", B=" + serverB + ", C=" + serverC);
			}
		} else {
			leader = null;
			leaderServiceDir = null;
			leaderListener = null;
		}

		if (leader == null || !leader.equals(serverA)) {
			destroyA();
		}

		if (leader == null || !leader.equals(serverB)) {
			destroyB();
		}

		if (leader == null || !leader.equals(serverC)) {
			destroyC();
		}

		if (leader == null || !leader.equals(serverD)) {
			destroyD();
		}

		if (leader == null || !leader.equals(serverE)) {
			destroyE();
		}

		// Destroy leader last
		if (leader != null) {
			safeDestroy(leader, leaderServiceDir, leaderListener);

			serverA = serverB = serverC = serverD = serverE = null;
			serviceListenerA = serviceListenerC = serviceListenerB = serviceListenerD = serviceListenerE = null;
		}

	}

    protected void destroyD() {
        safeDestroy(serverD, getServiceDirD(), serviceListenerD);
        serverD = null;
        serviceListenerD = null;
    }

    protected void destroyE() {
        safeDestroy(serverE, getServiceDirE(), serviceListenerE);
        serverE = null;
        serviceListenerE = null;
    }

}
