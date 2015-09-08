/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.webapp.WebAppContext;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.counters.PIDUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.IHAPipelineResetRequest;
import com.bigdata.ha.IHAPipelineResetResponse;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.RunState;
import com.bigdata.ha.msg.IHA2PhaseAbortMessage;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHAGatherReleaseTimeRequest;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.ha.msg.IHAReadRequest;
import com.bigdata.ha.msg.IHAReadResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.ha.msg.IHARootBlockResponse;
import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASnapshotDigestRequest;
import com.bigdata.ha.msg.IHASnapshotDigestResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateRequest;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITx;
import com.bigdata.journal.StoreState;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService.IHAProgressListener;
import com.bigdata.journal.jini.ha.HAJournalServer.RunStateEnum;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.jini.RemoteDestroyAdmin;

/**
 * Class extends {@link HAJournal} and allows the unit tests to play various
 * games with the services, simulating a variety of different kinds of problems.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAJournalTest extends HAJournal {

    private static final Logger log = Logger.getLogger(HAJournal.class);

    public HAJournalTest(final HAJournalServer server,
            final Configuration config,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
            throws ConfigurationException, IOException {

        super(server, config, quorum);

    }

    @Override
    protected HAGlue newHAGlue(final UUID serviceId) {

        return new HAGlueTestImpl(serviceId);

    }

    /**
     * The service implementation object that gets exported (not the proxy, but
     * the thing that gets exported as the {@link HAGlueTest} proxy).
     */
    public HAGlueTestImpl getRemoteImpl() {
        
        return (HAGlueTestImpl) getHAJournalServer().getRemoteImpl();
        
    }
    
    /**
     * Utility accessible for HAGlueTest methods and public static for
     * test purposes.
     * 
     * TODO This should use a one-up counter within the service and write
     * the thread dump into a file named by that counter. It shoudl include
     * the datetime stamp in the file contents.  It should log @ ERROR that
     * a thread dump was written and include the name of the file.
     */
	static public void dumpThreads() {

		final StringBuilder sb = new StringBuilder();
		
		final Map<Thread, StackTraceElement[]> dump = Thread
				.getAllStackTraces();
		
		for (Map.Entry<Thread, StackTraceElement[]> threadEntry : dump
				.entrySet()) {
			final Thread thread = threadEntry.getKey();
			sb.append("THREAD#" + thread.getId() + ", " + thread.getName() + "\n");
			for (StackTraceElement elem : threadEntry.getValue()) {
				sb.append("\t" + elem.toString() + "\n");
			}
			
		}
		
		log.warn("THREAD DUMP\n" + sb.toString());
	}

    /**
     * A {@link Remote} interface for new methods published by the service.
     */
    public static interface HAGlueTest extends HAGlue {

        /**
         * Logs a "hello world" message.
         */
        public void helloWorld() throws IOException;

        /**
         * Logs a message @ WARN on the HAGlue service.
         * 
         * @param msg
         *            The message.
         */
        public void log(String msg) throws IOException;

        /**
         * Have the child self-report its <code>pid</code>.
         * 
         * @return The child's PID.
         * 
         * @see PIDUtil
         */
        public int getPID() throws IOException;
        
        /**
         * Force the end point to enter into an error state from which it will
         * naturally move back into a consistent state.
         * <p>
         * Note: This method is intended primarily as an aid in writing various
         * HA unit tests.
         */
        public Future<Void> enterErrorState() throws IOException;

        /**
         * This method may be issued to force the service to close and then
         * reopen its zookeeper connection. This is a drastic action which will
         * cause all <i>ephemeral</i> tokens for that service to be retracted
         * from zookeeper. When the service reconnects, it will reestablish
         * those connections.
         * <p>
         * Note: This method is intended primarily as an aid in writing various
         * HA unit tests.
         * 
         * @see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
         */
        public Future<Void> bounceZookeeperConnection() throws IOException;

//        /**
//         * This method may be issued to force the service to close
//         * its zookeeper connection. This is a drastic action which will
//         * cause all <i>ephemeral</i> tokens for that service to be retracted
//         * from zookeeper.
//         * <p>
//         * Note: This method is intended primarily as an aid in writing various
//         * HA unit tests.
//         * 
//         * @see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
//         */
//        public Future<Void> dropZookeeperConnection() throws IOException;

//        /**
//         * This method may be issued to force the service to reopen
//         * its zookeeper connection. When the service reconnects, it will reestablish
//         * services retracted on a previouse dropZookeeperConnection.
//         * <p>
//         * Note: This method is intended primarily as an aid in writing various
//         * HA unit tests.
//         * 
//         * @see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
//         */
//        public Future<Void> reopenZookeeperConnection() throws IOException;

        /**
         * Set a fail point on a future invocation of the specified method. The
         * method must be declared by the {@link HAGlue} interface or by one of
         * the interfaces which it extends. The method will throw a
         * {@link SpuriousTestException} when the failure criteria are
         * satisifiesuper.
         * 
         * @param name
         *            The name method to fail.
         * @param parameterTypes
         *            The parameter types for that method.
         * @param nwait
         *            The #of invocations to wait before failing the method.
         * @param nfail
         *            The #of times to fail the method.
         */
        public void failNext(final String name,
                final Class<?>[] parameterTypes, final int nwait,
                final int nfail) throws IOException;

        /**
         * Clear any existing fail request for the specified method.
         * 
         * @param name
         *            The name method to fail.
         * @param parameterTypes
         *            The parameter types for that method.
         */
        public void clearFail(final String name, final Class<?>[] parameterTypes)
                throws IOException;

        /**
         * Instruct the service to vote "NO" on the next 2-phase PREPARE
         * request.
         */
        public void voteNo() throws IOException;

        /**
         * @see IHA2PhaseCommitMessage#failCommit_beforeWritingRootBlockOnJournal()
         */
        public void failCommit_beforeWritingRootBlockOnJournal() throws IOException;

        /**
         * @see IHA2PhaseCommitMessage#failCommit_beforeClosingHALog()
         */
        public void failCommit_beforeClosingHALog() throws IOException;

        /**
         * Message may be used to force a write replication failure when the
         * specified number of bytes have been received for the live or
         * historical writes for the given commit point. Only one such trigger
         * is honored at a time. This method sets the trigger. The trigger is
         * not removed automatically but may take responsiblity for removing
         * itself once it runs.
         * 
         * @see HAJournalServer.HAQuorumService#progressListenerRef
         */
        public void failWriteReplication(IHAProgressListener listener)
                throws IOException;

        /**
         * Set the next value to be reported by {@link BasicHA#nextTimestamp()}.
         * <p>
         * Note: Only a few specific methods call against
         * {@link BasicHA#nextTimestamp()}. They do so precisely because they
         * were written to allow us to override the clock in the test suite
         * using this method.
         * 
         * @param nextTimestamp
         *            when <code>-1L</code> the behavior will revert to the
         *            default. Any other value will be the next value reported
         *            by {@link BasicHA#nextTimestamp()}, after which the
         *            behavior will revert to the default.
         * 
         *            TODO Add a "clearNextTimestamp()" method.
         */
        public void setNextTimestamp(long nextTimestamp) throws IOException;
        
        /**
         * Enable remote lpgging of JVM thread state.
         */
        public void dumpThreads() throws IOException;

        /**
         * Report the internal run state of the {@link HAJournalServer}.
         */
        public RunStateEnum getRunStateEnum() throws IOException;
        
        /**
         * Run a simple update transaction on the quorum leader, but abort()
         * rather than committing the transaction.
         */
        public void simpleTransaction_abort() throws IOException, Exception;
        
        /**
         * Supports consistency checking between HA services
         */
        public StoreState getStoreState() throws IOException;
     
        /**
         * Gets and clears a root cause that was set on the remote service. This
         * is used to inspect the root cause when an RMI method is interrupted
         * in the local JVM. Since the RMI was interrupted, we can not observe
         * the outcome or root cause of the associated failure on the remote
         * service. However, test glue can explicitly set that root cause such
         * that it can then be reported using this method.
         */
        public Throwable getAndClearLastRootCause() throws IOException;

        /**
         * Variant that does not clear out the last root cause.
         */
        public Throwable getLastRootCause() throws IOException;

        /**
         * Set the {@link IHALoadBalancerPolicy} for each active instance of the
         * {@link HALoadBalancerServlet}.
         * 
         * @param policy
         *            The policy.
         *            
         * @throws IOException
         */
        public void setHALoadBalancerPolicy(final IHALoadBalancerPolicy policy)
                throws IOException;
        
    }

    /**
     * Identifies a method to be failed and tracks the #of invocations of that
     * method.
     */
    private static class MethodData {
        @SuppressWarnings("unused")
        public final Method m;
        public final int nwait;
        public final int nfail;
        final AtomicInteger ncall = new AtomicInteger();

        public MethodData(final Method m, final int nwait, final int nfail) {
            if (m == null)
                throw new IllegalArgumentException("method not specified");
            if (nwait < 0)
                throw new IllegalArgumentException("nwait must be GTE ZERO");
            if (nfail < 1)
                throw new IllegalArgumentException("nfail must be GTE ONE");
            this.m = m;
            this.nwait = nwait;
            this.nfail = nfail;
        }
    };

    /**
     * Lookup a method on the {@link HAGlue} interface.
     * <p>
     * Note: Any method lookup failures are logged @ ERROR. All of the
     * {@link HAGlue} methods on {@link HAGlueTestImpl} are annotated with this
     * method. If any of them can not resolve itself, then the method will fail,
     * which is why we log all such method resolution failures here.
     * 
     * @param name
     *            The name of the method.
     * @param parameterTypes
     *            The parameter types of the method.
     * 
     * @return The {@link Method} and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if anything goes wrong.
     */
    static protected Method getMethod(final String name,
            final Class<?>[] parameterTypes) {

        try {

            if (name == null)
                throw new IllegalArgumentException();

            if (parameterTypes == null)
                throw new IllegalArgumentException();

            final Method m = HAGlue.class.getMethod(name, parameterTypes);

            return m;

        } catch (RuntimeException e) {

            log.error(e, e);

            throw e;

        } catch (NoSuchMethodException e) {

            log.error(e, e);

            throw new RuntimeException(e);

            // } catch (SecurityException e) {
            //
            // log.error(e, e);
            //
            // throw new RuntimeException(e);

        }

    }

    /**
     * Exception thrown under test harness control.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static public class SpuriousTestException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public SpuriousTestException(final String msg) {

            super(msg);

        }

    }

    /**
     * Class implements the public RMI interface of the {@link HAJournal} using
     * a delegation pattern.
     * <p>
     * Note: Any new RMI methods must be (a) declared on an interface; and (b)
     * must throw {@link IOException}.
     * 
     * @see HAJournal.HAGlueService
     */
    class HAGlueTestImpl extends HAJournal.HAGlueService
            implements HAGlue, HAGlueTest, RemoteDestroyAdmin {

        /**
         * Collection of configured RMI method failures setup under test
         * control.
         * 
         * TODO If we send the desired outcome then we could have it return that
         * outcome or throw that exception. This could also let us setup a
         * failure on the reporting of the value (the method executes, but the
         * result is replaced by an exception). We might also try a stochastic
         * interrupt of the delegate method run as a Callable or Runnable.
         */
        private final Map<Method, MethodData> failSet = new ConcurrentHashMap<Method, MethodData>();

        /**
         * Flag used to force the service to vote "NO" on the next two-phase
         * commit.
         * 
         * @see IHA2PhasePrepareMessage#voteNo()
         */
        private final AtomicBoolean voteNo = new AtomicBoolean(false);

        /**
         * Flag used to force the service to fail rather than laying down the
         * new root block in the COMMIT message.
         * 
         * @see IHA2PhaseCommitMessage#failCommit_beforeWritingRootBlockOnJournal()
         */
        private final AtomicBoolean failCommit_beforeWritingRootBlockOnJournal = new AtomicBoolean(
                false);

        /**
         * Flag used to force the service to fail rather than laying down the
         * new root block in the COMMIT message.
         * 
         * @see IHA2PhaseCommitMessage#failCommit_beforeClosingHALog()
         */
        private final AtomicBoolean failCommit_beforeClosingHALog = new AtomicBoolean(
                false);

        private final AtomicLong nextTimestamp = new AtomicLong(-1L);

        private HAGlueTestImpl(final UUID serviceId) {

            super(serviceId);

        }

        /**
         * 
         * @see AbstractJournal#getQuorum()
         */
        protected Quorum<HAGlue, QuorumService<HAGlue>> getQuorum() {

            return super.getIndexManager().getQuorum();

        }

        @Override
        public void failNext(final String name,
                final Class<?>[] parameterTypes, final int nwait,
                final int nfail) {

            final Method m = getMethod(name, parameterTypes);

            final Class<?> declClass = m.getDeclaringClass();

            if (!declClass.isAssignableFrom(HAGlue.class)) {

                throw new RuntimeException("Method not declared by "
                        + HAGlue.class + " : declClass=" + declClass
                        + ", method=" + m);

            }

            failSet.put(m, new MethodData(m, nwait, nfail));

            /*
             * Verify we can find the method in the map.
             * 
             * Note: This is a cross check on method.hashCode() and
             * method.equals(), not a data race check on the CHM.
             */
            if (failSet.get(m) == null) {

                throw new AssertionError("Could not find method in map: " + m);

            }

        }

        @Override
        public void clearFail(final String name, final Class<?>[] parameterTypes) {

            failSet.remove(getMethod(name, parameterTypes));

        }

        @Override
        public void voteNo() throws IOException {

            voteNo.set(true);
            
        }
        
        @Override
        public void failCommit_beforeWritingRootBlockOnJournal() throws IOException {

            failCommit_beforeWritingRootBlockOnJournal.set(true);
            
        }
        
        @Override
        public void failCommit_beforeClosingHALog() throws IOException {

            failCommit_beforeClosingHALog.set(true);
            
        }
        
        @Override
        public void setNextTimestamp(long nextTimestamp) throws IOException {

            this.nextTimestamp.set(nextTimestamp);
            
        }

        /**
         * Conditionally fail the method if (a) it is registered in the
         * {@link #failSet} and (b) it is due to fail on this invocation.
         * 
         * @param name
         *            The name of the method.
         * @param parameterTypes
         *            The parameter types for the method.
         */
        protected void checkMethod(final String name,
                final Class<?>[] parameterTypes) {

            final Method m = getMethod(name, parameterTypes);

            final MethodData d = failSet.get(m);

            if (d == null)
                return;

            // increment, noting old value.
            final int n = d.ncall.getAndIncrement();

            // Determine whether method will fail on this invocation.
            final boolean willFail = n >= d.nwait && n < (d.nwait + d.nfail);

            // Check conditions for failing the method.
            if (willFail) {

                /*
                 * Fail the method.
                 */

                log.error("Will fail HAGlue method: m=" + m + ", n=" + n
                        + ", nwait=" + d.nwait + ", nfail=" + d.nfail);

                throw new SpuriousTestException(m.toString() + " : n=" + n);

            }

            // Method will run normally.

        }

        // /**
        // * Set a fail point on the next invocation of the specified method.
        // */
        // public void failNext(final Method m) {
        //
        // failNext(m, 0/* nwait */, 1/* nfail */);
        //
        // }

        @Override
        public void helloWorld() throws IOException {

            log.warn("Hello world!");

        }

        @Override
        public void log(final String msg) throws IOException {

            log.warn(msg);

        }

        @Override
        public int getPID() throws IOException {
            
            // Best guess at the child's PID.
            return PIDUtil.getPID();
            
        }
        
        @Override
        public Future<Void> enterErrorState() {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new EnterErrorStateTask(), null/* result */);

            ft.run();

            return super.getProxy(ft);

        }

        private class EnterErrorStateTask implements Runnable {

            @Override
            public void run() {

                @SuppressWarnings("unchecked")
                final HAQuorumService<HAGlue, HAJournal> service = (HAQuorumService<HAGlue, HAJournal>) getQuorum()
                        .getClient();

                if (service != null) {

                    // Note: Local method call on AbstractJournal.
                    final UUID serviceId = getServiceId();

                    haLog.warn("ENTERING ERROR STATE: " + serviceId);

                    service.enterErrorState();

                }

            }

        }

        @Override
        public Future<Void> bounceZookeeperConnection() {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new BounceZookeeperConnectionTask(), null/* result */);

            ft.run();

            return super.getProxy(ft);

        }

        private class BounceZookeeperConnectionTask implements Runnable {

            @SuppressWarnings("rawtypes")
            public void run() {

                // Note: Local method call on AbstractJournal.
                final UUID serviceId = getServiceId();

                try {

                    haLog.warn("BOUNCING ZOOKEEPER CONNECTION: " + serviceId);

                    // Close the current connection (if any).
                    ((ZKQuorumImpl) getQuorum()).getZookeeper().close();

                    // // Obtain a new connection.
                    // ((ZKQuorumImpl) getQuorum()).getZookeeper();
                    //
                    // haLog.warn("RECONNECTED TO ZOOKEEPER: " + serviceId);

                    /*
                     * Note: The HAJournalServer will not notice the zookeeper
                     * session expire unless it attempts to actually use
                     * zookeeper. Further, it will not try to use zookeeper
                     * unless there is a local change in the quorum state (i.e.,
                     * this service does a service leave).
                     * 
                     * We enter into the ErrorTask in order to force the service
                     * to recognize that the zookeeper session is expired.
                     */
                    
                    @SuppressWarnings("unchecked")
                    final HAQuorumService<HAGlue, HAJournal> service = (HAQuorumService<HAGlue, HAJournal>) getQuorum()
                            .getClient();

                    if (service != null) {

                        haLog.warn("ENTERING ERROR STATE: " + serviceId);

                        service.enterErrorState();

                    }

                } catch (InterruptedException e) {

                    // Propagate the interrupt.
                    Thread.currentThread().interrupt();

                }

            }

        }

        /*
         * Delegate methods
         */

        /*
         * IService
         */

        @Override
        public UUID getServiceUUID() throws IOException {

            checkMethod("getServiceUUID", new Class[] {});

            return super.getServiceUUID();

        }

        @Override
        public UUID getServiceId() {

            checkMethod("getServiceId", new Class[] {});

            return super.getServiceId();

        }

        @SuppressWarnings("rawtypes")
        @Override
        public Class getServiceIface() throws IOException {

            checkMethod("getServiceIface", new Class[] {});

            return super.getServiceIface();
        }

        @Override
        public String getHostname() throws IOException {

            checkMethod("getHostname", new Class[] {});

            return super.getHostname();
        }

        @Override
        public String getServiceName() throws IOException {

            checkMethod("getServiceName", new Class[] {});

            return super.getServiceName();

        }

        @Override
        public void destroy() {

            checkMethod("destroy", new Class[] {});

            super.destroy();

        }

        @Override
        public long awaitHAReady(final long timeout, final TimeUnit unit)
                throws InterruptedException, TimeoutException,
                AsynchronousQuorumCloseException {

            checkMethod("awaitHAReady",
                    new Class[] { Long.TYPE, TimeUnit.class });

            return super.awaitHAReady(timeout, unit);

        }

        @Override
        public IHARootBlockResponse getRootBlock(final IHARootBlockRequest msg) {

            checkMethod("getRootBlock",
                    new Class[] { IHARootBlockRequest.class });

            return super.getRootBlock(msg);
        }

        @Override
        public int getNSSPort() {

            checkMethod("getNSSPort", new Class[] {});

            return super.getNSSPort();

        }

        @Override
        public RunState getRunState() {

            checkMethod("getRunState", new Class[] {});

            return super.getRunState();
        }

        @Override
        public String getExtendedRunState() {

            checkMethod("getExtendedRunState", new Class[] {});

            return super.getExtendedRunState();

        }

        @Override
        public HAStatusEnum getHAStatus() {

            checkMethod("getHAStatus", new Class[] {});

            return super.getHAStatus();

        }

        @Override
        public IHADigestResponse computeDigest(final IHADigestRequest req)
                throws IOException, NoSuchAlgorithmException, DigestException {

            checkMethod("computeDigest", new Class[] { IHADigestRequest.class });

            return super.computeDigest(req);

        }

        @Override
        public IHALogDigestResponse computeHALogDigest(
                final IHALogDigestRequest req) throws IOException,
                NoSuchAlgorithmException, DigestException {

            checkMethod("computeHALogDigest",
                    new Class[] { IHALogDigestRequest.class });

            return super.computeHALogDigest(req);

        }

        @Override
        public IHASnapshotDigestResponse computeHASnapshotDigest(
                final IHASnapshotDigestRequest req) throws IOException,
                NoSuchAlgorithmException, DigestException {

            checkMethod("computeHASnapshotDigest",
                    new Class[] { IHASnapshotDigestRequest.class });

            return super.computeHASnapshotDigest(req);

        }

        @Override
        public Future<IHASnapshotResponse> takeSnapshot(
                final IHASnapshotRequest req) throws IOException {

            checkMethod("takeSnapshot",
                    new Class[] { IHASnapshotRequest.class });

            return super.takeSnapshot(req);

        }

        @Override
        public Future<Void> rebuildFromLeader(final IHARemoteRebuildRequest req)
                throws IOException {

            checkMethod("restoreFromLeader",
                    new Class[] { IHARemoteRebuildRequest.class });

            return super.rebuildFromLeader(req);

        }

//        @Override
//        public Future<Void> globalWriteLock(IHAGlobalWriteLockRequest req)
//                throws IOException, TimeoutException, InterruptedException {
//
//            checkMethod("globalWriteLock",
//                    new Class[] { IHAGlobalWriteLockRequest.class });
//
//            return super.globalWriteLock(req);
//
//        }

        /*
         * HATXSGlue
         */

        @Override
        public void gatherMinimumVisibleCommitTime(
                final IHAGatherReleaseTimeRequest req) throws IOException {

            checkMethod("gatherMinimumVisibleCommitTime",
                    new Class[] { IHAGatherReleaseTimeRequest.class });

            super.gatherMinimumVisibleCommitTime(req);

        }

        @Override
        public IHANotifyReleaseTimeResponse notifyEarliestCommitTime(
                final IHANotifyReleaseTimeRequest req) throws IOException,
                InterruptedException, BrokenBarrierException {

            checkMethod("notifyEarliestCommitTime",
                    new Class[] { IHANotifyReleaseTimeRequest.class });

            return super.notifyEarliestCommitTime(req);

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Note: This is NOT an RMI method, but we want to be able to override
         * it anyway to test the releaseTime consensus protocol.
         */
        @Override
        public long nextTimestamp() {

            final long t = nextTimestamp.get();

            if (t == -1L) {

                return super.nextTimestamp();

            }

            return t;

        }

        /*
         * HACommitGlue
         */

        @Override
        public Future<Boolean> prepare2Phase(
                final IHA2PhasePrepareMessage prepareMessage) {

            checkMethod("prepare2Phase",
                    new Class[] { IHA2PhasePrepareMessage.class });

            if (voteNo.compareAndSet(true/* expect */, false/* update */)) {

                return super
                        .prepare2Phase(new MyPrepareMessage(prepareMessage) {
                            
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean voteNo() {
                                return true;
                            }
                        });

            } else {
                
                return super.prepare2Phase(prepareMessage);
                
            }

        }

        @Override
        public Future<Void> commit2Phase(final IHA2PhaseCommitMessage commitMessage) {

            checkMethod("commit2Phase",
                    new Class[] { IHA2PhaseCommitMessage.class });

            if (failCommit_beforeWritingRootBlockOnJournal.compareAndSet(
                    true/* expect */, false/* update */)) {

                return super.commit2Phase(new MyCommitMessage(commitMessage) {
                
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean failCommit_beforeWritingRootBlockOnJournal() {
                        return true;
                    }
                });
            } else if (failCommit_beforeClosingHALog.compareAndSet(
                    true/* expect */, false/* update */)) {

                return super.commit2Phase(new MyCommitMessage(commitMessage) {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean failCommit_beforeClosingHALog() {
                        return true;
                    }
                });

            } else {
                
                return super.commit2Phase(commitMessage);
                
            }

        }

        @Override
        public Future<Void> abort2Phase(final IHA2PhaseAbortMessage abortMessage) {

            checkMethod("abort2Phase",
                    new Class[] { IHA2PhaseAbortMessage.class });

            return super.abort2Phase(abortMessage);

        }

        /*
         * HAReadGlue
         */

        @Override
        public Future<IHAReadResponse> readFromDisk(
                final IHAReadRequest readMessage) {

            checkMethod("readFromDisk", new Class[] { IHAReadResponse.class });

            return super.readFromDisk(readMessage);

        }

        /*
         * HAPipelineGlue
         */

        @Override
        public InetSocketAddress getWritePipelineAddr() {

            checkMethod("getWritePipelineAddr", new Class[] {});

            return super.getWritePipelineAddr();
        }

        @Override
        public Future<Void> moveToEndOfPipeline() {

            checkMethod("moveToEndOfPipeline", new Class[] {});

            return super.moveToEndOfPipeline();
        }

        @Override
        public Future<IHAPipelineResetResponse> resetPipeline(
                final IHAPipelineResetRequest req) throws IOException {

            checkMethod("resetPipeline", new Class[] {IHAPipelineResetRequest.class});

            return super.resetPipeline(req);
        }

        @Override
        public Future<Void> receiveAndReplicate(final IHASyncRequest req,
                final IHASendState snd, final IHAWriteMessage msg)
                throws IOException {

            checkMethod("receiveAndReplicate", new Class[] {
                    IHASyncRequest.class, IHASendState.class,
                    IHAWriteMessage.class });

            return super.receiveAndReplicate(req, snd, msg);

        }

        @Override
        public IHAWriteSetStateResponse getHAWriteSetState(
                final IHAWriteSetStateRequest req) {

            checkMethod("getHAWriteSetState",
                    new Class[] { IHAWriteSetStateRequest.class });

            return super.getHAWriteSetState(req);

        }

        @Override
        public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
                final IHALogRootBlocksRequest msg) throws IOException {

            checkMethod("getHALogRootBlocksForWriteSet",
                    new Class[] { IHALogRootBlocksRequest.class });

            return super.getHALogRootBlocksForWriteSet(msg);

        }

        @Override
        public Future<Void> sendHALogForWriteSet(final IHALogRequest msg)
                throws IOException {

            checkMethod("sendHALogForWriteSet",
                    new Class[] { IHALogRequest.class });

            return super.sendHALogForWriteSet(msg);

        }

        @Override
        public Future<IHASendStoreResponse> sendHAStore(final IHARebuildRequest msg)
                throws IOException {

            checkMethod("sendHAStore", new Class[] { IHARebuildRequest.class });

            return super.sendHAStore(msg);

        }

        /*
         * RemoteDestroyAdmin
         * 
         * Note: This interface is not part of the HAGlue interface. Hence you
         * can not fail these methods (checkMethod() will fail).
         */

        @Override
        public void shutdown() {

            // checkMethod("shutdown", new Class[] {});

            super.shutdown();

        }

        @Override
        public void shutdownNow() {

            // checkMethod("shutdownNow", new Class[] {});

            super.shutdown();

        }

		@Override
		public void dumpThreads() {
			HAJournalTest.dumpThreads();
		}

        @Override
        public RunStateEnum getRunStateEnum() {
            
            @SuppressWarnings("unchecked")
            final HAQuorumService<HAGlue, HAJournal> service = (HAQuorumService<HAGlue, HAJournal>) getQuorum()
                    .getClient();
            
            if (service != null) {
            
                return service.getRunStateEnum();
                
            }

            return null;
        
        }

		@Override
        public StoreState getStoreState() throws IOException {
            return ((IHABufferStrategy) (getIndexManager().getBufferStrategy()))
                    .getStoreState();
        }

//		@Override
//		public Future<Void> dropZookeeperConnection() throws IOException {
//
//            final FutureTask<Void> ft = new FutureTaskMon<Void>(
//                    new CloseZookeeperConnectionTask(), null/* result */);
//
//            ft.run();
//
//            return super.getProxy(ft);
//
//        }
//
//        private class CloseZookeeperConnectionTask implements Runnable {
//
//            @SuppressWarnings("rawtypes")
//            public void run() {
//
//                if (getQuorum() instanceof ZKQuorumImpl) {
//
//                    // Note: Local method call on AbstractJournal.
//                    final UUID serviceId = getServiceId();
//
//                    try {
//
//                        haLog.warn("DISCONNECTING FROM ZOOKEEPER: " + serviceId);
//
//                        // Close existing connection.
//                        ((ZKQuorumImpl) getQuorum()).getZookeeper().close();
//
//                    } catch (InterruptedException e) {
//
//                        // Propagate the interrupt.
//                        Thread.currentThread().interrupt();
//
//                    }
//
//                }
//            }
//
//        }

//		@Override
//		public Future<Void> reopenZookeeperConnection() throws IOException {
//
//            final FutureTask<Void> ft = new FutureTaskMon<Void>(
//                    new ReopenZookeeperConnectionTask(), null/* result */);
//
//            ft.run();
//
//            return super.getProxy(ft);
//
//        }
//
//        private class ReopenZookeeperConnectionTask implements Runnable {
//
//            @SuppressWarnings("rawtypes")
//            public void run() {
//
//                if (getQuorum() instanceof ZKQuorumImpl) {
//
//                    // Note: Local method call on AbstractJournal.
//                    final UUID serviceId = getServiceId();
//
//                    try {
//
//                        // Should already be closed, can we check this?
//
//                        // Obtain a new connection.
//                        ((ZKQuorumImpl) getQuorum()).getZookeeper();
//
//                        haLog.warn("RECONNECTED TO ZOOKEEPER: " + serviceId);
//
//                    } catch (InterruptedException e) {
//
//                        // Propagate the interrupt.
//                        Thread.currentThread().interrupt();
//
//                    }
//
//                }
//            }
//
//        }

        // @Override

        @Override
        public void simpleTransaction_abort() throws IOException, Exception {
        
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();
            
            // Note: Throws IllegalStateException if quorum is not running.
            final QuorumService<HAGlue> quorumService = quorum.getClient();

            final long token = quorum.token();

            // This service must be the quorum leader.
            quorumService.assertLeader(token);

            // The default namespace.
            final String namespace = BigdataSail.Options.DEFAULT_NAMESPACE;
            
            // resolve the default namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null) {

                throw new RuntimeException("Not found: namespace=" + namespace);

            }

            // Wrap with SAIL.
            final BigdataSail sail = new BigdataSail(tripleStore);

            final BigdataSailRepository repo = new BigdataSailRepository(sail);

            repo.initialize();

            final BigdataSailRepositoryConnection conn = (BigdataSailRepositoryConnection) repo
                    .getUnisolatedConnection();

            try {

//                conn.setAutoCommit(false);
//
//                final ValueFactory f = sail.getValueFactory();
//
//                conn.add(f.createStatement(
//                        f.createURI("http://www.bigdata.com"), RDF.TYPE,
//                        RDFS.RESOURCE), null/* contexts */);
//
//                conn.flush();
                
                // Fall through.
                
            } finally {

                // Force abort.
                conn.rollback();

            }

        }

        @Override
        public void failWriteReplication(final IHAProgressListener listener)
                throws IOException {

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();
            
            // Note: Throws IllegalStateException if quorum is not running.
            final QuorumService<HAGlue> quorumService = quorum.getClient();
            
            ((HAQuorumService<?, ?>) quorumService).progressListenerRef
                    .set(listener);

        }

        @Override
        public Throwable getAndClearLastRootCause() throws IOException {

            final Throwable t = lastRootCause.getAndSet(null/* newValue */);

            if (t != null)
                log.warn("lastRootCause: " + t, t);

            return t;

        }
        
        @Override
        public Throwable getLastRootCause() throws IOException {

            final Throwable t = lastRootCause.get();

            if (t != null)
                log.warn("lastRootCause: " + t, t);

            return t;

        }
        public void setLastRootCause(final Throwable t) {

            if (log.isInfoEnabled())
                log.info("Setting lastRootCause: " + t);
            
            lastRootCause.set(t);
            
        }
        
        /**
         * @see HAGlueTest#getAndClearLastRootCause()
         */
        private AtomicReference<Throwable> lastRootCause = new AtomicReference<Throwable>();

        /**
         * Change the {@link IHALoadBalancerPolicy}.
         * <p>
         * TODO There are some intrinsic problems with this method that should
         * be resolved before exposing it as an administrative API on the
         * {@link HAGlue} interface.
         * <p>
         * (1) This only applies to running instances of the
         * {@link HALoadBalancerServlet}. If an instance is started after this
         * method is called, it will run with the as-configured
         * {@link IHALoadBalancerPolicy} instance of the one specified in the
         * last invocation of this method.
         * <p>
         * (2) There are various race conditions that exist with respect to: (a)
         * the atomic change over of the {@link IHALoadBalancerPolicy} during an
         * in-flight request; and (b) the atomic destroy of the old policy once
         * there are no more in-flight requests using that old policy.
         * <p>
         * (3) Exposing this method is just begging for trouble with the WAR
         * artifact when deployed under a non-jetty container since it will drag
         * in the jetty ProxyServlet.
         * 
         * TODO Either the {@link IHALoadBalancerPolicy} needs to be
         * serializable or we need to pass along the class name and the
         * configuration parameters. For this case, the configuration should be
         * set from the caller specified values rather than those potentially
         * associated with <code>web.xml</code> , especially since
         * <code>web.xml</code> might not even have the necessary configuration
         * parameters defined for the caller specified policy.
         */
        @Override
        public void setHALoadBalancerPolicy(final IHALoadBalancerPolicy policy)
                throws IOException {

            if (policy == null)
                throw new IllegalArgumentException();

            if (log.isInfoEnabled())
                log.info("Will set LBS policy: " + policy);
            
            final HAJournalServer haJournalServer = getHAJournalServer();

            final WebAppContext wac = haJournalServer.getWebAppContext();
            
            if (log.isInfoEnabled())
                log.info("Will set LBS: wac=" + wac + ", policy: " + policy);

            HALoadBalancerServlet.setLBSPolicy(wac.getServletContext(), policy);

        }
        
        
    } // class HAGlueTestImpl

    /**
     * Delegation pattern allows us to override select methods easily.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class MyPrepareMessage implements IHA2PhasePrepareMessage {
        
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        private final IHA2PhasePrepareMessage delegate;
        
        MyPrepareMessage(final IHA2PhasePrepareMessage msg) {
            this.delegate = msg;
        }

        @Override
        public IHANotifyReleaseTimeResponse getConsensusReleaseTime() {
            return delegate.getConsensusReleaseTime();
        }

        @Override
        public boolean isGatherService() {
            return delegate.isGatherService();
        }

        @Override
        public boolean isJoinedService() {
            return delegate.isJoinedService();
        }

        @Override
        public boolean isRootBlock0() {
            return delegate.isRootBlock0();
        }

        @Override
        public IRootBlockView getRootBlock() {
            return delegate.getRootBlock();
        }

        @Override
        public long getTimeout() {
            return delegate.getTimeout();
        }

        @Override
        public TimeUnit getUnit() {
            return delegate.getUnit();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to force the PREPARE to vote NO.
         */
        @Override
        public boolean voteNo() {
            return delegate.voteNo();
        }
        
    }

    /**
     * Delegation pattern allows us to override select methods easily.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class MyCommitMessage implements IHA2PhaseCommitMessage {

        private static final long serialVersionUID = 1L;

        private final IHA2PhaseCommitMessage delegate;

        public MyCommitMessage(final IHA2PhaseCommitMessage msg) {
            this.delegate = msg;
        }

        @Override
        public boolean isJoinedService() {
            return delegate.isJoinedService();
        }

        @Override
        public long getCommitTime() {
            return delegate.getCommitTime();
        }

        @Override
        public boolean didAllServicesPrepare() {
            return delegate.didAllServicesPrepare();
        }

        @Override
        public boolean failCommit_beforeWritingRootBlockOnJournal() {
            return delegate.failCommit_beforeWritingRootBlockOnJournal();
        }

        @Override
        public boolean failCommit_beforeClosingHALog() {
            return delegate.failCommit_beforeClosingHALog();
        }

    }
    
} // class HAJournalTest
