/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reservesuper.

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

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.counters.PIDUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
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
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService;
import com.bigdata.journal.jini.ha.HAJournalServer.RunStateEnum;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;
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
         * Set a fail point on a future invocation of the specified methosuper. The
         * method must be declared by the {@link HAGlue} interface or by one of
         * the interfaces which it extends. The method will throw a
         * {@link SpuriousTestException} when the failure criteria are
         * satisifiesuper.
         * 
         * @param name
         *            The name method to fail.
         * @param parameterTypes
         *            The parameter types for that methosuper.
         * @param nwait
         *            The #of invocations to wait before failing the methosuper.
         * @param nfail
         *            The #of times to fail the methosuper.
         */
        public void failNext(final String name,
                final Class<?>[] parameterTypes, final int nwait,
                final int nfail) throws IOException;

        /**
         * Clear any existing fail request for the specified methosuper.
         * 
         * @param name
         *            The name method to fail.
         * @param parameterTypes
         *            The parameter types for that methosuper.
         */
        public void clearFail(final String name, final Class<?>[] parameterTypes)
                throws IOException;

        /**
         * Instruct the service to vote "NO" on the next 2-phase PREPARE
         * request.
         */
        public void voteNo() throws IOException;

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
         *            TODO Add a "clearNextTimestamp() method.
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
     * methosuper. If any of them can not resolve itself, then the method will fail,
     * which is why we log all such method resolution failures here.
     * 
     * @param name
     *            The name of the methosuper.
     * @param parameterTypes
     *            The parameter types of the methosuper.
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
    private class HAGlueTestImpl extends HAJournal.HAGlueService
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
         */
        private final AtomicBoolean voteNo = new AtomicBoolean(false);

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
             * Note: This is a cross check on Methosuper.hashCode() and
             * Methosuper.equals(), not a data race check on the CHM.
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
        public void setNextTimestamp(long nextTimestamp) throws IOException {

            this.nextTimestamp.set(nextTimestamp);
            
        }

        /**
         * Conditionally fail the method if (a) it is registered in the
         * {@link #failSet} and (b) it is due to fail on this invocation.
         * 
         * @param name
         *            The name of the methosuper.
         * @param parameterTypes
         *            The parameter types for the methosuper.
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

            // Check conditions for failing the methosuper.
            if (willFail) {

                /*
                 * Fail the methosuper.
                 */

                log.error("Will fail HAGlue method: m=" + m + ", n=" + n
                        + ", nwait=" + d.nwait + ", nfail=" + d.nfail);

                throw new SpuriousTestException(m.toString() + " : n=" + n);

            }

            // Method will run normally.

        }

        // /**
        // * Set a fail point on the next invocation of the specified methosuper.
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
        public long awaitHAReady(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException,
                AsynchronousQuorumCloseException {

            checkMethod("awaitHAReady",
                    new Class[] { Long.TYPE, TimeUnit.class });

            return super.awaitHAReady(timeout, unit);

        }

        @Override
        public IHARootBlockResponse getRootBlock(IHARootBlockRequest msg) {

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
        public IHADigestResponse computeDigest(IHADigestRequest req)
                throws IOException, NoSuchAlgorithmException, DigestException {

            checkMethod("computeDigest", new Class[] { IHADigestRequest.class });

            return super.computeDigest(req);

        }

        @Override
        public IHALogDigestResponse computeHALogDigest(IHALogDigestRequest req)
                throws IOException, NoSuchAlgorithmException, DigestException {

            checkMethod("computeHALogDigest",
                    new Class[] { IHALogDigestRequest.class });

            return super.computeHALogDigest(req);

        }

        @Override
        public IHASnapshotDigestResponse computeHASnapshotDigest(
                IHASnapshotDigestRequest req) throws IOException,
                NoSuchAlgorithmException, DigestException {

            checkMethod("computeHASnapshotDigest",
                    new Class[] { IHASnapshotDigestRequest.class });

            return super.computeHASnapshotDigest(req);

        }

        @Override
        public Future<IHASnapshotResponse> takeSnapshot(IHASnapshotRequest req)
                throws IOException {

            checkMethod("takeSnapshot",
                    new Class[] { IHASnapshotRequest.class });

            return super.takeSnapshot(req);

        }

        @Override
        public Future<Void> rebuildFromLeader(IHARemoteRebuildRequest req) throws IOException {

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
                IHAGatherReleaseTimeRequest req) throws IOException {

            checkMethod("gatherMinimumVisibleCommitTime",
                    new Class[] { IHAGatherReleaseTimeRequest.class });

            super.gatherMinimumVisibleCommitTime(req);

        }

        @Override
        public IHANotifyReleaseTimeResponse notifyEarliestCommitTime(
                IHANotifyReleaseTimeRequest req) throws IOException,
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

                return super.prepare2Phase(new MyPrepareMessage(prepareMessage));

            } else {
                
                return super.prepare2Phase(prepareMessage);
                
            }

        }

        @Override
        public Future<Void> commit2Phase(IHA2PhaseCommitMessage commitMessage) {

            checkMethod("commit2Phase",
                    new Class[] { IHA2PhaseCommitMessage.class });

            return super.commit2Phase(commitMessage);

        }

        @Override
        public Future<Void> abort2Phase(IHA2PhaseAbortMessage abortMessage) {

            checkMethod("abort2Phase",
                    new Class[] { IHA2PhaseAbortMessage.class });

            return super.abort2Phase(abortMessage);

        }

        /*
         * HAReadGlue
         */

        @Override
        public Future<IHAReadResponse> readFromDisk(IHAReadRequest readMessage) {

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
        public Future<Void> receiveAndReplicate(IHASyncRequest req,
                IHAWriteMessage msg) throws IOException {

            checkMethod("receiveAndReplicate", new Class[] {
                    IHASyncRequest.class, IHAWriteMessage.class });

            return super.receiveAndReplicate(req, msg);

        }

        @Override
        public IHAWriteSetStateResponse getHAWriteSetState(
                IHAWriteSetStateRequest req) {

            checkMethod("getHAWriteSetState",
                    new Class[] { IHAWriteSetStateRequest.class });

            return super.getHAWriteSetState(req);

        }

        @Override
        public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
                IHALogRootBlocksRequest msg) throws IOException {

            checkMethod("getHALogRootBlocksForWriteSet",
                    new Class[] { IHALogRootBlocksRequest.class });

            return super.getHALogRootBlocksForWriteSet(msg);

        }

        @Override
        public Future<Void> sendHALogForWriteSet(IHALogRequest msg)
                throws IOException {

            checkMethod("sendHALogForWriteSet",
                    new Class[] { IHALogRequest.class });

            return super.sendHALogForWriteSet(msg);

        }

        @Override
        public Future<IHASendStoreResponse> sendHAStore(IHARebuildRequest msg)
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
        public <T> Future<T> submit(final IIndexManagerCallable<T> callable,
                final boolean asyncFuture) throws IOException {

            callable.setIndexManager(getIndexManager());

            final Future<T> ft = getIndexManager().getExecutorService().submit(
                    callable);

            return getProxy(ft, asyncFuture);

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
//                        // FIXME: hould already be closed, can we check this?
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

    } // class HAGlueTestImpl

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
         * Force the PREPARE to vote NO.
         */
        @Override
        public boolean voteNo() {
            return true;
        }
        
    }
    
} // class HAJournalTest
