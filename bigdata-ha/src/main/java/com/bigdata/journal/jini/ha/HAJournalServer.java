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
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.rmi.Remote;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.halog.IHALogWriter;
import com.bigdata.ha.msg.HAAwaitServiceJoinRequest;
import com.bigdata.ha.msg.HALogRequest;
import com.bigdata.ha.msg.HALogRootBlocksRequest;
import com.bigdata.ha.msg.HARebuildRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HAWriteSetStateRequest;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.journal.jini.ha.HAClient.HAConnection;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.zk.ZKQuorumClient;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.CreateKBTask;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.AbstractHATransactionService;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.util.ClocksNotSynchronizedException;
import com.bigdata.util.InnerCause;
import com.bigdata.util.StackInfoReport;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.MonitoredFutureTask;
import com.bigdata.zookeeper.start.config.ZookeeperClientConfig;
import com.sun.jini.start.LifeCycle;

/**
 * An administratable server for an {@link HAJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/530"> Journal
 *      HA </a>
 * 
 *      TODO It would be nice if we could roll an {@link HAJournalServer} back
 *      (or forward) to a specific commit point while it was running (but
 *      naturally not while it was joined with a met quorum).
 */
public class HAJournalServer extends AbstractServer {

    private static final Logger log = Logger.getLogger(HAJournalServer.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * Configuration options for the {@link HAJournalServer}.
     */
    public interface ConfigurationOptions extends
            AbstractServer.ConfigurationOptions {

        String COMPONENT = HAJournalServer.class.getName();
        
        /**
         * The target replication factor (k).
         * 
         * <strong>CAUTION: </strong> In order to change the replication factor
         * of an existing quorum, all services MUST be shutdown. You may then
         * edit the {@link #REPLICATION_FACTOR} value in the configuration files
         * of each service. The replication factor may be changed to any odd
         * number, but the easiest migration is when it is increased or
         * decreased by TWO (2).
         * <p>
         * In the special case where the replication factor is changed from ONE
         * (not HA) to THREE (HA3), you MUST replicate the data by hand to at
         * least one new service (scp the journal and HALogs) before the quorum
         * can meet. This is because the minimum quorum for HA3 is TWO (2)
         * services.
         * <p>
         * If the replication factor was increased, then you will need to start
         * additional services to obtain a fully met quorum and those services
         * must be told to perform an online disaster recover (replication of
         * their entire state from the quorum).
         * <p>
         * If the replication factor is decreased, then you can simply delete
         * the services that you no longer wish to run and then restart the
         * remaining services.
         * 
         * TODO It is (conceptually) possible to automate a change of the
         * replication factor by +2 since the quorum would not break (except
         * when the replication factor is currently ONE (1)). This would require
         * the ability to replicate the backing store and HALogs without
         * escalating the new services from PIPELINE to JOINED.
         * 
         * TODO Automating a reduction in the replication factor would require
         * us to identify those services that would no longer be part of the
         * quorum and ensure that they are removed from the quorum and do not
         * reenter before changing the replication factor.
         */
        String REPLICATION_FACTOR = "replicationFactor";
        
        /**
         * The {@link InetSocketAddress} at which the managed {@link HAJournal}
         * exposes its write pipeline interface (required).
         */
        String WRITE_PIPELINE_ADDR = "writePipelineAddr";

        /**
         * The logical service identifier for this highly available journal.
         * There may be multiple logical highly available journals, each
         * comprised of <em>k</em> physical services. The logical service
         * identifier is used to differentiate these different logical HA
         * journals. The service {@link UUID} is used to differentiate the
         * physical service instances. By assigning a logical service identifier
         * to an {@link HAJournalServer} you associate that server instance with
         * the specified logical highly available journal.
         * <p>
         * The identifier may be any legal znode node.
         * 
         * TODO This needs to be reconciled with the federation. The federation
         * uses ephemeral sequential to create the logical service identifiers.
         * Here they are being assigned manually. This is basically the "flex"
         * versus "static" issue.
         */
        String LOGICAL_SERVICE_ID = "logicalServiceId";
        
        /**
         * The timeout in milliseconds that the leader will await the followers
         * during the release time consensus protocol.
         * <p>
         * Note: The timeout must be set with a realistic expectation concerning
         * the possibility of garbage collection. A long GC pause could
         * otherwise cause the 2-phase commit to fail. With this in mind, a
         * reasonable timeout is on the order of 10 seconds.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/623" >
         *      HA TXS / TXS Bottleneck </a>
         */
        String HA_RELEASE_TIME_CONSENSUS_TIMEOUT = "haReleaseTimeConsensusTimeout";

        long DEFAULT_HA_RELEASE_TIME_CONSENSUS_TIMEOUT = Long.MAX_VALUE; // milliseconds.
        
        long MIN_HA_RELEASE_TIME_CONSENSUS_TIMEOUT = 100; // milliseconds.
        
        /**
         * The timeout in milliseconds that the leader will await the followers
         * to prepare for a 2-phase commit.
         * <p>
         * Note: The timeout must be set with a realistic expectation concerning
         * the possibility of garbage collection. A long GC pause could
         * otherwise cause the 2-phase commit to fail. With this in mind, a
         * reasonable timeout is on the order of 10 seconds.
         */
        String HA_PREPARE_TIMEOUT = "haPrepareTimeout";

        long DEFAULT_HA_PREPARE_TIMEOUT = Long.MAX_VALUE; // milliseconds.
        
        long MIN_HA_PREPARE_TIMEOUT = 100; // milliseconds.

        /**
         * The maximum allowed clock skew (default
         * {@value #DEFAULT_MAXIMUM_CLOCK_SKEW} milliseconds). Clock skew is
         * identified during the commit protocol. A timestamp (A) is taken on
         * the leader. The leader then messages the followers. The followers
         * take timestamps (B) and message the leader. The leader then takes
         * another timestamp (C). A {@link ClocksNotSynchronizedException} will
         * be thrown if any of the following conditions are violated:
         * <ul>
         * <li>A is not <i>before</i> B (for each follower's value of B)</li>
         * <li>B is not <i>before</i> C (for each follower's value of B)</li>
         * </ul>
         * This option controls the maximum skew in the clocks and thus how much
         * error is allowable in the interpretation of the <i>before</i>
         * relation.
         * 
         * @see ClocksNotSynchronizedException
         */
        String MAXIMUM_CLOCK_SKEW = "maximumClockSkew";
        
        long DEFAULT_MAXIMUM_CLOCK_SKEW = 5000;
        
        /**
         * The mimimum allowed value for the {@link #MAXIMUM_CLOCK_SKEW}
         * configuration option.
         */
        long MIN_MAXIMUM_CLOCK_SKEW = 100;
        
        /**
         * The additional time to await successful replication beyond the
         * negotiated session timeout for the zookeeper client ({@default
         * #DEFAULT_HA_EXTRA_DELAY_FOR_RETRY_SEND})
         * <p>
         * The robust replication logic examines the negotiated session timeout
         * for zookeeper to decide how long it will retry transmission before
         * failing. Once transmission is failed, the associated update will
         * fail. Since the zookeeper ensemble takes some non-zero time to notify
         * the services that it has timed out a dead service, we need to add a
         * some latency beyond the negotiated session timeout or a transaction
         * will stochastically fail if a joined service is killed while the
         * leader is attempting to replicate writes along the pipeline. In
         * addition to the latency for the zookeeper ensemble to notify the
         * servers, there is also possible latency due to GC pauses. This value
         * thus provides some <em>additional</em> latency and is intended to
         * allow updates to succeed even when a service is killed and in the
         * face of other sources of latency in the processing of the ZK events
         * that reflect that the service is no longer "live".
         */
        String HA_EXTRA_DELAY_FOR_RETRY_SEND = "haExtraDelayForRetrySend";

        long DEFAULT_HA_EXTRA_DELAY_FOR_RETRY_SEND = 5000; // milliseconds.

        /**
         * The property whose value is the name of the directory in which write
         * ahead log files will be created to support resynchronization services
         * trying to join an HA quorum (default {@value #DEFAULT_HA_LOG_DIR}).
         * <p>
         * The directory should not contain any other files. It will be
         * populated with files whose names correspond to commit counters. The
         * commit counter is recorded in the root block at each commit. It is
         * used to identify the write set for a given commit. A log file is
         * written for each commit point. Each log files is normally deleted at
         * the commit. However, if the quorum is not fully met at the commit,
         * then the log files not be deleted. Instead, they will be used to
         * resynchronize the other quorum members.
         * <p>
         * The log file name includes the value of the commit counter for the
         * commit point that will be achieved when that log file is applied to a
         * journal whose current commit point is [commitCounter-1]. The commit
         * counter for a new journal (without any commit points) is ZERO (0).
         * This the first log file will be labeled with the value ONE (1). The
         * commit counter is written out with leading zeros in the log file name
         * so the natural sort order of the log files should correspond to the
         * ascending commit order.
         * <p>
         * The log files are a sequence of zero or more {@link IHAWriteMessage}
         * objects. For the {@link RWStore}, each {@link IHAWriteMessage} is
         * followed by the data from the corresponding {@link WriteCache} block.
         * For the {@link WORMStrategy}, the {@link WriteCache} block is omitted
         * since this data can be trivially reconstructed from the backing file.
         * When the quorum prepares for a commit, the proposed root block is
         * written onto the end of the log file.
         * <p>
         * The log files are deleted once the quorum is fully met (k out of k
         * nodes have met in the quorum). It is possible for a quorum to form
         * with only <code>(k+1)/2</code> nodes. When this happens, the nodes in
         * the quorum will all write log files into the {@link #HA_LOG_DIR}.
         * Those files will remain until the other nodes in the quorum
         * synchronize and join the quorum. Once the quorum is fully met, the
         * files in the log directory will be deleted.
         * <p>
         * If some or all log files are not present, then any node that is not
         * synchronized with the quorum must be rebuilt from scratch rather than
         * by incrementally applying logged write sets until it catches up and
         * can join the quorum.
         * 
         * @see IRootBlockView#getCommitCounter()
         */
        String HA_LOG_DIR = "haLogDir";
        
        /**
         * Note: The default is relative to the effective value of the
         * {@link AbstractServer.ConfigurationOptions#SERVICE_DIR}.
         */
        String DEFAULT_HA_LOG_DIR = "HALog";

        /**
         * The maximum amount of time in milliseconds to await the synchronous
         * release of older HALog files during a 2-phase commit (default
         * {@value #DEFAULT_HA_LOG_PURGE_TIMEOUT}). This MAY be ZERO to not
         * wait. Large timeouts can cause significant latency during a 2-phase
         * commit if a large number of HALog files should be released
         * accordinging to the {@link IRestorePolicy}.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/780"
         *      >Incremental or asynchronous purge of HALog files</a>
         */
        String HA_LOG_PURGE_TIMEOUT = "HALogPurgeTimeout";

        /**
         * The default is ZERO (0L) milliseconds, which is probably what we
         * always want. However, the existence of this option allows us to
         * revert to the old behavior using a configuration change or by
         * changing the default.
         */
        long DEFAULT_HA_LOG_PURGE_TIMEOUT = 0L; // milliseconds

        /**
         * The name of the directory in which periodic snapshots of the journal
         * will be written. Each snapshot is a full copy of the journal.
         * Snapshots are compressed and therefore may be much more compact than
         * the original journal. A snapshot, together with incremental HALog
         * files, may be used to regenerate a journal file for a specific commit
         * point.
         */
        String SNAPSHOT_DIR = "snapshotDir";

        /**
         * Note: The default is relative to the effective value of the
         * {@link AbstractServer.ConfigurationOptions#SERVICE_DIR}.
         */
        String DEFAULT_SNAPSHOT_DIR = "snapshot";

        /**
         * The number of threads that will be used for a parallel scan of the
         * files in the {@link #HA_LOG_DIR} and {@link #SNAPSHOT_DIR} in order
         * to accelerate the service start. The minimum is ONE (1). The default
         * is {@value #DEFAULT_STARTUP_THREADS}.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/775" > HAJournal start()
         *      (optimization) </a>
         */
        String STARTUP_THREADS = "startupThreads";
        
        int DEFAULT_STARTUP_THREADS = 20;

        /**
         * The policy that specifies when a new snapshot will be taken. The
         * decision to take a snapshot is a local decision and the snapshot is
         * assumed to be written to local disk. However, offsite replication of
         * the {@link #SNAPSHOT_DIR} and {@link #HA_LOG_DIR} is STRONGLY
         * recommended.
         * <p>
         * Each snapshot is a full backup of the journal. Incremental backups
         * (HALog files) are created for each transaction. Older snapshots and
         * HALog files will be removed once automatically.
         * 
         * @see ISnapshotPolicy
         */
        String SNAPSHOT_POLICY = "snapshotPolicy";

        ISnapshotPolicy DEFAULT_SNAPSHOT_POLICY = new DefaultSnapshotPolicy();

        /**
         * The policy identifies the first commit point whose backups MUST NOT
         * be released. The policy may be based on the age of the commit point,
         * the number of intervening commit points, etc. A policy that always
         * returns ZERO (0) will never release any backups.
         * 
         * @see IRestorePolicy
         */
        String RESTORE_POLICY = "restorePolicy";

        IRestorePolicy DEFAULT_RESTORE_POLICY = new DefaultRestorePolicy();

        /**
         * Permit override of the {@link HAJournal} implementation class.
         */
        String HA_JOURNAL_CLASS = "HAJournalClass";

        String DEFAULT_HA_JOURNAL_CLASS = HAJournal.class.getName();
        
        /**
         * When <code>true</code> the service will automatically perform online
         * disaster recovery (REBUILD). When <code>false</code>, it will enter
         * the OPERATOR state instead (human intervention required).
         */
        String ONLINE_DISASTER_RECOVERY = "onlineDisasterRecovery";

        /**
         * TODO This feature is disabled by default. We want to develop more
         * experience with the online disaster recovery and snapshot / restore
         * mechanisms before putting enabling it in a release. There are two
         * possible downsides to enabling this feature: (1) REBUILD should not
         * trigger unless it is necessary so we need to make sure that spurious
         * and curable errors do not result in a REBUILD; (2) The current
         * REBUILD implementation does not replicate the pinned HALogs from the
         * leader. This means that the rebuilt service is not as robust since it
         * can not replicate the missing HALogs to another service if that
         * service should then require disaster recovery as well.
         */
        boolean DEFAULT_ONLINE_DISASTER_RECOVERY = false;

        /**
         * The location of the <code>jetty.xml</code> file that will be used to
         * configure jetty (default {@value #DEFAULT_JETTY_XML}).
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
         *      Allow configuration of embedded NSS jetty server using
         *      jetty-web.xml </a>
         * 
         * @see #DEFAULT_JETTY_XML
         */
        String JETTY_XML = NanoSparqlServer.SystemProperties.JETTY_XML;

        /**
         * The default value works when deployed under the IDE with the
         * <code>bigdata-war/src</code> directory on the classpath. When
         * deploying outside of that context, the value needs to be set
         * explicitly.
         */
        String DEFAULT_JETTY_XML = NanoSparqlServer.SystemProperties.DEFAULT_JETTY_XML;

    }
    
    /**
     * The journal.
     */
    private HAJournal journal;
    
    private UUID serviceUUID;

    /**
     * @see ConfigurationOptions#ONLINE_DISASTER_RECOVERY
     */
    private boolean onelineDisasterRecovery;
    
    /**
     * An executor used to handle events that were received in the zk watcher
     * event thread. We can not take actions that could block in the watcher
     * event thread. Therefore, a task for the event is dropped onto this
     * service where it will execute asynchronously with respect to the watcher
     * thread.
     * <p>
     * Note: This executor will be torn down when the backing
     * {@link AbstractJournal#getExecutorService()} is torn down. Tasks
     * remaining on the backing queue for the {@link LatchedExecutor} will be
     * unable to execute successfuly and the queue will be drained as attempts
     * to run those tasks result in {@link RejectedExecutionException}s.
     */
    private LatchedExecutor singleThreadExecutor;
    
    private HAGlue haGlueService;
    
    /**
     * The znode name for the logical service.
     * 
     * @see ConfigurationOptions#LOGICAL_SERVICE_ID
     */
    private String logicalServiceId;
    
    /**
     * The zpath for the znode that dominates the logical service.
     */
    private String logicalServiceZPathPrefix;
    
    /**
     * The zpath for the logical service.
     * 
     * @see ConfigurationOptions#LOGICAL_SERVICE_ID
     */
    private String logicalServiceZPath;
    
    /**
     * The {@link HAQuorumService}.
     */
    private HAQuorumService<HAGlue, HAJournal> quorumService;
    
    /**
     * An embedded jetty server exposing the {@link NanoSparqlServer} webapp.
     * The {@link NanoSparqlServer} webapp exposes a SPARQL endpoint for the
     * Journal, which is how you read/write on the journal (the {@link HAGlue}
     * interface does not expose any {@link Remote} methods to write on the
     * {@link HAJournal}.
     */
    private volatile Server jettyServer;

    /**
     * Exposed to the test suite.
     */
    WebAppContext getWebAppContext() {

        final Server server = jettyServer;
        
        if (server == null)
            throw new IllegalStateException();

        final WebAppContext wac = NanoSparqlServer.getWebApp(server);

        return wac;
        
    }
    
    /**
     * Enum of the run states. The states are labeled by the goal of the run
     * state.
     */
    static public enum RunStateEnum {
        /**
         * Roll forward the database by applying local HALog files GT current
         * commit point.
         */
        Restore,
        /**
         * Seek a consensus with the other quorum members regarding the most
         * recent commit point on the database. If a consensus is established
         * then the quorum will meet. If the quorum is already met, then this
         * service must {@link RunStateEnum#Resync}.
         */
        SeekConsensus,
        /** Run while joined with met quorum. */
        RunMet,
        /**
         * Resynchronize with the leader of a met quorum, replicating and
         * applying HALogs and rolling forward the database until it catches up
         * with the quorum and joins.
         */
        Resync,
        /**
         * Online disaster recovery. The backing store is replicated from the
         * quorum leader and then the service enters {@link RunStateEnum#Resync}
         * to catch up with any missed writes since the start of the replication
         * procedure.
         */
        Rebuild,
        /**
         * Error state. This state should be self correcting.
         */
        Error,
        /**
         * Shutdown.
         * 
         * TODO SHUTDOWN: We are not using this systematically (no ShutdownTask
         * for this run state).
         */
        Shutdown,
        /**
         * Operator intervention required - service can not proceed.
         */
        Operator;
    }

    public HAJournalServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);

        /*
         * Start the HAJournalServer and wait for it to terminate.
         * 
         * Note: This is invoked from within the constructor of the concrete
         * service class. This ensures that all initialization of the service is
         * complete and is compatible with the Apache River ServiceStarter
         * (doing this in main() is not compatible since the ServiceStarter does
         * not expect the service to implement Runnable).
         */
        run();
        
    }

    /*
     * Operator alerts.
     * 
     * TODO If we keep this abstraction, then extract an interface for operator
     * alerts. However, this could also be captured as Entry[] attributes that
     * are published on the service registrar. Once those attributes are
     * published, it is easy enough to write utilities to monitor those Entry[]
     * attributes and then execute appropriate business logic.
     */
    
    private final AtomicReference<String> operatorAlert = new AtomicReference<String>();

    protected void sendOperatorAlert(final String msg) {
        operatorAlert.set(msg);
    }

    protected void clearOperatorAlert() {
        operatorAlert.set(null);
    }

    protected String getOperatorAlert() {
        return operatorAlert.get();
    }

    @Override
    protected void terminate() {

        super.terminate();
    
    }

    /**
     * Ensure key znodes exist.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void setupZNodes() throws KeeperException, InterruptedException {

        if (log.isInfoEnabled())
            log.info("Ensuring key znodes exist.");

        final ZookeeperClientConfig zkClientConfig = getHAClient()
                .getZookeeperClientConfig();

        final List<ACL> acl = zkClientConfig.acl;

        final ZooKeeper zk = getHAClient().getConnection().getZookeeper();

        /*
         * Ensure key znodes exist.
         */
        try {
            zk.create(zkClientConfig.zroot, new byte[] {/* data */}, acl,
                    CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            zk.create(logicalServiceZPathPrefix, new byte[] {/* data */}, acl,
                    CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            zk.create(logicalServiceZPath, new byte[] {/* data */}, acl,
                    CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }

    }
    
    @Override
    protected HAGlue newService(final Configuration config) throws Exception {

        if (log.isInfoEnabled())
            log.info("Creating service impl...");

        // Jini/River ServiceID.
        final ServiceID serviceID = getServiceID();

        if (serviceID == null)
            throw new AssertionError("ServiceID not assigned?");
        
        // UUID variant of that ServiceID.
        serviceUUID = JiniUtil.serviceID2UUID(serviceID);

        /*
         * Extract various configuration options.
         */
        onelineDisasterRecovery = (Boolean) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.ONLINE_DISASTER_RECOVERY, Boolean.TYPE,
                ConfigurationOptions.DEFAULT_ONLINE_DISASTER_RECOVERY);

        /*
         * Setup the Quorum / HAJournal.
         */

        final ZookeeperClientConfig zkClientConfig = getHAClient()
                .getZookeeperClientConfig();

        // znode name for the logical service.
        logicalServiceId = (String) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.LOGICAL_SERVICE_ID, String.class); 

        {

            logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                    + HAJournalServer.class.getName();

            // zpath for the logical service.
            logicalServiceZPath = logicalServiceZPathPrefix + "/"
                    + logicalServiceId;

            /*
             * Note: The replicationFactor is taken from the Configuration and
             * is imposed on the QUORUM znode's QuorumTokenState data by the
             * ZKQuorumImpl when the QuorumService starts.
             */
            final int replicationFactor = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.REPLICATION_FACTOR, Integer.TYPE);        

            /*
             * Zookeeper quorum.
             */
            @SuppressWarnings({ "unchecked", "rawtypes" })
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = (Quorum) new ZKQuorumImpl<HAGlue, HAQuorumService<HAGlue, HAJournal>>(
                    replicationFactor);

            /**
             * The HAJournal.
             * 
             * FIXME This step can block for a long time if we have a lot of
             * HALogs to scan. While it blocks, the REST API (including the LBS)
             * is down. This means that client requests to the service end point
             * can not be proxied to a service that is online. The problem is
             * the interaction with the BigdataRDFServletContextListener which
             * needs to (a) set the IIndexManager on the ServletContext; and (b)
             * initiate the default KB create (if it is the quorum leader).
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/775" > HAJournal
             *      start() (optimization) </a>
             */
            this.journal = newHAJournal(this, config, quorum);
            
        }

        // executor for events received in the watcher thread.
        singleThreadExecutor = new LatchedExecutor(
                journal.getExecutorService(), 1/* nparallel */);
        
        // our external interface.
        haGlueService = journal.newHAGlue(serviceUUID);

        // Setup the quorum client (aka quorum service).
        quorumService = newQuorumService(logicalServiceZPath, serviceUUID,
                haGlueService, journal);

        /*
         * Return our external interface object. This object will get proxied.
         * 
         * Note: If we wrap that object with a delegation pattern, then RMI
         * methods on a subclass of HAGlueService will not be visible on the
         * exported proxy.
         */

        return haGlueService;

    }

    /**
     * Permit override of the {@link HAJournal} implementation class.
     * 
     * @throws ConfigurationException
     */
    private HAJournal newHAJournal(final HAJournalServer server,
            final Configuration config,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
            throws ConfigurationException {

        final String className = (String) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.HA_JOURNAL_CLASS, String.class,
                ConfigurationOptions.DEFAULT_HA_JOURNAL_CLASS);

        try {
            @SuppressWarnings("unchecked")
            final Class<HAJournal> cls = (Class<HAJournal>) Class
                    .forName(className);

            if (!HAJournal.class.isAssignableFrom(cls)) {

                throw new ConfigurationException("Invalid option: "
                        + ConfigurationOptions.HA_JOURNAL_CLASS + "="
                        + className + ":: Class does not extend "
                        + HAJournal.class);

            }

            final Constructor<HAJournal> ctor = cls.getConstructor(new Class[] {
                    HAJournalServer.class, Configuration.class, Quorum.class });

            final HAJournal jnl = ctor.newInstance(new Object[] { server,
                    config, quorum });

            return jnl;

        } catch (ClassNotFoundException e) {

            throw new ConfigurationException(
                    ConfigurationOptions.HA_JOURNAL_CLASS + "=" + className, e);

        } catch (NoSuchMethodException e) {

            throw new ConfigurationException(
                    ConfigurationOptions.HA_JOURNAL_CLASS + "=" + className, e);

        } catch (InstantiationException e) {

            throw new ConfigurationException(
                    ConfigurationOptions.HA_JOURNAL_CLASS + "=" + className, e);

        } catch (IllegalAccessException e) {

            throw new ConfigurationException(
                    ConfigurationOptions.HA_JOURNAL_CLASS + "=" + className, e);

        } catch (IllegalArgumentException e) {

            throw new ConfigurationException(
                    ConfigurationOptions.HA_JOURNAL_CLASS + "=" + className, e);

        } catch (InvocationTargetException e) {

            throw new ConfigurationException(
                    ConfigurationOptions.HA_JOURNAL_CLASS + "=" + className, e);

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: called from {@link AbstractServer#run()}
     * 
     * FIXME We should be able to start the NSS while still reading the HALog
     * files from the disk. The action to start the {@link HAQuorumService}
     * should await a {@link Future} for the journal start. Thus, the
     * {@link HAJournal} start needs to be turned into a {@link Callable} or
     * {@link Runnable}.
     * <p>
     * In fact, the journal open is very fast. The slow part is the building an
     * index over the HALogs and (to a lesser extent) over the snapshots. Those
     * index builds can run in parallel, but we need to have a critical section
     * in which we check some necessary conditions, especially whether the last
     * HALog is valid.
     * <p>
     * We need to push a start() computation into both the {@link HALogNexus}
     * and the {@link SnapshotManager}. This could be done with an interface
     * that is also shared by the {@link HAJournal}. The interface could provide
     * some reporting on the startup process, but most critical is that it
     * provides a {@link Future} for evaluating that process.
     * <p>
     * The {@link Future} can evaluate to the outcome of that startup procedure.
     * <p>
     * The startup procedure should use multiple threads (or async IO) to reduce
     * the startup latency. It could use the executor on the journal for this.
     * <p>
     * We could parallelize the HALog and snapshot startup then enter a critical
     * section in which we validate the consistency of those resources with
     * respect to the HAJournal's current root block.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/775" > HAJournal start()
     *      (optimization) </a>
     */
    @Override
    protected void startUpHook() { 

        if (log.isInfoEnabled())
            log.info("Starting server.");

        // Start the NSS.
        startNSS();

        // Start the quorum.
        journal.getQuorum().start(quorumService);

    }
    
    /**
     * Shutdown the embedded NSS if it is running.
     */
    private void stopNSS() {

        if (jettyServer != null) {

            try {

                // Shutdown the embedded NSS.
                jettyServer.stop();

                // Wait for it to terminate.
                jettyServer.join();

                // Clear reference.
                jettyServer = null;

            } catch (Exception e) {

                log.error(e, e);

            }

        }

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to tear down the {@link NanoSparqlServer}, the {@link Quorum},
     * and the {@link HAJournal}.
     */
    @Override
    protected void beforeShutdownHook(final boolean destroy) {

        if (log.isInfoEnabled())
            log.info("destroy=" + destroy);

        final HAJournal tjournal = journal;

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = tjournal == null ? null
                : tjournal.getQuorum();

        if (quorum != null) {
            
            try {

                /*
                 * Terminate the quorum watcher threads that are monitoring and
                 * maintaining our reflection of the global quorum state.
                 * 
                 * Note: A deadlock can arise if there is a concurrent attempt
                 * to addMember(), addPipeline(), castVote(), serviceJoin(),
                 * etc. The deadlock arises because we are undoing all of those
                 * things in this thread and then waiting until the Condition is
                 * satisified. With a concurrent action to do the opposite
                 * thing, we can wind up with the end state that we are not
                 * expecting and just waiting forever inside of
                 * AbstractQuorum.doConditionXXXX() for our condition variable
                 * to reach the desired state. [This has been addressed with a
                 * timeout on the quorum.terminate() that is imposed from within
                 * that method.]
                 */
                quorum.terminate();
                
            } catch (Throwable t) {

                log.error(t, t);

            }
            
        }
        
        stopNSS();

        if (tjournal != null) {

            if (destroy) {

                tjournal.destroy();

            } else {

                tjournal.close();

            }

        }

    }

    /**
     * Factory for the {@link QuorumService} implementation.
     * 
     * @param logicalServiceZPath
     * @param serviceId
     * @param remoteServiceImpl
     *            The object that implements the {@link Remote} interfaces
     *            supporting HA operations.
     * @param store
     *            The {@link HAJournal}.
     */
    private HAQuorumService<HAGlue, HAJournal> newQuorumService(
            final String logicalServiceZPath,
            final UUID serviceId, final HAGlue remoteServiceImpl,
            final HAJournal store) {

        return new HAQuorumService<HAGlue, HAJournal>(logicalServiceZPath,
                serviceId, remoteServiceImpl, store, this);

    }

    /**
     * Concrete {@link QuorumServiceBase} implementation for the
     * {@link HAJournal}.
     */
    // Note: Exposed to HAJournal.enterErrorState()
    static /*private*/ class HAQuorumService<S extends HAGlue, L extends HAJournal>
            extends QuorumServiceBase<S, L> implements ZKQuorumClient<S> {

        private final L journal;
        private final HAJournalServer server;

        /**
         * Lock to guard the HALogWriter.
         */
        private final Lock logLock;
        
        /**
         * Future for task responsible for resynchronizing a node with
         * a met quorum.
         */
        private final AtomicReference<FutureTask<Void>> runStateFutureRef = new AtomicReference<FutureTask<Void>>(/*null*/);

        /**
         * The {@link RunStateEnum} for the current executing task. This is set
         * when the task actually begins to execute in its
         * {@link RunStateCallable#doRun()} method.
         */
        private final AtomicReference<RunStateEnum> runStateRef = new AtomicReference<RunStateEnum>(
                null/* none */);

        /**
         * The {@link RunStateEnum} for the last task submitted. This is used by
         * {@link #enterRunState(RunStateCallable)} to close a concurrency gap
         * where the last submitted task has not yet begun to execute and
         * {@link #runStateRef} has therefore not yet been updated.
         */
        private final AtomicReference<RunStateEnum> lastSubmittedRunStateRef = new AtomicReference<RunStateEnum>(
                null/* none */);

        /*
         * Exposed to HAJournal.HAGlueService.
         */
        protected RunStateEnum getRunStateEnum() {
        
            return runStateRef.get();
            
        }
        
        protected void setRunState(final RunStateEnum runState) {

            if (runStateRef.get() == RunStateEnum.Shutdown) {

                final String msg = "Shutting down: can not enter runState="
                        + runState;

                haLog.warn(msg);

                throw new IllegalStateException(msg);

            }
            
            final RunStateEnum oldRunState = runStateRef.getAndSet(runState);

            if (oldRunState == runState) {

                /*
                 * Note: This can be a real problem depending on what the run
                 * state was doing.
                 */
                
                haLog.warn("Rentering same state? runState=" + runState);
                
            }

            // Note: *should* be non-null. Just paranoid.
            final IRootBlockView rb = journal.getRootBlockView();
            
            final String commitCounterStr = (rb == null) ? "N/A" : Long
                    .toString(rb.getCommitCounter());
            
            haLog.warn("runState=" + runState //
                    + ", oldRunState=" + oldRunState //
                    + ", quorumToken=" + journal.getQuorumToken()//
                    + ", haStatus=" + journal.getHAStatus()//
                    + ", commitCounter=" + commitCounterStr//
                    + ", serviceName=" + server.getServiceName(),//
                    new StackInfoReport());

        }
        
        private abstract class RunStateCallable<T> implements Callable<T> {
            
            /**
             * The {@link RunStateEnum} for this task.
             */
            protected final RunStateEnum runState;
            
            protected RunStateCallable(final RunStateEnum runState) {

                if (runState == null)
                    throw new IllegalArgumentException();
                
                this.runState = runState;
                
            }
            
            @Override
            final public T call() throws Exception {

                /*
                 * Note: Will throw IllegalStateException if this service is
                 * already shutting down.
                 */
                setRunState(runState);
                
                try {

                    return doRun();

                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        // Note: This is a normal exit condition.
                        if (log.isInfoEnabled())
                            log.info("Interrupted: " + runState);

                        // Done.
                        return null;
                        
                    } else {

                        /*
                         * Unhandled error.
                         */
                        
                        log.error(t, t);

                        /*
                         * Sleep for a moment to avoid tight error handling
                         * loops that can generate huge log files.
                         */
                        Thread.sleep(250/* ms */);
                        
                        /*
                         * Transition to the Error task.
                         * 
                         * Note: The ErrorTask can not interrupt itself even if
                         * it was the current task since the current task has
                         * been terminated by the throws exception!
                         * 
                         * Note: The ErrorTask CAN terminate through an internal exception
                         * so we need to check if this is the case and clear the runstateRef
                         * so we will be able to enter a new ErrorTask. This was being observed
                         * when we lose a zookeeper connection - the error task was unable to
                         * obtain the new zk connection, which caused an error that required us
                         * to retry the ErrorTask.  This is also handled inside of the ErrorTask
                         * now, which explicitly retries to obtain the zk connection, but it is
                         * handled here for the general case of unchecked errors thrown out of the
                         * ErrorTask.
                         */
                        if (runState == RunStateEnum.Error) {
                            haLog.warn("Detected error from ErrorTask, so clear runStateRef to allow re-entry");
                            runStateRef.set(null);
                        }

                        enterErrorState();

                        // Done.
                        return null;

                    }

//                    if (t instanceof Exception)
//                        throw (Exception) t;
//
//                    throw new RuntimeException(t);

                } finally {

                    /*
                     * Has a future been set? NOTE: THIS DOES NOT WAIT ON THE
                     * FUTURE ITSELF, it just displays the reference for the
                     * Future.
                     */

                    haLog.warn(runState + ": exit, runStateFuture="
                            + runStateFutureRef.get());

                    /*
                     * Note: Do NOT clear the run state since it could have been
                     * already changed by enterRunState() for the new runState.
                     */
                    
                }

            }

            /**
             * Core method.
             * <p>
             * Note: The service will AUTOMATICALLY transition to
             * {@link RunStateEnum#SeekConsensus} if there is an abnormal exit
             * from this method UNLESS it has entered
             * {@link RunStateEnum#Shutdown}.
             * 
             * @return <T> if this is a normal exit.
             * 
             * @throws InterruptedException
             *             if this is a normal exit.
             * @throws Exception
             *             if this is an abnormal exit.
             */
            abstract protected T doRun() throws Exception;
            
            /**
             * Block the thread in an interruptable manner. This is used when a
             * task must wait until it is interrupted.
             * 
             * @throws InterruptedException
             */
            protected void blockInterruptably() throws InterruptedException {

                if (haLog.isInfoEnabled())
                    haLog.info(this.toString());

                while (true) {

                    Thread.sleep(Long.MAX_VALUE);

                }

            }

            public String toString() {

                return getClass().getName() + "{runState=" + runState + "}";
                
            }
            
        } // RunStateCallable
        
        /**
         * Hooked to ensure that the key znodes exist before the quorum watcher
         * starts and to enter {@link RunStateEnum#Restore} to get the service
         * moving again.
         * 
         * @param quorum
         *            The quorum.
         */
        @Override
        public void start(final Quorum<?,?> quorum) {

            if (log.isInfoEnabled())
                log.info("", new StackInfoReport());

            if (!quorumStartStopGuard
                    .compareAndSet(false/* expect */, true/* update */)) {

                throw new IllegalStateException();
                
            }

            try {

                // Start the HAClient (River and Zookeeper).
                server.getHAClient().connect();

                // /*
                // * Verify discovery of at least one ServiceRegistrar.
                // */
                // try {
                // log.info("Awaiting service registrar discovery.");
                // server.getHAClient()
                // .getConnection()
                // .awaitServiceRegistrars(10/* timeout */,
                // TimeUnit.SECONDS);
                // } catch (TimeoutException e1) {
                // throw new RuntimeException(e1);
                // } catch (InterruptedException e1) {
                // throw new RuntimeException(e1);
                // }

                // Ensure key znodes exist.
                try {
                    server.setupZNodes();
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // Start the quorum (relies on zookeeper).
                super.start(quorum);

                /*
                 * Setup listener that logs quorum events @ TRACE.
                 * 
                 * Note: Listeners are cleared by Quorum.terminate(), so we need
                 * to do this each time we do quorum.start(...).
                 */
                journal.getQuorum().addListener(new QuorumListener() {
                    @Override
                    public void notify(final QuorumEvent e) {
                        if (log.isTraceEnabled())
                            log.trace(e);
                    }
                });

            } finally {

                // disable guard.
                quorumStartStopGuard.set(false);
                
            }

            // clear reference so we can enter a run state.
            runStateRef.set(null);
            
            // Enter a run state for the HAJournalServer.
            enterRunState(new RestoreTask());

        }
        
        private final AtomicBoolean quorumStartStopGuard = new AtomicBoolean(false);
        
        /**
         * {@inheritDoc}
         * <p>
         * Extended to tear down anything that we setup in
         * {@link #start(Quorum)}.
         */
        @Override
        public void terminate() {

            if (log.isInfoEnabled())
                log.info("", new StackInfoReport());

            // Unconditionally set guard. terminate() overrides anything.
            quorumStartStopGuard.set(true);

            try {

                /*
                 * Ensure that the HAQuorumService will not attempt to cure any
                 * serviceLeave or related actions. (This gets cleared if we go
                 * through quorumService.start(...) again.)
                 */
                runStateRef.set(RunStateEnum.Shutdown);

                /*
                 * Terminate any running task.
                 */
                final FutureTask<?> ft = runStateFutureRef.get();

                if (ft != null) {

                    ft.cancel(true/* mayInterruptIfRunning */);

                }

                // Disconnect. Terminate River and Zookeeper processing.
                server.getHAClient().disconnect(true/* immediateShutdown */);

                /*
                 * Note: This can cause a deadlock on AbstractJournal's internal
                 * read/write lock if there is a concurrent 2-phase commit. It
                 * is better to leave this to the transitions among the
                 * RunStateCallable tasks (note that we interrupt the current
                 * task above).
                 */
                // // Discard any pending writes.
                // journal.doLocalAbort();

                super.terminate();

            } finally {
                
                // disable guard.
                quorumStartStopGuard.set(false);
                
            }

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Overridden to report the negotiated zookeeper session timeout.
         * 
         * @throws QuorumException
         *             if we are not connected to zookeeper.
         */
        @Override
        protected long getRetrySendTimeoutNanos() {
            
            final ZooKeeper zk = getZooKeeper();
            int negotiatedSessionTimeoutMillis;
//            try {
                if (zk == null || !zk.getState().isAlive()) {
                    /*
                     * This service is not connected to zookeeper.
                     * 
                     * Note: We do not wait AT ALL. This method is called from
                     * within pipeline replication. If we are not connected,
                     * then we have no business participating in the pipeline
                     * replication and should fail fast.
                     */
                    throw new QuorumException("ZK not connected");
                }
//                if (!zka.awaitZookeeperConnected(
//                        journal.getHAClient().zooConfig.sessionTimeout,
//                        TimeUnit.MILLISECONDS)) {
//                    /*
//                     * This service is not connected to zookeeper.
//                     * 
//                     * Note: We are only waiting up to the desired session
//                     * timeout for the client. If the client is not connected by
//                     * the end of that timeout, then we will not wait further to
//                     * determine the current negotiated session timeout for the
//                     * client and the ZK Quorum.
//                     */
//                    throw new RuntimeException("ZK not connected");
//                }
                negotiatedSessionTimeoutMillis = zk//zka.getZookeeper()
                        .getSessionTimeout();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }

            /*
             * Note: An additional delay is added in here to give the server a
             * chance to handle messages that will start to arrive after the
             * session timeout has caused some service to be expired.
             */
            final long ms = negotiatedSessionTimeoutMillis
                    + journal.getHAExtraDelayForRetrySend();
            
            final long ns = TimeUnit.MILLISECONDS.toNanos(ms);
            
            return ns;
            
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Note: Invoked from {@link AbstractJournal#doLocalAbort()}.
         */
        @Override 
        public void discardWriteSet() {
            
            logLock.lock();
			try {
				log.warn("");

				// Clear the last live message out.
				journal.getHALogNexus().lastLiveHAWriteMessage = null;

				if (journal.getHALogNexus().isHALogOpen()) {
                    /**
                     * Note: Closing the HALog is necessary for us to be able to
                     * re-enter SeekConsensus without violating a pre-condition
                     * for that run state.
                     * 
                     * @see <a
                     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/764"
                     *      > RESYNC fails (HA) </a>
                     */
					try {
						journal.getHALogNexus().disableHALog();
					} catch (IOException e) {
						log.error(e, e);
					}
                    final long token = getQuorum().token();
                    if (isJoinedMember(token)) {
                        try {
                            journal.getHALogNexus().createHALog(
                                    journal.getRootBlockView());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
			} finally {
				logLock.unlock();
			}

		}
        
        /**
         * {@inheritDoc}
         * <p>
         * Transition to {@link RunStateEnum#Error}.
         * <p>
         * Note: if the current {@link Thread} is a {@link Thread} executing one
         * of the {@link RunStateCallable#doRun()} methods, then it will be
         * <strong>interrupted</strong> when entering the new run state (but we
         * will not re-enter the current active state). Thus, the caller MAY
         * observe an {@link InterruptedException} in their thread, but only if
         * they are being run out of {@link RunStateCallable}.
         * <p>
         * Note: This is written defensively.  It will not throw out anything.
         */
        @Override
        public void enterErrorState() {

            try {

                log.warn(new StackInfoReport("Will enter error state"));

                enterRunState(new ErrorTask());
                
            } catch (Throwable t) {

                /*
                 * Problem attempting to enter the error state.
                 * 
                 * Note: If the HAQuorumService is not running, then an
                 * IllegalStateException is thrown out. This is logged and not
                 * rethrown. If the HAQuorumService is not running, then there
                 * is nothing that needs to be torn down by entering the Error
                 * state. When (if) the HAQuorumService is re-started, it will
                 * come back up from close to scratch.
                 */
                try {

                    log.error(t, t);

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        // propagate interrupt.
                        Thread.currentThread().interrupt();

                    }

                } finally {

                    // ignore

                }

            }
            
        }
        
        /**
         * Enter RESTORE.
         * 
         * @return
         * @throws IOException
         * 
         * @see HAGlue#rebuildFromLeader(IHARemoteRebuildRequest)
         */
        public Future<Void> rebuildFromLeader(final IHARemoteRebuildRequest req)
                throws IOException {

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

            final QuorumService<HAGlue> localService = quorum.getClient();

            if (localService == null)
                return null;

            final long token = quorum.token();

            if (journal.getHAStatus() != HAStatusEnum.NotReady)
                return null;

            final UUID leaderId = quorum.getLeaderId();

            if (leaderId == null)
                return null;

            final HAGlue leader = localService.getService(leaderId);

            if (leader.getHAStatus() != HAStatusEnum.Leader) {

                return null;
                
            }
                
            final IRootBlockView leaderRB = leader.getRootBlock(
                    new HARootBlockRequest(null/* storeUUID */)).getRootBlock();

            final IRootBlockView localRB = journal.getRootBlockView();

            if (leaderRB.getCommitCounter() == localRB.getCommitCounter()) {

                // At the same commit point.
                return null;

            }

            // Re-verify.
            if (journal.getHAStatus() != HAStatusEnum.NotReady)
                return null;

            return enterRunState(new RebuildTask(token));
            
        }
        
        /**
         * Change the run state (but it will not re-enter the currently active
         * state).
         * 
         * @param runStateTask
         *            The task for the new run state.
         * 
         * @return The {@link Future} of the newly submitted run state -or-
         *         <code>null</code> if the service is already in that run
         *         state.
         */
        private Future<Void> enterRunState(
                final RunStateCallable<Void> runStateTask) {

            if (runStateTask == null)
                throw new IllegalArgumentException();

            if (quorumStartStopGuard.get()) {

                /*
                 * We can not change the run state while we are in either
                 * quorumService.start() or quorumService.terminate().
                 */
                
                throw new IllegalStateException();

            }
            
            synchronized (runStateRef) {

                if (runStateTask.runState
                        .equals(lastSubmittedRunStateRef.get())) {
                    
                    // But also need to check if future isDone
                    final FutureTask<Void> ft = runStateFutureRef.get();

                    if (!(ft.isDone() || ft.isCancelled())) {
                    
                        /*
                         * Do not re-enter the same run state.
                         * 
                         * Note: This was added to prevent re-entry of the
                         * ErrorState when we are already in the ErrorState.
                         */
                        
                        haLog.warn("Will not reenter active run state: "
                                + runStateTask.runState);
                        
                        return null;
                        
                    }

                }
               
                final FutureTask<Void> ft = new FutureTaskMon<Void>(
                        runStateTask);

                final Future<?> oldFuture = runStateFutureRef.get();

                boolean success = false;

                try {

                    runStateFutureRef.set(ft);
                    
                    // set before we submit the task.
                    lastSubmittedRunStateRef.set(runStateTask.runState);

                    // submit future task.
                    journal.getExecutorService().submit(ft);

                    success = true;

//                    if (haLog.isInfoEnabled())
//                        haLog.info("Entering runState="
//                                + runStateTask.getClass().getSimpleName());

                    return ft;
                    
                } finally {

                    if (oldFuture != null) {

                        oldFuture.cancel(true/* interruptIfRunning */);

                    }

                    if (!success) {

                        log.error("Unable to submit task: " + runStateTask);
                        
                        ft.cancel(true/* interruptIfRunning */);

                        runStateFutureRef.set(null);

                        lastSubmittedRunStateRef.set(null);
                        
                    }

                }

            }

        }
        
//        /**
//         * Used to submit {@link RunStateCallable} tasks for execution from
//         * within the zk watcher thread.
//         * 
//         * @param task
//         *            The task to be run.
//         */
//        private void enterRunStateFromWatcherThread(
//                final RunStateCallable<Void> task) {
//
//            // Submit task to handle this event.
//            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
//                    new SubmitRunStateTask(task)));
//
//        }
//        
//        /**
//         * Class is used to force a run state transition based on an event
//         * received in the zk watcher thread. 
//         */
//        private class SubmitRunStateTask implements Callable<Void> {
//            private final RunStateCallable<Void> task;
//            public SubmitRunStateTask(final RunStateCallable<Void> task) {
//                this.task = task;
//            }
//            public Void call() throws Exception {
//                enterRunState(task);
//                return null;
//            }
//        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Cleans up the return type.
         */
        @SuppressWarnings("unchecked")
        @Override
        public Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {

            return (Quorum<HAGlue, QuorumService<HAGlue>>) super.getQuorum();
            
        }

        /**
         * @param logicalServiceZPath
         * @param serviceId
         * @param remoteServiceImpl
         *            The object that implements the {@link Remote} interfaces
         *            supporting HA operations.
         * @param store
         *            The {@link HAJournal}.
         */
        public HAQuorumService(final String logicalServiceZPath,
                final UUID serviceId, final S remoteServiceImpl, final L store,
                final HAJournalServer server) {

            super(logicalServiceZPath, serviceId, remoteServiceImpl, store);

            this.journal = store;
            this.logLock = store.getHALogNexus().getLogLock();
            this.server = server;

        }

        @Override
        public int getPID() {

            return server.getPID();
            
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * This implementation resolves an {@link HAGlue} object from its
         * Service UUID using the <strong>pre-existing</strong> connection for
         * the {@link HAClient} and the cached service discovery lookup for that
         * connection. If the {@link HAClient} is not connected, then an
         * {@link IllegalStateException} will be thrown.
         * 
         * @param serviceId
         *            The {@link UUID} of the service to be resolved.
         * 
         * @return The proxy for the service having the specified {@link UUID}
         *         and never <code>null</code>.
         * 
         * @throws IllegalStateException
         *             if the {@link HAClient} is not connected.
         * @throws QuorumException
         *             if no service can be discovered for that {@link UUID}.
         */
        @Override
        public S getService(final UUID serviceId) {
            
            // Throws IllegalStateException if not connected (HAClient).
            final HAGlueServicesClient discoveryClient = server
                    .getHAClient().getConnection().getHAGlueServicesClient();

            final ServiceItem serviceItem = discoveryClient
                    .getServiceItem(serviceId);
            
            if (serviceItem == null) {

                // Not found (per the API).
                throw new QuorumException("Service not found: uuid="
                        + serviceId);

            }

            @SuppressWarnings("unchecked")
            final S service = (S) serviceItem.service;

            return service;
            
        }

        @SuppressWarnings("unchecked")
        protected void doLocalCommit(final IRootBlockView rootBlock) {

            journal.doLocalCommit((QuorumService<HAGlue>) HAQuorumService.this,
                    rootBlock);
            
        }

        /*
         * QUORUM EVENT HANDLERS
         * 
         * Note: DO NOT write event handlers that submit event transitions to
         * any state other than the ERROR state. The ERROR state will eventually
         * transition to SeekConsensus. Once we are no longer in the ERROR
         * state, the states will naturally transition among themselves (until
         * the next serviceLeave(), quorumBreak(), etc.)
         */
        
        @Override
        public void quorumMeet(final long token, final UUID leaderId) {

            super.quorumMeet(token, leaderId);

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new QuorumMeetTask(token, leaderId)));

        }

        private class QuorumMeetTask implements Callable<Void> {
            private final long token;
            public QuorumMeetTask(final long token, final UUID leaderId) {
                this.token = token;
            }
            @Override
            public Void call() throws Exception {
                journal.setQuorumToken(token);
                if (isJoinedMember(token)) {
                    /*
                     * When a quorum meets, the write replication pipeline will
                     * cause the HALog to be opened for live writes. However, we
                     * also need to cause the log to be opened if there are no
                     * replicated writes so a service can resync with the
                     * leader, including with the live log (which will be empty
                     * if there are no writes on the leader during the resync).
                     */
                    logLock.lock();
                    try {
                        if (!journal.getHALogNexus().isHALogOpen()) {
                            if (log.isInfoEnabled())
                         	log.info("Disable log on QuorumMeet");
                            journal.getHALogNexus().disableHALog();
                            journal.getHALogNexus().createHALog(
                                    journal.getRootBlockView());
                        }
                    } finally {
                        logLock.unlock();
                    }
                }
                return null;
            }
        }
        
        @Override
        public void quorumBreak() {

            super.quorumBreak();

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new EnterErrorStateTask()));

        }

        /**
         * {@inheritDoc}
         * <p>
         * If there is a fully met quorum, then we can purge all HA logs
         * <em>EXCEPT</em> the current one.
         */
        @Override
        public void serviceLeave() {

            super.serviceLeave();

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new EnterErrorStateTask()));
        }

        /**
         * {@inheritDoc}
         * <p>
         * Force the service to enter the {@link ErrorTask} when if the
         * zookeeper session is expired.
         */
        @Override
        public void disconnected() {

            enterErrorState();
            
        }
        
        /**
         * Transition to {@link RunStateEnum#Error}.
         */
        private class EnterErrorStateTask implements Callable<Void> {

            protected EnterErrorStateTask() {
                /*
                 * Note: This tells us the code path from which submitted the
                 * task to enter the ERROR state.
                 */
                log.warn("", new StackInfoReport());
            }

            public Void call() throws Exception {
                enterErrorState();
                return null;
            }

        }

//        /**
//         * {@inheritDoc}
//         * <p>
//         * If there is a fully met quorum, then we can purge all HA logs
//         * <em>EXCEPT</em> the current one.
//         */
//        @Override
//        public void serviceJoin() {
//
//            super.serviceJoin();
//
////            // Submit task to handle this event.
////            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
////                    new ServiceJoinTask()));
//        }
//
//        /**
//         * Purge HALog files on a fully met quorum.
//         */
//        private class ServiceJoinTask implements Callable<Void> {
//            public Void call() throws Exception {
//
//                final long token = getQuorum().token();
//
//                if (getQuorum().isQuorumFullyMet(token)) {
//                    /*
//                     * TODO Even though the quorum is fully met, we should wait
//                     * until we have a positive indication from the leader that
//                     * it is "ha ready" before purging the HA logs and aging put
//                     * snapshots. The leader might need to explicitly schedule
//                     * this operation against the joined services and the
//                     * services should then verify that the quorum is fully met
//                     * before they actually age out the HALogs and snapshots.
//                     */
//                    purgeHALogs(token);
//
//                }
//                
//                return null;
//                
//            }
//        }

        @Override
        public void memberRemove() {

            super.memberRemove();

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new EnterErrorStateTask()));

        }

        /**
         * Handle an error condition on the service.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
         *      HAJournalServer reports "follower" but is in SeekConsensus and
         *      is not participating in commits</a>
         */
        private class ErrorTask extends RunStateCallable<Void> {
            
            protected ErrorTask() {

                super(RunStateEnum.Error);
                // Note: This stack trace does not have any useful info.
//                log.warn("", new StackInfoReport());
                
            }
            
            @Override
            public Void doRun() throws Exception {
            	
                while (true) {

                    log.warn("Will do error handler.");
//                    ((AbstractQuorum<HAGlue, QuorumService<HAGlue>>) getQuorum()).interruptAll();
                    /*
                     * Discard the current write set.
                     * 
                     * Note: This is going to call through to discardWriteSet().
                     * That method will close out the current HALog and discard
                     * the last live write message.
                     * 
                     * FIXME the setQuorumToken() after the serviceLeave() will
                     * also cause doLocalAbort() to be called, so we probably do
                     * NOT want to call it here.
                     */
                    journal.doLocalAbort();

                    /*
                     * Set token. Journal will notice that it is no longer
                     * "HA Ready"
                     * 
                     * Note: We update the haReadyToken and haStatus regardless
                     * of whether the quorum token has changed in case this
                     * service is no longer joined with a met quorum.
                     * 
                     * Note: AbstractJournal.setQuorumToken() will detect case
                     * where it transitions from a met quorum through a service
                     * leave and will clear its haReady token and update its
                     * haStatus field appropriately. (This is why we pass in
                     * quorum.token() rather than NO_QUORUM.)
                     * 
                     * TODO There are cases where nothing changes that may hit
                     * an AssertionError in setQuorumToken().
                     * 
                     * TODO This will (conditionally) trigger doLocalAbort().
                     * Since we did this explicitly above, that can be do
                     * invocations each time we pass through here!
                     */
                    if (log.isInfoEnabled())
                        log.info("Current Token: haJournalReady="
                                + journal.getHAReady()
                                + ", getQuorum().token()=: "
                                + getQuorum().token());

                    journal.clearQuorumToken(getQuorum().token());

                    /*
                     * Spin for up to the configured timeout (or the negotiated
                     * timeout, whichever is less) before failing a zookeeper
                     * session that is not connected (if not cured within a
                     * timeout, then we treat this the same as if not alive).
                     * 
                     * If we are (or become) connected with zookeeper before the 
                     * timeout, then do serviceLeave() for this service.
                     */
                    // session timeout as configured.
                    final int sessionTimeout1 = server.getHAClient().zooConfig.sessionTimeout;
                    int sessionTimeout = sessionTimeout1;
                    final long begin = System.nanoTime();
                    while (true) {
                        final HAConnection cxn;
                        try {
                            cxn = server.getHAClient().getConnection();
                        } catch (IllegalStateException ex) {
                            log.error("Tearing down service: HAClient not connected.");
                            restartHAQuorumService();
                            break;
                        }
                        final ZooKeeper zk = cxn.getZookeeper();
                        if (!zk.getState().isAlive()) {
                            log.error("Tearing down service: ZK Session is expired");
                            restartHAQuorumService();
                        }
                        // negotiated session timeout -or- ZERO (0).
                        final int sessionTimeout2 = zk.getSessionTimeout();
                        if (sessionTimeout2 > 0
                                && sessionTimeout2 < sessionTimeout1) {
                            // Reduce to negotiated timeout GT ZERO.
                            sessionTimeout = sessionTimeout2;
                        }
                        if (zk.getState() == ZooKeeper.States.CONNECTED) {
                            break;
                        }
                        final long elapsed = System.nanoTime() - begin;
                        if (elapsed > TimeUnit.MILLISECONDS
                                .toNanos(sessionTimeout)) {
                            /*
                             * TODO This forces the connection from CONNECTING
                             * to CLOSED if we are in the Error state and unable
                             * to connect to zookeeper. This might not be
                             * strictly necessary.
                             */
                            log.error("Tearing down service: ZK Session remains disconnected for "
                                    + TimeUnit.NANOSECONDS.toMillis(elapsed)
                                    + "ms, effectiveTimeout=" + sessionTimeout);
                            restartHAQuorumService();
                            break;
                        }
                        // Sleep a bit, then check again.
                        Thread.sleep(100/* ms */);
                    }
                        
                    log.warn("Will attempt SERVICE LEAVE");
                    getActor().serviceLeave(); // Just once(!)

                    /**
                     * Dispatch Events before entering SeekConsensus! Otherwise
                     * the events triggered by the serviceLeave() and
                     * setQuorumToken will not be handled until we enter
                     * SeekConsensus, and then when they are received
                     * SeekConsensus will fail.
                     * 
                     * The intention of this action is to ensure that when
                     * SeekConsensus is entered the service is in a "clean"
                     * state.
                     */
                    processEvents();

                    /**
                     * The error task needs to validate that it does not need to
                     * re-execute and may transition to SeekConsensus. The
                     * specific problem is when a SERVICE_LEAVE leads to a
                     * QUORUM_BREAK event. When we handle the SERVICE_LEAVE the
                     * quorum's token has not yet been cleared. However, it must
                     * be cleared when the QUORUM_BREAK comes around. Since we
                     * do not permit re-entry into the ErrorTask in
                     * enterRunState(), we need to check this post-condition
                     * here.
                     * <p>
                     * The relevant tests are testAB_BounceFollower and
                     * testAB_BounceLeader and also testAB_RestartLeader and
                     * testAB_RestartFollower. In addition, any test where we
                     * fail the leader could require this 2nd pass through
                     * ErrorTask.doRun().
                     */
                    final long t1 = journal.getQuorumToken();
                    
                    final long t2 = journal.getQuorum().token();
                    
                    if (t1 == t2) {

                        haLog.warn("Will not re-do error handler"
                                + ": journal.quorumToken=" + t1
                                + ", quorum.token()=" + t2);

                        break;

                    }

                    // Sleep here to avoid a tight loop?

                } // while(true)

                // Seek consensus.
                enterRunState(new SeekConsensusTask());

                return null;

            } // doRun()

            /**
             * Restart the {@link HAQuorumService}. This gets invoked from the
             * {@link ErrorTask} if it notices that the {@link ZooKeeper} client
             * session has been expired or if the {@link ZooKeeper} client
             * connection is disconnected and that disconencted state is not
             * cured within a reasonable period of time.
             * <p>
             * The {@link HAQuorumService} is torn down, which includes closing
             * the {@link HAClient}.
             * <p>
             * The {@link HAQuorumService} is then restarted. This includes
             * obtaining a new {@link HAClient}, which means that we also have a
             * new {@link ZooKeeper} client and a new sesssion.
             */
            private void restartHAQuorumService()  {
                journal.getExecutorService().submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            /*
                             * Note: This will interrupt thread running the
                             * ErrorTask, which is the thread that submitted
                             * this Runnable. However, the Runnable itself is
                             * not interrupted.
                             */
                            log.warn("HAQuorumService: TERMINATE");
                            journal.getQuorum().terminate();
                            /*
                             * Force the clear of the token since disconnected
                             * from zookeeper.
                             */
                            journal.clearQuorumToken(Quorum.NO_QUORUM);
                        } catch (Throwable t) {
                            log.error(t, t);
                            // Re-enter the ErrorTask.
                            enterErrorState();
                        }
                        try {
                            log.warn("HAQuorumService: START");
                            journal.getQuorum()
                                    .start((QuorumService<HAGlue>) HAQuorumService.this);
                        } catch (Throwable t) {
                            log.error(t, t);
                            // Re-enter the ErrorTask.
                            enterErrorState();
                        }
                        /*
                         * Done successfully. The HAQuorumService should have
                         * been restarted.
                         */
                    }
                });
                // Sleep until interrupted.
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    /*
                     * Note: We expect to be interrupted when the ErrorTask is
                     * cancelled. We do not want code in the caller to resume so
                     * throw out this exception.
                     */
                    throw new RuntimeException(e);
                }
            }

        } // class ErrorTask
        
        /**
         * Task to handle a quorum break event.
         */
        private class SeekConsensusTask extends RunStateCallable<Void> {

            protected SeekConsensusTask() {

                super(RunStateEnum.SeekConsensus);
                
            }

            @Override
            public Void doRun() throws Exception {

                /*
                 * Pre-conditions.
                 */
                {

                    final long token = getQuorum().token();

                    if (isJoinedMember(token))  {
                        // Service already joined.
                        throw new IllegalStateException("Service joined.");
                    }

                    if (getQuorum().getCastVote(getServiceId()) != null) {
                        // Vote already cast.
                        throw new IllegalStateException("Vote already cast.");
                    }

                    if (journal.getHALogNexus().isHALogOpen())
                        throw new IllegalStateException("HALogWriter is open.");

                }

                /*
                 * Attempt to add the service as a member, add the service to
                 * the pipeline, and conditionally cast a vote for the last
                 * commit time IFF the quorum has NOT met.
                 * 
                 * If the service is already a quorum member or already in the
                 * pipeline, then those are NOPs.
                 * 
                 * If the quorum is already met, then the service DOES NOT cast
                 * a vote for its lastCommitTime - it will need to resynchronize
                 * instead.
                 */

                // ensure member.
                getActor().memberAdd();

                // ensure in pipeline.
                getActor().pipelineAdd();

                /**
                 * Make sure that the pipelineAdd() is handled before
                 * continuing. Handling this event will setup the
                 * HAReceiveService. This is necessary since the event is no
                 * longer synchronously handled.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
                 *      HAJournalServer reports "follower" but is in
                 *      SeekConsensus and is not participating in commits</a>
                 */
                processEvents();
                
                {

                    final long token = getQuorum().token();

                    if (token != Quorum.NO_QUORUM) {

                        /*
                         * Quorum is already met.
                         */
                        
                        // Resync since quorum met before we cast a vote.
                        enterRunState(new ResyncTask(token));

                        // Done.
                        return null;
                        
                    }
                    
                }

                /*
                 * Cast a vote for our lastCommitTime.
                 * 
                 * Note: If the quorum is already met, then we MUST NOT cast a
                 * vote for our lastCommitTime since we can not join the met
                 * quorum without attempting to synchronize.
                 */

                final long lastCommitTime = journal.getLastCommitTime();

                getActor().castVote(lastCommitTime);

                // await the quorum meet.
                final long token = getQuorum().awaitQuorum();

                if (!isJoinedMember(token)) {

                    /*
                     * The quorum met on a different vote.
                     */

                    // Resync with the quorum.
                    enterRunState(new ResyncTask(token));

                } else {

                    final UUID leaderId = getQuorum().getLeaderId();

                    // Transition to RunMet.
                    enterRunState(new RunMetTask(token, leaderId));

                }

                // Done.
                return null;
                
            }

        }

        /**
         * While the quorum is met, accept replicated writes, laying them down
         * on the HALog and the backing store, and participate in the 2-phase
         * commit protocol.
         */
        private class RunMetTask extends RunStateCallable<Void> {

            private final long token;
            private final UUID leaderId;

            public RunMetTask(final long token, final UUID leaderId) {
                
                super(RunStateEnum.RunMet);

                this.token = token;
                this.leaderId = leaderId;
                
            }
            
            @Override
            public Void doRun() throws Exception {

                /*
                 * Guards on entering this run state.
                 */
                {

                    // Verify leader is still the same.
                    if (!leaderId.equals(getQuorum().getLeaderId()))
                        throw new InterruptedException();

                    // Verify we are talking about the same token.
                    getQuorum().assertQuorum(token);

                    if (!isJoinedMember(token)) {
                        /*
                         * The quorum met, but we are not in the met quorum.
                         * 
                         * Note: We need to synchronize in order to join an
                         * already met quorum. We can not just vote our
                         * lastCommitTime. We need to go through the
                         * synchronization protocol in order to make sure that
                         * we actually have the same durable state as the met
                         * quorum.
                         */
                        throw new InterruptedException();
                    }
                    
                } // validation of pre-conditions.

                /*
                 * Conditionally take a snapshot of the journal iff there is no
                 * existing snapshot. The journal may or may not be empty, but
                 * we do not have any existing snapshots and we need to have one
                 * to serve as a restore point. The service MUST be joined with
                 * a met quorum in order to take a snapshot.
                 */
                {

                    if (isLeader(token)) {

                        /*
                         * Conditionally create the default KB instance.
                         * 
                         * Note: This task is *also* launched by the NSS life
                         * cycle class (BigdataRDFServletContextListener).
                         * However, that class only makes one attempt. If the
                         * quorum is not running at the time the attempt is
                         * made, it will fail with an
                         * AsynchronousQuorumCloseException. By submitting this
                         * task on the leader, we will make the attempt once the
                         * quorum meets.
                         */
                        
                        server.conditionalCreateDefaultKB();

                    }

                    // Await the initial KB create commit point.
                    while (journal.getRootBlockView().getCommitCounter() < 1) {

                        Thread.sleep(100/* ms */);
                        
                    }

                    // Conditionally request initial snapshot.
                    final Future<IHASnapshotResponse> ft = journal
                            .getSnapshotManager().takeInitialSnapshot();

                    if (ft != null) {

                        /*
                         * Wait for outcome.
                         * 
                         * Note: Even though we are blocking on the Future, the
                         * service is live and can receive writes. Once the
                         * Future is done, we are just going to block anyway in
                         * blockInterruptably().
                         * 
                         * Note: An exception thrown here will cause the service
                         * to transition into the error state.
                         */
                        ft.get();

                    }
                    
                }

                // Block until this run state gets interrupted.
                blockInterruptably();
                
                // Done.
                return null;
                
            } // call()
            
        } // class RunMetTask

        /**
         * While the quorum is met, accept replicated writes, laying them down
         * on the HALog and the backing store, and participate in the 2-phase
         * commit protocol.
         */
        private class OperatorTask extends RunStateCallable<Void> {

            final String msg;

            public OperatorTask(final String msg) {

                super(RunStateEnum.Operator);

                this.msg = msg;

            }

            @Override
            public Void doRun() throws Exception {

                try {

                    server.sendOperatorAlert(msg);
                    
                    // Block until this run state gets interrupted.
                    blockInterruptably();

                    // Done.
                    return null;
                    
                } finally {
                    
                    server.clearOperatorAlert();
                    
                }
                
            } // call()
            
        } // class OperatorTask

        /**
         * Run state responsible for replaying local HALog files during service
         * startup. This allows an offline restore of the backing journal file
         * (a full backup) and zero or more HALog files (each an incremental
         * backup corresponding to a single commit point). When the service
         * starts, it will replay each HALog file for the successor of its then
         * current commit counter.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class RestoreTask extends RunStateCallable<Void> {
         
            protected RestoreTask() {

                super(RunStateEnum.Restore);

            }

            @Override
            protected Void doRun() throws Exception {

                while (true) {

                    final long commitCounter = journal.getRootBlockView()
                            .getCommitCounter();

                    IHALogReader r = null;

                    try {

                        r = journal.getHALogNexus()
                                .getReader(commitCounter + 1);

                        if (r.isEmpty()) {
                            
                            /*
                             * There is an empty HALog file. We can not apply it
                             * since it has no data. This ends our restore
                             * procedure.
                             */
                            
                            break;

                        }
                        
                        if (r.getOpeningRootBlock().getCommitCounter() != commitCounter) {
                            // Sanity check
                            throw new AssertionError();
                        }

                        if (r.getClosingRootBlock().getCommitCounter() != commitCounter + 1) {
                            // Sanity check
                            throw new AssertionError();
                        }
                        
                        applyHALog(r);

                        doLocalCommit(r.getClosingRootBlock());

                    } catch (FileNotFoundException ex) {

                        /*
                         * No such HALog file. Ignore and exit this loop.
                         */
                        break;

                    } catch (IOException ex) {

                        log.error("Problem reading HALog file: commitCounter="
                                + commitCounter + ": " + ex, ex);
                        
                        break;

                    } finally {
                        
                        if (r != null) {

                            r.close();

                        }
                        
                    }

                }

                // Submit task to seek consensus.
                enterRunState(new SeekConsensusTask());

                // Done.
                return null;

            }

            /**
             * Apply the write set to the local journal.
             * 
             * @param r
             *            The {@link IHALogReader} for the HALog file containing
             *            the write set.
             *            
             * @throws IOException
             * @throws InterruptedException
             */
            private void applyHALog(final IHALogReader r) throws IOException,
                    InterruptedException {

                final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

                try {

                    while (r.hasMoreBuffers()) {

                        // get message and fill write cache buffer (unless
                        // WORM).
                        final IHAWriteMessage msg = r.processNextBuffer(buf
                                .buffer());

                        writeWriteCacheBlock(msg, buf.buffer());
                        
                    }

                    haLog.warn("Applied HALog: closingCommitCounter="
                            + r.getClosingRootBlock().getCommitCounter());

                } finally {

                    buf.release();

                }
            }

        }
        
        /**
         * Rebuild the backing store from scratch.
         * <p>
         * If we can not replicate ALL log files for the commit points that we
         * need to catch up on this service, then we can not incrementally
         * resynchronize this service and we will have to do a full rebuild of
         * the service instead.
         * <p>
         * A rebuild begins by pinning the history on the quorum by asserting a
         * read lock (a read-only tx against then current last commit time).
         * This prevents the history from being recycled, but does not prevent
         * concurrent writes on the existing backing store extent, or extension
         * of the backing store. This read lock is asserted by the quourm leader
         * when we ask it to send its backing store over the pipeline.
         * <p>
         * The copy backing file will be consistent with the root blocks on the
         * leader at the point where we have taken the read lock. Once we have
         * copied the backing file, we then put down the root blocks reported
         * back from the {@link HAPipelineGlue#sendHAStore(IHARebuildRequest)}
         * method.
         * <p>
         * At this point, we are still not up to date. We transition to
         * {@link ResyncTask} to finish catching up with the quorum.
         * <p>
         * Note: Rebuild only guarantees consistent data, but not binary
         * identity of the backing files since there can be ongoing writes on
         * the leader.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class RebuildTask extends RunStateCallable<Void> {

            /**
             * The quorum token in effect when we began the rebuild.
             */
            private final long token;

            public RebuildTask(final long token) {

                super(RunStateEnum.Rebuild);
                
                // run while quorum is met.
                this.token = token;

            }

            @Override
            protected Void doRun() throws Exception {

                /*
                 * DO NOT start a rebuild if the quorum is broken. Instead, we
                 * will try again after SeekConsensus.
                 */
                getQuorum().assertQuorum(token);
                
                /*
                 * The quorum leader (RMI interface). This is fixed until the
                 * quorum breaks.
                 */
                final S leader = getLeader(token);

                /*
                 * Rebuild needs to throw away anything that is buffered on the
                 * local backing file to prevent any attempts to interpret the
                 * data on the backing file in light of its current root blocks.
                 * To do this, we overwrite the root blocks.
                 * 
                 * FIXME Do not wipe out our root block until we have positive
                 * information from the leader that it is *running* as the
                 * leader. In particular, it needs to have the quorum token set
                 * on the journal and a few other things in place. This does not
                 * exactly correspond to the pre-conditions on RunMet. It is
                 * more like the post-conditions on pipelineSetup().
                 * 
                 * Note: We MUST NOT install the local root blocks unless both
                 * this service and the leader at at commitCounter ZERO(0L).
                 */
                // Wait for the new root blocks.
                awaitJournalToken(token);
                {

                    /*
                     * The current root block on the leader (We want to get some
                     * immutatable metadata from the leader's root block).
                     */
                    final IRootBlockView rb = leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock();// journal.getRootBlockView();

                    // Use timestamp.
                    final long createTime = System.currentTimeMillis();

                    // New root blocks for a (logically) empty Journal.
                    final RootBlockUtility rbu = new RootBlockUtility(//
                            journal.getBufferStrategy().getBufferMode(), //
                            rb.getOffsetBits(),  // from leader.
                            createTime, //
                            token, //
                            rb.getUUID() // storeUUID from leader.
                            );

                    // Verify that the quorum remains met on this token.
                    getQuorum().assertQuorum(token);

                    /*
                     * Critical section.
                     * 
                     * Up to now we have not modified anything on the disk. Now
                     * we are going to destroy the local data (both backups and
                     * the root blocks of the journal).
                     */
                    {

                        /*
                         * Get rid of any existing backups. They will not be
                         * consistent with the rebuild.
                         */
                        deleteBackups();

                        /*
                         * Install both root blocks.
                         * 
                         * Note: This will take us through a local abort. That
                         * is important. We need to discard any writes that
                         * might have been buffered before we start the
                         * resynchronization of the local store.
                         */
                        installRootBlocks(rbu.rootBlock0, rbu.rootBlock1);

                    }
                    
                    // Note: Snapshot requires joined with met quorum.
//                    /*
//                     * Take a snapshot.
//                     */
//                    journal.getSnapshotManager().takeInitialSnapshot();

                }

                /*
                 * Replicate the backing store of the leader.
                 * 
                 * Note: This remoteFuture MUST be cancelled if the RebuildTask
                 * is interrupted.
                 */
                final Future<IHASendStoreResponse> remoteFuture = leader
                        .sendHAStore(new HARebuildRequest(getServiceId()));

                final IHASendStoreResponse resp;
                
                try {

                    // Wait for the raw store to be replicated.
                    resp = remoteFuture.get();
                    
                    log.warn("REBUILD: Copied backing store from leader.");
                    
                } finally {

                    // Ensure remoteFuture is cancelled.
                    remoteFuture.cancel(true/* mayInterruptIfRunning */);
                    
                }
                
                // Verify that the quorum remains met on this token.
                getQuorum().assertQuorum(token);
                
                // Caught up on the backing store as of that copy.
                installRootBlocks(resp.getRootBlock0(), resp.getRootBlock1());

                log.warn("REBUILD: installed root blocks @ commitCounter="
                        + journal.getRootBlockView().getCommitCounter()
                        + ": rb0=" + resp.getRootBlock0() + ", rb1="
                        + resp.getRootBlock1());
                
                // Resync.
                enterRunState(new ResyncTask(token));

                // Done.
                return null;
                
            }
            
        } // class RebuildTask
        
        /**
         * This class handles the resynchronization of a node that is not at the
         * same commit point as the met quorum. The task will replicate write
         * sets (HA Log files) from the services in the met quorum, and apply
         * those write sets (in pure commit sequence) in order to advance its
         * own committed state to the same last commit time as the quorum
         * leader. Once it catches up with the quorum, it still needs to
         * replicate logged writes from a met services in the quorum until it
         * atomically can log from the write pipeline rather than replicating a
         * logged write. At that point, the service can vote its lastCommitTime
         * and will join the met quorum.
         * <p>
         * Note: In fact, the service can always begin logging from the write
         * pipeline as soon as it observes (or acquires) the root block and
         * observes the seq=0 write cache block (or otherwise catches up with
         * the write pipeline). However, it can not write those data onto the
         * local journal until it is fully caught up. This optimization might
         * allow the service to catch up slightly faster, but the code would be
         * a bit more complex.
         */
        private class ResyncTask extends RunStateCallable<Void> {

            /**
             * The quorum token in effect when we began the resync.
             */
            private final long token;

            public ResyncTask(final long token) {
                
                super(RunStateEnum.Resync);

                // run while quorum is met.
                this.token = token;

            }

            /**
             * Replicate each write set for commit points GT the current commit
             * point on this service. As each write set is replicated, is also
             * applied and we advance to another commit point. This method loops
             * until we have all write sets locally replicated and the service
             * is able to begin accepting write cache blocks from the write
             * pipeline rather than through the resynchronization protocol.
             * <p>
             * Note: This task will be interrupted when the service catches up
             * and is able to log and write the write cache block from the write
             * pipeline. At that point, we no longer need to replicate the write
             * sets from the leader.
             * 
             * @throws Exception
             */
            @Override
            protected Void doRun() throws Exception {

//                // Wait for the token to be set, root blocks to be valid.
//                awaitJournalToken(token, true/* awaitRootBlocks */);
                pipelineSetup();

                /*
                 * Note: We need to discard any writes that might have been
                 * buffered before we start the resynchronization of the local
                 * store. Otherwise they could wind up flushed through by the
                 * RWStore (for example, when handling a file extension).
                 * 
                 * Note: This IS necessary. We do a low-level abort when we
                 * install the root blocks from the quorum leader before we sync
                 * the first commit point, but we do not do the low-level abort
                 * if we already have the correct root blocks in place.
                 */

                journal.doLocalAbort();

                /*
                 * We will do a local commit with each HALog (aka write set)
                 * that is replicated. This let's us catch up incrementally with
                 * the quorum.
                 */

                /*
                 * The quorum leader (RMI interface). This is fixed until the
                 * quorum breaks.
                 */
                
                final UUID leaderId = getQuorum().getLeaderId();
                
                final S leader = getLeader(token);

                /*
                 * Loop until joined with the met quorum (and HAReady).
                 * 
                 * Note: The transition will occur atomically either when we
                 * catch up with the live write or at a commit point.
                 * 
                 * Note: This loop will go through an abnormal exit if the
                 * quorum breaks or reforms (thrown error). The control when
                 * then pass through the ErrorTask and back into SeekConsensus.
                 */
                while (true) {

                    // The current commit point on the local store.
                    final long commitCounter = journal.getRootBlockView()
                            .getCommitCounter();

                    // Replicate and apply the next write set
                    replicateAndApplyWriteSet(leaderId, leader, token,
                            commitCounter + 1);

                }
                
            }

        } // class ResyncTask

        /**
         * Replicate the write set having the specified commit counter, applying
         * each {@link WriteCache} block as it is received and eventually going
         * through a local commit when we receiving the closing
         * {@link IRootBlockView} for that write set.
         * 
         * @param leader
         *            The quorum leader.
         * @param token
         *            The quorum token that must remain valid throughout this
         *            operation.
         * @param closingCommitCounter
         *            The commit counter for the <em>closing</em> root block of
         *            the write set to be replicated.
         * 
         * @throws IOException
         * @throws FileNotFoundException
         * @throws ExecutionException
         * @throws InterruptedException
         */
        private void replicateAndApplyWriteSet(final UUID leaderId,
                final S leader, final long token,
                final long closingCommitCounter) throws FileNotFoundException,
                IOException, InterruptedException, ExecutionException {

            if (leader == null)
                throw new IllegalArgumentException();

            if (closingCommitCounter <= 0)
                throw new IllegalArgumentException();

            // Abort if the quorum breaks.
            getQuorum().assertQuorum(token);

            if (haLog.isInfoEnabled())
                haLog.info("RESYNC: commitCounter=" + closingCommitCounter);

            final IHALogRootBlocksResponse resp;
            try {
                
                // Request the root blocks for the write set.
                resp = leader
                        .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                closingCommitCounter));

            } catch (FileNotFoundException ex) {

                /*
                 * Oops. The leader does not have that log file.
                 * 
                 * We will have to rebuild the service from scratch since we do
                 * not have the necessary HA Log files to synchronize with the
                 * existing quorum.
                 * 
                 * Note: If we are already doing a REBUILD (incremental=false),
                 * then we will restart the REBUILD. Rationale: if someone
                 * restarts the service after the shutdown, it is going to do
                 * another REBUILD anyway. Likely causes for the missing HALog
                 * on the leader are file system errors or someone deleting the
                 * HALog files that we need. However, REBUILD is still
                 * semantically correct so long as we restart the procedure.
                 * 
                 * TODO RESYNC: It is possible to go to another service in the
                 * met quorum for the same log file, but it needs to be a
                 * service that is UPSTREAM of this service.
                 */

                final String msg = "HALog not available: commitCounter="
                        + closingCommitCounter;

                log.error(msg);

                if (server.onelineDisasterRecovery) {
                    enterRunState(new RebuildTask(token));
                } else {
                    enterRunState(new OperatorTask(msg));
                }

                // Force immediate exit of the resync protocol.
                throw new InterruptedException(msg);

            }

            // root block when the quorum started that write set.
            final IRootBlockView openRootBlock = resp.getOpenRootBlock();
            final IRootBlockView tmpCloseRootBlock = resp.getCloseRootBlock();

            if (openRootBlock.getCommitCounter() != closingCommitCounter - 1) {
                
                /*
                 * The opening root block for the write set must have a
                 * commit counter that is ONE less than the requested commit
                 * point.
                 */
                
                throw new AssertionError(
                        "Should start at the previous commit point: requested commitCounter="
                                + closingCommitCounter + ", openRootBlock="
                                + openRootBlock);
            }
            
            /*
             * If the local journal is empty, then we need to replace both of
             * it's root blocks with the opening root block.
             */
            if (closingCommitCounter == 1) {

                // Install the initial root blocks.
                installRootBlocks(
                        openRootBlock.asRootBlock(true/* rootBlock0 */),
                        openRootBlock.asRootBlock(false/* rootBlock0 */));

                // Note: snapshot requires joined with met quorum.
//                /*
//                 * Take a snapshot.
//                 */
//                journal.getSnapshotManager().takeInitialSnapshot();

            }

            // Make sure we have the correct HALogWriter open.
            // TODO Replace with pipelineSetup()?
            logLock.lock();
            try {
            
                if (getQuorum().getMember().isJoinedMember(token)) {
                    /*
                     * This can happen if there is a data race with a live write
                     * that is the first write cache block for the write set
                     * that that we would replicate from the ResyncTask. In this
                     * case, we have lost the race to the live write and this
                     * service has already joined as a follower. We can safely
                     * return here since the test in this if() is the same as
                     * the condition variable in the loop for the ResyncTask.
                     * 
                     * @see #resyncTransitionToMetQuorum()
                     */

                    if (journal.getHAReady() != token) {
                        /*
                         * Service must be HAReady before exiting RESYNC
                         * normally.
                         */
                        throw new AssertionError();
                    }

                    // Transition to RunMet.
                    enterRunState(new RunMetTask(token, leaderId));
                    
                    // Force immediate exit of the resync protocol.
                    throw new InterruptedException();

                }
                
                /*
                 * Since we are not joined, the HAReady token must not have been
                 * set.
                 */
                if (journal.getHAReady() != Quorum.NO_QUORUM) {
                    /*
                     * The HAReady token is set by setQuorumToken() and this
                     * should have been done atomically in runWithBarrierLock().
                     * Thus, it is a problem if the HAReady token is set here to
                     * any valid token value.
                     */
                    throw new AssertionError();
                }
                journal.getHALogNexus().disableHALog();
                journal.getHALogNexus().createHALog(openRootBlock);
            } finally {
                logLock.unlock();
            }

            /*
             * Atomic decision whether HALog *was* for live write set when root
             * blocks were obtained from leader.
             * 
             * Note: Even if true, it is possible that the write set could have
             * been committed since then. However, if false then guaranteed that
             * this write set is historical.
             */
            final boolean liveHALog = openRootBlock.getCommitCounter() == tmpCloseRootBlock
                    .getCommitCounter();

            if (liveHALog) {
	            	if (log.isInfoEnabled())
	            		log.info("Joining live log");

                /*
                 * If this was the live write set at the moment when we
                 * requested the root blocks from the leader, then we want to
                 * decide whether or not there have been writes on the leader
                 * for that write set. If there have NOT been any replicated
                 * writes observed on the pipeline for that commitCounter, then
                 * the leader might be quiescent. In this case we want to join
                 * immediately since who knows when the next write cache block
                 * will come through (maybe never if the deployment is
                 * effectively read-only).
                 */

                if (conditionalJoinWithMetQuorum(leader, token,
                        closingCommitCounter - 1)) {

	             if (log.isInfoEnabled())
	             	log.info("CAUGHT UP");
                    /*
                     * We are caught up and have joined the met quorum.
                     * 
                     * Note: Future will be canceled in finally clause.
                     */

                    throw new InterruptedException("Joined with met quorum.");

                }

            }

            /*
             * We need to transfer and apply the write cache blocks from the HA
             * Log file on some service in the met quorum. This code works with
             * the leader, which is known to be upstream from all other services
             * in the write pipeline and is known to be joined with the met
             * quorum. Replicating from the leader is simpler conceptually and
             * makes for simpler code, but we could replicate from any upstream
             * service that is joined with the met quorum.
             */
	     if (log.isInfoEnabled())
            	log.info("replicateAndApplyHALog: " + closingCommitCounter);
            final IRootBlockView closeRootBlock = replicateAndApplyHALog(
                    leader, closingCommitCounter, resp);

            // Local commit.
            doLocalCommit(closeRootBlock);

            // Close out the current HALog writer.
            logLock.lock();
            try {
                journal.getHALogNexus().closeHALog(closeRootBlock);
            } finally {
                logLock.unlock();
            }

            if (haLog.isInfoEnabled())
                haLog.info("Replicated write set: commitCounter="
                        + closingCommitCounter);

        }

        private IRootBlockView replicateAndApplyHALog(final S leader,
                final long closingCommitCounter,
                final IHALogRootBlocksResponse resp) throws IOException,
                InterruptedException, ExecutionException {

            /*
             * Request the HALog from the leader.
             */
            {

                Future<Void> ft = null;
                boolean success = false;
                try {

                    if (haLog.isDebugEnabled())
                        haLog.debug("HALOG REPLICATION START: closingCommitCounter="
                                + closingCommitCounter);

                    ft = leader.sendHALogForWriteSet(new HALogRequest(
                            server.serviceUUID, closingCommitCounter
                    // , true/* incremental */
                            ));

                    // Wait until all write cache blocks are received.
                    ft.get();

                    success = true;

                    /*
                     * Note: Uncaught exception will result in SeekConsensus.
                     */
                } finally {

                    if (ft != null) {
                        // ensure terminated.
                        ft.cancel(true/* mayInterruptIfRunning */);
                    }

                    if (haLog.isDebugEnabled())
                        haLog.debug("HALOG REPLICATION DONE : closingCommitCounter="
                                + closingCommitCounter + ", success=" + success);

                }

            }

            /*
             * Figure out the closing root block. If this HALog file was active
             * when we started reading from it, then the open and close root
             * blocks would have been identical in the [resp] and we will need
             * to grab the root blocks again now that it has been closed.
             */
            final IRootBlockView closeRootBlock;
            {

                final IRootBlockView openRootBlock = resp.getOpenRootBlock();

                // root block when the quorum committed that write set.
                IRootBlockView tmp = resp.getCloseRootBlock();

                if (openRootBlock.getCommitCounter() == tmp.getCommitCounter()) {

                    /*
                     * The open and close commit counters were the same when we
                     * first requested them, so we need to re-request the close
                     * commit counter now that we are done reading on the file.
                     */

                    // Re-request the root blocks for the write set.
                    final IHALogRootBlocksResponse resp2 = leader
                            .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                    closingCommitCounter));

                    tmp = resp2.getCloseRootBlock();

                }

                closeRootBlock = tmp;

                if (closeRootBlock.getCommitCounter() != closingCommitCounter) {

                    throw new AssertionError(
                            "Wrong commitCounter for closing root block: expected commitCounter="
                                    + closingCommitCounter
                                    + ", but closeRootBlock=" + closeRootBlock);

                }

            }

            return closeRootBlock;

        }
        
        /**
         * Conditional join of a service attempting to synchronize with the met
         * quorum. If the current commit point that is being replicated (as
         * indicated by the <i>commitCounter</i>) is thought to be the most
         * current root block on the leader AND we have not received any writes
         * on the HALog, then we assume that the leader is quiescent (no write
         * activity) and we attempt to join the qourum.
         * 
         * @param leader
         *            The quorum leader.
         * @param openingCommitCounter
         *            The commit counter for the <em>opening</em> root block of
         *            the write set that is currently being replicated.
         * 
         * @return <code>true</code> iff we were able to join the met quorum.
         * 
         * @throws InterruptedException
         */
        private boolean conditionalJoinWithMetQuorum(final S leader,
                final long token, final long openingCommitCounter)
                throws IOException, InterruptedException {

//            // Get the current root block from the quorum leader.
//            final IRootBlockView currentRootBlockOnLeader = leader
//                    .getRootBlock(new HARootBlockRequest(null/* storeId */))
//                    .getRootBlock();

            final IHAWriteSetStateResponse currentWriteSetStateOnLeader = leader
                    .getHAWriteSetState(new HAWriteSetStateRequest());

            final boolean sameCommitCounter = currentWriteSetStateOnLeader
                    .getCommitCounter() == openingCommitCounter;

            if (haLog.isDebugEnabled())
                haLog.debug("sameCommitCounter=" + sameCommitCounter
                        + ", openingCommitCounter=" + openingCommitCounter
                        + ", currentWriteSetStateOnLeader="
                        + currentWriteSetStateOnLeader);

            if (!sameCommitCounter
                    || currentWriteSetStateOnLeader.getSequence() > 0L) {

                /*
                 * We can not join immediately. Either we are not at the same
                 * commit point as the quorum leader or the block sequence on
                 * the quorum leader is non-zero.
                 */

                return false;

            }
            
            /*
             * This is the same commit point that we are trying to replicate
             * right now. Check the last *live* HAWriteMessage. If we have not
             * observed any write cache blocks, then we can attempt to join the
             * met quorum.
             * 
             * Note: We can not accept replicated writes while we are holding
             * the logLock (the lock is required to accept replicated writes).
             * Therefore, by taking this lock we can make an atomic decision
             * about whether to join the met quorum.
             * 
             * Note: We did all RMIs before grabbing this lock to minimize
             * latency and the possibility for distributed deadlocks.
             */
            logLock.lock();

            try {

                final IHAWriteMessage lastLiveMsg = journal.getHALogNexus().lastLiveHAWriteMessage;

                if (lastLiveMsg != null
                        && lastLiveMsg.getCommitCounter() >= currentWriteSetStateOnLeader
                                .getCommitCounter()) {

                    /*
                     * Can not join. Some write has been received for this
                     * commit point (or a greater commit point). Leader has
                     * moved on. (Note: any live write for either the then
                     * current commit point on the leader or any subsequent
                     * commit point on the leader is sufficient to reject a
                     * conditional join. We do not need to check the block
                     * sequence as well.)
                     * 
                     * Note: [lastLiveMsg] was cleared to [null] when we did a
                     * local abort at the top of resync() or rebuild().
                     */

                    return false;

                }
                
                final IHALogWriter logWriter = journal.getHALogNexus();

                if (haLog.isDebugEnabled())
                    haLog.debug("HALog.commitCounter="
                            + logWriter.getCommitCounter()
                            + ", HALog.getSequence="
                            + logWriter.getSequence());

                if (logWriter.getCommitCounter() != openingCommitCounter
                        || logWriter.getSequence() != 0L) {

                    /*
                     * We verified the last live message above (i.e., none)
                     * while holding the [logLock]. The HALogWriter must be
                     * consistent with the leader at this point since we are
                     * holding the [logLock] and that lock is blocking pipeline
                     * replication.
                     */

                    throw new AssertionError("openingCommitCount="
                            + openingCommitCounter
                            + ", logWriter.commitCounter="
                            + logWriter.getCommitCounter()
                            + ", logWriter.sequence=" + logWriter.getSequence());

                }

                /*
                 * Cast the leader's vote, join with the met quorum, and
                 * transition to RunMet.
                 * 
                 * Note: We are blocking the write pipeline on this code path
                 * because the [logLock] is held. We verify the pre-conditions
                 * for the join with the met quorum while holding the [logLock].
                 */
                
                doCastLeadersVoteAndServiceJoin(token);
                                
                // Done with resync.
                return true;

            } finally {

                logLock.unlock();

            }
            
        }
        
        /**
         * Make sure the pipeline is setup properly.
         * <p>
         * We need to block until the quorum meet has caused the quorumToken to
         * be set on the Journal and the HALogWriter to be configured so we can
         * begin to accept replicated live writes.
         * <p>
         * Also, if this is the first quorum meet (commit counter is ZERO (0)),
         * then we need to make sure that the root blocks have been replicated
         * from the leader before proceeding.
         * 
         * @throws IOException
         * @throws FileNotFoundException
         * @throws InterruptedException
         */
        private void pipelineSetup() throws FileNotFoundException, IOException,
                InterruptedException {

            // The current token (if any).
            final long token = getQuorum().token();

            // Verify that the quorum is met.
            getQuorum().assertQuorum(token);

            // Verify that we have valid root blocks
            awaitJournalToken(token);

            // Note: used to do conditionalCreateHALog() here.
            
        }

        @Override
        protected void incReceive(final IHASyncRequest req,
                final IHAWriteMessage msg, final int nreads,
                final int rdlen, final int rem) throws Exception {

//            if (log.isTraceEnabled())
//                log.trace("HA INCREMENTAL PROGRESS: msg=" + msg + ", nreads="
//                        + nreads + ", rdlen=" + rdlen + ", rem=" + rem);
            
            final IHAProgressListener l = progressListenerRef.get();
            
            if (l != null) {

                l.incReceive(req, msg, nreads, rdlen, rem);
                
            }
            
        }

        /**
         * Interface for receiving notice of incremental write replication
         * progress.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         */
        public static interface IHAProgressListener {

            void incReceive(final IHASyncRequest req,
                    final IHAWriteMessage msg, final int nreads,
                    final int rdlen, final int rem) throws Exception;
            
        }

        // Note: Exposed to HAJournal's HAGlue implementation.
        final AtomicReference<IHAProgressListener> progressListenerRef = new AtomicReference<IHAProgressListener>();

        @Override
        protected void handleReplicatedWrite(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws Exception {

            if (req == null //&& journal.getQuorumToken() == Quorum.NO_QUORUM
                    && journal.getRootBlockView().getCommitCounter() == 0L
                    && (msg.getUUID() != null && !journal.getUUID().equals(msg.getUUID()))) {
                /*
                 * This is a live write (part of the current write set).
                 * However, our root blocks have not yet been updated to reflect
                 * the leader's root blocks. In this case, if we attempt to
                 * setup the pipeline before the root blocks
                 * have been installed, then we risk a distributed deadlock
                 * where the leader can be waiting for this service to handle
                 * the replicated write in WriteCacheService.flush() as invoked from commitNow()
                 * during a commit, but this service is waiting on the 
                 * 
                 * Note: Deadlocks had been observed before this fast path was
                 * added. This occurred when the leader was attempting to commit
                 * the initial KB create, the 3rd service is attempting to
                 * synchronize with the met quorum and was stuck in
                 * pipelineSetup() where it was waiting for the quorum token to
                 * be set on the journal, and an HTTP client is attempting to
                 * discover whether or not the KB has been created.
                 * 
                 * @see TestHA3JournalServer#testABCStartSimultaneous
                 */
                return;
            }
            
            pipelineSetup();
            
            logLock.lock();
            try {

                journal.getHALogNexus().conditionalCreateHALog();

                if (haLog.isDebugEnabled())
                    haLog.debug("msg=" + msg + ", buf=" + data);

                if (req == null) {

                    // Save off reference to most recent *live* message.
                    journal.getHALogNexus().lastLiveHAWriteMessage = msg;
                    
                } else 
                
                if (/*req != null &&*/ req instanceof IHARebuildRequest) {

                    /*
                     * This message and payload are part of a ground up service
                     * rebuild (disaster recovery from the quorum) rather than
                     * an incremental resynchronization.
                     * 
                     * Note: HALog blocks during rebuild are written onto the
                     * appropriate HALog file using the same rules that apply to
                     * resynchronization. We also capture the root blocks for
                     * those replicated HALog files. However, during a REBUILD,
                     * we DO NOT go through a local commit for the replicated
                     * HALog files until the service is fully synchronized. This
                     * prevents the service from having root blocks that are
                     * "non-empty" when the file state is not fully consistent
                     * with the leader.
                     * 
                     * Note: Replicated backing store write blocks MUST be
                     * written directly onto the backing FileChannel after
                     * correcting the offset (which is relative to the root
                     * block). Even for the RWStore, the replicated backing
                     * store blocks represent a contiguous extent on the file
                     * NOT scattered writes.
                     * 
                     * REBUILD : Replicated HALog write blocks are handled just
                     * like resync (see below).
                     */

                    journal.getBufferStrategy().writeRawBuffer(
                            (HARebuildRequest) req, msg, data);
                    
                    return;

                }

                final IHALogWriter logWriter = journal.getHALogNexus();

                assert logWriter.isHALogOpen();
                
                if (msg.getCommitCounter() == logWriter.getCommitCounter()
                        && msg.getSequence() == (logWriter.getSequence() - 1)) {

                    /*
                     * Duplicate message. This can occur due retrySend() in
                     * QuorumPipelineImpl#replicate(). retrySend() is used to
                     * make the pipeline robust if a service (other than the
                     * leader) drops out and we need to change the connections
                     * between the services in the write pipeline in order to
                     * get the message through.
                     */

                    if (log.isInfoEnabled())
                        log.info("Ignoring message (dup): " + msg);

                    return;

                }

                final RunStateEnum runState = runStateRef.get();

                if (RunStateEnum.Resync.equals(runState)) {

                    /*
                     * If we are resynchronizing, then pass ALL messages (both
                     * live and historical) into handleResyncMessage().
                     * 
                     * Note: This method handles the transition into the met
                     * quorum when we observe a LIVE message that is the
                     * successor of the last received HISTORICAL message. This
                     * is the signal that we are caught up on the writes on the
                     * met quorum and may join.
                     */
                    
                    handleResyncMessage((IHALogRequest) req, msg, data);

                    return;

                } else if (req != null) {

                    /*
                     * A historical message that is being ignored on this node.
                     */
                    
                    dropMessage(req, msg, data);

                    return;
                    
                } else {

                    assert req == null; // Note: MUST be a live message!

                    if (journal.getHAReady() == Quorum.NO_QUORUM
                            || !isJoinedMember(msg.getQuorumToken())) {

                        /*
                         * If we are not joined, we can not do anything with a
                         * live write.
                         */
                        
                        dropMessage(req, msg, data);

                        return;
                        
                    }

                    try {

                        /*
                         * We are not resynchronizing this service.
                         * 
                         * The service is joined with the quorum.
                         * 
                         * The message SHOULD be for the current commit counter
                         * and the expected next write cache block sequence. If
                         * it is not, then we will enter error handling logic
                         * below.
                         */

                        // write on the log and the local store.
                        acceptHAWriteMessage(msg, data);

                        return;

                    } catch(Throwable t) {
                        if (InnerCause.isInnerCause(t,
                                InterruptedException.class)) {
                            // propagate interrupt
                            Thread.currentThread().interrupt();
                            return;
                        }
                        // Add check for ClosedByInterruptException - but is this sufficient if the channel is now closed?
                        if (InnerCause.isInnerCause(t,
                                ClosedByInterruptException.class)) {
                            // propagate interrupt
                            // Thread.currentThread().interrupt();
                            
                            // wrap and re-throw
                            throw new RuntimeException(t);
                            // return;
                        }
                        /*
                         * Error handler.
                         * 
                         * Live write is not for expected commit counter and
                         * write cache block sequence.
                         */
                        log.error(t, t);
                        enterErrorState();
                        /*
                         * Note: DO NOT rethrow the exception. This service will
                         * leave the met quorum. If we rethrow the exception,
                         * then the update operation that that generated the
                         * live replicated write will be failed with the
                         * rethrown exception as the root cause. However, we
                         * want the update operation to complete successfully as
                         * long as we can retain an met quorum (and the same
                         * leader) for the duration of the update operation.
                         */
//                        // rethrow exception.
//                        throw new RuntimeException(t);
                        return;
                    }
                    
//                    /*
//                     * Drop the pipeline message.
//                     * 
//                     * Note: It is a live message, but this node is not caught
//                     * up and therefore can not log the message yet.
//                     */
//
//                    dropMessage(req, msg, data);

                }

            } finally {

                logLock.unlock();

            }

        }
        
        private void dropMessage(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer data) {

            if (log.isInfoEnabled())
                log.info("Ignoring message: req=" + req + ", msg=" + msg);

        }
        
        /**
         * Adjust the size on the disk of the local store to that given in the
         * message.
         * <p>
         * Note: When historical messages are being replayed, the caller needs
         * to decide whether the message should applied to the local store. If
         * so, then the extent needs to be updated. If not, then the message
         * should be ignored (it will already have been replicated to the next
         * follower).
         */
        private void setExtent(final IHAWriteMessage msg) throws IOException {

            try {

                ((IHABufferStrategy) journal.getBufferStrategy())
                        .setExtentForLocalStore(msg.getFileExtent());

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            } catch (RuntimeException t) {

                // Wrap with the HA message.
                throw new RuntimeException("msg=" + msg + ": " + t, t);
                
            }

        }

        /**
         * Handle a replicated write requested to resynchronize this service
         * with the quorum. The {@link WriteCache} messages for HA Logs are
         * delivered over the write pipeline, along with messages for the
         * current write set. This method handles those that are for historical
         * write sets (replayed from HA Log files) as well as those that are
         * historical writes for the current write set (that is, messages that
         * this service missed because it joined the write pipeline after the
         * first write message for the current write set and was thus not able
         * to log and/or apply the write message even if it did observe it).
         * <p>
         * Note: The quorum token associated with historical message needs to be
         * ignored. The quorum could have broken and met again since, in which
         * case any attempt to use that old token will cause a QuorumException.
         * 
         * @throws InterruptedException
         * @throws IOException
         */
        private void handleResyncMessage(final IHALogRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws IOException, InterruptedException {

            logLock.lock();

            try {

                final IHALogWriter logWriter = journal.getHALogNexus();

                if (req == null) {
                    
                    /*
                     * Live message.
                     */

                    if ((msg.getCommitCounter() == journal.getRootBlockView().getCommitCounter())
                            && (msg.getSequence() == logWriter.getSequence())) {

                        /*
                         * We just received a live message that is the successor
                         * of the last resync message. We are caught up. We need
                         * to log and apply this live message, cancel the resync
                         * task, and enter RunMet.
                         */

                    	if (haLog.isInfoEnabled())
                    		haLog.info("Transition to MET after seeing LIVE that is NEXT after last resync, lastLiveNexusMsg " + journal.getHALogNexus().lastLiveHAWriteMessage);
                    	
                        resyncTransitionToMetQuorum(msg, data);

                        return;

                    } else {

                        /*
                         * Drop live messages since we are not caught up.
                         */

                        if (haLog.isDebugEnabled())
                            log.debug("Ignoring write cache block: msg=" + msg);

                        return;

                    }

                } else {

                    /*
                     * A historical message (replay of an HALog file).
                     * 
                     * Note: We will see ALL messages. We can only log the
                     * message if it is for our commit point.
                     */

                    if (!server.serviceUUID.equals(req.getServiceId())) {

                        /*
                         * Not our request. Drop the message.
                         */
                        
                        if (haLog.isDebugEnabled())
                            log.debug("Ignoring write cache block: msg=" + msg);

                        return;

                    }

                    // log and write cache block.
                    acceptHAWriteMessage(msg, data);

                }
                
            } finally {

                logLock.unlock();

            }

        }

        /**
         * Atomic transition to the met quorum, invoked when we receive the same
         * sequence number for some {@link WriteCache} block in the current
         * write set twice. This happens when we get it once from the explicit
         * resynchronization task and once from the normal write pipeline
         * writes. In both cases, the block is transmitted over the write
         * pipeline. The double-presentation of the block is our signal that we
         * are caught up with the normal write pipeline writes.
         */
        private void resyncTransitionToMetQuorum(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            final IHALogWriter logWriter = journal.getHALogNexus();
            
            final IRootBlockView rootBlock = journal.getRootBlockView();

            if (logWriter.getCommitCounter() != rootBlock.getCommitCounter()) {

                throw new AssertionError("HALogWriter.commitCounter="
                        + logWriter.getCommitCounter() + ", but rootBlock="
                        + rootBlock);

            }

            if (msg.getCommitCounter() != rootBlock.getCommitCounter()
                    || msg.getLastCommitTime() != rootBlock.getLastCommitTime()) {

                throw new AssertionError("msg=" + msg + ", but rootBlock="
                        + journal.getRootBlockView());

            }

            /*
             * Service is not joined but is caught up with the write
             * pipeline and is ready to join.
             */

            // Accept the message - log and apply.
            acceptHAWriteMessage(msg, data);

            /*
             * Cast the leader's vote, join with the met quorum, and
             * transition to RunMet.
             */
            doCastLeadersVoteAndServiceJoin(msg.getQuorumToken());
            
        }

        /**
         * Cast the leader's vote, join with the met quorum, and transition to
         * RunMet.
         * <p>
         * Note: The write pipeline will be blocked during this method. This is
         * achieved via two different mechanisms. If the call stack goes through
         * {@link #handleReplicatedWrite(IHASyncRequest, IHAWriteMessage, ByteBuffer)}
         * , then the write pipeline is naturally blocked. For
         * {@link #conditionalJoinWithMetQuorum(HAGlue, long, long)}, we are
         * holding the {@link #logLock} which ensures that new write messages
         * can not be processed.
         * 
         * @param token
         *            The token that must remain valid throughout this
         *            operation.
         */
        private void doCastLeadersVoteAndServiceJoin(final long token) {
            
            // Vote the consensus for the met quorum.
            final Quorum<?, ?> quorum = getQuorum();

            // UUID of the quorum leader.
            final UUID leaderId = quorum.getLeaderId();

            // Resolve the leader.
            final S leader = getLeader(token);
            
            // Verify that the quorum is valid.
            quorum.assertQuorum(token);

            /*
             * Get the vote cast by the leader.
             * 
             * Note: Concurrent quorum break will cause NPE here.
             */
            final long leadersVote = quorum.getCastVote(leaderId);
            
            if (haLog.isInfoEnabled())
                haLog.info("Will attempt to join met quorum: " + token
                        + ", leadersVote=" + leadersVote);

            // Cast that vote.
            getActor().castVote(leadersVote);

            // Verify that the quorum is valid.
            getQuorum().assertQuorum(token);
            
            if (haLog.isInfoEnabled())
                haLog.info("Successful attempt to cast vote for met quorum: " + token
                        + ", leadersVote=" + leadersVote);

            /*
             * Attempt to join the met quorum.
             * 
             * Note: This will throw an exception if this services is not in the
             * consensus.
             * 
             * Note: The write pipeline is BLOCKED. Either we are handling a
             * replicated write -or- we are holding the logLock (or both).
             * 
             * Note: The serviceJoin() needs to be MUTEX with the critical
             * section of the consensus protocol to identify the new release
             * time. This is necessary to ensure that the follower does not
             * start a new Tx against a commit point after the follower has
             * notified the leader about its earliest visible commit point and
             * before the leader has notified the followers about the new
             * consensus release time.
             * 
             * TODO What happens if we are blocked here?
             */
            final AbstractHATransactionService txs = (AbstractHATransactionService) journal
                    .getTransactionService();

            txs.runWithBarrierLock(new Runnable() {

                @Override
                public void run() {

                    // Verify that the quorum is valid.
                    getQuorum().assertQuorum(token);

                    // Synchronous service join (blocks until success or
                    // failure).
                    getActor().serviceJoin();

                    // Verify that the quorum is valid.
                    getQuorum().assertQuorum(token);

//                    // Set the token on the journal.
//                    journal.setQuorumToken(token);
//
//                    // Verify that the quorum is valid.
//                    getQuorum().assertQuorum(token);

                    /*
                     * We need to block until the leader observes our service
                     * join. We are blocking replicated writes. That prevents
                     * the leader from initiating a 2-phase commit. By blocking
                     * until our service join becomes visible to the leader, we
                     * are able to ensure that we will participate in a 2-phase
                     * commit where the leader might otherwise have failed to
                     * observe that we are a joined service.
                     * 
                     * This addresses a failure mode demonstrated by the test
                     * suite where a service join during a series of short
                     * transactions could fail. The failure mode was that the
                     * newly joined follower was current on the write set and
                     * had invoked serviceJoin(), but the leader did not include
                     * it in the 2-phase commit because the service join event
                     * had not been delivered from zk in time (visibility).
                     * 
                     * Note: There is a gap between the GATHER and the PREPARE.
                     * If this service joins with a met quorum after the GATHER
                     * and before the PREPARE, then it MUST set the most recent
                     * consensus release time from the leader on its local
                     * journal. This ensures that the newly joined follower will
                     * not allow a transaction start against a commit point that
                     * was recycled by the leader.
                     * 
                     * TODO The leader should use a real commit counter in its
                     * response and the follower should verify that the commit
                     * counter is consistent with its assumptions.
                     */
                    final IHANotifyReleaseTimeResponse resp;
                    try {

                        resp = leader
                                .awaitServiceJoin(new HAAwaitServiceJoinRequest(
                                        getServiceId(),
                                        Long.MAX_VALUE/* timeout */,
                                        TimeUnit.SECONDS/* unit */));

                        if (haLog.isInfoEnabled())
                            haLog.info("Obtained releaseTime from leader: "
                                    + resp);

                    } catch (Exception t) {
                        throw new QuorumException(
                                "Service join not observed by leader.", t);
                    }

                    /*
                     * Set the token on the journal.
                     * 
                     * Note: We need to do this after the leader has observed
                     * the service join. This is necessary in order to have the
                     * precondition checks in the GatherTask correctly reject a
                     * GatherTask when the service is not yet HAReady. If we set
                     * the quorumToken *before* we await the visibility of the
                     * service join on the leader, then the GatherTask will see
                     * that the follower (this service) that is attempting to
                     * join is HAReady and will incorrectly attempt to execute
                     * the GatherTask, which can result in a deadlock.
                     */

                    journal.setQuorumToken(token);

                    // Verify that the quorum is valid.
                    getQuorum().assertQuorum(token);

                    // Update the release time on the local journal.
                    txs.setReleaseTime(resp.getCommitTime());

                    // Verify that the quorum is valid.
                    getQuorum().assertQuorum(token);

                }
            }); // runWithBarrierLock()

            if (haLog.isInfoEnabled())
                haLog.info("TRANSITION", new StackInfoReport());
            
            // Transition to RunMet.
            enterRunState(new RunMetTask(token, leaderId));

        }
        
        /**
         * Verify commitCounter in the current log file and the message are
         * consistent, then log and apply the {@link WriteCache} block.
         */
        private void acceptHAWriteMessage(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            // Note: Caller must be holding the logLock!
            
            final long expectedCommitCounter = journal.getHALogNexus()
                    .getCommitCounter();

            final long expectedBlockSequence = journal.getHALogNexus()
                    .getSequence();

            if (msg.getCommitCounter() != expectedCommitCounter)
                throw new IllegalStateException("expectedCommitCounter="
                        + expectedCommitCounter+ ", but msg=" + msg);
            
            if (msg.getSequence() != expectedBlockSequence)
                throw new IllegalStateException("expectedBlockSequence="
                        + expectedBlockSequence + ", but msg=" + msg);

            /*
             * Log the message and write cache block.
             */
            logWriteCacheBlock(msg, data);

            writeWriteCacheBlock(msg, data);

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Writes the {@link IHAWriteMessage} and the data onto the
         * {@link HALogWriter}
         */
        @Override
        public void logWriteCacheBlock(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException {

            try {

                // Make sure the pipeline is setup properly.
                pipelineSetup();
                
            } catch (InterruptedException e) {
                
                // Propagate the interrupt.
                Thread.currentThread().interrupt();
                
                return;
                
            }
            
            logLock.lock();

            try {

                journal.getHALogNexus().conditionalCreateHALog();
                
                /*
                 * Throws IllegalStateException if the message is not
                 * appropriate for the state of the log.
                 * 
                 * Throws IOException if we can not write on the log.
                 * 
                 * We catch, log, and rethrow these messages to help diagnose
                 * problems where the message state is not consistent with the
                 * log state.
                 */
                journal.getHALogNexus().writeOnHALog(msg, data);
                
            } catch(RuntimeException ex) {
                
                haLog.error(ex, ex);

                throw ex;

            } catch (IOException ex) {

                haLog.error(ex, ex);

                throw ex;

            } finally {
                
                logLock.unlock();
                
            }
            
        }

        /**
         * Write the raw {@link WriteCache} block onto the backing store.
         */
        private void writeWriteCacheBlock(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            setExtent(msg);
            
            /*
             * Note: the ByteBuffer is owned by the HAReceiveService. This just
             * wraps up the reference to the ByteBuffer with an interface that
             * is also used by the WriteCache to control access to ByteBuffers
             * allocated from the DirectBufferPool. However, release() is a NOP
             * on this implementation since the ByteBuffer is owner by the
             * HAReceiveService.
             */
            
            final IBufferAccess b = new IBufferAccess() {

                @Override
                public void release(long timeout, TimeUnit unit)
                        throws InterruptedException {
                    // NOP
                }

                @Override
                public void release() throws InterruptedException {
                    // NOP
                }

                @Override
                public ByteBuffer buffer() {
                    return data;
                }
            };

            ((IHABufferStrategy) journal.getBufferStrategy())
                    .writeRawBuffer(msg, b);
            
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Writes the root block onto the {@link HALogWriter} and closes the log
         * file. A new log is then opened, using the given root block as the
         * starting point for that log file.
         */
        @Override
        public void logRootBlock(//final boolean isJoinedService,
                final IRootBlockView rootBlock) throws IOException {

            logLock.lock();

            try {
                
                // Close off the old log file with the root block.
                journal.getHALogNexus().closeHALog(rootBlock);

                // Open up a new log file with this root block.
                journal.getHALogNexus().createHALog(rootBlock);

            } finally {

                logLock.unlock();

            }

        }

        /**
         * {@inheritDoc}
         * <p>
         * Deletes HA log files and snapshots that are no longer retained by the
         * {@link IRestorePolicy}.
         * <p>
         * Note: The current HALog file is NOT deleted by this method.
         */
        @Override
        public void purgeHALogs(final long token) {

            logLock.lock();

            try {

                if (!getQuorum().isQuorumFullyMet(token)) {
                    /*
                     * Halt operation.
                     * 
                     * Note: This is not an error, but we can not remove
                     * snapshots or HALogs if this invariant is violated.
                     * 
                     * Note: We do not permit HALog files to be purged if the
                     * quorum is not fully met. This is done in order to prevent
                     * a situation a leader would not have sufficient log files
                     * on hand to restore the failed service. If this were to
                     * occur, then the failed service would have to undergo a
                     * disaster rebuild rather than simply resynchronizing from
                     * the leader. Hence, HALog files are NOT purged unless the
                     * quorum is fully met (all services for its replication
                     * count are joined with the met quorum).
                     */
                    return;
                }

                // We need to retain the backups for this commit point.
                final long earliestRestorableCommitPoint = journal
                        .getSnapshotManager().getRestorePolicy()
                        .getEarliestRestorableCommitPoint(journal);

                /*
                 * Release snapshots and HALog files no longer required by the
                 * restore policy.
                 * 
                 * Note: The current HALog is NOT deleted.
                 */

                // Delete snapshots, returning commit counter of the oldest
                // retained snapshot.
                final long earliestRetainedSnapshotLastCommitCounter = journal
                        .getSnapshotManager().deleteSnapshots(token,
                                earliestRestorableCommitPoint);

                // Delete HALogs not retained by that snapshot.
                journal.getHALogNexus().deleteHALogs(token,
                        earliestRetainedSnapshotLastCommitCounter);

            } finally {

                logLock.unlock();

            }

        }

        /**
         * We need to destroy the local backups if we do a REBUILD. Those files
         * are no longer guaranteed to be consistent with the history of the
         * journal.
         * <p>
         * Note: This exists as a distinct code path because we will destroy
         * those backups without regard to the quorum token. The normal code
         * path requires a fully met journal in order to delete snapshots and
         * HALog files.
         * 
         * @throws IOException
         *             if a file could not be deleted.
         */
        private void deleteBackups() throws IOException {
            
            logLock.lock();

            try {

                haLog.warn("Destroying local backups.");

                // Delete all snapshots.
                journal.getSnapshotManager().deleteAllSnapshots();

                /*
                 * Delete all HALogs (except the current one).
                 * 
                 * Note: The current HALog will wind up being disabled (and
                 * destroyed) and a new one created at an appropriate time.
                 */
                journal.getHALogNexus().deleteAllHALogsExceptCurrent();
                                
            } finally {

                logLock.unlock();

            }
            
        }

        @Override
        public void installRootBlocks(final IRootBlockView rootBlock0,
                final IRootBlockView rootBlock1) {

            journal.installRootBlocks(rootBlock0, rootBlock1);

        }

//        @Override
//        public void didMeet(final long token, final long commitCounter,
//                final boolean isLeader) {
//            // NOP
//        }

        @Override
        public File getServiceDir() {

            return server.getServiceDir();
            
        }
        
        /**
         * Spin until the journal.quorumToken is set (by the event handler).
         * <p>
         * Note: The {@link #quorumMeet(long, UUID)} arrives in the zookeeper
         * event thread. We can not take actions that could block in that
         * thread, so it is pumped into a single threaded executor. When that
         * executor handles the event, it sets the token on the journal. This
         * process is of necessity asynchronous with respect to the run state
         * transitions for the {@link HAJournalServer}. Futher, when the local
         * journal is empty (and this service is joined with the met quorum as a
         * follower), setting the quorum token will also cause the root blocks
         * from the leader to be installed on this service. This method is used
         * to ensure that the local journal state is consistent (token is set,
         * root blocks are have been copied if the local journal is empty)
         * before allowing certain operations to proceed.
         * 
         * @see #quorumMeet(long, UUID)
         * @see HAJournal#setQuorumToken(long)
         */
        private void awaitJournalToken(final long token) throws IOException,
                InterruptedException {
            /*
             * Note: This is called for each HA write message received. DO NOT
             * use any high latency or RMI calls unless we need to wait for the
             * root blocks. That condition is detected below without any high
             * latency operations.
             */
            S leader = null;
            IRootBlockView rbLeader = null;
            final long sleepMillis = 10;
            int ntimes = 0;
            while (true) {
                ntimes++;
                // while token is valid.
                getQuorum().assertQuorum(token);
                final long journalToken = journal.getQuorumToken();
                if (journalToken != token) {
                    Thread.sleep(sleepMillis/* ms */);
                    continue;
                }
                if (isFollower(token)) {// if (awaitRootBlocks) { 
                    // Check root block, using lock for synchronization barrier.
                    final IRootBlockView rbSelf = journal.getRootBlockViewWithLock();
                    if (rbSelf.getCommitCounter() == 0L) {
                        /*
                         * Only wait if this is an empty Journal.
                         * 
                         * Note: We MUST NOT install the local root blocks
                         * unless both this service and the leader at at
                         * commitCounter ZERO(0L).
                         */
                        if (leader == null) {
                            /*
                             * RMI to the leader for its current root block
                             * (once).
                             */
                            leader = getLeader(token);
                            rbLeader = leader
                                    .getRootBlock(
                                            new HARootBlockRequest(null/* storeUUID */))
                                    .getRootBlock();
                        }
                        if (!rbSelf.getUUID().equals(rbLeader.getUUID())) {
                            /*
                             * Wait if the leader's root block has not yet been
                             * installed.
                             */
                            Thread.sleep(sleepMillis/* ms */);
                            continue;
                        }
                    }
                }
                /*
                 * Good to go.
                 */
                break;
            }
            if (ntimes > 1 && haLog.isInfoEnabled())
                haLog.info("Journal quorumToken is set.");
        }

        @Override
        public ZooKeeper getZooKeeper() {
            /*
             * Note: This automatically connects if not connected. That allows
             * us to disconnect in quorumService.terminate(). Note that we can
             * not defer the connect() until quorumService.start() since the
             * quorum itself requires access to ZooKeeper, which it obtains
             * through this method. So the HAClient connection is made when the
             * quorum first requests access to ZooKeeper if it did not already
             * exist.
             */
            return server.getHAClient().connect().getZookeeper();
        }

        @Override
        public List<ACL> getACL() {
            return server.getHAClient().getZookeeperClientConfig().acl;
        }
        
    } // class HAQuorumService

    /**
     * Setup and start the {@link NanoSparqlServer}.
     * <p>
     * Note: The NSS will start on each service in the quorum. However, only the
     * leader will create the default KB (if that option is configured).
     * <p>
     * Note: We need to wait for a quorum meet since this will create the KB
     * instance if it does not exist and we can not write on the
     * {@link HAJournal} until we have a quorum meet.
     * 
     * @see <a href="http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty">
     *      Embedding Jetty </a>
     * @see <a href="http://trac.blazegraph.com/ticket/730" > Allow configuration
     *      of embedded NSS jetty server using jetty-web.xml </a>
     */
    private void startNSS() {

        try {

            if (jettyServer != null && jettyServer.isRunning()) {

                throw new RuntimeException("Already running");

            }

            // The location of the jetty.xml file.
            final String jettyXml = (String) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.JETTY_XML, String.class,
                    ConfigurationOptions.DEFAULT_JETTY_XML);

                // Note: if we do this, push the serviceDir down into newInstance().
//                if (!jettyXml.startsWith("/")) {
//                    // Assume that the path is relative to the serviceDir.
//                    jettyXml = getServiceDir() + File.separator + jettyXml;
//                }
                
                // Setup the embedded jetty server for NSS webapp.
            jettyServer = NanoSparqlServer
                    .newInstance(jettyXml, journal, null/* initParams */);

            // Wait until the server starts (up to a timeout).
            NanoSparqlServer.awaitServerStart(jettyServer);

        } catch (Exception e1) {

            // Log and ignore.
            log.error("Could not start NanoSparqlServer: " + e1, e1);

        }

    }
    
    /**
     * The actual port depends on how jetty was configured in
     * <code>jetty.xml</code>. This returns the port associated with the first
     * connection for the jetty {@link Server}.
     * 
     * @return The port associated with the first connection for the jetty
     *         {@link Server}.
     * 
     * @throws IllegalArgumentException
     *             if the jetty {@link Server} is not running.
     */
    int getNSSPort() {

        return NanoSparqlServer.getLocalPort(jettyServer);

    }
    
    /**
     * Conditionally create the default KB instance as identified in
     * <code>web.xml</code>.
     * 
     * @see ConfigParams
     * 
     * @throws ConfigurationException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void conditionalCreateDefaultKB() throws ConfigurationException,
            InterruptedException, ExecutionException {

        final Server server = this.jettyServer;

        if (server == null)
            throw new IllegalStateException();

        /*
         * TODO This currently relies on the WebAppContext's initParams. This is
         * somewhat fragile, but that is where this information is declared.
         */
        final WebAppContext wac = NanoSparqlServer.getWebApp(server);

        if (wac == null)
            throw new RuntimeException("Could not locate webapp.");
        
        final String namespace;
        {
         
            String s = wac.getInitParameter(ConfigParams.NAMESPACE);

            if (s == null)
                s = ConfigParams.DEFAULT_NAMESPACE;

            namespace = s;
            
            if (log.isInfoEnabled())
                log.info(ConfigParams.NAMESPACE + "=" + namespace);

        }

        final boolean create;
        {
            
            final String s = wac.getInitParameter(ConfigParams.CREATE);

            if (s != null)
                create = Boolean.valueOf(s);
            else
                create = ConfigParams.DEFAULT_CREATE;
        
            if (log.isInfoEnabled())
                log.info(ConfigParams.CREATE + "=" + create);

        }

//        final String COMPONENT = NSSConfigurationOptions.COMPONENT;
//
//        final String namespace = (String) config.getEntry(COMPONENT,
//                NSSConfigurationOptions.NAMESPACE, String.class,
//                NSSConfigurationOptions.DEFAULT_NAMESPACE);
//
//        final boolean create = (Boolean) config.getEntry(COMPONENT,
//                NSSConfigurationOptions.CREATE, Boolean.TYPE,
//                NSSConfigurationOptions.DEFAULT_CREATE);

      if (create) {

         AbstractApiTask.submitApiTask(journal,
               new CreateKBTask(namespace, journal.getProperties())).get();

      }

    }
    
    /**
     * Start an {@link HAJournal}.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file.
     */
    public static void main(final String[] args) {

        if (args.length == 0) {

            System.err.println("usage: <config-file> [config-overrides]");

            System.exit(1);

        }

        final HAJournalServer server = new HAJournalServer(args,
                new FakeLifeCycle());

        /*
         * Note: The server.run() call was pushed into the constructor to be
         * compatible with the ServiceStarter pattern.
         */
//        // Wait for the HAJournalServer to terminate.
//        server.run();
        
        /*
         * Note: The System.exit() call here appears to be required for the
         * timely release of allocated ports. Commenting out this line tends to
         * cause startup failures in CI due to ports that are already (aka,
         * "still") bound.
         */
        System.exit(0);

    }

}
