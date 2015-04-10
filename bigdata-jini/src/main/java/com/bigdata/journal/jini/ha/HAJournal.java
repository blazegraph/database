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
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.tcp.TcpServerEndpoint;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.FutureTaskInvariantMon;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.RunState;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HADigestResponse;
import com.bigdata.ha.msg.HALogDigestResponse;
import com.bigdata.ha.msg.HALogRootBlocksResponse;
import com.bigdata.ha.msg.HARemoteRebuildRequest;
import com.bigdata.ha.msg.HASendStoreResponse;
import com.bigdata.ha.msg.HASnapshotDigestResponse;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASnapshotDigestRequest;
import com.bigdata.ha.msg.IHASnapshotDigestResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService;
import com.bigdata.journal.jini.ha.HAJournalServer.RunStateEnum;
import com.bigdata.quorum.Quorum;
import com.bigdata.resources.StoreManager.IStoreManagerCounters;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.service.proxy.ClientFuture;
import com.bigdata.service.proxy.RemoteFuture;
import com.bigdata.service.proxy.RemoteFutureImpl;
import com.bigdata.service.proxy.ThickFuture;
import com.bigdata.util.StackInfoReport;

/**
 * A {@link Journal} that that participates in a write replication pipeline. The
 * {@link HAJournal} is configured an River {@link Configuration}. The
 * configuration includes properties to configured the underlying
 * {@link Journal} and information about the {@link HAJournal}s that will
 * participate in the replication pattern.
 * <p>
 * All instances declared in the {@link Configuration} must be up, running, and
 * able to connect for any write operation to succeed. All instances must vote
 * to commit for an operation to commit. If any units fail (or timeout) then the
 * operation will abort. All instances are 100% synchronized at all commit
 * points. Read-only operations can be load balanced across the instances and
 * uncommitted data will never be visible to readers. Writes must be directed to
 * the first instance in the write replication pipeline. A read error on an
 * instance will internally failover to another instance in an attempt to read
 * from good data.
 * <p>
 * The write replication pipeline is statically configured. If an instance is
 * lost, then the configuration file must be changed, the change propagated to
 * all nodes, and the services "bounced" before writes can resume. Bouncing a
 * service only requires that the Journal is closed and reopened. Services do
 * not have to be "bounced" at the same time, and (possibly new) leader must be
 * "bounced" last to ensure that writes do not propagate until the write
 * pipeline is in a globally consistent order that excludes the down node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/530"> Journal HA </a>
 */
public class HAJournal extends Journal {

    private static final Logger log = Logger.getLogger(HAJournal.class);

//    private static final String ACQUIRED_GLOBAL_WRITE_LOCK = "Acquired global write lock.";
//
//    private static final String RELEASED_GLOBAL_WRITE_LOCK = "Released global write lock.";

    public interface Options extends Journal.Options {
        
//        /**
//         * The address at which this journal exposes its write pipeline
//         * interface (a socket level interface for receiving write cache blocks
//         * from another service in the met quorum).
//         */
//        String WRITE_PIPELINE_ADDR = HAJournal.class.getName()
//                + ".writePipelineAddr";
//        /**
//         * The timeout in milliseconds that the leader will await the followers
//         * to prepare for a 2-phase commit.
//         * <p>
//         * Note: The timeout must be set with a realistic expectation concerning
//         * the possibility of garbage collection. A long GC pause could
//         * otherwise cause the 2-phase commit to fail. With this in mind, a
//         * reasonable timeout is on the order of 10 seconds.
//         */
//        String HA_PREPARE_TIMEOUT = HAJournal.class.getName() + ".HAPrepareTimeout";
//
//        String DEFAULT_HA_PREPARE_TIMEOUT = "10000"; // milliseconds.
//        
//        long HA_MIN_PREPARE_TIMEOUT = 100; // milliseconds.
//        
    }
    
    /**
     * The {@link HAJournalServer} instance that is managing this {@link HAJournal}.
     */
    private final HAJournalServer server;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#WRITE_PIPELINE_ADDR
     */
    private final InetSocketAddress writePipelineAddr;

    /**
     * @see HAJournalServer.ConfigurationOptions#HA_PREPARE_TIMEOUT
     */
    private final long haPrepareTimeout;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#HA_RELEASE_TIME_CONSENSUS_TIMEOUT
     */
    private final long haReleaseTimeConsensusTimeout;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#MAXIMUM_CLOCK_SKEW
     */
    private final long maximumClockSkew;

    /**
     * @see HAJournalServer.ConfigurationOptions#HA_EXTRA_DELAY_FOR_RETRY_SEND
     */
    private final long haExtraDelayForRetrySend;
    
//    /**
//     * @see HAJournalServer.ConfigurationOptions#HA_LOG_DIR
//     */
//    private final File haLogDir;
//    
//    /**
//     * Write ahead log for replicated writes used to resynchronize services that
//     * are not in the met quorum.
//     * 
//     * @see HAJournalServer.ConfigurationOptions#HA_LOG_DIR
//     * @see HALogWriter
//     */
//    private final HALogWriter haLogWriter;
//    
//    /**
//     * Lock to guard the HALogWriter.
//     */
//    final Lock logLock = new ReentrantLock(); 
//    
//    /**
//     * The most recently observed *live* {@link IHAWriteMessage}.
//     * <p>
//     * Note: The {@link HALogWriter} will log live messages IFF they are
//     * consistent with the state of the {@link HAJournalServer} when they are
//     * received. In contrast, this field notices each *live* message that is
//     * replicated along the HA pipline.
//     * <p>
//     * Note: package private - exposed to {@link HAJournalServer}.
//     * 
//     * @see QuorumServiceBase#handleReplicatedWrite(IHASyncRequest,
//     *      IHAWriteMessage, ByteBuffer)
//     */
//    volatile IHAWriteMessage lastLiveHAWriteMessage = null;

    /**
     * Manager for journal snapshots.
     */
    private final SnapshotManager snapshotManager;
    
    /**
     * Manager for HALog files.
     */
    private final HALogNexus haLogNexus;
    
    /**
     * The manager for HALog files for this {@link HAJournal} and never
     * <code>null</code>.
     */
    public HALogNexus getHALogNexus() {
        
        return haLogNexus;
        
    }

    /**
     * Return the {@link HAClient} instance that is in use by the
     * {@link HAJournalServer}.
     * 
     * @see HAClient#getConnection()
     */
    public HAClient getHAClient() {
        
        return server.getHAClient();
        
    }
    
    /**
     * The {@link HAJournalServer} instance that is managing this
     * {@link HAJournal}.
     */
    public HAJournalServer getHAJournalServer() {
        
        return server;
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to strengthen the return type.
     */
    @Override
    public IHABufferStrategy getBufferStrategy() {
        
        return (IHABufferStrategy) super.getBufferStrategy();
        
    }

    /**
     * @param server
     *            The {@link HAJournalServer} instance.
     * @param config
     *            The {@link Configuration} object.
     * @param quorum
     *            The {@link Quorum} implementation.
     * 
     * @throws ConfigurationException
     * @throws IOException
     */
    public HAJournal(final HAJournalServer server, final Configuration config,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
            throws ConfigurationException, IOException {
     
        /*
         * Note: Pulls out the Properties object for the HAJournal from the
         * HAJournal component in the Configuration.
         */
        
        this(//
                server, //
                config, //
                JiniClient.getProperties(HAJournal.class.getName(), config),//
                quorum //
        );

    }

    private HAJournal(final HAJournalServer server, final Configuration config,
            final Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
            throws ConfigurationException, IOException {

        /*
         * Note: This checks properties BEFORE passing them up since we do not
         * want to create the Journal file if the properties are invalid.
         */

        super(checkProperties(properties), quorum);

        this.server = server;
        
        {

            // The address at which this service exposes its write pipeline.
            writePipelineAddr = (InetSocketAddress) config.getEntry(
                    HAJournalServer.ConfigurationOptions.COMPONENT,
                    HAJournalServer.ConfigurationOptions.WRITE_PIPELINE_ADDR,
                    InetSocketAddress.class);

        }

        {
            haReleaseTimeConsensusTimeout = (Long) config
                    .getEntry(
                            HAJournalServer.ConfigurationOptions.COMPONENT,
                            HAJournalServer.ConfigurationOptions.HA_RELEASE_TIME_CONSENSUS_TIMEOUT,
                            Long.TYPE,
                            HAJournalServer.ConfigurationOptions.DEFAULT_HA_RELEASE_TIME_CONSENSUS_TIMEOUT);

            if (haReleaseTimeConsensusTimeout < HAJournalServer.ConfigurationOptions.MIN_HA_RELEASE_TIME_CONSENSUS_TIMEOUT) {
                throw new ConfigurationException(
                        HAJournalServer.ConfigurationOptions.HA_RELEASE_TIME_CONSENSUS_TIMEOUT
                                + "="
                                + haReleaseTimeConsensusTimeout
                                + " : must be GTE "
                                + HAJournalServer.ConfigurationOptions.MIN_HA_RELEASE_TIME_CONSENSUS_TIMEOUT);
            }

        }
        
        {
            haPrepareTimeout = (Long) config
                    .getEntry(
                            HAJournalServer.ConfigurationOptions.COMPONENT,
                            HAJournalServer.ConfigurationOptions.HA_PREPARE_TIMEOUT,
                            Long.TYPE,
                            HAJournalServer.ConfigurationOptions.DEFAULT_HA_PREPARE_TIMEOUT);

            if (haPrepareTimeout < HAJournalServer.ConfigurationOptions.MIN_HA_PREPARE_TIMEOUT) {
                throw new ConfigurationException(
                        HAJournalServer.ConfigurationOptions.HA_PREPARE_TIMEOUT
                                + "="
                                + haPrepareTimeout
                                + " : must be GTE "
                                + HAJournalServer.ConfigurationOptions.MIN_HA_PREPARE_TIMEOUT);
            }

        }

        {
            maximumClockSkew = (Long) config
                    .getEntry(
                            HAJournalServer.ConfigurationOptions.COMPONENT,
                            HAJournalServer.ConfigurationOptions.MAXIMUM_CLOCK_SKEW,
                            Long.TYPE,
                            HAJournalServer.ConfigurationOptions.DEFAULT_MAXIMUM_CLOCK_SKEW);

            if (maximumClockSkew < HAJournalServer.ConfigurationOptions.MIN_MAXIMUM_CLOCK_SKEW) {
                throw new ConfigurationException(
                        HAJournalServer.ConfigurationOptions.MAXIMUM_CLOCK_SKEW
                                + "="
                                + maximumClockSkew
                                + " : must be GTE "
                                + HAJournalServer.ConfigurationOptions.MIN_MAXIMUM_CLOCK_SKEW);
            }

        }
        
        {
            haExtraDelayForRetrySend = (Long) config
                    .getEntry(
                            HAJournalServer.ConfigurationOptions.COMPONENT,
                            HAJournalServer.ConfigurationOptions.HA_EXTRA_DELAY_FOR_RETRY_SEND,
                            Long.TYPE,
                            HAJournalServer.ConfigurationOptions.DEFAULT_HA_EXTRA_DELAY_FOR_RETRY_SEND);

            if (haExtraDelayForRetrySend < 0L) {
                throw new ConfigurationException(
                        HAJournalServer.ConfigurationOptions.HA_EXTRA_DELAY_FOR_RETRY_SEND
                                + "="
                                + haExtraDelayForRetrySend
                                + " : must be non-negative");
            }

        }

        // HALog manager.
        haLogNexus = new HALogNexus(server, this, config);
        
        // Snapshot manager.
        snapshotManager = new SnapshotManager(server, this, config);

        try {
            getExecutorService().submit(snapshotManager.init()).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e); // TODO Do not wrap.
        } catch (CancellationException e) {
            throw e;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Perform some checks on the {@link HAJournal} configuration properties.
     * 
     * @param properties
     *            The configuration properties.
     *            
     * @return The argument.
     */
    protected static Properties checkProperties(final Properties properties) {

        final long minReleaseAge = Long.valueOf(properties.getProperty(
                AbstractTransactionService.Options.MIN_RELEASE_AGE,
                AbstractTransactionService.Options.DEFAULT_MIN_RELEASE_AGE));

        final BufferMode bufferMode = BufferMode.valueOf(properties
                .getProperty(Options.BUFFER_MODE, Options.DEFAULT_BUFFER_MODE));

        switch (bufferMode) {
        case DiskRW: {
            if (minReleaseAge <= 0) {
                /*
                 * Note: Session protection is used by the RWStore when the
                 * minReleaseAge is ZERO (0). However, session protection is not
                 * compatible with HA. We MUST log delete blocks in order to
                 * ensure that the question of allocation slot recycling is
                 * deferred until the ACID decision at the commit point,
                 * otherwise we can not guarantee that the read locks asserted
                 * when lose a service and no longer have a fully met quorum
                 * will be effective as of the *start* of the writer (that is,
                 * atomically as of the last commit point).)
                 */
                throw new IllegalArgumentException(
                        AbstractTransactionService.Options.MIN_RELEASE_AGE
                                + "=" + minReleaseAge
                                + " : must be GTE ONE (1) for HA.");
            }
            break;
        }
        case DiskWORM:
            break;
        default:
            throw new IllegalArgumentException(Options.BUFFER_MODE + "="
                    + bufferMode + " : does not support HA");
        }

        final boolean writeCacheEnabled = Boolean.valueOf(properties
                .getProperty(Options.WRITE_CACHE_ENABLED,
                        Options.DEFAULT_WRITE_CACHE_ENABLED));

        if (!writeCacheEnabled)
            throw new IllegalArgumentException(Options.WRITE_CACHE_ENABLED
                    + " : must be true.");

        return properties;

    }
    
    /*
     * Note: The HAJournal and HAGlueService MAY be subclassed. Therefore, do
     * not perform any initialization in this factory method.
     */
    @Override
    protected HAGlue newHAGlue(final UUID serviceId) {

        return new HAGlueService(serviceId);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to expose this method to the {@link HAJournalServer}.
     */
    @Override
    protected final void clearQuorumToken(final long newValue) {
    
        super.clearQuorumToken(newValue);
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to expose this method to the {@link HAJournalServer}.
     */
    @Override
    protected final void setQuorumToken(final long newValue) {
    
        super.setQuorumToken(newValue);

        /*
         * Note: This is now handled by an invariant listener in
         * SnapshotManager.
         */
//        if (getHAReady() == Quorum.NO_QUORUM) {
//
//            /*
//             * If there is a running snapshot, then cancel it since the quorum
//             * has broken (or this service has left the quorum). (HAReady will
//             * be NO_QUORUM if the quorum is still met, but this service is no
//             * longer joined with the met quorum.)
//             * 
//             * Note: The snapshot task will automatically terminate if it
//             * observes a quorum break or similar event. This is just being
//             * proactive.
//             */
//            
//            final Future<IHASnapshotResponse> ft = getSnapshotManager()
//                    .getSnapshotFuture();
//
//            if (ft != null && !ft.isDone()) {
//
//                haLog.info("Canceling snapshot.");
//
//                ft.cancel(true/* mayInterruptIfRunning */);
//
//            }
//
//        }
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to expose this method to the {@link HAJournalServer}.
     */
    @Override
    protected final long getQuorumToken() {
    
        return super.getQuorumToken();

    }
    
    /**
     * {@inheritDoc}
     * 
     * @see HAJournalServer.ConfigurationOptions#HA_PREPARE_TIMEOUT
     */
    @Override
    public final long getHAPrepareTimeout() {

        return haPrepareTimeout;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @see HAJournalServer.ConfigurationOptions#HA_RELEASE_TIME_CONSENSUS_TIMEOUT
     */
    @Override
    public final long getHAReleaseTimeConsensusTimeout() {

        return haReleaseTimeConsensusTimeout;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @see HAJournalServer.ConfigurationOptions#MAXIMUM_CLOCK_SKEW
     */
    @Override
    public final long getMaximumClockSkewMillis() {

        return maximumClockSkew;
        
    }

//  * {@inheritDoc}
    /**
     * 
     * @see HAJournalServer.ConfigurationOptions#HA_EXTRA_DELAY_FOR_RETRY_SEND
     */
//    @Override
    public final long getHAExtraDelayForRetrySend() {

        return haExtraDelayForRetrySend;
        
    }

//    @Override
//    public final File getHALogDir() {
//
//        return haLogNexus.getHALogDir();
//        
//    }

    public SnapshotManager getSnapshotManager() {
        
        return snapshotManager;
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to close the {@link HALogWriter}.
     */
    @Override
    protected void _close() {
    
        try {
            haLogNexus.getHALogWriter().disableHALog();
        } catch (IOException e) {
            haLog.error(e, e);
        }
        
        super._close();
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to destroy the HALog files and HALog directory.
     */
    @Override
    public void deleteResources() {

        super.deleteResources();

        recursiveDelete(getHALogNexus().getHALogDir(),
                IHALogReader.HALOG_FILTER);
        
        recursiveDelete(getSnapshotManager().getSnapshotDir(),
                SnapshotManager.SNAPSHOT_FILTER);
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. Only files recognized by filter will be
     * deleted.
     * 
     * @param f
     *            A file or directory.
     * @param fileFilter
     *            The filter.
     */
    private void recursiveDelete(final File f, final FileFilter fileFilter) {

        try {
            CommitCounterUtility.recursiveDelete(false/* errorIfDeleteFails */,
                    f, fileFilter);
        } catch (IOException e) {
            /*
             * Note: IOException is not thrown here since
             * errorIfDeleteFails:=false.
             */
            throw new RuntimeException(e);
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to expose this method to the {@link SnapshotManger}.
     */
    @Override
    public IRootBlockView[] getRootBlocks() {
        
        return super.getRootBlocks();
        
    }
    /**
     * {@inheritDoc}
     * <p>
     * Extended to expose this method to the {@link HAQuorumService}.
     */
    @Override
    protected void installRootBlocks(final IRootBlockView rootBlock0,
            final IRootBlockView rootBlock1) {

        super.installRootBlocks(rootBlock0, rootBlock1);

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to expose this method to the {@link HAQuorumService}.
     */
    @Override
    protected void doLocalCommit(final QuorumService<HAGlue> localService,
            final IRootBlockView rootBlock) {

        super.doLocalCommit(localService, rootBlock);

    }
    
//    /**
//     * {@inheritDoc}
//     * <p>
//     * Extended to expose this method to the {@link HAQuorumService}.
//     */
//    @Override
//    protected void doLocalAbort() {
//
//        // Clear the last live message out.
//        haLogNexus.lastLiveHAWriteMessage = null;
//        
//        super.doLocalAbort();
//
//    }
    
    /**
     * Interface for additional performance counters exposed by the
     * {@link HAJournal}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface IHAJournalCounters {

        /**
         * The namespace for counters pertaining to free space on the various
         * volumes.
         */
        String Volumes = "Volumes";

//        /**
//         * The configured service directory.
//         */
//        String ServiceDir = "ServiceDir";
//
//        /**
//         * The configured data directory (the directory in which the Journal
//         * file is stored).
//         */
//        String DataDir = "DataDir";
//
//        /**
//         * The configured HALog directory.
//         */
//        String HALogDir = "HALogDir";
//
//        /**
//         * The configured Snapshot directory.
//         */
//        String SnapshotDir = "ShapshotDir";
//
//        /**
//         * The configured tmp directory.
//         */
//        String TmpDir = "TmpDir";

        /**
         * The #of bytes available on the disk volume on which the service
         * directory is located.
         * 
         * @see HAJournalServer#getServiceDir()
         */
        String ServiceDirBytesAvailable = "Service Volume Bytes Available";

        /**
         * The #of bytes available on the disk volume on which the data
         * directory is located (the directory in which the Journal file
         * is stored).
         * 
         * @see Journal#getDataDir()
         */
        String DataDirBytesAvailable = "Data Volume Bytes Available";

        /**
         * The #of bytes available on the disk volume on which the HALog
         * directory is located.
         * 
         * @see HALogNexus#getHALogDir()
         */
        String HALogDirBytesAvailable = "HALog Volume Bytes Available";

        /**
         * The #of bytes available on the disk volume on which the snapshot
         * directory is located.
         * 
         * @see SnapshotManager#getSnapshotDir()
         */
        String SnapshotDirBytesAvailable = "Snapshot Volume Bytes Available";

        /**
         * The #of bytes available on the disk volume on which the temporary
         * directory is located.
         * 
         * @see Journal#getTmpDir()
         */
        String TmpDirBytesAvailable = "Temp Volume Bytes Available";

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to attach additional performance counters.
     * 
     * @see IHAJournalCounters
     */
    @Override
    public CounterSet getCounters() {
    
        final CounterSet root = super.getCounters();
        {

            final CounterSet tmp = root.makePath(IHAJournalCounters.Volumes);

            tmp.addCounter(IHAJournalCounters.ServiceDirBytesAvailable,
                    new Instrument<Long>() {
                        @Override
                        public void sample() {
                            setValue(getFreeSpace(server.getServiceDir()));
                        }
                    });

            tmp.addCounter(IHAJournalCounters.DataDirBytesAvailable,
                    new Instrument<Long>() {
                        @Override
                        public void sample() {
                            final File dir = getDataDir();
                            if (dir != null) {
                                // Note: in case Journal is not durable.
                                setValue(getFreeSpace(dir));
                            }
                        }
                    });

            tmp.addCounter(IHAJournalCounters.HALogDirBytesAvailable,
                    new Instrument<Long>() {
                        @Override
                        public void sample() {
                            setValue(getFreeSpace(getHALogNexus().getHALogDir()));
                        }
                    });

            tmp.addCounter(IHAJournalCounters.SnapshotDirBytesAvailable,
                    new Instrument<Long>() {
                        @Override
                        public void sample() {
                            setValue(getFreeSpace(getSnapshotManager()
                                    .getSnapshotDir()));
                        }
                    });

            tmp.addCounter(IStoreManagerCounters.TmpDirBytesAvailable,
                    new Instrument<Long>() {
                        @Override
                        public void sample() {
                            setValue(getFreeSpace(getTmpDir()));
                        }
                    });

        }

        return root;

    }
    
    /**
     * Return the free space in bytes on the volume hosting some directory.
     * 
     * @param dir
     *            A directory hosted on some volume.
     * 
     * @return The #of bytes of free space remaining for the volume hosting the
     *         directory -or- <code>-1L</code> if the free space could not be
     *         determined.
     */
    /*
     * Note: This was written using Apache FileSystemUtil originally. That would
     * shell out "df" under un*x. Unfortunately, shelling out a child process
     * requires a commitment from the OS to support a process with as much
     * process space as the parent. For the data service, that is a lot of RAM.
     * In general, the O/S allows "over committment" of the available swap
     * space, but you can run out of swap and then you have a problem. If the
     * host was configured with scanty swap, then this problem could be
     * triggered very easily and would show up as "Could not allocate memory".
     * 
     * See http://forums.sun.com/thread.jspa?messageID=9834041#9834041
     */
    static protected long getFreeSpace(final File dir) {
        
        try {

            if(!dir.exists()) {
                
                return -1;
                
            }

            /*
             * Note: This return 0L if there is no free space or if the File
             * does not "name" a partition in the file system semantics. That
             * is why we check dir.exists() above.
             */

            return dir.getUsableSpace();
            
        } catch(Throwable t) {
            
            log.error("Could not get free space: dir=" + dir + " : "
                            + t, t);
            
            // the error is logger and ignored.
            return -1L;
            
        }

    }

    /**
     * Extended implementation supports RMI.
     */
    protected class HAGlueService extends BasicHA implements
            RemoteAdministrable, RemoteDestroyAdmin {

        protected HAGlueService(final UUID serviceId) {

            super(serviceId, writePipelineAddr);
            
        }

        @Override
        protected void validateNewRootBlock(//final boolean isJoined,
                final boolean isLeader, final IRootBlockView oldRB,
                final IRootBlockView newRB) {

            super.validateNewRootBlock(/*isJoined,*/ isLeader, oldRB, newRB);

            if (/*isJoined &&*/ !isLeader) {
                
                /*
                 * Verify that the [lastLiveHAWriteMessage] is consisent with
                 * the proposed new root block.
                 * 
                 * Note: The [lastLiveHAWriteMessage] is only tracked on the
                 * followers. Hence we do not use this code path for the leader.
                 */
                
                final IHAWriteMessage msg = getHALogNexus().lastLiveHAWriteMessage;

                if (msg == null) {

                    /*
                     * We should not go through a 2-phase commit without a write
                     * set. If there is a write set, then the
                     * lastLiveHAWriteMessage will not be null.
                     * 
                     * Note: One possible explanation of this exception would be
                     * a concurrent local abort. That could discard the
                     * lastLiveHAWriteMessage.
                     */

                    throw new IllegalStateException("Commit without write set?");

                }

                if (!msg.getUUID().equals(newRB.getUUID())) {

                    /*
                     * The root block has a different UUID. We can not accept
                     * this condition.
                     */

                    throw new IllegalStateException("Store UUID: lastLiveMsg="
                            + msg.getUUID() + " != newRB=" + newRB.getUUID());

                }

                // Validate the new commit counter.
                if ((msg.getCommitCounter() + 1) != newRB.getCommitCounter()) {

                    /*
                     * Each message is tagged with the commitCounter for the
                     * last commit point on the disk. The new root block must
                     * have a commit counter is that PLUS ONE when compared to
                     * the last live message.
                     */

                    throw new IllegalStateException(
                            "commitCounter: ( lastLiveMsg="
                                    + msg.getCommitCounter()
                                    + " + 1 ) != newRB="
                                    + newRB.getCommitCounter());

                }

                // Validate the write cache block sequence.
                if ((msg.getSequence() + 1) != newRB.getBlockSequence()) {

                    /*
                     * This checks two conditions:
                     * 
                     * 1. The new root block must reflect each live
                     * HAWriteMessage received.
                     * 
                     * 2. The service must not PREPARE until all expected
                     * HAWriteMessages have been received.
                     */

                    throw new IllegalStateException(
                            "blockSequence: lastLiveMsg=" + msg.getSequence()
                                    + " + 1 != newRB="
                                    + newRB.getBlockSequence());

                }

            }
            
        }

        /**
         * {@inheritDoc}
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/678" >
         *      DGC Thread Leak: sendHALogForWriteSet() </a>
         */
        @Override
        public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
                final IHALogRootBlocksRequest msg) throws IOException {

            /*
             * Note: This makes the reporting of the open and close root blocks
             * for the HALog file atomic with respect to other operations on the
             * HALog. This lock is shared by the HAQuorumService.
             */
            final Lock logLock = getHALogNexus().getLogLock();
            logLock.lock();
            try {

                /*
                 * Verify that this service is the quorum leader.
                 */
                final long token = getQuorum().token();
                
                // Method is only allowed on the quorum leader.
                getQuorumService().assertLeader(token);

                if (!getHALogNexus().isHALogOpen()) {

                    /**
                     * The live HALog should always exist on the leader.
                     * However, the leader is defined by the zookeeper state and
                     * it is possible that the leader has not yet gone through
                     * the code path to create the HALog. Thus, for safety, we
                     * ensure that the live HALog exists here.
                     * 
                     * Note: we are holding the logLock.
                     * 
                     * Note: This only causes the HALog to be created on the
                     * quorum leader. HAJournalServer.discardWriteSet() handles
                     * this for both the leader and the joined followers.
                     * 
                     * @see <a
                     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/764"
                     *      > RESYNC fails (HA) </a>
                     */

                    if (haLog.isInfoEnabled())
                        log.info(
                                "Live HALog does not exist on the quorum leader",
                                new StackInfoReport());

                    getHALogNexus().conditionalCreateHALog();

                }
                
                // The commit counter of the desired closing root block.
                final long commitCounter = msg.getCommitCounter();

                final File logFile = getHALogNexus()
                        .getHALogFile(commitCounter);

                if (!logFile.exists()) {

                    // No log for that commit point.
                    throw new FileNotFoundException(logFile.getName());

                }

                final IHALogReader r = getHALogNexus().getReader(logFile);

                try {

                    final HALogRootBlocksResponse resp = new HALogRootBlocksResponse(
                            r.getOpeningRootBlock(), r.getClosingRootBlock());

                    if (haLog.isDebugEnabled())
                        haLog.debug("msg=" + msg + ", resp=" + resp);

                    return resp;

                } finally {

                    r.close();

                }

            } finally {
            
                logLock.unlock();
                
            }

        }

        @Override
        public Future<Void> sendHALogForWriteSet(final IHALogRequest req)
                throws IOException {
            
            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            // The commit counter of the desired closing root block.
            final long commitCounter = req.getCommitCounter();

            // Note the token on entry.
            final long token = getQuorum().token();
            
            /*
             * Open the HALog file. If it exists, then we will run a task to
             * send it along the pipeline.
             * 
             * Note: The choice of the "live" versus a historical "closed" log
             * file needs to be an atomic decision and thus MUST be made by the
             * HALogManager.
             * 
             * Note: Once opened, the HALog file must be closed. Once we submit
             * the task for execution, the SendHALogTask() is responsible for
             * closing the HALog file. If we do not get that far, then the file
             * is closed by this code block.
             * 
             * Note: This can leak an open file handle in the case where the
             * ExecutorService is shutdown before the task runs, but that only
             * occurs on Journal shutdown.
             */
            final boolean isLive;
            final FutureTask<Void> ft;
            {

                IHALogReader r = null;
                
                try {

                    // Note: open file handle - must be closed eventually.
                    r = getHALogNexus().getReader(commitCounter);

                    // true iff is live log at moment reader was opened.
                    isLive = r.isLive();

                    // Task sends an HALog file along the pipeline.
                    ft = new FutureTaskInvariantMon<Void>(new SendHALogTask(
                            req, r), getQuorum()) {

                        @Override
                        protected void establishInvariants() {
                            assertQuorumMet();
                            assertJoined(getServiceId());
                            assertMember(req.getServiceId());
                            assertInPipeline(req.getServiceId());
                            /*
                             * Note: This is a pre-condition, not an invariant.
                             * We verify on entry that this service is the
                             * leader. The invariant is that the quorum remains
                             * met on the current token, which is handled by
                             * assertQuorumMet().
                             */
                            getQuorum().assertLeader(token);
                        }

                    };

                    // Run task.
                    getExecutorService().submit(ft);

                    // Clear reference. File handle will be closed by task.
                    r = null;

                } finally {

                    if (r != null) {

                        try {

                            r.close();

                        } catch (Throwable t) {

                            log.error(t, t);
                        }

                    }

                }
                
            }
            
            /**
             * Return Future.
             * 
             * TODO This relies on DGC, which is supposed to be fixed in river
             * 2.2.1 (it was leaking one thread per RMI in 2.2.0). Even if this
             * leaks a thread every time we return an asynchronous proxy, we
             * stil need the ability to interrupt the transfer of an HALog file.
             * 
             * If DGC is still leaking thread, then look at HAJournalServer and
             * how it manages the transition to a joined service in RESYNC and
             * identify a different mechanism for interrupting the transfer of
             * the HALog.
             * 
             * Consider using a well known exception thrown back long the write
             * pipeline to indicate that a receiver is done recieving data for
             * some HALog (or backing store) or sending a message which
             * explicitly cancels a transfer using an identifier for that
             * transfer. If this is done synchronously while in
             * handleReplicatedWrite then we will get the same decidability as
             * using an asyncrhonous future, but without the thread leak
             * problem.
             * 
             * This issue is most pressing for sendHALogForWriteSet() since we
             * can synchronous many 1000s of HALog files when resynchronizing a
             * service. However, the same DGC thread leak exists for several
             * other methods as specified on the ticket below.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/678"
             *      > DGC Thread Leak: sendHALogForWriteSet() </a>
             */
            return getProxy(ft, isLive/* asynch */);

        }

        /**
         * Class sends the {@link IHAWriteMessage}s and {@link WriteCache}
         * buffer contents along the write pipeline for the requested HALog
         * file.
         */
        private class SendHALogTask implements Callable<Void> {

            private final IHALogRequest req;
            private final IHALogReader r;

            public SendHALogTask(final IHALogRequest req, final IHALogReader r) {

                this.req = req;
                this.r = r;

            }

            public Void call() throws Exception {
				try {

					long nsent = 0;
					boolean success = false;

					final IBufferAccess buf = DirectBufferPool.INSTANCE
							.acquire();

					try {

						while (r.hasMoreBuffers()) {

							// IHABufferStrategy
							final IHABufferStrategy strategy = HAJournal.this
									.getBufferStrategy();

							// get message and fill write cache buffer (unless
							// WORM).
							final IHAWriteMessage msg = r.processNextBuffer(buf
									.buffer());

							if (haLog.isDebugEnabled())
								haLog.debug("req=" + req + ", msg=" + msg);

							// drop them into the write pipeline.
							final Future<Void> ft = strategy.sendHALogBuffer(
									req, msg, buf);

							try {
								// wait for message to make it through the
								// pipeline.
								ft.get();
								nsent++;
							} finally {
								/*
								 * DO NOT PROPAGATE THE INTERRUPT TO THE
								 * PIPELINE.
								 * 
								 * Note: Either the Future isDone() or *this*
								 * thread was interrupted. If ft.isDone() then
								 * we do not need to cancel the Future. If
								 * *this* thread was interrupted, then we DO NOT
								 * propagate that interrupt to the pipeline.
								 * 
								 * This thread will be interrupted if the
								 * service that requested the HALog file is has
								 * caught up and transitioned from Resync to
								 * RunMet.
								 * 
								 * Propagating the interrupt to the pipeline (by
								 * calling ft.cancel(true)) would causes the
								 * socket channel to be asynchronously closed.
								 * The sends on the pipeline do not have a frame
								 * that marks off one from the next, so the next
								 * queued send can wind up being interrupted.
								 * 
								 * Because we do not cancel the Future, the
								 * send() will complete normally. This is Ok.
								 * The receiver can discard the message and
								 * payload.
								 */
								// ft.cancel(true/* mayInterruptIfRunning */);
							}

						} // while(hasMoreBuffers())

						success = true;

						return null;

					} catch (Exception e) {
						if (haLog.isDebugEnabled())
							haLog.debug("Interrupted", e);

						throw e;
					} finally {

						buf.release();

						if (haLog.isDebugEnabled())
							haLog.debug("req=" + req + ", nsent=" + nsent
									+ ", success=" + success);

					}

				} finally {
                    // Close the open log file.
                    r.close();

				}

            } // call()

        } // class SendHALogTask

        /*
         * REBUILD: Take a read lock and send everything from the backing file,
         * but do not include the root blocks. The first buffer can be short (to
         * exclude the root blocks). That will put the rest of the buffers on a
         * 1MB boundary which will provide more efficient IOs.
         */
        @Override
        public Future<IHASendStoreResponse> sendHAStore(
                final IHARebuildRequest req) throws IOException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            // Note the token on entry.
            final long token = getQuorum().token();
            
            // Task sends an HALog file along the pipeline.
            final FutureTask<IHASendStoreResponse> ft = new FutureTaskInvariantMon<IHASendStoreResponse>(
                    new SendStoreTask(req), getQuorum()) {

                @Override
                protected void establishInvariants() {
                    assertQuorumMet();
                    assertJoined(getServiceId());
                    assertMember(req.getServiceId());
                    assertInPipeline(req.getServiceId());
                    /*
                     * Note: This is a pre-condition, not an invariant. We
                     * verify on entry that this service is the leader. The
                     * invariant is that the quorum remains met on the current
                     * token, which is handled by assertQuorumMet().
                     */
                    getQuorum().assertLeader(token);
                }

            };

            // Run task.
            getExecutorService().submit(ft);

            // Return *ASYNCHRONOUS* proxy (interruptable).
            return getProxy(ft, true/* asynch */);

        }

        /**
         * Class sends the backing file along the write pipeline.
         */
        private class SendStoreTask implements Callable<IHASendStoreResponse> {
            
            private final IHARebuildRequest req;
            
            public SendStoreTask(final IHARebuildRequest req) {
                
                if(req == null)
                    throw new IllegalArgumentException();
                
                this.req = req;
                
            }
            
            public IHASendStoreResponse call() throws Exception {
                
            	// The quorum token (must remain valid through this operation).
                final long quorumToken = getQuorumToken();
                
                // Grab a read lock.
                final long txId = newTx(ITx.READ_COMMITTED);

                /*
                 * Get both root blocks (atomically).
                 * 
                 * Note: This is done AFTER we take the read-lock and BEFORE we
                 * copy the data from the backing store. These root blocks MUST
                 * be consistent for the leader's backing store because we are
                 * not recycling allocations (since the read lock has pinned
                 * them). The journal MIGHT go through a concurrent commit
                 * before we obtain these root blocks, but they are still valid
                 * for the data on the disk because of the read-lock.
                 */
                final IRootBlockView[] rootBlocks = getRootBlocks();
                
                IBufferAccess buf = null;
                try {

                    try {
                        // Acquire a buffer.
                        buf = DirectBufferPool.INSTANCE.acquire();
                    } catch (InterruptedException ex) {
                        // Wrap and re-throw.
                        throw new IOException(ex);
                    }
                    
                    // The backing ByteBuffer.
                    final ByteBuffer b = buf.buffer();

                    // The capacity of that buffer (typically 1MB).
                    final int bufferCapacity = b.capacity();

                    // The size of the root blocks (which we skip).
                    final int headerSize = FileMetadata.headerSize0;

                    /*
                     * The size of the file at the moment we begin. We will not
                     * replicate data on new extensions of the file. Those data will
                     * be captured by HALog files that are replayed by the service
                     * that is doing the rebuild.
                     */
                    final long fileExtent = getBufferStrategy().getExtent();

                    // The #of bytes to be transmitted.
                    final long totalBytes = fileExtent - headerSize;
                    
                    // The #of bytes remaining.
                    long remaining = totalBytes;
                    
                    // The offset from which data is retrieved.
                    long offset = headerSize;
                    
                    long sequence = 0L;
                    
                    if (haLog.isInfoEnabled())
                        haLog.info("Sending store file: nbytes=" + totalBytes);

                    while (remaining > 0) {

                        int nbytes = (int) Math.min((long) bufferCapacity,
                                remaining);
                        
                        if (sequence == 0L && nbytes == bufferCapacity
                                && remaining > bufferCapacity) {
                            
                            /*
                             * Adjust the first block so the remainder will be
                             * aligned on the bufferCapacity boundaries (IO
                             * efficiency).
                             */
                            nbytes -= headerSize;

                        }

                        if (haLog.isDebugEnabled())
                            haLog.debug("Sending block: sequence=" + sequence
                                    + ", offset=" + offset + ", nbytes=" + nbytes);

                        final Future<?> ft = getBufferStrategy()
                                .sendRawBuffer(req, sequence, quorumToken,
                                        fileExtent, offset, nbytes, b);
                        
                        try {

                            ft.get(); // wait for data sent!
                            
                        } finally {
                            /*
                             * Note: As per sendHAStore(), we DO NOT want to do
                             * ft.cancel() here. This would cause the
                             * asynchronous close of the socket channel for the
                             * write pipeline.
                             * 
                             * See sendHAStore().
                             */
                        }

                        remaining -= nbytes;
                        
                        offset += nbytes;

                        sequence++;
                        
                    }

                    if (haLog.isInfoEnabled())
                        haLog.info("Sent store file: #blocks=" + sequence
                                + ", #bytes=" + (fileExtent - headerSize));

                    // The root blocks (from above) and stats on the operation.
                    final IHASendStoreResponse resp = new HASendStoreResponse(
                            rootBlocks[0], rootBlocks[1], totalBytes, sequence);

                    // Done.
                    return resp;

                } finally {
                    
                    if (buf != null) {
                        try {
                            // Release the direct buffer.
                            buf.release();
                        } catch (InterruptedException e) {
                            haLog.warn(e);
                        }
                    }
                    
                    // Release the read lock.
                    abort(txId);

                }

            }

        } // class SendStoreTask
        
        @Override
        public IHADigestResponse computeDigest(final IHADigestRequest req)
                throws IOException, NoSuchAlgorithmException, DigestException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            final MessageDigest digest = MessageDigest.getInstance("MD5");

            getBufferStrategy().computeDigest(null/* snapshot */, digest);

            final IHADigestResponse resp = new HADigestResponse(req.getStoreUUID(), 
                    digest.digest());
            
            if (haLog.isInfoEnabled())
                log.info("Request=" + req + ", Response=" + resp);

            return resp;
            
        }
        
        @Override
        public IHALogDigestResponse computeHALogDigest(final IHALogDigestRequest req)
                throws IOException, NoSuchAlgorithmException, DigestException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            // The commit counter of the desired closing root block.
            final long commitCounter = req.getCommitCounter();

            /*
             * Note: The choice of the "live" versus a historical "closed" log
             * file needs to be an atomic decision and thus MUST be made by the
             * HALogManager.
             */
            final IHALogReader r = getHALogNexus().getReader(commitCounter);

            final MessageDigest digest = MessageDigest.getInstance("MD5");

            r.computeDigest(digest);

            final IHALogDigestResponse resp = new HALogDigestResponse(
                    req.getCommitCounter(), digest.digest());

            if (haLog.isInfoEnabled())
                log.info("Request=" + req + ", Response=" + resp);

            return resp;
            
        }

        @Override
        public IHASnapshotDigestResponse computeHASnapshotDigest(
                final IHASnapshotDigestRequest req) throws IOException,
                NoSuchAlgorithmException, DigestException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            // The commit counter of the desired closing root block.
            final long commitCounter = req.getCommitCounter();

            final MessageDigest digest = MessageDigest.getInstance("MD5");

            /*
             * Compute digest for snapshot for that commit point.
             * 
             * Note: throws FileNotFoundException if no snapshot for that commit
             * point.
             */
            getSnapshotManager().getDigest(commitCounter, digest);

            final IHASnapshotDigestResponse resp = new HASnapshotDigestResponse(
                    req.getCommitCounter(), digest.digest());

            if (haLog.isInfoEnabled())
                log.info("Request=" + req + ", Response=" + resp);

            return resp;
            
        }

//        /**
//         * {@inheritDoc}
//         * 
//         * TODO This method relies on the unisolated semaphore. That provides a
//         * sufficient guarantee for updates that original through the NSS since
//         * all such updates will eventually require the unisolated connection to
//         * execute. However, if we support multiple concurrent unisolated
//         * connections distinct KBs per the ticket below, then we will need to
//         * have a different global write lock - perhaps via the
//         * {@link WriteExecutorService}.
//         * 
//         * @deprecated This method is no longer necessary to support backups
//         *             since we can now take snapshots without suspending
//         *             writers.
//         * 
//         * @see https://sourceforge.net/apps/trac/bigdata/ticket/566 (
//         *      Concurrent unisolated operations against multiple KBs on the
//         *      same Journal)
//         */
//        @Override
//        public Future<Void> globalWriteLock(final IHAGlobalWriteLockRequest req)
//                throws IOException, InterruptedException, TimeoutException {
//
//            if (req == null)
//                throw new IllegalArgumentException();
//
//            /*
//             * This token will be -1L if there is no met quorum. This method may
//             * only execute while there is a met quorum and this service is the
//             * leader for that met quorum.
//             * 
//             * Note: This method must stop waiting for the global lock if this
//             * service is no longer the leader (quorum break).
//             * 
//             * Note: This method must stop holding the global lock if this
//             * service is no longer the leader (quorum break).
//             */
//            final long token = getQuorum().token();
//
//            // Verify that the quorum is met and that this is the leader.
//            getQuorum().assertLeader(token);
//
//            // Set true IFF we acquire the global write lock.
//            final AtomicBoolean didAcquire = new AtomicBoolean(false);
//
//            // Task to acquire the lock
//            final FutureTask<Void> acquireLockTaskFuture = new FutureTask<Void>(
//                    new AcquireGlobalLockTask(didAcquire));
//
//            // Task used to interrupt task acquiring the lock if quorum breaks.
//            final FutureTask<Void> interruptLockTaskFuture = new FutureTask<Void>(
//                    new InterruptAcquireLockTask(token, acquireLockTaskFuture,
//                            req));
//
//            // Task to release the lock.
//            final FutureTask<Void> releaseLockTaskFuture = new FutureTask<Void>(
//                    new ReleaseGlobalLockTask(token, req));
//
//            // Service to run those tasks.
//            final Executor executor = getExecutorService();
//
//            // Set true iff we will run with the global lock.
//            boolean willRunWithLock = false;
//            try {
//
//                /*
//                 * Submit task to interrupt the task that is attempting to
//                 * acquire the lock if the quorum breaks. This prevents us
//                 * waiting for the global long beyond a quorum break.
//                 */
//                executor.execute(interruptLockTaskFuture);
//
//                /*
//                 * Submit task to acquire the lock.
//                 */
//                executor.execute(acquireLockTaskFuture);
//
//                /*
//                 * Wait for the global lock (blocks up to the timeout).
//                 */
//                acquireLockTaskFuture.get(req.getLockWaitTimeout(),
//                        req.getLockWaitUnits());
//                
//                // We will run with the global lock.
//                willRunWithLock = true;
//                
//            } catch (RejectedExecutionException ex) {
//                
//                /*
//                 * Note: This will throw a RejectedExecutionException if the
//                 * executor has been shutdown. That unchecked exception will be
//                 * thrown back to the client. Since the lock has not been
//                 * acquired if that exception is thrown, we do not need to do
//                 * anything else here.
//                 */
//                
//                haLog.warn(ex);
//                
//                throw ex;
//
//            } catch (ExecutionException e) {
//            
//                haLog.error(e, e);
//                
//                throw new RuntimeException(e);
//                
//            } finally {
//
//                /*
//                 * Make sure these tasks are cancelled.
//                 */
//
//                interruptLockTaskFuture.cancel(true/* mayInterruptIfRunning */);
//                
//                acquireLockTaskFuture.cancel(true/* mayInterruptIfRunning */);
//
//                /*
//                 * Release the global lock if we acquired it but will not run
//                 * with that lock held (e.g., due to some error).
//                 */
//
//                if (!willRunWithLock && didAcquire.get()) {
//            
//                    HAJournal.this.releaseUnisolatedConnection();
//                    
//                    log.warn(RELEASED_GLOBAL_WRITE_LOCK);
//                    
//                }
//            
//            }
//
//            if (!didAcquire.get())
//                throw new AssertionError();
//
//            if (!willRunWithLock)
//                throw new AssertionError();
//
//            try {
//                
//                // Run task that will eventually release the global lock.
//                executor.execute(releaseLockTaskFuture);
//                
//            } catch (RejectedExecutionException ex) {
//                /*
//                 * If we could not run this task, then make sure that we release
//                 * the global write lock.
//                 */
//                HAJournal.this.releaseUnisolatedConnection();
//                haLog.warn(RELEASED_GLOBAL_WRITE_LOCK);
//            }
//
//            // Return *ASYNCHRONOUS* proxy (interruptable).
//            return getProxy(releaseLockTaskFuture, true/* asynch */);
//            
//        }
//
//        /**
//         * Task interrupts the {@link AcquireGlobalLockTask} if the quorum
//         * breaks.
//         */
//        private class InterruptAcquireLockTask implements Callable<Void> {
//
//            private final long token;
//            private final Future<Void> acquireLockTaskFuture;
//            private final IHAGlobalWriteLockRequest req;
//
//            public InterruptAcquireLockTask(final long token,
//                    final FutureTask<Void> acquireLockTaskFuture,
//                    final IHAGlobalWriteLockRequest req) {
//
//                this.token = token;
//
//                this.acquireLockTaskFuture = acquireLockTaskFuture;
//
//                this.req = req;
//
//            }
//
//            public Void call() throws Exception {
//
//                try {
//
//                    // This service must be the leader.
//                    getQuorum().assertLeader(token);
//
//                    // Exit if quorum breaks before the timeout.
//                    getQuorum().awaitBreak(req.getLockWaitTimeout(),
//                            req.getLockWaitUnits());
//
//                } catch (InterruptedException ex) {
//
//                    // Ignore. Expected.
//
//                } catch (AsynchronousQuorumCloseException e) {
//
//                    // Cancel task waiting for global lock.
//                    acquireLockTaskFuture
//                            .cancel(true/* mayInterruptIfRunning */);
//
//                } catch (TimeoutException e) {
//
//                    // Cancel task waiting for global lock.
//                    acquireLockTaskFuture
//                            .cancel(true/* mayInterruptIfRunning */);
//
//                }
//                
//                // Done.
//                return null;
//
//            }
//            
//        }
//        
//        /**
//         * Task to wait up to a timeout to acquire the global write lock.
//         */
//        private class AcquireGlobalLockTask implements Callable<Void> {
//
//            private final AtomicBoolean didAcquire;
//
//            public AcquireGlobalLockTask(final AtomicBoolean didAcquire) {
//
//                this.didAcquire = didAcquire;
//
//            }
//
//            public Void call() throws Exception {
//
//                // Acquire the global lock.
//                HAJournal.this.acquireUnisolatedConnection();
//
//                didAcquire.set(true);
//
//                haLog.warn(ACQUIRED_GLOBAL_WRITE_LOCK);
//
//                // Done.
//                return null;
//
//            }
//
//        }
//
//        /**
//         * Task to hold the global write lock and release it after a timeout.
//         */
//        private class ReleaseGlobalLockTask implements Callable<Void> {
//
//            private final long token;
//            private final IHAGlobalWriteLockRequest req;
//
//            public ReleaseGlobalLockTask(final long token,
//                    final IHAGlobalWriteLockRequest req) {
//
//                this.token = token;
//                this.req = req;
//
//            }
//
//            public Void call() throws Exception {
//
//                try {
//
//                    /*
//                     * Wait up to the timeout.
//                     * 
//                     * Note: If the quorum breaks such that this service is no
//                     * longer the leader for the specified token, then this loop
//                     * will exit before the timeout would have expired.
//                     */
//
//                    // This service must be the leader.
//                    getQuorum().assertLeader(token);
//
//                    // Wait up to the timeout for a quorum break.
//                    getQuorum().awaitBreak(req.getLockHoldTimeout(),
//                            req.getLockHoldUnits());
//                    
//                } catch (InterruptedException ex) {
//                    
//                    // Ignore. Expected.
//
//                } catch (TimeoutException ex) {
//
//                    haLog.warn("Timeout.");
//                    
//                } finally {
//                    
//                    // Release the global lock.
//                    HAJournal.this.releaseUnisolatedConnection();
//                
//                    haLog.warn(RELEASED_GLOBAL_WRITE_LOCK);
//                    
//                }
//
//                // Done.
//                return null;
//
//            }
//            
//        }
        
        @Override
        public Future<IHASnapshotResponse> takeSnapshot(
                final IHASnapshotRequest req) throws IOException {

            final Future<IHASnapshotResponse> ft = getSnapshotManager()
                    .takeSnapshot(req);

            return ft == null ? null : getProxy(ft, true/* async */);

        }
        
        @Override
        public Future<Void> rebuildFromLeader(final IHARemoteRebuildRequest req)
                throws IOException {

            final HAQuorumService<HAGlue, HAJournal> localService = getQuorumService();

            final RunStateEnum innerRunState = (localService == null ? null
                    : localService.getRunStateEnum());
            
            if(innerRunState == null)
                return null;

            switch (innerRunState) {
            case Error:
            case SeekConsensus:
            case Operator: {

                if (localService == null)
                    return null;

                final Future<Void> f = localService
                        .rebuildFromLeader(new HARemoteRebuildRequest());

                if (f == null)
                    return null;

                haLog.warn("Started REBUILD: runState=" + innerRunState);
                
                return getProxy(f, true/* async */);
                
            }
            case Rebuild:
                // Already running rebuild.
            case Restore:
                // Running restore. Can not do rebuild.
            case Resync:
                // Running resync.  Can not do rebuild.
            case RunMet:
                // RunMet.  Can not do rebuild.
            case Shutdown:
                // Shutting down.  Can not do rebuild.
                haLog.warn("Can not REBUILD: runState=" + innerRunState);
                return null;
            default:
                // Unknown run state.
                throw new AssertionError("innerRunState=" + innerRunState);
            }

        }
        
        /**
         * Note: The invocation layer factory is reused for each exported proxy (but
         * the exporter itself is paired 1:1 with the exported proxy).
         */
        final private InvocationLayerFactory invocationLayerFactory = new BasicILFactory();
        
        /**
         * Return an {@link Exporter} for a single object that implements one or
         * more {@link Remote} interfaces.
         * <p>
         * Note: This uses TCP Server sockets.
         * <p>
         * Note: This uses [port := 0], which means a random port is assigned.
         * <p>
         * Note: The VM WILL NOT be kept alive by the exported proxy (keepAlive
         * is <code>false</code>).
         * 
         * @param enableDGC
         *            if distributed garbage collection should be used for the
         *            object to be exported.
         * 
         * @return The {@link Exporter}.
         * 
         *         TODO This should be based on the {@link Configuration} object
         *         (the EXPORTER field). See AbstractServer.
         */
        protected Exporter getExporter(final boolean enableDGC) {

            return new BasicJeriExporter(TcpServerEndpoint
                    .getInstance(0/* port */), invocationLayerFactory, enableDGC,
                    false/* keepAlive */);
            
        }

        /**
         * {@inheritDoc}
         * <p>
         * Note: {@link Future}s generated by <code>java.util.concurrent</code>
         * are NOT {@link Serializable}. Further note the proxy as generated by
         * an {@link Exporter} MUST be encapsulated so that the object returned
         * to the caller can implement {@link Future} without having to declare
         * that the methods throw {@link IOException} (for RMI).
         * 
         * @param future
         *            The future.
         * 
         * @return A proxy for that {@link Future} that masquerades any RMI
         *         exceptions.
         */
        @Override
        protected <E> Future<E> getProxy(final Future<E> future,
                final boolean asyncFuture) {

            if (!asyncFuture) {
                /*
                 * This was borrowed from a fix for a DGC thread leak on the
                 * clustered database. Returning a Future so the client can wait
                 * on the outcome is often less desirable than having the
                 * service compute the Future and then return a think future.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/433
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/437
                 */
                return new ThickFuture<E>(future);
            }

            /**
             * Setup the Exporter for the Future.
             * 
             * Note: Distributed garbage collection is enabled since the proxied
             * future CAN become locally weakly reachable sooner than the client
             * can get() the result. Distributed garbage collection handles this
             * for us and automatically unexports the proxied iterator once it
             * is no longer strongly referenced by the client.
             * 
             * Note: DGC is observed to leak native threads and should not be
             * used for any common operations.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/433"
             *      > Cluster leaks threads under read-only index operations
             *      </a>
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/437"
             *      > Thread-local cache combined with unbounded thread pools
             *      causes effective memory leak </a>
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/673"
             *      >Native thread leak in HAJournalServer process</a>
             */
            
            final boolean enableDGC = true;
            
            final Exporter exporter = getExporter(enableDGC);

            // wrap the future in a proxyable object.
            final RemoteFuture<E> impl = new RemoteFutureImpl<E>(future);

            /*
             * Export the proxy.
             */
            final RemoteFuture<E> proxy;
            try {

                // export proxy.
                proxy = (RemoteFuture<E>) exporter.export(impl);

                if (log.isDebugEnabled()) {

                    log.debug("Exported proxy: proxy=" + proxy + "("
                            + proxy.getClass() + ")");

                }

            } catch (ExportException ex) {

                throw new RuntimeException("Export error: " + ex, ex);

            }

            // return proxy to caller.
            return new ClientFuture<E>(proxy);

        }

        /*
         * 
         */
        
        /**
         * Returns an object that implements whatever administration interfaces
         * are appropriate for the particular service.
         * 
         * @return an object that implements whatever administration interfaces
         *         are appropriate for the particular service.
         */
        @Override
        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info("serviceID=" + server.getServiceID());

            return server.getProxy();

        }
                
        /*
         * DestroyAdmin
         */

        @Override
        public void destroy() {

            server.runShutdown(true/* destroy */);

        }

        @Override
        public void shutdown() {

            server.runShutdown(false/* destroy */);

        }

        @Override
        public void shutdownNow() {

            server.runShutdown(false/* destroy */);

        }

        /*
         * Misc.
         */
        
        /**
         * {@inheritDoc}
         * <p>
         * Note: The actual port depends on how jetty was configured in
         * <code>jetty.xml</code>. This returns the port associated with the
         * first jetty connection.
         * 
         * @see <a
         *      href="http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty">
         *      Embedding Jetty </a>
         */
        @Override
        public int getNSSPort() {

            return server.getNSSPort();

        }
//        @Override
//        public int getNSSPort() {
//
//            final String COMPONENT = NSSConfigurationOptions.COMPONENT;
//
//            try {
//
//                final Integer port = (Integer) server.config.getEntry(
//                        COMPONENT, NSSConfigurationOptions.PORT, Integer.TYPE,
//                        NSSConfigurationOptions.DEFAULT_PORT);
//
//                return port;
//
//            } catch (ConfigurationException e) {
//
//                throw new RuntimeException(e);
//                
//            }
//
//        }

        @Override
        public RunState getRunState() {
            
            return server.getRunState();
            
        }

//        /**
//         * {@inheritDoc}
//         * <p>
//         * Extended to kick the {@link HAJournalServer} into an error state. It
//         * will recover from that error state by re-entering seek consensus.
//         */
//        @Override
//        protected void doRejectedCommit() {
//        
//            super.doRejectedCommit();
//            
//            getQuorumService().enterErrorState();
//            
//        }
        
        /**
         * Return this quorum member, appropriately cast.
         * 
         * @return The quorum member -or- <code>null</code> if the quorum is not
         *         running.
         */
        protected HAQuorumService<HAGlue, HAJournal> getQuorumService() {

            // This quorum member.
            @SuppressWarnings("unchecked")
            final HAQuorumService<HAGlue, HAJournal> quorumService = (HAQuorumService<HAGlue, HAJournal>) getQuorum()
                    .getClient();

            return quorumService;

        }
        
        @Override
        public String getExtendedRunState() {

            final HAJournalServer server = getHAJournalServer();
            
            // This quorum member.
            final HAQuorumService<HAGlue, HAJournal> quorumService = getQuorumService();

            final RunStateEnum innerRunState = (quorumService == null ? null
                    : quorumService.getRunStateEnum());

            final HAJournal journal = HAJournal.this;

            final StringBuilder innerRunStateStr = new StringBuilder();
            if (innerRunState != null) {
                innerRunStateStr.append(innerRunState.name());
//                switch (innerRunState) {
//                case Resync:
//                    innerRunStateStr.append(" @ "
//                            + journal.getRootBlockView().getCommitCounter());
//                    break;
//                case Operator: {
//                    final String msg = server.getOperatorAlert();
//                    innerRunStateStr.append("msg=" + msg);
//                    break;
//                }
//                default:
//                    break;
//                }
            } else {
                innerRunStateStr.append("N/A");
            }
            final boolean debug = true;
            innerRunStateStr.append(" @ "
                    + journal.getRootBlockView().getCommitCounter());
            if(debug)
                innerRunStateStr.append(", haReady=" + getHAReady());
            innerRunStateStr.append(", haStatus=" + getHAStatus());
            if(debug)
                innerRunStateStr.append(", serviceId="
                    + (quorumService == null ? "N/A" : quorumService
                            .getServiceId()));
            /*
             * TODO This is not a TXS timestamp. That would be more useful but I
             * want to avoid taking the TXS lock. [It looks like the TXS does
             * not need that synchronized keyword on nextTimestamp(). Try
             * removing it and then using it here.]
             */
            if(debug)
                innerRunStateStr.append(", now=" + System.currentTimeMillis());
            final String msg = server.getOperatorAlert();
            if (msg != null)
                innerRunStateStr.append(", msg=[" + msg + "]");
            return "{server=" + server.getRunState() + ", quorumService="
                    + innerRunStateStr + "}";

        }

    }
    
}
