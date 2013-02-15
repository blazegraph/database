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
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jini.config.Configuration;
import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.tcp.TcpServerEndpoint;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.halog.HALogReader;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HADigestResponse;
import com.bigdata.ha.msg.HALogDigestResponse;
import com.bigdata.ha.msg.HALogRootBlocksResponse;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHAGlobalWriteLockRequest;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ValidationError;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.proxy.ClientFuture;
import com.bigdata.service.proxy.RemoteFuture;
import com.bigdata.service.proxy.RemoteFutureImpl;
import com.bigdata.service.proxy.ThickFuture;

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

    private static final String ACQUIRED_GLOBAL_WRITE_LOCK = "Acquired global write lock.";

    private static final String RELEASED_GLOBAL_WRITE_LOCK = "Released global write lock.";

    public interface Options extends Journal.Options {
        
        /**
         * The address at which this journal exposes its write pipeline
         * interface (a socket level interface for receiving write cache blocks
         * from another service in the met quorum).
         */
        String WRITE_PIPELINE_ADDR = HAJournal.class.getName()
                + ".writePipelineAddr";

//        /**
//         * The address at which this journal exposes its resynchronization
//         * interface (a socket level interface for receiving write cache blocks
//         * from another service in the met quorum).
//         */
//        String RESYNCH_ADDR = HAJournal.class.getName() + ".resyncAddr";

        /**
         * The timeout in milliseconds that the leader will await the followers
         * to prepare for a 2-phase commit.
         * <p>
         * Note: The timeout must be set with a realistic expectation concerning
         * the possibility of garbage collection. A long GC pause could
         * otherwise cause the 2-phase commit to fail. With this in mind, a
         * reasonable timeout is on the order of 10 seconds.
         */
        String HA_PREPARE_TIMEOUT = HAJournal.class.getName() + ".HAPrepareTimeout";

        String DEFAULT_HA_PREPARE_TIMEOUT = "10000"; // milliseconds.
        
        long HA_MIN_PREPARE_TIMEOUT = 100; // milliseconds.
        
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
        String HA_LOG_DIR = HAJournal.class.getName() + ".haLogDir";
        
        String DEFAULT_HA_LOG_DIR = "HALog";
        
    }
    
    /**
     * @see Options#WRITE_PIPELINE_ADDR
     */
    private final InetSocketAddress writePipelineAddr;

    /**
     * @see Options#HA_PREPARE_TIMEOUT
     */
    private final long haPrepareTimeout;
    
    /**
     * @see Options#HA_LOG_DIR
     */
    private final File haLogDir;
    
    /**
     * Write ahead log for replicated writes used to resynchronize services that
     * are not in the met quorum.
     * 
     * @see Options#HA_LOG_DIR
     * @see HALogWriter
     */
    private final HALogWriter haLogWriter;

    /**
     * The {@link HALogWriter} for this {@link HAJournal} and never
     * <code>null</code>.
     */
    HALogWriter getHALogWriter() {

        return haLogWriter;
        
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
    
    public HAJournal(final Properties properties) {

        this(properties, null);
        
    }

    public HAJournal(final Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        super(checkProperties(properties), quorum);

        /*
         * Note: We need this so pass it through to the HAGlue class below.
         * Otherwise this service does not know where to setup its write
         * replication pipeline listener.
         */
        
        writePipelineAddr = (InetSocketAddress) properties
                .get(Options.WRITE_PIPELINE_ADDR);

        haPrepareTimeout = Long.valueOf(properties.getProperty(
                Options.HA_PREPARE_TIMEOUT, Options.DEFAULT_HA_PREPARE_TIMEOUT));

        final String logDirStr = properties.getProperty(Options.HA_LOG_DIR,
                Options.DEFAULT_HA_LOG_DIR);

        haLogDir = new File(logDirStr);

        if (!haLogDir.exists()) {

            // Create the directory.
            haLogDir.mkdirs();

        }

        // Set up the HA log writer.
        haLogWriter = new HALogWriter(haLogDir);

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

        if (properties.get(Options.WRITE_PIPELINE_ADDR) == null) {

            throw new RuntimeException(Options.WRITE_PIPELINE_ADDR
                    + " : required property not found.");
        
        }

        final long prepareTimeout = Long.valueOf(properties
                .getProperty(Options.HA_PREPARE_TIMEOUT,
                        Options.DEFAULT_HA_PREPARE_TIMEOUT));

        if (prepareTimeout < Options.HA_MIN_PREPARE_TIMEOUT) {
            throw new RuntimeException(Options.HA_PREPARE_TIMEOUT + "="
                    + prepareTimeout + " : must be GTE "
                    + Options.HA_MIN_PREPARE_TIMEOUT);
        }
        
        return properties;

    }
    
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
    protected final void setQuorumToken(final long newValue) {
    
        super.setQuorumToken(newValue);

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
    
    @Override
    public final File getHALogDir() {

        return haLogDir;
        
    }

    @Override
    public final long getHAPrepareTimeout() {

        return haPrepareTimeout;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to close the {@link HALogWriter}.
     */
    @Override
    protected void _close() {
    
        try {
            haLogWriter.disable();
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
    
        recursiveDelete(getHALogDir(), new FileFilter() {

            @Override
            public boolean accept(File f) {
        
                if (f.isDirectory())
                    return true;
                
                return f.getName().endsWith(HALogWriter.HA_LOG_EXT);
            }

        });
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. Only files recognized by
     * {@link #getFileFilter()} will be deleted.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f,final FileFilter fileFilter) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(fileFilter);

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i], fileFilter);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to expose this method to the {@link HAQuorumService}.
     */
    @Override
    protected void installRootBlocksFromQuorum(
            final QuorumService<HAGlue> localService,
            final IRootBlockView rootBlock) {

        super.installRootBlocksFromQuorum(localService, rootBlock);
        
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
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to expose this method to the {@link HAQuorumService}.
     */
    @Override
    protected void doLocalAbort() {

        super.doLocalAbort();

    }
    
    /**
     * Extended implementation supports RMI.
     */
    protected class HAGlueService extends BasicHA {

        protected HAGlueService(final UUID serviceId) {

            super(serviceId, writePipelineAddr);

        }

        /*
         * ITransactionService
         * 
         * This interface is delegated to the Journal's local transaction
         * service. This service MUST be the quorum leader.
         * 
         * Note: If the quorum breaks, the service which was the leader will
         * invalidate all open transactions. This is handled in AbstractJournal.
         * 
         * FIXME We should really pair the quorum token with the transaction
         * identifier in order to guarantee that the quorum token does not
         * change (e.g., that the quorum does not break) across the scope of the
         * transaction. That will require either changing the
         * ITransactionService API and/or defining an HA variant of that API.
         */
        
        @Override
        public long newTx(final long timestamp) throws IOException {

            getQuorum().assertLeader(getQuorumToken());

            // Delegate to the Journal's local transaction service.
            return HAJournal.this.newTx(timestamp);

        }

        @Override
        public long commit(final long tx) throws ValidationError, IOException {

            getQuorum().assertLeader(getQuorumToken());

            // Delegate to the Journal's local transaction service.
            return HAJournal.this.commit(tx);

        }

        @Override
        public void abort(final long tx) throws IOException {

            getQuorum().assertLeader(getQuorumToken());
            
            // Delegate to the Journal's local transaction service.
            HAJournal.this.abort(tx);

        }

        @Override
        public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
                final IHALogRootBlocksRequest msg) throws IOException {

            // The commit counter of the desired closing root block.
            final long commitCounter = msg.getCommitCounter();

            final File logFile = new File(haLogDir,
                    HALogWriter.getHALogFileName(commitCounter));

            if (!logFile.exists()) {

                // No log for that commit point.
                throw new FileNotFoundException(logFile.getName());

            }

            final HALogReader r = new HALogReader(logFile);

            final HALogRootBlocksResponse resp = new HALogRootBlocksResponse(
                    r.getOpeningRootBlock(), r.getClosingRootBlock());

            if (haLog.isDebugEnabled())
                haLog.debug("msg=" + msg + ", resp=" + resp);

            return resp;

        }

        @Override
        public Future<Void> sendHALogForWriteSet(final IHALogRequest req)
                throws IOException {
            
            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            // The commit counter of the desired closing root block.
            final long commitCounter = req.getCommitCounter();

            /*
             * Note: The choice of the "live" versus a historical "closed" log
             * file needs to be an atomic decision and thus MUST be made by the
             * HALogManager.
             */
            final IHALogReader r = getHALogWriter().getReader(commitCounter);

            // Task sends an HALog file along the pipeline.
            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new SendHALogTask(req, r));

            // Run task.
            getExecutorService().submit(ft);
            
            // Return *ASYNCHRONOUS* proxy (interruptable).
            return getProxy(ft, true/* asynch */);

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

                final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

                long nsent = 0;
                try {

                    while (r.hasMoreBuffers()) {

                        // IHABufferStrategy
                        final IHABufferStrategy strategy = HAJournal.this
                                .getBufferStrategy();

                        // get message and fill write cache buffer (unless WORM).
                        final IHAWriteMessage msg = r.processNextBuffer(buf.buffer());
                        
                        if (haLog.isDebugEnabled())
                            haLog.debug("req=" + req + ", msg=" + msg);

                        // drop them into the write pipeline.
                        final Future<Void> ft = strategy.sendHALogBuffer(req, msg,
                                buf);

                        try {
                            // wait for message to make it through the pipeline.
                            ft.get();
                        } finally {
                            ft.cancel(true/* mayInterruptIfRunning */);
                        }
                        
                        nsent++;
                        
                    }

                    if (haLog.isDebugEnabled())
                        haLog.debug("req=" + req + ", nsent=" + nsent);

                    return null;

                } finally {

                    buf.release();

                }

            }

        }

        /*
         * REBUILD: Take a read lock and send everything from the backing
         * file, but do not include the root blocks. The first buffer can be
         * short (to exclude the root blocks). That will put the rest of the
         * buffers on a 1MB boundary which will provide more efficient IOs.
         */
        @Override
        public Future<Void> sendHAStore(final IHARebuildRequest req)
                throws IOException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req);

            // Task sends an HALog file along the pipeline.
            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new SendStoreTask(req));

            // Run task.
            getExecutorService().submit(ft);

            // Return *ASYNCHRONOUS* proxy (interruptable).
            return getProxy(ft, true/* asynch */);

        }

        /**
         * Class sends the backing file along the write pipeline.
         */
        private class SendStoreTask implements Callable<Void> {
            
            private final IHARebuildRequest req;
            
            public SendStoreTask(final IHARebuildRequest req) {
                
                if(req == null)
                    throw new IllegalArgumentException();
                
                this.req = req;
                
            }
            
            public Void call() throws Exception {
                
                // The quorum token (must remain valid through this operation).
                final long quorumToken = getQuorumToken();
                
                // Grab a read lock.
                final long txId = newTx(ITx.READ_COMMITTED);
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
                    
                    // The offset (relative to the root blocks).
                    long offset = 0L;
                    
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

                        getBufferStrategy().sendRawBuffer(req, sequence,
                                quorumToken, fileExtent, offset, nbytes, b);

                        remaining -= nbytes;

                        sequence++;
                        
                    }

                    if (haLog.isInfoEnabled())
                        haLog.info("Sent store file: #blocks=" + sequence
                                + ", #bytes=" + (fileExtent - headerSize));

                    // Done.
                    return null;

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

            return new HADigestResponse(req.getStoreUUID(), digest.digest());
            
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
            final IHALogReader r = getHALogWriter().getReader(commitCounter);

            final MessageDigest digest = MessageDigest.getInstance("MD5");

            r.computeDigest(digest);

            return new HALogDigestResponse(req.getCommitCounter(),
                    digest.digest());

        }

        /**
         * {@inheritDoc}
         * 
         * TODO HA BACKUP: This method relies on the unisolated semaphore. That
         * provides a sufficient guarantee for updates that original through the
         * NSS since all such updates will eventually require the unisolated
         * connection to execute. However, if we support multiple concurrent
         * unisolated connections distinct KBs per the ticket below, then we
         * will need to have a different global write lock - perhaps via the
         * {@link WriteExecutorService}.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/566 (
         *      Concurrent unisolated operations against multiple KBs on the
         *      same Journal)
         */
        @Override
        public Future<Void> globalWriteLock(final IHAGlobalWriteLockRequest req)
                throws IOException, InterruptedException, TimeoutException {

            if (req == null)
                throw new IllegalArgumentException();

            /*
             * This token will be -1L if there is no met quorum. This method may
             * only execute while there is a met quorum and this service is the
             * leader for that met quorum.
             * 
             * Note: This method must stop waiting for the global lock if this
             * service is no longer the leader (quorum break).
             * 
             * Note: This method must stop holding the global lock if this
             * service is no longer the leader (quorum break).
             */
            final long token = getQuorum().token();

            // Verify that the quorum is met and that this is the leader.
            getQuorum().assertLeader(token);

            // Set true IFF we acquire the global write lock.
            final AtomicBoolean didAcquire = new AtomicBoolean(false);

            // Task to acquire the lock
            final FutureTask<Void> acquireLockTaskFuture = new FutureTask<Void>(
                    new AcquireGlobalLockTask(didAcquire));

            // Task used to interrupt task acquiring the lock if quorum breaks.
            final FutureTask<Void> interruptLockTaskFuture = new FutureTask<Void>(
                    new InterruptAcquireLockTask(token, acquireLockTaskFuture,
                            req));

            // Task to release the lock.
            final FutureTask<Void> releaseLockTaskFuture = new FutureTask<Void>(
                    new ReleaseGlobalLockTask(token, req));

            // Service to run those tasks.
            final Executor executor = getExecutorService();

            // Set true iff we will run with the global lock.
            boolean willRunWithLock = false;
            try {

                /*
                 * Submit task to interrupt the task that is attempting to
                 * acquire the lock if the quorum breaks. This prevents us
                 * waiting for the global long beyond a quorum break.
                 */
                executor.execute(interruptLockTaskFuture);

                /*
                 * Submit task to acquire the lock.
                 */
                executor.execute(acquireLockTaskFuture);

                /*
                 * Wait for the global lock (blocks up to the timeout).
                 */
                acquireLockTaskFuture.get(req.getLockWaitTimeout(),
                        req.getLockWaitUnits());
                
                // We will run with the global lock.
                willRunWithLock = true;
                
            } catch (RejectedExecutionException ex) {
                
                /*
                 * Note: This will throw a RejectedExecutionException if the
                 * executor has been shutdown. That unchecked exception will be
                 * thrown back to the client. Since the lock has not been
                 * acquired if that exception is thrown, we do not need to do
                 * anything else here.
                 */
                
                haLog.warn(ex);
                
                throw ex;

            } catch (ExecutionException e) {
            
                haLog.error(e, e);
                
                throw new RuntimeException(e);
                
            } finally {

                /*
                 * Make sure these tasks are cancelled.
                 */

                interruptLockTaskFuture.cancel(true/* mayInterruptIfRunning */);
                
                acquireLockTaskFuture.cancel(true/* mayInterruptIfRunning */);

                /*
                 * Release the global lock if we acquired it but will not run
                 * with that lock held (e.g., due to some error).
                 */

                if (!willRunWithLock && didAcquire.get()) {
            
                    HAJournal.this.releaseUnisolatedConnection();
                    
                    log.warn(RELEASED_GLOBAL_WRITE_LOCK);
                    
                }
            
            }

            if (!didAcquire.get())
                throw new AssertionError();

            if (!willRunWithLock)
                throw new AssertionError();

            try {
                
                // Run task that will eventually release the global lock.
                executor.execute(releaseLockTaskFuture);
                
            } catch (RejectedExecutionException ex) {
                /*
                 * If we could not run this task, then make sure that we release
                 * the global write lock.
                 */
                HAJournal.this.releaseUnisolatedConnection();
                haLog.warn(RELEASED_GLOBAL_WRITE_LOCK);
            }

            // Return *ASYNCHRONOUS* proxy (interruptable).
            return getProxy(releaseLockTaskFuture, true/* asynch */);
            
        }

        /**
         * Task interrupts the {@link AcquireGlobalLockTask} if the quorum
         * breaks.
         */
        private class InterruptAcquireLockTask implements Callable<Void> {

            private final long token;
            private final Future<Void> acquireLockTaskFuture;
            private final IHAGlobalWriteLockRequest req;

            public InterruptAcquireLockTask(final long token,
                    final FutureTask<Void> acquireLockTaskFuture,
                    final IHAGlobalWriteLockRequest req) {

                this.token = token;

                this.acquireLockTaskFuture = acquireLockTaskFuture;

                this.req = req;

            }

            public Void call() throws Exception {

                try {

                    // This service must be the leader.
                    getQuorum().assertLeader(token);

                    // Exit if quorum breaks before the timeout.
                    getQuorum().awaitBreak(req.getLockWaitTimeout(),
                            req.getLockWaitUnits());

                } catch (InterruptedException ex) {

                    // Ignore. Expected.

                } catch (AsynchronousQuorumCloseException e) {

                    // Cancel task waiting for global lock.
                    acquireLockTaskFuture
                            .cancel(true/* mayInterruptIfRunning */);

                } catch (TimeoutException e) {

                    // Cancel task waiting for global lock.
                    acquireLockTaskFuture
                            .cancel(true/* mayInterruptIfRunning */);

                }
                
                // Done.
                return null;

            }
            
        }
        
        /**
         * Task to wait up to a timeout to acquire the global write lock.
         */
        private class AcquireGlobalLockTask implements Callable<Void> {

            private final AtomicBoolean didAcquire;

            public AcquireGlobalLockTask(final AtomicBoolean didAcquire) {

                this.didAcquire = didAcquire;

            }

            public Void call() throws Exception {

                // Acquire the global lock.
                HAJournal.this.acquireUnisolatedConnection();

                didAcquire.set(true);

                haLog.warn(ACQUIRED_GLOBAL_WRITE_LOCK);

                // Done.
                return null;

            }

        }

        /**
         * Task to hold the global write lock and release it after a timeout.
         */
        private class ReleaseGlobalLockTask implements Callable<Void> {

            private final long token;
            private final IHAGlobalWriteLockRequest req;

            public ReleaseGlobalLockTask(final long token,
                    final IHAGlobalWriteLockRequest req) {

                this.token = token;
                this.req = req;

            }

            public Void call() throws Exception {

                try {

                    /*
                     * Wait up to the timeout.
                     * 
                     * Note: If the quorum breaks such that this service is no
                     * longer the leader for the specified token, then this loop
                     * will exit before the timeout would have expired.
                     */

                    // This service must be the leader.
                    getQuorum().assertLeader(token);

                    // Wait up to the timeout for a quorum break.
                    getQuorum().awaitBreak(req.getLockHoldTimeout(),
                            req.getLockHoldUnits());
                    
                } catch (InterruptedException ex) {
                    
                    // Ignore. Expected.

                } catch (TimeoutException ex) {

                    haLog.warn("Timeout.");
                    
                } finally {
                    
                    // Release the global lock.
                    HAJournal.this.releaseUnisolatedConnection();
                
                    haLog.warn(RELEASED_GLOBAL_WRITE_LOCK);
                    
                }

                // Done.
                return null;

            }
            
        }
        
        @Override
        public Future<Void> bounceZookeeperConnection() {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new BounceZookeeperConnectionTask(), null/* result */);

            ft.run();
            
            return getProxy(ft);

        }

        private class BounceZookeeperConnectionTask implements Runnable {

            @SuppressWarnings("rawtypes")
            public void run() {

                if (getQuorum() instanceof ZKQuorumImpl) {

                    try {

                        haLog.warn("BOUNCING ZOOKEEPER CONNECTION.");

                        // Close the current connection (if any).
                        ((ZKQuorumImpl) getQuorum()).getZookeeper().close();

                        // Obtain a new connection.
                        ((ZKQuorumImpl) getQuorum()).getZookeeper();

                        haLog.warn("RECONNECTED TO ZOOKEEPER.");

                    } catch (InterruptedException e) {

                        // Propagate the interrupt.
                        Thread.currentThread().interrupt();

                    }

                }
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
         * Note: The VM WILL NOT be kept alive by the exported proxy (keepAlive is
         * <code>false</code>).
         * 
         * @param enableDGC
         *            if distributed garbage collection should be used for the
         *            object to be exported.
         * 
         * @return The {@link Exporter}.
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

            /*
             * Setup the Exporter for the Future.
             * 
             * Note: Distributed garbage collection is enabled since the proxied
             * future CAN become locally weakly reachable sooner than the client
             * can get() the result. Distributed garbage collection handles this
             * for us and automatically unexports the proxied iterator once it
             * is no longer strongly referenced by the client.
             */
            final Exporter exporter = getExporter(true/* enableDGC */);

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

    }

}
