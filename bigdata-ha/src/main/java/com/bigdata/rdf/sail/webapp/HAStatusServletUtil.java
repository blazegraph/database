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
package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HARemoteRebuildRequest;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.journal.jini.ha.HALogIndex.IHALogRecord;
import com.bigdata.journal.jini.ha.HALogNexus;
import com.bigdata.journal.jini.ha.ISnapshotPolicy;
import com.bigdata.journal.jini.ha.SnapshotIndex.ISnapshotRecord;
import com.bigdata.journal.jini.ha.SnapshotManager;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumClient;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.StatusServlet.DigestEnum;
import com.bigdata.util.Banner;
import com.bigdata.util.BigdataStatics;
import com.bigdata.zookeeper.DumpZookeeper;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Class supports the {@link StatusServlet} and isolates code that has a
 * dependency on zookeeper so we do not drag in zookeeper for embedded
 * {@link NanoSparqlServer} deployments.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/612 (Bigdata scale-up
 *      depends on zookeper)
 */
public class HAStatusServletUtil extends HAStatusServletUtilProxy {

    private static final boolean debug = true;
    
    static private final transient Logger log = Logger.getLogger(HAStatusServletUtil.class);
    
    /**
     * Disaster recover of this service from the leader (REBUILD).
     * 
     * @see HAGlue#rebuildFromLeader(IHARemoteRebuildRequest)
     * 
     * TODO Move this declaration to {@link StatusServlet} once we are done
     * reconciling between the 1.2.x maintenance branch and the READ_CACHE
     * branch.
     */
    static final String REBUILD = "rebuild";

    /**
     * Force this service into the error state. It will automatically attempt to
     * recover from the error state. This basically "kicks" the service.
     * 
     * @see QuorumService#enterErrorState()
     * 
     *      TODO Move this declaration to {@link StatusServlet} once we are done
     *      reconciling between the 1.2.x maintenance branch and the READ_CACHE
     *      branch.
     */
    static final String ERROR = "error";

    final private IIndexManager indexManager;

    public HAStatusServletUtil(final IIndexManager indexManager) {
    	
    	super(indexManager);
    	
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;

    }

    /* (non-Javadoc)
	 * @see com.bigdata.rdf.sail.webapp.IHAStatusServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse, com.bigdata.rdf.sail.webapp.XMLBuilder.Node)
	 */
    @Override
	public void doGet(final HttpServletRequest req,
            final HttpServletResponse resp, final XMLBuilder.Node current)
            throws IOException {

        if (!(indexManager instanceof HAJournal))
            return;

        final HAJournal journal = (HAJournal) indexManager;

        @SuppressWarnings({ "unchecked", "rawtypes" })
		final ZKQuorumImpl<HAGlue, ZKQuorumClient<HAGlue>> quorum = (ZKQuorumImpl) journal
                .getQuorum();

        // The current token.
        final long quorumToken = quorum.token();

        // The last valid token.
        final long lastValidToken = quorum.lastValidToken();

        // This token is a bit different. It is sensitive to the journal role in
        // the quorum (joined or not).
        final long haReadyToken = journal.getHAReady();
        
        final int njoined = quorum.getJoined().length;

        /*
         * Note: This is the *local* HAGlueService.
         * 
         * This page must be robust to some new failure modes. The ZooKeeper
         * client can now be associated with an expired session, River discovery
         * can now be disabled, and the HAQuorumService might not be available
         * from quorum.getClient(). All of those things can happen if there is a
         * zookeeper session expiration that forces us to terminate the
         * HAQuorumService. This condition will be cured automatically (unless
         * the service is being shutdown), but only limited status information
         * can be provided while the HAQuorumService is not running.
         */
        final QuorumService<HAGlue> quorumService;
        {
            QuorumService<HAGlue> t;
            try {
                t = (QuorumService<HAGlue>) quorum.getClient();
            } catch (IllegalStateException ex) {
                // Note: Not available (quorum.start() not called)./
                t = null;
            }
            quorumService = t;
        }

        final DigestEnum digestEnum;
        {
            final String str = req.getParameter(StatusServlet.DIGESTS);
            if (str == null) {
                digestEnum = null;
            } else {
                if (str.trim().isEmpty()) {
                    digestEnum = StatusServlet.DEFAULT_DIGESTS;
                } else {
                    digestEnum = DigestEnum.valueOf(str.trim());
                }
            }
        }
        
        current.node("h1", "High Availability");

        {

            final XMLBuilder.Node p = current.node("p");

            // The quorum state
            if (quorumService == null) {

                p.text("The local quorum service is ")
                   .node("span").attr("id", "quorum-state").text("not running")
                   .close().text(".")
                .node("br").close();

            } else {
                    
                p.text("The quorum is ")
                   .node("span").attr("id", "quorum-state")
                   .text((quorum.isQuorumMet() ? "" : "not ") + "met")
                   .close().text(".")
                .node("br").close();

                p.node("span").attr("id", "njoined").text("" + njoined).close()
                .text(" out of ")
                   .node("span").attr("id", "replication-factor")
                   .text("" + quorum.replicationFactor()).close()
                .text(" services are joined.")
                .node("br").close();

                p.text("quorumToken=")
                   .node("span").attr("id", "quorum-token")
                   .text("" + quorumToken).close()
                .text(", lastValidToken=")
                   .node("span").attr("id", "last-valid-token")
                   .text("" + lastValidToken).close()
                .node("br").close();

                p.text("logicalServiceZPath=")
                   .node("span").attr("id", "logical-service-z-path")
                   .text(quorumService.getLogicalServiceZPath()).close()
                .node("br").close();

                p.text("PlatformStatsPlugIn=")
                   .node("span").attr("id", "platform-stats-plugin")
                   .text(journal.getPlatformStatisticsCollector() == null ?
                   "N/A" : "Running").close()
                .node("br").close();

                p.text("GangliaPlugIn=")
                   .node("span").attr("id", "ganglia-plugin")
                   .text(journal.getGangliaService() == null ? "N/A" :
                   "Running").close()
                .node("br").close();

                // Note: This is the *local* value of getHAStatus().
                // Note: The HAReady token reflects whether or not the service
                // is
                // joined.
                p.text("HAStatus: ")
                   .node("span").attr("id", "ha-status")
                   .text("" + quorumService.getService().getHAStatus()).close()
                .text(", HAReadyToken=")
                   .node("span").attr("id", "ha-ready-token")
                   .text("" + haReadyToken).close()
                .node("br").close();

                /*
                 * Report on the Service.
                 */
                {
                    p.text("Service: serviceId=")
                       .node("span").attr("id", "service-id")
                       .text("" + quorumService.getServiceId()).close()
                    .node("br").close();
                    p.text("Service: pid=")
                       .node("span").attr("id", "service-pid")
                       .text("" + quorumService.getPID()).close()
                    .node("br").close();
                    p.text("Service: path=")
                       .node("span").attr("id", "service-path")
                       .text("" + quorumService.getServiceDir()).close()
                    .node("br").close();
                    p.text("Service: proxy=")
                       .node("span").attr("id", "service-proxy")
                       .text("" + journal.getHAJournalServer().getProxy())
                       .close()
                    .node("br").close();

                }

            }

            /*
             * Report on the HA backup status (snapshot and restore policy).
             * 
             * Note: The age and commit counter for the available snapshots
             * are provided in another section (below).
             */
            {

                // snapshot policy.
                {
                    final SnapshotManager mgr = journal.getSnapshotManager();
//                    final IRootBlockView lastSnapshotRB = mgr
//                            .getNewestSnapshot();
//                    final long sinceCommitCounter = lastSnapshotRB == null ? -1L
//                            : lastSnapshotRB.getCommitCounter();
//                    final long haLogBytesOnDiskSinceLastSnapshot = mgr
//                            .getHALogFileBytesSinceCommitCounter(sinceCommitCounter);
                    final ISnapshotPolicy snapshotPolicy = mgr
                            .getSnapshotPolicy();
                    final boolean takeSnapshot = mgr
                            .isReadyToSnapshot(snapshotPolicy
                                    .newSnapshotRequest());
                    p.text("Service: snapshotPolicy=")
                       .node("span").attr("id", "snapshot-policy")
                       .text("" + snapshotPolicy).close()
                    .text(", shouldSnapshot=")
                       .node("span").attr("id", "take-snapshot")
                       .text("" + takeSnapshot).close()
//                            + ", lastSnapshotCommitCounter="
//                            + sinceCommitCounter//
//                            + ", HALogFileBytesOnDiskSinceLastSnapshot="
//                            + haLogBytesOnDiskSinceLastSnapshot//
                    .node("br").close();
                }
                // restore policy.
                p.text("Service: restorePolicy=")
                   .node("span").attr("id", "restore-policy")
                   .text("" + journal.getSnapshotManager().getRestorePolicy())
                   .close()
                .node("br").close();

                // HA Load Balancer.
                {
                    /*
                     * Note: MUST NOT HAVE A DIRECT REFERENCE TO THIS CLASS OR
                     * IT WILL BREAK THE WAR ARTIFACT WHEN DEPLOYED TO A
                     * NON-JETTY CONTAINER SINCE THE JETTY ProxyServlet WILL NOT
                     * BE FOUND.
                     */
                    try {
                        final Class<?> cls = Class
                                .forName("com.bigdata.rdf.sail.webapp.HALoadBalancerServlet");
                        final Method m = cls.getMethod("toString",
                                new Class[] { ServletContext.class });
                        final String rep = (String) m.invoke(null/* static */,
                                new Object[] { req.getServletContext() });
                        p.text("Service: LBSPolicy=").node("span")
                                .attr("id", "lbs-policy").text(rep).close()
                                .node("br").close();
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    } catch (SecurityException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalArgumentException e) {
                        throw new RuntimeException(e);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                }
//                if(true) {
//                    /*
//                     * HABackup: disable this code block. It is for
//                     * debug purposes only.
//                     */
//                    p.text("Service: getEarliestRestorableCommitPoint()="
//                            + journal.getSnapshotManager().getRestorePolicy().getEarliestRestorableCommitPoint(journal))
//                            .node("br").close();
//                }
                    
            }

            /*
             * Report on the Journal.
             */
            {
                final File file = journal.getFile();
                if (file != null) {
                    String digestStr = null;
                    if (digestEnum != null
                            && (digestEnum == DigestEnum.All || digestEnum == DigestEnum.Journal)) {
                        try {
                            final MessageDigest digest = MessageDigest
                                    .getInstance("MD5");
                            journal.getBufferStrategy().computeDigest(
                                    null/* snapshot */, digest);
                            digestStr = new BigInteger(1, digest.digest())
                                    .toString(16);
                        } catch (NoSuchAlgorithmException ex) {
                            // ignore
                        } catch (DigestException ex) {
                            // ignore
                        }
                    }
                    final IRootBlockView rb = journal.getRootBlockView();
                    final long commitCounter = rb.getCommitCounter();
//                    // Move this stuff to a TXS Status section?
//                    long releaseTime = -1;
//                    try {
//                        // Note: Can throw exception if quorum is not met.
//                        releaseTime = journal.getTransactionService()
//                                .getReleaseTime();
//                    } catch (QuorumException ex) {
//                        // Ignore.
//                    }
                    final long fileSize = file == null ? 0L : file.length();
                    p.text("HAJournal: file=")
                       .node("span").attr("id", "ha-journal-file")
                       .text("" + file).close()
                    .text(", commitCounter=")
                       .node("span").attr("id", "ha-journal-commit-counter")
                       .text("" + commitCounter).close()
                    .text(", nbytes=")
                       .node("span").attr("id", "ha-journal-nbytes")
                       .text("" + fileSize).close();
                    if(digestStr != null) {
                       p.text(", md5=")
                          .node("span").attr("id", "ha-journal-md5")
                          .text(digestStr).close();
                    }
//                            + (releaseTime != -1L ? ", releaseTime="
//                                    + RootBlockView.toString(releaseTime)//
//                            : "")//
                    p.node("br").close();

                    // Show the current root block.
                    if(debug) {
                       current.node("span").attr("id", "root-block")
                          .text(rb.toString()).close();
                    }
                }
            }

            /**
             * Report #of files and bytes in the HALog directory.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/670">
             *      Accumulating HALog files cause latency for HA commit</a>
             */
            {
                final HALogNexus nexus = journal.getHALogNexus();
                {
                    /*
                     * Use efficient index to compute the #of bytes (scan with
                     * sum) and the #of files.
                     */
                    int nfiles = 0;
                    long nbytes = 0L;
                    final Iterator<IHALogRecord> itr = nexus.getHALogs();
                    IHALogRecord r = null;
                    while (itr.hasNext()) {
                        r = itr.next();
                        nbytes += r.sizeOnDisk();
                        nfiles++;
                    }
                    /*
                     * Add in the current HALog file (if any).
                     */
                    final File currentFile = nexus.getHALogWriter().getFile();
                    if (currentFile != null) {
                        nbytes += currentFile.length();
                        nfiles++;
                    }
                    final String compressorKey = journal
                            .getProperties()
                            .getProperty(
                                    com.bigdata.journal.Options.HALOG_COMPRESSOR,
                                    com.bigdata.journal.Options.DEFAULT_HALOG_COMPRESSOR);
                    p.text("HALogDir: nfiles=")
                       .node("span").attr("id", "ha-log-dir-nfiles")
                       .text("" + nfiles).close()
                    .text(", nbytes=")
                       .node("span").attr("id", "ha-log-dir-nbytes")
                       .text("" + nbytes).close()
                    .text(", path=")
                       .node("span").attr("id", "ha-log-dir-path")
                       .text("" + nexus.getHALogDir()).close()
                    .text(", compressorKey=")
                       .node("span").attr("id", "ha-log-dir-compressor-key")
                       .text(compressorKey).close()
                    .text(", lastHALogClosed=")
                       .node("span").attr("id", "ha-log-dir-last-ha-log-closed")
                       .text((r == null ? "N/A" : CommitCounterUtility
                             .getCommitCounterStr(r.getCommitCounter())))
                             .close()
                    .text(", liveLog=")
                       .node("span").attr("id", "ha-log-dir-live-log")
                       .text((currentFile == null ? "N/A" : 
                          currentFile.getName())).close()
                    .node("br").close();
                }
                if (digestEnum != null
                        && (digestEnum == DigestEnum.All || digestEnum == DigestEnum.HALogs)) {
                    /*
                     * List each historical HALog file together with its digest.
                     * 
                     * Note: This can be VERY expensive.
                     */
                    final Iterator<IHALogRecord> itr = nexus.getHALogs();
                    while (itr.hasNext()) {
                        final IHALogRecord rec = itr.next();
                        final long nbytes = rec.sizeOnDisk();
                        final long closingCommitCounter = rec.getRootBlock()
                                .getCommitCounter();
                        String digestStr = null;
                        final File file = nexus
                                .getHALogFile(closingCommitCounter);
                        final IHALogReader r = nexus.getHALogWriter()
                                .getReader(closingCommitCounter);
                        try {
                            if (!r.isEmpty()) {
                                try {
                                    final MessageDigest digest = MessageDigest
                                            .getInstance("MD5");
                                    r.computeDigest(digest);
                                    digestStr = new BigInteger(1,
                                            digest.digest()).toString(16);
                                } catch (NoSuchAlgorithmException ex) {
                                    // ignore
                                } catch (DigestException ex) {
                                    // ignore
                                }
                            }
                        } finally {
                            r.close();
                        }
                        p.text("HALogFile: closingCommitCounter=")
                           .node("span").attr("id", "ha-log-file-closing-commit-counter")
                           .text("" + closingCommitCounter).close()
                        .text(", file=")
                           .node("span").attr("id", "ha-log-file-file")
                           .text("" + file).close()
                        .text(", nbytes=")
                           .node("span").attr("id", "ha-log-file-nbytes")
                           .text("" + nbytes).close();
                        if(digestStr != null) {
                           p.text(", md5=")
                              .node("span").attr("id", "ha-log-file-digest-str")
                              .text(digestStr).close();
                        }
                        p.node("br").close();
                    }
                }
            }

            /*
             * Report #of files and bytes in the snapshot directory.
             * 
             * Note: This uses the in-memory index rather than scanning the
             * directory in order to reduce latency associated with the file
             * system.
             */
            {
                {
//                    final File snapshotDir = journal
//                            .getSnapshotManager().getSnapshotDir();
//                    final File[] a = snapshotDir.listFiles(new FilenameFilter() {
//                        @Override
//                        public boolean accept(File dir, String name) {
//                            return name.endsWith(SnapshotManager.SNAPSHOT_EXT);
//                        }
//                    });
//                    for (File file : a) {
//                        nbytes += file.length();
//                        nfiles++;
//                    }
                    /*
                     * List the available snapshots (in order by increasing
                     * commitTime).
                     */
                    final Iterator<ISnapshotRecord> itr = journal
                            .getSnapshotManager().getSnapshots();
                    int nfiles = 0;
                    long nbytes = 0L;
                    while (itr.hasNext()) {
                        final ISnapshotRecord sr = itr.next();
                        nbytes += sr.sizeOnDisk();
                        nfiles++;
                    }
                    p.text("SnapshotDir: nfiles=")
                       .node("span").attr("id", "snapshot-dir-nfiles")
                       .text("" + nfiles).close()
                    .text(", nbytes=")
                       .node("span").attr("id", "snapshot-dir-nbytes")
                       .text("" + nbytes).close()
                    .text(", path=")
                       .node("span").attr("id", "snapshot-dir-path")
                       .text("" + journal.getSnapshotManager().getSnapshotDir()).close()
                    .node("br").close();
                }
                if (true) {

                    /*
                     * List the available snapshots (in order by increasing
                     * commitTime).
                     */
                    final Iterator<ISnapshotRecord> itr = journal
                            .getSnapshotManager().getSnapshots();

                    while(itr.hasNext()) {
                        final ISnapshotRecord r = itr.next();
                        final IRootBlockView rb = r.getRootBlock();
                        final long nbytes = r.sizeOnDisk();
//                        final File file = journal.getSnapshotManager()
//                                .getSnapshotFile(rb.getCommitCounter());
                        String digestStr = null;
                        if (digestEnum != null
                                && (digestEnum == DigestEnum.All || digestEnum == DigestEnum.Snapshots)) {
                            try {
                                final MessageDigest digest = MessageDigest
                                        .getInstance("MD5");
                                journal.getSnapshotManager().getDigest(
                                        rb.getCommitCounter(), digest);
                                digestStr = new BigInteger(1, digest.digest())
                                        .toString(16);
                            } catch (NoSuchAlgorithmException ex) {
                                // ignore
                            } catch (DigestException ex) {
                                // ignore
                            }
                        }

                        p.text("SnapshotFile: commitTime=")
                           .node("span").attr("id", "snapshot-file-commit-time")
                           .text(RootBlockView.toString(rb.getLastCommitTime()))
                           .close()
                        .text(", commitCounter=")
                           .node("span").attr("id", "snapshot-file-commit-counter")
                           .text("" + rb.getCommitCounter()).close()
                        .text(", nbytes=")
                           .node("span").attr("id", "snapshot-file-nbytes")
                           .text("" + nbytes).close();
                        if(digestStr != null) {
                           p.text(", md5=")
                              .node("span").attr("id", "snapshot-file-md5")
                              .text(digestStr).close();
                        }
                        p.node("br").close();

                    }
                    
                }
                
            }

            /*
             * If requested, conditionally start a snapshot.
             */
            {
                final String val = req.getParameter(StatusServlet.SNAPSHOT);

                if (val != null) {

                    /*
                     * Attempt to interpret the parameter as a percentage
                     * (expressed as an integer).
                     * 
                     * Note: The default threshold will trigger a snapshot
                     * regardless of the size of the journal and the #of HALog
                     * files. A non-default value of 100 will trigger the
                     * snapshot if the HALog files occupy as much space on the
                     * disk as the Journal. Other values may be used as
                     * appropriate.
                     */
                    int percentLogSize = 0;
                    try {
                        percentLogSize = Integer.parseInt(val);
                    } catch (NumberFormatException ex) {
                        // ignore.
                    }

                    journal.getSnapshotManager().takeSnapshot(
                            new HASnapshotRequest(percentLogSize));

                }
                
            }
            
            /*
             * Report if a snapshot is currently running.
             */
            if (journal.getSnapshotManager().getSnapshotFuture() != null) {

                p.text("Snapshot running.").node("br").close();
                
            }
            
            p.close();

            if(debug) {
               current.node("span").attr("id", "quorum").text(quorum.toString())
                  .close();
            }

        }

        /**
         * If requested, conditionally REBUILD the service from the leader
         * (disaster recover).
         * 
         * FIXME This should only be triggered by a POST (it is modestly safe
         * since a REBUILD can not be triggered if the service is joined, at the
         * same commit point as the leader, or already running, but it is not so
         * safe that you should be able to use a GET to demand a REBUILD).
         */
        if (quorumService != null) {
            
            final String val = req.getParameter(HAStatusServletUtil.REBUILD);

            if (val != null) {

                // Local HAGlue interface for this service (not proxy).
                final HAGlue haGlue = quorumService.getService();

                // Request RESTORE.
                if (haGlue.rebuildFromLeader(new HARemoteRebuildRequest()) != null) {

                    current.node("h2").attr("id", "rebuild")
                       .text("Running Disaster Recovery for this service (REBUILD).");

                }

            }

        }

        if (quorumService != null) {

            /*
             * Force the service into the "ERROR" state. It will automatically
             * attempt to recover.
             */

            final String val = req.getParameter(HAStatusServletUtil.ERROR);

            if (val != null) {

                quorumService.enterErrorState();

            }
    
        }
        
        /*
         * Display the NSS port, host, and leader/follower/not-joined
         * status for each service in the quorum.
         */
        if (quorumService != null) {

            current.node("h2", "Quorum Services");
            
            {
                
                final XMLBuilder.Node ul = current.node("ul")
                      .attr("id", "quorum-services");

                final UUID[] joined = quorum.getJoined();

                final UUID[] pipeline = quorum.getPipeline();

                // In pipeline order.
                for (UUID serviceId : pipeline) {

                    final HAGlue remoteService;
                    try {

                        remoteService = quorumService.getService(serviceId);

                    } catch (RuntimeException ex) {

                        /*
                         * Ignore. Might not be an HAGlue instance.
                         */

                        if (log.isInfoEnabled())
                            log.info(ex, ex);

                        continue;

                    }

                    final XMLBuilder.Node li = ul.node("li");
                    
                    /*
                     * Do all RMIs to the remote service in a try/catch. This
                     * allows us to catch problems with communications to the
                     * remote service and continue to paint the page.
                     */
                    final String hostname;
                    final int nssPort;
                    final InetSocketAddress writePipelineAddr;
                    final String extendedRunState;
                    try {

                        hostname = remoteService.getHostname();

                        /*
                         * TODO When there are multiple ethernet interfaces, is
                         * not necessarily reporting the interface(s) that the
                         * port is exposed to.
                         */
                        nssPort = remoteService.getNSSPort();

                        // address where the downstream service will listen.
                        writePipelineAddr = remoteService
                                .getWritePipelineAddr();

                        // The AbstractServer and HAQuorumService run states.
                        extendedRunState = remoteService.getExtendedRunState();

                    } catch (IOException ex) {

                        /*
                         * Note error and continue with the next service.
                         */

                        li.text("Unable to reach service: ")
                           .node("span").attr("class", "unreachable")
                           .text("" + remoteService).close()
                        .close();

                        log.error(ex, ex);

                        continue;

                    }

                    final boolean isLeader = serviceId.equals(quorum
                            .getLeaderId());

                    final boolean isFollower = indexOf(serviceId, joined) > 0;

                    final boolean isSelf = serviceId.equals(quorumService
                            .getServiceId());

                    final int pipelineIndex = indexOf(serviceId, pipeline);

                    /*
                     * TODO This assumes that the context path at the remote
                     * service is the same as the context path for the local
                     * service.
                     */
                    final String nssUrl = "http://" + hostname + ":" + nssPort
                            + BigdataStatics.getContextPath();

                    // hyper link to NSS service.
                    li.node("a").attr("class", "nss-url").attr("href", nssUrl)
                       .text(nssUrl).close();

                    // plus the other metadata.
                    li.text(" : ")
                       .node("span").attr("class", "service-status")
                       .text((isLeader ? "leader" : (isFollower ? "follower"
                             : " is not joined"))).close()
                    .text(", pipelineOrder=")
                       .node("span").attr("class", "service-pipeline-order")
                       .text("" + (pipelineIndex == -1 ? "N/A" : pipelineIndex))
                       .close()
                    .text(", writePipelineAddr=")
                       .node("span").attr("class", "service-write-pipeline-addr")
                       .text("" + writePipelineAddr).close()
                    .text(", service=")
                       .node("span").attr("class", "service-service")
                       .text((isSelf ? "self" : "other")).close()
                    .text(", extendedRunState=")
                       .node("span").attr("class", "service-extended-run-state")
                       .text(extendedRunState).close()
                    .node("br").close();

                    li.close();
                }

                ul.close();

            }
            
            // DumpZookeeper
            {

                current.node("h2", "Zookeeper");

                ZooKeeper zk;
                try {
                    zk = quorum.getZookeeper();
                } catch (InterruptedException e1) {
                    // Continue, but ignore zookeeper.
                    zk = null;
                }

                if (zk == null || !zk.getState().isAlive()) {

                    final XMLBuilder.Node p = current.node("p");

                    p.text("ZooKeeper is not available.")
                    .attr("id", "zookeeper-unavailable").close();

                } else {

                    // final XMLBuilder.Node section = current.node("pre");
                    // flush writer before writing on PrintStream.
                    current.getBuilder().getWriter().flush();

                    // dump onto the response.
                    final PrintWriter out = new PrintWriter(
                            resp.getOutputStream(), true/* autoFlush */);

                    out.print("<span id=\"zookeeper\">\n");

                    try {

                        final DumpZookeeper dump = new DumpZookeeper(zk);

                        dump.dump(
                                out,
                                true/* showDatatrue */,
                                quorumService.getLogicalServiceZPath()/* zpath */,
                                0/* depth */);

                    } catch (InterruptedException e) {

                        e.printStackTrace(out);

                    } catch (KeeperException e) {

                        e.printStackTrace(out);

                    }

                    // close section.
                    out.print("\n</span>");

                    // flush PrintWriter before resuming writes on Writer.
                    out.flush();

                }

            }

        }

    }

    /**
     * Return the index of the given {@link UUID} in the array of {@link UUID}s.
     * 
     * @param x
     *            The {@link UUID}
     * @param a
     *            The array of {@link UUID}s.
     *            
     * @return The index of the {@link UUID} in the array -or- <code>-1</code>
     *         if the {@link UUID} does not appear in the array.
     */
    static private int indexOf(final UUID x, final UUID[] a) {

        if (x == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < a.length; i++) {

            if (x.equals(a[i])) {

                return i;

            }

        }

        return -1;

    }

    /* (non-Javadoc)
	 * @see com.bigdata.rdf.sail.webapp.IHAStatusServlet#doHAStatus(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
    @Override
	public void doHAStatus(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!(indexManager instanceof HAJournal))
            return;

        final HAJournal journal = (HAJournal) indexManager;

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();
        
        final QuorumService<HAGlue> quorumService = quorum.getClient();

        // Local (non-RMI) HAGlue implementation object.
        final HAGlue haGlue = quorumService.getService();
        
        // Local method all (non-RMI).
        final HAStatusEnum status = haGlue.getHAStatus();
        
        // TODO Alternatively "max-age=1" for max-age in seconds.
        resp.addHeader("Cache-Control", "no-cache");

        BigdataRDFServlet.buildAndCommitResponse(resp, BigdataRDFServlet.HTTP_OK,
                BigdataRDFServlet.MIME_TEXT_PLAIN, status.name());
        
        log.warn("Responding to HA status request");

        return;

    }

   /* (non-Javadoc)
 * @see com.bigdata.rdf.sail.webapp.IHAStatusServlet#doHealthStatus(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
 */
   @Override
public void doHealthStatus(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      final StringWriter writer = new StringWriter();
      final JsonFactory factory = new JsonFactory();
      final JsonGenerator json = factory.createGenerator(writer);

      json.writeStartObject();
      json.writeStringField("version", Banner.getVersion());
      json.writeNumberField("timestamp", new Date().getTime());

      if (!(indexManager instanceof HAJournal)) {

         // standalone
         json.writeStringField("deployment", "standalone");

      } else {

         // HA
         json.writeStringField("deployment", "HA");

         final HAJournal journal = (HAJournal) indexManager;

         final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
               .getQuorum();

         if (quorum.isQuorumFullyMet(quorum.token())) {
            json.writeStringField("status", "Good");
            json.writeStringField("details",
                  "All servers (" + quorum.replicationFactor() + ") joined");
         } else {
            // at least one server is not available
            // status is either Warning or Bad
            if (quorum.isQuorumMet()) {
               json.writeStringField("status", "Warning");
            } else {
               json.writeStringField("status", "Bad");
            }
            json.writeStringField(
                  "details",
                  "Only " + quorum.getJoined().length + " of target "
                        + quorum.replicationFactor() + " servers joined");
         }

         json.writeFieldName("services");
         json.writeStartArray();

         final UUID[] members = quorum.getMembers();
         final UUID[] joined = quorum.getJoined();

         for (UUID serviceId : members) {
            final boolean isLeader = serviceId.equals(quorum.getLeaderId());
            final boolean isFollower = indexOf(serviceId, joined) > 0;

            json.writeStartObject();
            json.writeStringField("id", serviceId.toString());
            json.writeStringField("status", isLeader ? "leader"
                  : (isFollower ? "follower" : "unready"));
            json.writeEndObject();
         }

         json.writeEndArray();
      }

      json.writeEndObject();
      json.close();

      // TODO Alternatively "max-age=1" for max-age in seconds.
      resp.addHeader("Cache-Control", "no-cache");

      BigdataRDFServlet.buildAndCommitResponse(resp, BigdataRDFServlet.HTTP_OK,
            BigdataRDFServlet.MIME_APPLICATION_JSON, writer.toString());

      return;

   }
    
//    /**
//     * Impose a lexical ordering on the file names. This is used for the HALog
//     * and snapshot file names. The main component of those file names is the
//     * commit counter, so this places the files into order by commit counter.
//     */
//    private static class FilenameComparator implements Comparator<File> {
//
//        @Override
//        public int compare(File o1, File o2) {
//            
//            return o1.getName().compareTo(o2.getName());
//        }
//        
//    }
    
}
