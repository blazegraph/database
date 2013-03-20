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
package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.zookeeper.KeeperException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.halog.HALogReader;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.journal.jini.ha.SnapshotManager;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.client.HAStatusEnum;
import com.bigdata.zookeeper.DumpZookeeper;

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
public class HAStatusServletUtil {

    final private IIndexManager indexManager;

    public HAStatusServletUtil(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;

    }

    /**
     * Show the interesting things about the quorum.
     * <ol>
     * <li>QuorumState</li>
     * <li>Who is the leader, who is a follower.</li>
     * <li>What is the SPARQL end point for each leader and follower.</li>
     * <li>Dump of the zookeeper state related to the quorum.</li>
     * <li>listServices (into pre element).</li>
     * </ol>
     * 
     * @throws IOException
     */
    public void doGet(final HttpServletRequest req,
            final HttpServletResponse resp, final XMLBuilder.Node current)
            throws IOException {

        if (!(indexManager instanceof HAJournal))
            return;

        final HAJournal journal = (HAJournal) indexManager;

        final ZKQuorumImpl<HAGlue, QuorumService<HAGlue>> quorum = (ZKQuorumImpl<HAGlue, QuorumService<HAGlue>>) journal
                .getQuorum();

        // The current token.
        final long quorumToken = quorum.token();

        // The last valid token.
        final long lastValidToken = quorum.lastValidToken();

        final int njoined = quorum.getJoined().length;

        final QuorumService<HAGlue> quorumService = quorum.getClient();

        final boolean digests = req.getParameter(StatusServlet.DIGESTS) != null;
        
        current.node("h1", "High Availability");

        // The quorum state.
        {

            final XMLBuilder.Node p = current.node("p");

            p.text("The quorum is " + (quorum.isQuorumMet() ? "" : "not")
                    + " met.").node("br").close();

            p.text("" + njoined + " out of " + quorum.replicationFactor()
                    + " services are joined.").node("br").close();

            p.text("quorumToken=" + quorumToken + ", lastValidToken="
                    + lastValidToken).node("br").close();

            p.text("logicalServiceId=" + quorumService.getLogicalServiceId())
                    .node("br").close();

            // Note: This is the *local* value of getHAStatus(). 
            p.text("HAStatus: " + getHAStatus(journal)).node("br").close();
            
            /*
             * Report on the Service.
             */
            {
                p.text("Service: serviceId=" + quorumService.getServiceId())
                        .node("br").close();
                p.text("Service: pid=" + quorumService.getPID()).node("br")
                        .close();
                p.text("Service: path=" + quorumService.getServiceDir())
                        .node("br").close();
            }
            
            /*
             * Report on the Journal.
             */
            {
                final File file = journal.getFile();
                if (file != null) {
                    String digestStr = null;
                    if (digests) {
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
                    p.text("HAJournal: file=" + file + ", nbytes="
                            + journal.size()
                            + (digestStr == null ? "" : ", md5=" + digestStr))
                            .node("br").close();
                }
            }

            /*
             * Report #of files and bytes in the HALog directory.
             */
            {
                final File haLogDir = quorumService.getHALogDir();
                final File[] a = haLogDir
                        .listFiles(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return name
                                        .endsWith(IHALogReader.HA_LOG_EXT);
                            }
                        });
                {
                    int nfiles = 0;
                    long nbytes = 0L;
                    for (File file : a) {
                        nbytes += file.length();
                        nfiles++;
                    }
                    p.text("HALogDir: nfiles=" + nfiles + ", nbytes=" + nbytes
                            + ", path=" + haLogDir).node("br").close();
                }
                if (digests) {
                    /*
                     * List each HALog file together with its digest.
                     * 
                     * FIXME We need to request the live log differently and use
                     * the lock for it. That makes printing the HALog digests
                     * here potentially probemantic if there are outstanding
                     * writes.
                     */
                    // Note: sort to order by increasing commit counter.
                    Arrays.sort(a, 0/* fromIndex */, a.length,
                            new FilenameComparator());
                    for (File file : a) {
                        final long nbytes = file.length();
                        String digestStr = null;
                        final IHALogReader r = new HALogReader(file);
                        try {
                            if (digests && !r.isEmpty()) {
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
                        p.text("HALogFile: closingCommitCounter="
                                + r.getClosingRootBlock().getCommitCounter()
                                + ", file="
                                + file
                                + ", nbytes="
                                + nbytes
                                + (digestStr == null ? "" : ", md5="
                                        + digestStr)).node("br").close();
                    }
                }
            }

            /*
             * Report #of files and bytes in the snapshot directory.
             */
            {
                final File snapshotDir = journal
                        .getSnapshotManager().getSnapshotDir();
                final File[] a = snapshotDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(SnapshotManager.SNAPSHOT_EXT);
                    }
                });
                int nfiles = 0;
                long nbytes = 0L;
                for (File file : a) {
                    nbytes += file.length();
                    nfiles++;
                }
                p.text("SnapshotDir: nfiles=" + nfiles + ", nbytes=" + nbytes
                        + ", path=" + snapshotDir).node("br").close();
                
                if (true) {

                    /*
                     * List the available snapshots.
                     */
                    final List<IRootBlockView> snapshots = journal
                            .getSnapshotManager().getSnapshots();

                    // Note: list is ordered by increasing commitTime.
                    for (IRootBlockView rb : snapshots) {

                        String digestStr = null;
                        if (digests) {
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

                        p.text("SnapshotFile: commitTime="
                                + RootBlockView.toString(rb.getLastCommitTime())
                                + ", commitCounter="
                                + rb.getCommitCounter()
                                + (digestStr == null ? "" : ", md5="
                                        + digestStr)).node("br").close();

                    }
                    
                }
                
            }

            /*
             * If requested, conditional start a snapshot.
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

                    ((HAJournal) journal).getSnapshotManager().takeSnapshot(
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

            current.node("pre", quorum.toString());

        }

        /*
         * Display the NSS port, host, and leader/follower/not-joined
         * status for each service in the quorum.
         */
        current.node("h2", "Quorum Services");
        {
            final XMLBuilder.Node p = current.node("p");
            
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

                    continue;

                }

                /*
                 * TODO When there are multiple ethernet interfaces, is not
                 * necessarily reporting the interface(s) that the port is
                 * exposed to.
                 */

                final String hostname = remoteService.getHostname();

                final int nssPort = remoteService.getNSSPort();

                final boolean isLeader = serviceId.equals(quorum
                        .getLeaderId());

                final boolean isFollower = indexOf(serviceId, joined) > 0;

                final boolean isSelf = serviceId.equals(quorumService
                        .getServiceId());

                final int pipelineIndex = indexOf(serviceId, pipeline);
                
                final String nssUrl = "http://" + hostname + ":" + nssPort;
                
                // hyper link to NSS service.
                p.node("a").attr("href", nssUrl).text(nssUrl).close();

                // plus the other metadata.
                p.text(" : "//
                        + (isLeader ? "leader" : (isFollower ? "follower"
                                : " is not joined"))//
                        + ", pipelineOrder="
                        + (pipelineIndex == -1 ? "N/A" : pipelineIndex)//
                        + (isSelf ? " (this service)" : "")//
                ).node("br").close();

            }

            p.close();
            
        }

        // DumpZookeeper
        {
            
            current.node("h2", "Zookeeper");

            // final XMLBuilder.Node section = current.node("pre");
            // flush writer before writing on PrintStream.
            current.getBuilder().getWriter().flush();

            // dump onto the response.
            final PrintWriter out = new PrintWriter(
                    resp.getOutputStream(), true/* autoFlush */);

            out.print("<pre>\n");

            try {

                final DumpZookeeper dump = new DumpZookeeper(
                        quorum.getZookeeper());

                dump.dump(out, true/* showDatatrue */,
                        quorumService.getLogicalServiceId()/* zpath */,
                        0/* depth */);

            } catch (InterruptedException e) {

                e.printStackTrace(out);

            } catch (KeeperException e) {

                e.printStackTrace(out);

            }

            // flush PrintWriter before resuming writes on Writer.
            out.flush();

            // close section.
            out.print("\n</pre>");

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

    /**
     * Return the {@link HAStatusEnum} for <em>this</em> {@link HAJournal}.
     * 
     * @param journal
     *            The {@link HAJournal}.
     *            
     * @return The {@link HAStatusEnum}.
     */
    public HAStatusEnum getHAStatus(final HAJournal journal) {

        final ZKQuorumImpl<HAGlue, QuorumService<HAGlue>> quorum = (ZKQuorumImpl<HAGlue, QuorumService<HAGlue>>) journal
                .getQuorum();

        final QuorumService<HAGlue> quorumService = quorum.getClient();

        // check, but do not wait.
        final long haReadyToken = journal.getHAReady();

        final HAStatusEnum status;
        
        if (haReadyToken == Quorum.NO_QUORUM) {
        
            // Quorum is not met (as percieved by the HAJournal).
            status = HAStatusEnum.NotReady;
            
        } else {
            
            if (quorumService.isLeader(haReadyToken)) {
            
                // Service is leader.
                status = HAStatusEnum.Leader;
                
            } else if (quorumService.isFollower(haReadyToken)) {
                
                // Service is follower.
                status = HAStatusEnum.Follower;
                
            } else {
                
                /*
                 * awaitHAReady() should only return successfully (and hence
                 * haReadyToken should only be a valid token) if the service was
                 * elected as either a leader or a follower. However, it is
                 * possible for the service to have concurrently left the met
                 * quorum, in which case the if/then/else pattern will fall
                 * through to this code path.
                 */
                
                // Quorum is not met (as percieved by the HAJournal).
                status = HAStatusEnum.NotReady;
                
            }
            
        }

        return status;

    }
    
    /**
     * Special reporting request for HA status.
     * 
     * @param req
     * @param resp
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws AsynchronousQuorumCloseException
     * @throws IOException
     */
    public void doHAStatus(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        if (!(indexManager instanceof HAJournal))
            return;

        final HAJournal journal = (HAJournal) indexManager;

        final HAStatusEnum status = getHAStatus(journal);
        
        // TODO Alternatively "max-age=1" for max-age in seconds.
        resp.addHeader("Cache-Control", "no-cache");

        BigdataRDFServlet.buildResponse(resp, BigdataRDFServlet.HTTP_OK,
                BigdataRDFServlet.MIME_TEXT_PLAIN, status.name());

        return;

    }

    /**
     * Impose a lexical ordering on the file names. This is used for the HALog
     * and snapshot file names. The main component of those file names is the
     * commit counter, so this places the files into order by commit counter.
     */
    private static class FilenameComparator implements Comparator<File> {

        @Override
        public int compare(File o1, File o2) {
            
            return o1.getName().compareTo(o2.getName());
        }
        
    }
    
}
