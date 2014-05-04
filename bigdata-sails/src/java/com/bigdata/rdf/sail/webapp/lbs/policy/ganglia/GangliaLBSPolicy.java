/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.lbs.policy.ganglia;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.ganglia.GangliaListener;
import com.bigdata.ganglia.GangliaService;
import com.bigdata.ganglia.HostReportComparator;
import com.bigdata.ganglia.IHostReport;
import com.bigdata.journal.GangliaPlugIn;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.PlatformStatsPlugIn;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.journal.jini.ha.HAJournalServer;
import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.rdf.sail.webapp.IHALoadBalancerPolicy;
import com.bigdata.rdf.sail.webapp.lbs.AbstractLBSPolicy;
import com.bigdata.rdf.sail.webapp.lbs.HostScore;
import com.bigdata.rdf.sail.webapp.lbs.ServiceScore;
import com.bigdata.util.InnerCause;

/**
 * Stochastically proxy the request to the services based on their load.
 * <p>
 * Note: This {@link IHALoadBalancerPolicy} has a dependency on the
 * {@link GangliaPlugIn}. The {@link GangliaPlugIn} must be setup to listen to
 * the Ganglia protocol and build up an in-memory model of the load on each
 * host. Ganglia must be reporting metrics for each host running an
 * {@link HAJournalServer} instance. This can be achieved either using the
 * <code>gmond</code> utility from the ganglia distribution or using the
 * {@link GangliaPlugIn}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GangliaLBSPolicy extends AbstractLBSPolicy {

    private static final Logger log = Logger.getLogger(GangliaLBSPolicy.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Servlet <code>init-param</code> values understood by the
     * {@link GangliaLBSPolicy}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface InitParams extends AbstractLBSPolicy.InitParams {

        /**
         * A list of space and/or comma delimited performance metric names that
         * will be requested from the {@link GangliaService} when obtaining the
         * {@link IHostReport}s. The default is whatever is reported by the
         * {@link GangliaService}. If you rely on specific metrics for your
         * {@link IHostScoringRule}, then you may need to explicitly specify
         * those metrics here.
         * <p>
         * Note: There is NO guarantee that any specific performance metric will
         * be reported. This depends on the actual performance metric collection
         * that is enabled on the hosts. Some key performance metrics may not be
         * available on all hosts. For example, IO Wait is not available under
         * OSX. You can monitor the available performance metrics by running the
         * {@link GangliaListener} utility.
         * 
         * @see GangliaService#getDefaultHostReportOn()
         * @see GangliaListener#main(String[])
         * @see <a
         *      href="https://sourceforge.net/apps/trac/ganglia/wiki/Ganglia%203.1.x%20Installation%20and%20Configuration"
         *      >Ganglia Wiki (has a section on the defined metics and the
         *      platform where they are available)</a>
         */
        String REPORT_ON = GangliaLBSPolicy.class.getName() + ".reportOn";

        /**
         * The {@link IHostScoringRule} that will be used to score the
         * {@link IHostReport}s. The {@link IHostReport}s are obtained
         * periodically from the {@link GangliaPlugIn}. The reports reflect the
         * best local knowledge of the metrics on each of the hosts. The hosts
         * will each self-report their metrics periodically using the ganglia
         * protocol.
         * <p>
         * The purpose of the scoring rule is to compute a single workload
         * number based on those host metrics. The resulting scores are then
         * normalized. Load balancing decisions are made based on those
         * normalized scores.
         */
        String HOST_SCORING_RULE = GangliaLBSPolicy.class.getName()
                + ".hostScoringRule";

        String DEFAULT_HOST_SCORING_RULE = DefaultHostScoringRule.class
                .getName();

        /**
         * The delay in milliseconds between scheduled tasks that query the
         * in-memory ganglia peer to discovery the updated per-host performance
         * metrics (default {@value #DEFAULT_HOST_DISCOVERY_DELAY}).
         * <p>
         * Note: the ganglia updates are not synchronized across a cluster. They
         * pour in ever N seconds from each host. However, the hosts do not
         * begin to report on the same N second boundary. All you know is that
         * (on average) all hosts should have reported in within N seconds.
         */
        String HOST_DISCOVERY_DELAY = GangliaLBSPolicy.class.getName()
                + ".hostDiscoveryDelay";

        String DEFAULT_HOST_DISCOVERY_DELAY = "5000"; // ms.

        /**
         * Read requests are forwarded to the local service if the load on that
         * service is less than the configured threshold when considering the
         * normalized workload of the hosts. The value must be in (0:1) and
         * represents a normalized workload threshold for the hosts having
         * services that are joined with the met quorum. This may be set to ZERO
         * (0) to disable this bias. The default is
         * {@value #DEFAULT_LOCAL_FORWARD_THRESHOLD}.
         * <p>
         * This bias is designed for use when an external round-robin policy is
         * distributing the requests evenly across the services. In this case,
         * the round-robin smooths out most of the load and the
         * {@link HALoadBalancerServlet} {@link #POLICY} takes over only when
         * there is a severe load imbalance (as defined by the value of this
         * parameter).
         */
        String LOCAL_FORWARD_THRESHOLD = GangliaLBSPolicy.class.getName()
                + ".localForwardThreshold";

        String DEFAULT_LOCAL_FORWARD_THRESHOLD = "0";

    }

    /**
     * Place into descending order by load_one.
     * <p>
     * Note: We do not rely on the ordering imposed by this comparator. Instead,
     * we filter the hosts for those that correspond to the joined services in
     * the met quorum, compute a score for each such host, and then normalize
     * those scores.
     */
    private final static Comparator<IHostReport> comparator = new HostReportComparator(
            "load_one", false/* asc */);

    /**
     * @see InitParams#LOCAL_FORWARD_THRESHOLD
     */
    private final AtomicReference<Double> localForwardThresholdRef = new AtomicReference<Double>();

    /**
     * The set of metrics that we are requesting in the ganglia host reports.
     * 
     * @see InitParams#REPORT_ON
     */
    private String[] reportOn;

    /**
     * The rule used to score the {@link IHostReport}s.
     * 
     * @see InitParams#HOST_SCORING_RULE
     */
    private final AtomicReference<IHostScoringRule> scoringRuleRef = new AtomicReference<IHostScoringRule>();

    /**
     * Random number generator used to load balance the read-requests.
     */
    private final Random rand = new Random();

    /**
     * The ganglia service - it must be configured at least as a listener.
     */
    private final AtomicReference<GangliaService> gangliaServiceRef = new AtomicReference<GangliaService>();

    /**
     * The {@link Future} of a task that periodically queries the ganglia peer
     * for its up to date host counters for each discovered host.
     */
    private ScheduledFuture<?> scheduledFuture;

    /**
     * The current {@link HostTable} data.
     * 
     * @see #updateHostTable()
     */
    private final AtomicReference<HostTable> hostTableRef = new AtomicReference<HostTable>(
            null);

    @Override
    protected void toString(final StringBuilder sb) {

        super.toString(sb);

        sb.append(",localForwardThreshold=" + localForwardThresholdRef.get());

        sb.append(",reportOn=" + Arrays.toString(reportOn));

        sb.append(",scoringRule=" + scoringRuleRef.get());

        sb.append(",gangliaService=" + gangliaServiceRef.get());

        sb.append(",hostTable=" + hostTableRef.get());
        
        {
            final ScheduledFuture<?> tmp = scheduledFuture;
            final boolean futureIsDone = tmp == null ? true : tmp.isDone();
            sb.append(",scheduledFuture="
                    + (tmp == null ? "N/A"
                            : (futureIsDone ? "done" : "running")));
            if (futureIsDone && tmp != null) {
                // Check for error.
                Throwable cause = null;
                try {
                    tmp.get();
                } catch (CancellationException ex) {
                    cause = ex;
                } catch (ExecutionException ex) {
                    cause = ex;
                } catch (InterruptedException ex) {
                    cause = ex;
                }
                if (cause != null) {
                    sb.append("(cause=" + cause + ")");
                }
            }
        }

    }

    // @SuppressWarnings("unchecked")
    @Override
    public void init(final ServletConfig servletConfig,
            final IIndexManager indexManager) throws ServletException {

        super.init(servletConfig, indexManager);

        // comparator = newInstance(servletConfig, Comparator.class,
        // InitParams.COMPARATOR, InitParams.DEFAULT_COMPARATOR);

        final HAJournal journal = (HAJournal) indexManager;

        if (journal.getPlatformStatisticsCollector() == null) {
            // LBS requires platform stats to load balance requests.
            throw new ServletException("LBS requires "
                    + PlatformStatsPlugIn.class.getName());
        }

        gangliaServiceRef.set((GangliaService) journal.getGangliaService());

        if (gangliaServiceRef.get() == null) {
            // LBS requires ganglia to load balance requests.
            throw new ServletException("LBS requires "
                    + GangliaPlugIn.class.getName());
        }

        {

            final String s = HALoadBalancerServlet.getConfigParam(
                    servletConfig, InitParams.REPORT_ON, null/* default */);

            if (s == null) {

                this.reportOn = gangliaServiceRef.get()
                        .getDefaultHostReportOn();

            } else {

                final String[] a = s.split("[ ,]+");

                for (int i = 0; i < a.length; i++) {

                    a[i].trim();

                }

                this.reportOn = a;

            }

            if (log.isInfoEnabled())
                log.info(InitParams.REPORT_ON + "=" + Arrays.toString(reportOn));

        }

        {

            final String s = HALoadBalancerServlet.getConfigParam(
                    servletConfig, InitParams.LOCAL_FORWARD_THRESHOLD,
                    InitParams.DEFAULT_LOCAL_FORWARD_THRESHOLD);

            final double d = Double.valueOf(s);

            if (log.isInfoEnabled())
                log.info(InitParams.LOCAL_FORWARD_THRESHOLD + "=" + d);

            setLocalForwardThreshold(d);

        }

        {

            scoringRuleRef.set(HALoadBalancerServlet.newInstance(servletConfig,
                    IHostScoringRule.class, InitParams.HOST_SCORING_RULE,
                    InitParams.DEFAULT_HOST_SCORING_RULE));

            if (log.isInfoEnabled())
                log.info(InitParams.HOST_SCORING_RULE + "="
                        + scoringRuleRef.getClass().getName());

        }

        /*
         * Setup a scheduled task to discover and rank the hosts on a periodic
         * basis.
         */
        final long initialDelay = 0L;
        final long delay;
        {

            final String s = HALoadBalancerServlet.getConfigParam(
                    servletConfig, InitParams.HOST_DISCOVERY_DELAY,
                    InitParams.DEFAULT_HOST_DISCOVERY_DELAY);

            delay = Long.valueOf(s);

            if (log.isInfoEnabled())
                log.info(InitParams.HOST_DISCOVERY_DELAY + "=" + delay);

        }

        scheduledFuture = journal.addScheduledTask(new Runnable() {
            @Override
            public void run() {
                try {
                    updateHostTable();
                } catch (RuntimeException ex) {
                    if (InnerCause.isInnerCause(ex, InterruptedException.class)) {
                        // Terminate if interrupted.
                        throw ex;
                    }
                    /*
                     * Note: If the task thows an exception it will not be
                     * rescheduled, therefore log @ ERROR rather than allowing
                     * the unchecked exception to be propagated.
                     */
                    log.error(ex, ex);
                }
            }
        }, initialDelay, delay, TimeUnit.MILLISECONDS);

    }

    @Override
    public void destroy() {

        // comparator = null;

        reportOn = null;

        gangliaServiceRef.set(null);

        if (scheduledFuture != null) {

            scheduledFuture.cancel(true/* mayInterruptIfRunning */);

            scheduledFuture = null;

        }

        super.destroy();

    }

    public void setLocalForwardThreshold(final double newValue) {

        if (newValue < 0 || newValue > 1)
            throw new IllegalArgumentException();

        localForwardThresholdRef.set(newValue);

    }

    /**
     * Extended to conditionally update the {@link #hostTableRef} iff it does
     * not exist or is empty.
     */
    @Override
    protected void conditionallyUpdateServiceTable() {

        super.conditionallyUpdateServiceTable();

        final HostTable hostTable = hostTableRef.get();

        final HostScore[] hostScores = hostTable == null ? null
                : hostTable.hostScores;

        if (hostScores == null || hostScores.length == 0) {

            /*
             * Synchronize so we do not do the same work for each concurrent
             * request on a service start.
             */

            synchronized (hostTable) {

                // Ensure that the host table exists.
                updateHostTable();

            }

        }

    }

    /**
     * Overridden to also update the hosts table in case we add/remove a service
     * and the set of hosts that cover the member services is changed as a
     * result.
     */
    @Override
    protected void updateServiceTable() {

        super.updateServiceTable();

        updateHostTable();

    }

    /**
     * Update the per-host scoring table. The host table will only contain
     * entries for hosts associated with at least one service that is joined
     * with the met quorum.
     * 
     * @see #hostTableRef
     */
    private void updateHostTable() {

        final GangliaService gangliaService = gangliaServiceRef.get();
        
        // Snapshot of the per service scores.
        final ServiceScore[] serviceScores = serviceTableRef.get();

        if (gangliaService == null || serviceScores == null
                || serviceScores.length == 0) {

            /*
             * No ganglia?
             * 
             * No joined services?
             */

            // clear the host table.
            hostTableRef.set(null);

            return;

        }

        // Obtain the host reports for those services.
        final IHostReport[] hostReport = getHostReportForKnownServices(
                gangliaService, serviceScores);

        /*
         * Compute the per-host scores and the total score across those hosts.
         */

        final HostScore[] scores = new HostScore[hostReport.length];

        final double[] hostScores = new double[hostReport.length];

        double totalScore = 0d;

        final IHostScoringRule scoringRule = scoringRuleRef.get();
        
        for (int i = 0; i < hostReport.length; i++) {

            final IHostReport theHostScore = hostReport[i];

            double hostScore = scoringRule.getScore(theHostScore);

            if (hostScore < 0) {

                log.error("Negative score: " + theHostScore.getHostName());

                hostScore = 0d;

            }

            totalScore += (hostScores[i] = hostScore);

        }

        /*
         * Normalize the per-hosts scores.
         * 
         * Note: This needs to be done only for those hosts that are associated
         * with the quorum members. If we do it for the other hosts then the
         * normalization is not meaningful. That is why we first filter for only
         * those hosts that are running services associated with this quorum.
         */

        HostScore thisHostScore = null;

        for (int i = 0; i < hostReport.length; i++) {

            final String hostname = hostReport[i].getHostName();

            final HostScore hostScore;
            if (totalScore == 0d) {

                // If totalScore is zero, then weight all hosts equally as
                // (1/nhosts).
                hostScore = new HostScore(hostname, 1d, hostReport.length);

            } else {

                // Normalize host scores.
                hostScore = new HostScore(hostname, hostScores[i], totalScore);

            }

            scores[i] = hostScore;

            if (hostScore.thisHost) {

                thisHostScore = hostScore;

            }

        }

        // sort into ascending order (increasing activity).
        Arrays.sort(scores);

        for (int i = 0; i < scores.length; i++) {

            scores[i].rank = i;

            scores[i].drank = ((double) i) / scores.length;

        }

        if (scores.length > 0) {

            if (log.isDebugEnabled()) {

                log.debug("The most active index was: "
                        + scores[scores.length - 1]);

                log.debug("The least active index was: " + scores[0]);

                log.debug("This host: " + thisHostScore);

            }

        }

        // Set the host table.
        hostTableRef.set(new HostTable(thisHostScore, scores));

    }

    /**
     * Return an {@link IHostReport} for each host that is known to be running
     * an {@link ServiceScore} associated with the same quorum as this service.
     * <p>
     * Note: If there is more than one service on the same host, then we will
     * have one record per host, not per service.
     * <p>
     * Note: The actual metrics that are available depend on the OS and on
     * whether you are running gmond or having the GangliaPlugIn do its own
     * reporting. The policy that ranks the host reports should be robust to
     * these variations.
     * 
     * @return A dense array of {@link IHostReport}s for our services.
     */
    private IHostReport[] getHostReportForKnownServices(
            final GangliaService gangliaService,
            final ServiceScore[] serviceScores) {

        /*
         * The set of hosts having services that are joined with the met quorum.
         */
        final String[] hosts;
        {
            final List<String> tmp = new LinkedList<String>();
            for (ServiceScore serviceScore : serviceScores) {
                if (serviceScore == null) // should never be null.
                    continue;
                tmp.add(serviceScore.getHostname());
            }
            hosts = tmp.toArray(new String[tmp.size()]);
        }

        final IHostReport[] hostReport = gangliaService.getHostReport(//
                hosts,// the hosts for our joined services.
                reportOn,// metrics to be reported.
                comparator// imposes order on the host reports (which we
                          // ignore).
                );

        final List<IHostReport> hostReportList = new LinkedList<IHostReport>();

        int nhostsWithService = 0;

        for (int i = 0; i < hostReport.length; i++) {

            final IHostReport theHostReport = hostReport[i];

            boolean foundServiceOnHost = false;

            for (int j = 0; j < serviceScores.length; j++) {

                ServiceScore theServiceScore = serviceScores[j];

                if (theHostReport.getHostName()
                        .equals(theServiceScore.getHostname())) {

                    // Found service on this host.
                    foundServiceOnHost = true;

                    break;

                }

            }

            if (!foundServiceOnHost) {

                // Ignore host if not running our service.
                continue;

            }

            hostReportList.add(theHostReport);

            nhostsWithService++; // one more host with at least one service.

        }

        if (log.isInfoEnabled())
            log.info("hostReport=" + hostReportList);

        // A dense list of host reports for our services.
        return hostReportList.toArray(new IHostReport[nhostsWithService]);

    }

    @Override
    public String getReaderURL(final HttpServletRequest req) {

        final HostTable hostTable = hostTableRef.get();

        final HostScore[] hostScores = hostTable == null ? null
                : hostTable.hostScores;

        if (hostScores == null || hostScores.length == 0) {

            // Can't do anything.
            log.warn("Empty host table.");
            return null;

        }

        final ServiceScore[] services = serviceTableRef.get();

        if (services == null) {

            // No services. Can't proxy.
            return null;

        }

        /*
         * Stochastically select the target host based on the current host
         * workload.
         * 
         * We need to ignore any host that is not joined with the met quorum....
         */

        final double d = rand.nextDouble();
        double sum = 0d;
        HostScore hostScore = null;
        final List<ServiceScore> foundServices = new LinkedList<ServiceScore>();
        for (int i = 0; i < hostScores.length; i++) {
            hostScore = hostScores[i];
            sum += hostScore.score;
            if (hostScore.hostname == null) // can't use w/o hostname.
                continue;
            if (d > sum) // scan further.
                continue;
            /*
             * We found a host having a position in the cumultive ordering of
             * the normalized host workloads that is GTE to the random number.
             * Now we need to find one (or more) service(s) on that host.
             * 
             * Note: The main time when we will have multiple services on a host
             * is the CI test suite. Normal deployment would put allocate only
             * one service per host.
             */
            for (ServiceScore tmp : services) {

                if (tmp == null) // should never happen.
                    continue;

                if (tmp.getRequestURI() == null) // can't proxy.
                    continue;

                if (hostScore.hostname.equals(tmp.getHostname())) {

                    // Found a joined service on that host.
                    foundServices.add(tmp);

                }

                if (!foundServices.isEmpty()) {
                    // Done. Found at least one service.
                    break;
                }

            }

        }

        final int nservices = foundServices.size();

        if (nservices == 0) {
            // Can't find a service.
            return null;
        }

        // Figure out which service to use.
        final int n = rand.nextInt(nservices);

        final ServiceScore serviceScore = foundServices.get(n);

        // track #of requests to each service.
        serviceScore.nrequests.increment();
        
        return serviceScore.getRequestURI();

    }

    /**
     * {@inheritDoc}
     * <p>
     * If the normalized workload for this host is under a configured threshold,
     * then we forward the request to this service. This help to reduce the
     * latency of the request since it is not being proxied.
     */
    protected boolean conditionallyForwardReadRequest(
            final HttpServletRequest request, final HttpServletResponse response)
            throws IOException {

        final HostTable hostTable = hostTableRef.get();

        final HostScore thisHostScore = hostTable == null ? null
                : hostTable.thisHost;

        if (thisHostScore != null
                && thisHostScore.score <= localForwardThresholdRef.get()) {

            HALoadBalancerServlet.forwardToThisService(
                    false/* isLeaderRequest */, request, response);

            // request was handled.
            return true;

        }

        return false;

    }

}
