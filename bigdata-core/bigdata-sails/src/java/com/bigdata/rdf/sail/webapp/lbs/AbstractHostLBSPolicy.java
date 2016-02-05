/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.sail.webapp.lbs;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
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

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.PlatformStatsPlugIn;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.util.InnerCause;

/**
 * Abstract base class for an LBS policy that uses per-host load metrics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractHostLBSPolicy extends AbstractLBSPolicy {

    private static final Logger log = Logger.getLogger(AbstractHostLBSPolicy.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * 
     * @see HALoadBalancerServlet#getConfigParam(ServletConfig, Class, String,
     *      String) for how these <code>init-param</code> values can be set in
     *      <code>web.xml</code> and via environment variables.
     */
    public interface InitParams extends AbstractLBSPolicy.InitParams {
        
        /**
         * The {@link IHostScoringRule} that will be used to score the
         * {@link IHostMetrics}. The {@link IHostMetrics} are obtained
         * periodically from the from some source (specified by a concrete
         * derived class).
         * <p>
         * The purpose of the {@link IHostScoringRule} is to compute a single
         * workload number based on those host metrics. The resulting scores are
         * then normalized. Load balancing decisions are made based on those
         * normalized scores.
         * <p>
         * Note: The default policy is specific to the concrete instance of the
         * outer class.
         */
        String HOST_SCORING_RULE = "hostScoringRule";

        /**
         * Read requests are forwarded to the local service if the availability
         * on that service is greater than or equal to the configured threshold
         * when considering the normalized workload of the hosts. The value must
         * be in [0:1] and represents a normalized availability threshold for
         * the hosts having services that are joined with the met quorum. This
         * may be set to ONE (1.0) to disable this bias. The default is
         * {@value #DEFAULT_LOCAL_FORWARD_THRESHOLD}.
         * <p>
         * This bias is designed for use when an external round-robin policy is
         * distributing the requests evenly across the services. In this case,
         * the round-robin smooths out most of the workload and the
         * {@link IHALoadBalancerPolicy} takes over only when there is a severe
         * workload imbalance (as defined by the value of this parameter).
         * <p>
         * For example, if you have 3 hosts and they are equally available, then
         * their normalized availability scores will be <code>.3333</code>. If
         * the score for a given host is close to this normalized availability,
         * then a local forward is a reasonable choice.
         * 
         * TODO In fact, we could automatically compute and use a reasonable
         * value based on the quorum size as
         * <code>ceil((1/replicationFactor)-.01)</code>. With this approach, the
         * local forward bias is automatic. However, we still only want to do
         * this if there is a round-robin over the services. Otherwise we will
         * slam this host whenever its load gets below the threshold while not
         * assigning any work to the other hosts until the next update of the
         * {@link HostTable}.
         */
        String LOCAL_FORWARD_THRESHOLD = "localForwardThreshold";

        String DEFAULT_LOCAL_FORWARD_THRESHOLD = "1.0";

        /**
         * The initial delay in milliseconds before the first scheduled task
         * that updates the in-memory snapshots of the performance metrics for
         * the joined services (default
         * {@value #DEFAULT_HOST_DISCOVERY_INITIAL_DELAY}).
         */
        String HOST_DISCOVERY_INITIAL_DELAY = "hostDiscoveryInitialDelay";

        String DEFAULT_HOST_DISCOVERY_INITIAL_DELAY = "10000"; // ms.

        /**
         * The delay in milliseconds between scheduled tasks that update the
         * in-memory snapshots of the performance metrics for the joined
         * services (default {@value #DEFAULT_HOST_DISCOVERY_DELAY}).
         */
        String HOST_DISCOVERY_DELAY = "hostDiscoveryDelay";

        String DEFAULT_HOST_DISCOVERY_DELAY = "10000"; // ms.

    }
    
    /*
     * Static declarations of some common exceptions to reduce overhead
     * associated with filling in the stack traces.
     */

    /**
     * The {@link HostTable} is empty (no hosts).
     */
    private static final RuntimeException CAUSE_EMPTY_HOST_TABLE = new RuntimeException(
            "Empty host table.");

    /**
     * The service table is empty (no services).
     */
    private static final RuntimeException CAUSE_EMPTY_SERVICE_TABLE = new RuntimeException(
            "Empty service table.");

    /**
     * The load balancing logic failed to select a host to handle the
     * request.
     */
    private static final RuntimeException CAUSE_NO_HOST_SELECTED = new RuntimeException(
            "No host selected for request.");

    /**
     * The load balancing logic failed to select a service to handle the
     * request.
     */
    private static final RuntimeException CAUSE_NO_SERVICE_SELECTED = new RuntimeException(
            "No service selected for request.");

    /**
     * @see InitParams#LOCAL_FORWARD_THRESHOLD
     */
    private final AtomicReference<Double> localForwardThresholdRef = new AtomicReference<Double>();

    /**
     * The rule used to score the {@link IHostMetrics}.
     * 
     * @see InitParams#HOST_SCORING_RULE
     */
    private final AtomicReference<IHostScoringRule> scoringRuleRef = new AtomicReference<IHostScoringRule>();

    /**
     * The initial delay before the first discovery cycle that updates our local
     * knowledge of the load on each host.
     * 
     * @see InitParams#HOST_DISCOVERY_INITIAL_DELAY
     */
    private long hostDiscoveryInitialDelay = -1L;

    /**
     * The delay between discovery cycles that updates our local knowledge of
     * the load on each host.
     * 
     * @see InitParams#HOST_DISCOVERY_DELAY
     */
    private long hostDiscoveryDelay = -1L;

    /**
     * Random number generator used to load balance the read-requests.
     */
    private final Random rand = new Random();
    
    /**
     * The current {@link HostTable} data.
     * 
     * @see #updateHostTable()
     */
    private final AtomicReference<HostTable> hostTableRef = new AtomicReference<HostTable>(
            null);

    /**
     * The {@link Future} of a task that periodically queries the ganglia peer
     * for its up to date host counters for each discovered host.
     */
    private ScheduledFuture<?> scheduledFuture;

    /**
     * Return the name of the {@link IHostScoringRule} that provides default
     * value for the {@link InitParams#HOST_SCORING_RULE} configuration
     * parameter.
     * <p>
     * Note: The policy needs to be specific to the LBS implementation since the
     * names of the host metrics depend on the system that is being used to
     * collect and report them.
     */
    abstract protected String getDefaultScoringRule();

    /**
     * The delay between discovery cycles that updates our local knowledge of
     * the load on each host.
     * 
     * @see InitParams#HOST_DISCOVERY_DELAY
     */
    protected long getHostDiscoveryDelay() {
        
        return hostDiscoveryDelay;
        
    }
    
    @Override
    protected void toString(final StringBuilder sb) {

        super.toString(sb);

        sb.append(",localForwardThreshold=" + localForwardThresholdRef.get());

        sb.append(",hostDiscoveryInitialDelay=" + hostDiscoveryInitialDelay);

        sb.append(",hostDiscoveryDelay=" + hostDiscoveryDelay);

        sb.append(",scoringRule=" + scoringRuleRef.get());

        // report whether or not the scheduled future is still running.
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

        sb.append(",hostTable=" + hostTableRef.get());

    }
    
    public AbstractHostLBSPolicy() {
        super();
    }

    @Override
    public void init(final ServletConfig servletConfig,
            final IIndexManager indexManager) throws ServletException {

        super.init(servletConfig, indexManager);

        final Journal journal = (Journal) indexManager;

        if (journal.getPlatformStatisticsCollector() == null) {
            // LBS requires platform stats to load balance requests.
            throw new ServletException("LBS requires "
                    + PlatformStatsPlugIn.class.getName());
        }

        {

            final String s = HALoadBalancerServlet.getConfigParam(
                    servletConfig, //
                    AbstractHostLBSPolicy.class,// owningClass
                    InitParams.LOCAL_FORWARD_THRESHOLD,
                    InitParams.DEFAULT_LOCAL_FORWARD_THRESHOLD);

            final double d = Double.valueOf(s);

            if (log.isInfoEnabled())
                log.info(InitParams.LOCAL_FORWARD_THRESHOLD + "=" + d);

            setLocalForwardThreshold(d);

        }

        {

            scoringRuleRef.set(HALoadBalancerServlet.newInstance(//
                    servletConfig,//
                    AbstractHostLBSPolicy.class,// owningClass
                    IHostScoringRule.class, InitParams.HOST_SCORING_RULE,
                    getDefaultScoringRule()));

            if (log.isInfoEnabled())
                log.info(InitParams.HOST_SCORING_RULE + "="
                        + scoringRuleRef.getClass().getName());

        }

        {

            final String s = HALoadBalancerServlet.getConfigParam(
                    servletConfig, //
                    AbstractHostLBSPolicy.class,// owningClass
                    InitParams.HOST_DISCOVERY_INITIAL_DELAY,
                    InitParams.DEFAULT_HOST_DISCOVERY_INITIAL_DELAY);

            hostDiscoveryInitialDelay = Long.valueOf(s);

            if (log.isInfoEnabled())
                log.info(InitParams.HOST_DISCOVERY_DELAY + "="
                        + hostDiscoveryDelay);

        }

        {

            final String s = HALoadBalancerServlet.getConfigParam(//
                    servletConfig, //
                    AbstractHostLBSPolicy.class,// owningClass
                    InitParams.HOST_DISCOVERY_DELAY,//
                    InitParams.DEFAULT_HOST_DISCOVERY_DELAY);

            hostDiscoveryDelay = Long.valueOf(s);

            if (log.isInfoEnabled())
                log.info(InitParams.HOST_DISCOVERY_DELAY + "="
                        + hostDiscoveryDelay);

        }

        /*
         * Setup a scheduled task to discover and rank the hosts on a periodic
         * basis.
         */
        scheduledFuture = ((Journal) indexManager).addScheduledTask(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            updateHostTable();
                        } catch (RuntimeException ex) {
                            if (InnerCause.isInnerCause(ex,
                                    InterruptedException.class)) {
                                // Terminate if interrupted.
                                throw ex;
                            }
                            /*
                             * Note: If the task thows an exception it will not
                             * be rescheduled, therefore log @ ERROR rather than
                             * allowing the unchecked exception to be
                             * propagated.
                             */
                            log.error(ex, ex);
                        }
                    }
                }, hostDiscoveryInitialDelay, hostDiscoveryDelay,
                TimeUnit.MILLISECONDS);

    }

    @Override
    public void destroy() {

        super.destroy();

        localForwardThresholdRef.set(null);

        scoringRuleRef.set(null);

        hostTableRef.set(null);

        if (scheduledFuture != null) {

            scheduledFuture.cancel(true/* mayInterruptIfRunning */);

            scheduledFuture = null;

        }

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
    
            synchronized (hostTableRef) {
    
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
    protected void updateHostTable() {

        // Snapshot of the per service scores.
        final ServiceScore[] serviceScores = serviceTableRef.get();

        // The scoring rule that will be applied for this update.
        final IHostScoringRule scoringRule = scoringRuleRef.get();
       
        if (serviceScores == null || serviceScores.length == 0
                || scoringRule == null) {

            /*
             * No joined services?
             * 
             * No scoring rule?
             */

            // clear the host table.
            hostTableRef.set(null);

            return;

        }

        // Obtain the host reports for those services.
        final Map<String/* hostname */, IHostMetrics> hostMetricsMap = getHostReportForKnownServices(
                scoringRule, serviceScores);

        if (hostMetricsMap == null || hostMetricsMap.isEmpty()) {

            // clear the host table.
            hostTableRef.set(null);

            return;

        }

        if (log.isTraceEnabled())
            log.trace("hostMetricsMap=" + hostMetricsMap);

        final HostTable newHostTable = normalizeHostScores(scoringRule,
                hostMetricsMap);

        if (log.isTraceEnabled())
            log.trace("newHostTable=" + newHostTable);

        // Set the host table.
        hostTableRef.set(newHostTable);

    }

    /**
     * Compute and return the normalized load across the known hosts.
     * <p>
     * Note: This needs to be done only for those hosts that are associated with
     * the {@link Quorum} members. If we do it for the other hosts then the
     * normalization is not meaningful since we will only load balance across
     * the services that are joined with a met {@link Quorum}. 
     * 
     * @param scoringRule
     *            The {@link IHostScoringRule} used to integrate the per-host
     *            performance metrics.
     * @param hostMetricsMap
     *            The per-host performance metrics for the known hosts.
     * 
     * @return The normalized host workload.
     */
    private static HostTable normalizeHostScores(
            final IHostScoringRule scoringRule,//
            final Map<String/* hostname */, IHostMetrics> hostMetricsMap//
    ) {

        /*
         * Compute the per-host scores and the total score across those hosts.
         * This produces some dense arrays. The head of the array contains
         * information about the hosts that are associated with known services
         * for this HA replication cluster.
         */

        final int nhosts = hostMetricsMap.size();

        final String[] hostnames = new String[nhosts];

        final IHostMetrics[] metrics2 = new IHostMetrics[nhosts];

        final double[] load = new double[nhosts];

        double totalLoad = 0d;

        {

            /*
             * TODO Since the scoring rule does not produce normalized host
             * scores, we do not know how the NO_INFO will be ordered with
             * respect to those hosts for which the scoring rule was
             * successfully applied. This could be made to work either by
             * flagging hosts without metrics or by pushing down the handling of
             * a [null] metrics reference into the scoring rule, which would
             * know how to return a "median" value.
             */
            final double NO_INFO = .5d;

            int i = 0;

            for (Map.Entry<String, IHostMetrics> e : hostMetricsMap.entrySet()) {

                final String hostname = e.getKey();

                assert hostname != null; // Note: map keys are never null.

                final IHostMetrics metrics = e.getValue();

                // flag host if no load information is available.
                double hostScore = metrics == null ? NO_INFO : scoringRule
                        .getScore(metrics);

                if (hostScore < 0) {

                    log.error("Negative score: " + hostname);

                    hostScore = NO_INFO;

                }

                hostnames[i] = hostname;

                load[i] = hostScore;

                metrics2[i] = metrics;
                
                totalLoad += hostScore;

                i++;

            }
            
        }

        /*
         * Convert from LOAD to AVAILABILITY.
         * 
         * AVAILABILITY := TOTAL - LOAD[i]
         * 
         * Note: The per-host metrics and scoring rule give us LOAD. However, we
         * want to distribute the requests based on the inverse of the load,
         * which is the AVAILABILITY to do more work.
         */
        double totalAvailability = 0;
        double availability[] = new double[nhosts];
        {
            for (int i = 0; i < nhosts; i++) {

                final double avail = availability[i] = totalLoad - load[i];
                
                totalAvailability += avail;
                
            }
        }
        
        /*
         * Normalize the per-hosts scores.
         */

        HostScore thisHostScore = null;
        final HostScore[] scores = new HostScore[nhosts];
        {

            for (int i = 0; i < nhosts; i++) {

                final String hostname = hostnames[i];

                final double normalizedAvailability;
                if (totalAvailability == 0) {
                    // divide the work evenly.
                    normalizedAvailability = 1d / nhosts;
                } else {
                    normalizedAvailability = availability[i]
                            / totalAvailability;
                }

                // Normalize host scores.
                final HostScore hostScore = scores[i] = new HostScore(hostname,
                        normalizedAvailability);

                if (thisHostScore != null && hostScore.isThisHost()) {

                    // The first score discovered for this host.
                    thisHostScore = hostScore;

                }

                if (log.isDebugEnabled())
                    log.debug("hostname=" + hostname + ", metrics="
                            + metrics2[i] + ", score=" + hostScore.getAvailability());

            }

        }

//        for (int i = 0; i < scores.length; i++) {
//
//            scores[i].rank = i;
//
//            scores[i].drank = ((double) i) / scores.length;
//
//        }

//      // Sort into order by decreasing load.
//      Arrays.sort(scores);

//        if (scores.length > 0) {
//
//            if (log.isDebugEnabled()) {
//
//                log.debug("The most active index was: "
//                        + scores[scores.length - 1]);
//
//                log.debug("The least active index was: " + scores[0]);
//
//                log.debug("This host: " + thisHostScore);
//
//            }
//
//        }

        /*
         * Sort into order by hostname (useful for /bigdata/status view).
         * 
         * Note: The ordering is not really material to anything. Stochastic
         * load balancing decisions are made without regard to this ordering.
         */
        Arrays.sort(scores, HostScore.COMPARE_BY_HOSTNAME);

        return new HostTable(thisHostScore, scores);
        
    }
    
    @Override
    public String getReaderURI(final HttpServletRequest req) {

        final HostTable hostTable = hostTableRef.get();

        final HostScore[] hostScores = hostTable == null ? null
                : hostTable.hostScores;

        final ServiceScore[] serviceScores = serviceTableRef.get();

        if (hostScores == null || hostScores.length == 0) {
            // Can't do anything.
            throw CAUSE_EMPTY_HOST_TABLE;
        }

        if (serviceScores == null) {
            // No services.
            throw CAUSE_EMPTY_SERVICE_TABLE;
        }

        final HostScore hostScore = getHost(rand.nextDouble(), hostScores);

        if (hostScore == null) {
            // None found.
            throw CAUSE_NO_HOST_SELECTED;
        }

        final ServiceScore serviceScore = getService(rand, hostScore,
                serviceScores);

        if (serviceScore == null) {
            // None found.
            throw CAUSE_NO_SERVICE_SELECTED;
        }

        /*
         * Track #of requests to each service.
         * 
         * Note: ServiceScore.nrequests is incremented before we make the
         * decision to do a local forward when the target is *this* host. This
         * means that the /status page will still show the effect of the load
         * balancer for local forwards. This is a deliberate decision.
         */
        serviceScore.nrequests.increment();

        if (serviceScore.getServiceUUID().equals(serviceIDRef.get())) {
            /*
             * The target is *this* service. As an optimization, we return
             * [null] so that the caller will perform a local forward (as part
             * of its exception handling logic). The local foward has less
             * latency than proxying to this service.
             * 
             * Note: ServiceScore.nrequests *is* incremented before we make this
             * decision so the /status page will still show the effect of the
             * load balancer for local forwards. This is a deliberate decision.
             */
            return null;
        }
            
        // We will return the Request-URI for that service.
        final String requestURI = serviceScore.getRequestURI();

        return requestURI;

    }
    
    /**
     * Stochastically select the target host based on the current host workload.
     * <p>
     * Note: This is package private in order to expose it to the test suite.
     * 
     * @param d
     *            A random number in the half-open [0:1).
     * @param hostScores
     *            The {@link HostScore}s.
     * 
     * @return The {@link HostScore} of the host to which a read request should
     *         be proxied -or- <code>null</code> if the request should not be
     *         proxied (because we lack enough information to identify a target
     *         host).
     * 
     * @see bigdata/src/resources/architecture/HA_LBS.xls
     */
    static HostScore getHost(//
            final double d, //
            final HostScore[] hostScores
            ) {
        
        if (d < 0 || d >= 1d)
            throw new IllegalArgumentException();
        
        if (hostScores == null)
            throw new IllegalArgumentException();
        
        /*
         * Stochastically select the target host based on the current host
         * workload.
         * 
         * Note: The host is selected with a probability that is INVERSELY
         * proportional to normalized host load. If the normalized host load is
         * .75, then the host is selected with a probability of .25.
         * 
         * Note: We need to ignore any host that is does not have a service that
         * is joined with the met quorum....
         */
        HostScore hostScore = null;
        {
            if (hostScores.length == 1) {
                /*
                 * Only one host.
                 */
                hostScore = hostScores[0];
            } else {
                /*
                 * Multiple hosts.
                 * 
                 * Note: Choice is inversely proportional to normalized workload
                 * (1 - load).
                 */
                double sum = 0d;
                for (HostScore tmp : hostScores) {
                    hostScore = tmp;
                    sum += hostScore.getAvailability();
                    if (sum >= d) {
                        // found desired host.
                        break;
                    }
                    // scan further.
                }
            }

        }

        return hostScore;
        
    }
    
    /**
     * Stochastically select the target service on the given host.
     * <p>
     * Note: There can be multiple services on the same host. However, this
     * mostly happens in CI. Normal deployment allocates only one service per
     * host.
     * <p>
     * Note: This is package private in order to expose it to the test suite.
     * 
     * @param rand
     *            A random number generator.
     * @param hostScore
     *            The {@link HostScore} of the selected host.
     * @param serviceScores
     *            The {@link ServiceScore}s for the joined services.
     * 
     * @return The {@link ServiceScore} of a service on the host identified by
     *         the caller to which a read request should be proxied -or-
     *         <code>null</code> if the request should not be proxied (because
     *         we lack enough information to identify a target service on that
     *         host).
     * 
     *         TODO Optimize the lookup of the services on a given host. The
     *         trick is making sure that this mapping remains consistent as
     *         services join/leave. However, the normal case is one service per
     *         host. For that case, this loop wastes the maximum effort since it
     *         scans all services.
     */
    static ServiceScore getService(//
            final Random rand, //
            final HostScore hostScore,//
            final ServiceScore[] serviceScores//
    ) {

        // The set of services on the given host.
        final List<ServiceScore> foundServices = new LinkedList<ServiceScore>();

        for (ServiceScore tmp : serviceScores) {

            if (tmp == null) // should never happen.
                continue;

            if (tmp.getRequestURI() == null) // can't proxy.
                continue;

            if (hostScore.getHostname().equals(tmp.getHostname())) {

                // Found a joined service on that host.
                foundServices.add(tmp);

            }

        }

        /*
         * Report a service on that host. If there is more than one, then we
         * choose the service randomly.
         */
        final int nservices = foundServices.size();

        if (nservices == 0) {
            // Can't find a service.
            log.warn("No services on host: hostname=" + hostScore.getHostname());
            return null;
        }

        // Figure out which service to use.
        final int n = rand.nextInt(nservices);

        final ServiceScore serviceScore = foundServices.get(n);

        return serviceScore;

    }

    /**
     * {@inheritDoc}
     * <p>
     * If the normalized availability for this host is over a configured
     * threshold, then we forward the request to the local service. This help to
     * reduce the latency of the request since it is not being proxied.
     * 
     * @see InitParams#LOCAL_FORWARD_THRESHOLD
     */
    @Override
    protected boolean conditionallyForwardReadRequest(
            final HALoadBalancerServlet servlet,//
            final HttpServletRequest request, //
            final HttpServletResponse response//
    ) throws IOException {

        final HostTable hostTable = hostTableRef.get();

        final HostScore thisHostScore = hostTable == null ? null
                : hostTable.thisHost;

        if (thisHostScore != null
                && thisHostScore.getAvailability() >= localForwardThresholdRef.get()) {

            servlet.forwardToLocalService(false/* isLeaderRequest */, request,
                    response);

            // request was handled.
            return true;

        }

        return false;

    }

    /**
     * Return a map from the known canonical hostnames (as self-reported by the
     * services) of the joined services to the {@link IHostMetrics}s for those
     * hosts.
     * 
     * @param scoringRule
     *            The {@link IHostScoringRule} to be applied.
     * @param serviceScores
     *            The set of known services.
     * 
     * @return The map.
     * 
     *         TODO If there is more than one service on the same host, then we
     *         will have one record per host, not per service. This means that
     *         we can not support JVM specific metrics, such as GC time. This
     *         could be fixed if the map was indexed by Service {@link UUID} and
     *         the host metrics were combined into the map once for each
     *         service.
     */
    abstract protected Map<String, IHostMetrics> getHostReportForKnownServices(
            IHostScoringRule scoringRule, ServiceScore[] serviceScores);

}
