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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.proxy.ProxyServlet;

import com.bigdata.ganglia.GangliaService;
import com.bigdata.ganglia.HostReportComparator;
import com.bigdata.ganglia.IHostReport;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.GangliaPlugIn;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.PlatformStatsPlugIn;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.quorum.Quorum;

/**
 * 
 The HA Load Balancer servlet provides a transparent proxy for requests
 * arriving its configured URL pattern (the "external" interface for the load
 * balancer) to the root of the web application.
 * <P>
 * The use of the load balancer is entirely optional. If the security rules
 * permit, then clients MAY make requests directly against a specific service.
 * Thus, no specific provision exists to disable the load balancer servlet, but
 * you may choose not to deploy it.
 * <p>
 * When successfully deployed, requests having prefix corresponding to the URL
 * pattern for the load balancer (typically, "/bigdata/LBS/*") are automatically
 * redirected to a joined service in the met quorum based on the configured load
 * balancer policy.
 * <p>
 * The load balancer policies are "HA aware." They will always redirect update
 * requests to the quorum leader. The default polices will load balance read
 * requests over the leader and followers in a manner that reflects the CPU, IO
 * Wait, and GC Time associated with each service. The PlatformStatsPlugIn and
 * GangliaPlugIn MUST be enabled for the default load balancer policy to
 * operate. It depends on those plugins to maintain a model of the load on the
 * HA replication cluster. The GangliaPlugIn should be run only as a listener if
 * you are are running the real gmond process on the host. If you are not
 * running gmond, then the GangliaPlugIn should be configured as both a listener
 * and a sender.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/624"> HA Load Balancer </a>
 * 
 *      TODO Define some interesting load balancer policies. We can start with
 *      HA aware round robin and an HA aware policy that is load balanced based
 *      on the ganglia reported metrics model.
 * 
 *      All policies must be HA aware - we only want to send requests to
 *      services that are joined with the met quorum.
 * 
 *      TODO If the target service winds up not joined with the met quorum by
 *      the time we get there, what should it do? Report an error since we are
 *      already on its internal interface? Will this servlet see that error? If
 *      it does, should it handle it?
 */
public class HALoadBalancerServlet extends ProxyServlet {

    private static final Logger log = Logger
            .getLogger(HALoadBalancerServlet.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface InitParams {

//        String ENABLED = "enabled";
//        
//        String DEFAULT_ENABLED = "false";
        
        /**
         * The prefix at which the load balancer is deployed (its URL pattern,
         * less any wildcard). This is typically
         * 
         * <pre>
         * /bigdata/LBS
         * </pre>
         * 
         * but the actual value depends on the servlet mappined established in
         * <code>web.xml</code>.
         */
        String PREFIX = "prefix";
        
        /**
         * The load balancer policy (optional). This must be an instance of
         * {@link IHALoadBalancerPolicy}.
         */
        String POLICY = "policy";
        
        String DEFAULT_POLICY = DefaultLBSPolicy.class.getName();

        /**
         * A {@link Comparator} that places {@link IHostReport}s into a total
         * ordering from the host with the least load to the host with the
         * greatest load (optional).
         */
        String COMPARATOR = "comparator";
        
        String DEFAULT_COMPARATOR = DefaultHostReportComparator.class.getName();
        
    }
    
    public HALoadBalancerServlet() {
        super();
    }

    private boolean enabled = false;
    private String prefix = null;
    private IHALoadBalancerPolicy policy;
    private Comparator<IHostReport> comparator;
    private GangliaService gangliaService;
    private String[] reportOn;

    @SuppressWarnings("unchecked")
    @Override
    public void init() throws ServletException {

        super.init();
        
        final ServletConfig servletConfig = getServletConfig();

        final ServletContext servletContext = servletConfig.getServletContext();

        prefix = servletConfig.getInitParameter(InitParams.PREFIX);
        
        policy = newInstance(servletConfig, IHALoadBalancerPolicy.class,
                InitParams.POLICY, InitParams.DEFAULT_POLICY);

        comparator = newInstance(servletConfig, Comparator.class,
                InitParams.COMPARATOR, InitParams.DEFAULT_COMPARATOR);
        
        final IIndexManager indexManager = BigdataServlet
                .getIndexManager(servletContext);

        if (!(indexManager instanceof HAJournal)) {
            throw new ServletException("Not HA");
        }

        final HAJournal journal = (HAJournal) indexManager;

        if (journal.getPlatformStatisticsCollector() == null) {
            throw new ServletException("LBS requires "
                    + PlatformStatsPlugIn.class.getName());
        }

        gangliaService = (GangliaService) journal.getGangliaService();

        if (gangliaService == null) {
            throw new ServletException("LBS requires "
                    + GangliaPlugIn.class.getName());
        }

        reportOn = gangliaService.getDefaultHostReportOn();
        
        enabled = true;
        
        servletContext.setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                prefix);

        if (log.isInfoEnabled())
            log.info(servletConfig.getServletName() + " @ " + prefix);

    }

    @Override
    public void destroy() {

        enabled = false;

        prefix = null;
        
        policy = null;
        
        comparator = null;
        
        reportOn = null;
        
        gangliaService = null;
        
        getServletContext().setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                null);

        super.destroy();
        
    }

    /**
     * Create an instance of some type based on the servlet init parameters.
     * 
     * @param servletConfig
     *            The {@link ServletConfig}.
     * @param iface
     *            The interface that the type must implement.
     * @param name
     *            The name of the servlet init parameter.
     * @param def
     *            The default value for the servlet init parameter.
     * 
     * @return The instance of the configured type.
     * 
     * @throws ServletException
     *             if anything goes wrong.
     */
    @SuppressWarnings("unchecked")
    private static <T> T newInstance(final ServletConfig servletConfig,
            final Class<? extends T> iface, final String name, final String def)
            throws ServletException {

        final T t;

        String s = servletConfig.getInitParameter(name);

        if (s == null || s.trim().length() == 0) {

            s = def;

        }
        
        final Class<? extends T> cls;
        try {
            cls = (Class<? extends T>) Class.forName(s);
        } catch (ClassNotFoundException e) {
            throw new ServletException("cls=" + s + "cause=" + e, e);
        }

        if (!iface.isAssignableFrom(cls))
            throw new IllegalArgumentException(name + ":: " + s
                    + " must extend " + iface.getName());
        
        try {
            t = (T) cls.newInstance();
        } catch (InstantiationException e) {
            throw new ServletException(e);
        } catch (IllegalAccessException e) {
            throw new ServletException(e);
        }
    
        return t;
        
    }
        
    @Override
    protected void service(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException,
            IOException {

        if (!enabled) {
            // The LBS is not available.
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
        
        final HostScore[] hosts = hostTable.get();

        if (hosts == null || hosts.length == 0) {

            // Ensure that the host table exists.
            updateHostsTable();

        }

        final HAGlueScore[] services = serviceTable.get();

        if (services == null || services.length == 0) {

            /*
             * Ensure that the service table exists (more correctly, attempt to
             * populate it, but we can only do that if the HAQuorumService is
             * running.)
             */

            updateServicesTable();
            
        }

        /*
         * TODO if rewriteURL() returns null, then the base class (ProxyServlet)
         * returns SC_FORBIDDEN. It should return something less ominous, like a
         * 404. With an explanation.  Or a RETRY.
         */
        super.service(request, response);
        
    }

    /**
     * Update the per-host scoring table.
     * 
     * @see #hostTable
     * 
     *      FIXME This MUST be updated on a periodic basis. We can probably
     *      query the gangliaService to figure out how often it gets updates, or
     *      we can do this every 5 seconds or so (the ganglia updates are not
     *      synchronized across a cluster - they just pour in).
     * 
     *      TODO For scalability on clusters with a lot of ganglia chatter, we
     *      should only keep the data from those hosts that are of interest for
     *      a given HA replication cluster. The load on other hosts has no
     *      impact on our decision when load balancing within an HA replication
     *      cluster.
     */
    private void updateHostsTable() {

        /*
         * Note: If there is more than one service on the same host, then we
         * will have one record per host, not per service.
         * 
         * Note: The actual metrics that are available depend on the OS and on
         * whether you are running gmond or having the GangliaPlugIn do its own
         * reporting. The policy that ranks the host reports should be robust to
         * these variations.
         */
        final IHostReport[] hostReport = gangliaService.getHostReport(//
                reportOn,// metrics to be reported.
                comparator// imposes order on the host reports.
                );

        log.warn("hostReport=" + Arrays.toString(hostReport));

        final HostScore[] scores = new HostScore[hostReport.length];

        for (int i = 0; i < hostReport.length; i++) {
            
            final IHostReport r = hostReport[i];

            /*
             * TODO This is ignoring the metrics for the host and weighting all
             * hosts equally.
             */
            scores[i++] = new HostScore(r.getHostName(), 1.0,
                    (double) hostReport.length);

        }
        
        // sort into ascending order (increasing activity).
        Arrays.sort(scores);

        for (int i = 0; i < scores.length; i++) {

            scores[i].rank = i;

            scores[i].drank = ((double) i) / scores.length;

        }

        if (log.isDebugEnabled()) {

            log.debug("The most active index was: " + scores[scores.length - 1]);

            log.debug("The least active index was: " + scores[0]);

        }

        this.hostTable.set(scores);

    }

    /**
     * Update the per-service table.
     * 
     * @see #serviceTable
     * 
     *      FIXME This MUST be maintained by appropriate watchers such that we
     *      just consult the as maintained information and act immediately on
     *      it. We can not afford any latency for RMI or even figuring out which
     *      the host has the least load. That should all be maintained by a
     *      scheduled thread and listeners.
     */
    private void updateServicesTable() {

        final ServletContext servletContext = getServletContext();

        final HAJournal journal = (HAJournal) BigdataServlet
                .getIndexManager(servletContext);

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal.getQuorum();

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
                t = (QuorumService) quorum.getClient();
            } catch (IllegalStateException ex) {
                // Note: Not available (quorum.start() not called).
                return;
            }
            quorumService = t;
        }

        final UUID[] joined = quorum.getJoined();
        final HAGlueScore[] serviceScores = new HAGlueScore[joined.length];

        for (int i = 0; i < joined.length; i++) {
            final UUID serviceId = joined[i];
            try {

                /*
                 * TODO Scan the existing table before doing an RMI to the
                 * service. We only need to do the RMI for a new service, not
                 * one in the table.
                 * 
                 * TODO A services HashMap<UUID,HAGlueScore> would be much more
                 * efficient than a table. If we use a CHM, then we can do this
                 * purely asynchronously as the HAGlue services entire the set
                 * of joined services.
                 */
                serviceScores[i] = new HAGlueScore(servletContext, serviceId);

            } catch (RuntimeException ex) {

                /*
                 * Ignore. Might not be an HAGlue instance.
                 */

                if (log.isInfoEnabled())
                    log.info(ex, ex);

                continue;

            }

        }

        this.serviceTable.set(serviceScores);

    }
    
    /*
     * FIXME Choose among pre-computed and maintained proxy targets based on the
     * LBS policy. 
     */
    private static final String _proxyTo = "http://localhost:8091/bigdata";
    
    /**
     * The table of pre-scored hosts.
     * 
     * TODO There is an entry for all known hosts, but not all hosts are running
     * service that we care about. So we have to run over the table, filtering
     * for hosts that have services that we care about.
     */
    private final AtomicReference<HostScore[]> hostTable = new AtomicReference<HostScore[]>(
            null);

    /**
     * This is the table of known services. We can scan the table for a service
     * {@link UUID} and then forward a request to the pre-computed requestURL
     * associated with that {@link UUID}. If the requestURL is <code>null</code>
     * then we do not know how to reach that service and can not proxy the
     * request.
     */
    private final AtomicReference<HAGlueScore[]> serviceTable = new AtomicReference<HAGlueScore[]>(
            null);

    /**
     * For update requests, rewrite the requestURL to the service that is the
     * quorum leader. For read requests, rewrite the requestURL to the service
     * having the least load.
     */
    @Override
    protected URI rewriteURI(final HttpServletRequest request)
    {
        final String path = request.getRequestURI();
        if (!path.startsWith(prefix))
            return null;

        final boolean isUpdate = isUpdateRequest(request);
        final String proxyTo;
        if(isUpdate) {
            // Proxy to leader.
            proxyTo = getLeaderURL(request);
        } else {
            // Proxy to any joined service.
            proxyTo = getReaderURL(request);
        }
        if (proxyTo == null) {
            // Could not rewrite.
            return null;
        }
        final StringBuilder uri = new StringBuilder(proxyTo);
        if (proxyTo.endsWith("/"))
            uri.setLength(uri.length() - 1);
        final String rest = path.substring(prefix.length());
        if (!rest.startsWith("/"))
            uri.append("/");
        uri.append(rest);
        final String query = request.getQueryString();
        if (query != null)
            uri.append("?").append(query);
        final URI rewrittenURI = URI.create(uri.toString()).normalize();

        if (!validateDestination(rewrittenURI.getHost(), rewrittenURI.getPort()))
            return null;
        
        if (log.isInfoEnabled())
            log.info("rewrote: " + path + " => " + rewrittenURI);
        
        return rewrittenURI;
    }

    /**
     * Return <code>true</code> iff this is an UPDATE request that must be
     * proxied to the quorum leader.
     * 
     * FIXME How do we identify "UPDATE" requests? DELETE and PUT are update
     * requests, but POST is not always an UPDATE. It can also be used for
     * QUERY. GET is never an UPDATE request, and that is what this is based on
     * right now.
     */
    private boolean isUpdateRequest(HttpServletRequest request) {

        return !request.getMethod().equalsIgnoreCase("GET");

    }

    private String getLeaderURL(final HttpServletRequest request) {
        
        final ServletContext servletContext = request.getServletContext();

        final HAJournal journal = (HAJournal) BigdataServlet
                .getIndexManager(servletContext);

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal.getQuorum();

        final UUID leaderId = quorum.getLeaderId();
        
        if (leaderId == null) {
            // No quorum, so no leader. Can not proxy the request.
            return null;
        }

        /*
         * Scan the services table to locate the leader and then proxy the
         * request to the pre-computed requestURL for the leader. If that
         * requestURL is null then we do not know about a leader and can not
         * proxy the request at this time.
         */

        final HAGlueScore[] services = serviceTable.get();
        
        if (services == null) {

            // No services. Can't proxy.
            return null;

        }

        for (HAGlueScore s : services) {

            if (s.serviceUUID.equals(leaderId)) {

                // Found it. Proxy if the serviceURL is defined.
                return s.requestURL;

            }
            
        }

        // Not found.  Won't proxy.
        return null;
        
    }

    /**
     * Return the requestURL to which we will proxy a read request.
     * 
     * @param request
     *            The request.
     * 
     * @return The proxyTo URL -or- <code>null</code> if we could not find a
     *         service to which we could proxy this request.
     */
    private String getReaderURL(final HttpServletRequest request) {

        final HostScore[] hostScores = this.hostTable.get();
        
        if (hostScores == null) {
            // Can't proxy to anything.
            return null;
        }

        // Choose a host : TODO This is just a round robin over the hosts.
        HostScore hostScore = null;
        for (int i = 0; i < hostScores.length; i++) {

            final int hostIndex = (i + nextHost) % hostScores.length;

            hostScore = hostScores[hostIndex];

            if (hostScore == null)
                continue;

            nextHost = hostIndex + 1;
            
        }

        if (hostScore == null) {

            // No hosts. Can't proxy.
            return null;

        }

        final HAGlueScore[] services = this.serviceTable.get();

        if (services == null) {

            // No services. Can't proxy.
            return null;

        }
        
        /*
         * Find a service on that host.
         * 
         * TODO If none found, the try other hosts until we have tried each host
         * once and then give up by returning null. This will require pushing
         * down the service finder into a method that we call from the hosts
         * loop.
         */
        for(HAGlueScore x : services) {
            
            if (x.hostname == null) {
                // Can't use if no hostname.
                continue;
            }

            if (x.requestURL == null) {
                // Can't use if no requestURL.
                continue;
            }
            
            if (!x.hostname.equals(hostScore.hostname)) {
                // This service is not on the host we are looking for.
                continue;
            }
            
            return x.requestURL;
            
        }

        // No service found on that host.
        return null;
        
    }
    int nextHost = 0;

    /** Place into descending order by load_one. */
    public static class DefaultHostReportComparator extends
            HostReportComparator implements Comparator<IHostReport> {

        public DefaultHostReportComparator() {
            super("load_one", true/* asc */);
        }

    }

    /**
     * Stochastically proxy the request to the services based on their load.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class DefaultLBSPolicy implements IHALoadBalancerPolicy {

        @Override
        public String proxyTo(HttpServletRequest req) {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    /**
     * Always proxy the request to the local service even if it is not HA ready
     * (this policy defeats the load balancer).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class NOPLBSPolicy implements IHALoadBalancerPolicy {

        @Override
        public String proxyTo(HttpServletRequest req) {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    // TODO Define a host report comparator which uses the metrics that we care
    // about (CPU, IOWait, GC Time).

//    /**
//     * The default out-of-the-box comparator for ordering the hosts based on the
//     * metrics that we care about (CPU, IO Wait, GC Time).
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     */
//    public class MyHostReportComparator implements Comparator<IHostReport> {
//
//        public MyHostReportComparator() {
//
//        }
//
//        @Override
//        public int compare(final IHostReport o1, final IHostReport o2) {
//
//            final int ret = comp(o1, o2);
//
//            return -ret;
//
//        }
//
//        private int comp(final IHostReport o1, final IHostReport o2) {
//
//            final IGangliaMetricMessage m1 = o1.getMetrics().get(metricName);
//
//            final IGangliaMetricMessage m2 = o2.getMetrics().get(metricName);
//
//            if (m1 == null && m2 == null)
//                return 0;
//            else if (m1 == null)
//                return -1;
//            else if (m2 == null)
//                return -1;
//
//            final double d1 = Double.parseDouble(m1.getStringValue());
//
//            final double d2 = Double.parseDouble(m2.getStringValue());
//
//            if (d1 < d2)
//                return -1;
//            else if (d2 > d1)
//                return 1;
//            
//            /*
//             * Order by host name in case of a tie on the metric. This makes the
//             * results more stable. (We could also round the metric a little to
//             * improve stability. But that can be done in a custom comparator.)
//             */
//        
//            return o1.getHostName().compareTo(o2.getHostName());
//
//        }
//
//    }
    
    /**
     * Helper class caches metadata about an {@link HAGlue} service.
     * <p>
     * Note: This class is written fairly defensively. The fields can wind up
     * being left at their default values (typically <code>null</code>) if we
     * are not able to find the necessary information for the {@link HAGlue}
     * service. Users of this class must test for <code>null</code> values and
     * skip over those services since they have not been pre-resolved to a host
     * and requestURL.
     */
    private static class HAGlueScore {

        final UUID serviceUUID;
        HAGlue haGlue;
        String hostname;
        int port;
        /**
         * The {@link #requestURL} is assigned IFF everything succeeds. This is
         * what we will use to proxy a request to the service having the
         * {@link UUID} given to the constuctor.
         * 
         * Note: This needs to be a URL, not just a relative path. At least with
         * the rewriteURI() code in the outer class. Otherwise you get an NPE.
         */
        String requestURL;        
    
        public HAGlueScore(final ServletContext servletContext,
                final UUID serviceUUID) {

            if (servletContext == null)
                throw new IllegalArgumentException();

            if (serviceUUID == null)
                throw new IllegalArgumentException();

            this.serviceUUID = serviceUUID;
            
            final HAJournal journal = (HAJournal) BigdataServlet
                    .getIndexManager(servletContext);

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                    .getQuorum();

            if (quorum == null) {
                // No quorum.
                return;
            }

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
                    t = (QuorumService) quorum.getClient();
                } catch (IllegalStateException ex) {
                    // Note: Not available (quorum.start() not called).
                    return;
                }
                quorumService = t;
            }

            try {
                haGlue = quorumService.getService(serviceUUID);
            } catch (IllegalArgumentException ex) {
                // No such service.
                return;
            }
      
            /*
             * TODO The hostname and port are RMIs. Use a smart proxy.
             */
            try {
                hostname = haGlue.getHostname();
                port = haGlue.getNSSPort();
            } catch (IOException ex) {
                // RMI error.
                return;
            }

            final String contextPath = servletContext.getContextPath();

            requestURL = "http://" + hostname + ":" + port
                    + contextPath;

        }

    }
    
    /**
     * Helper class assigns a raw and a normalized score to each host based on
     * its per-host.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class HostScore implements Comparable<HostScore> {

        /** The hostname. */
        private final String hostname;
        
        /** The raw (write) score computed for that index partition. */
        private final double rawScore;

        /** The normalized score computed for that index partition. */
        private final double score;

        /** The rank in [0:#scored].  This is an index into the Scores[]. */
        private int rank = -1;

        /** The normalized double precision rank in [0.0:1.0]. */
        private double drank = -1d;

        @Override
        public String toString() {

            return "Score{hostname=" + hostname + ", rawScore=" + rawScore
                    + ", score=" + score + ", rank=" + rank + ", drank="
                    + drank + "}";

        }

        public HostScore(final String hostname, final double rawScore,
                final double totalRawScore) {

            this.hostname = hostname;

            this.rawScore = rawScore;

            score = normalize(rawScore, totalRawScore);

        }

        /**
         * Places elements into order by ascending {@link #rawScore}. The
         * {@link #hostname} is used to break any ties.
         */
        public int compareTo(final HostScore arg0) {

            if (rawScore < arg0.rawScore) {

                return -1;

            } else if (rawScore > arg0.rawScore) {

                return 1;

            }

            return hostname.compareTo(arg0.hostname);

        }

    }

    /**
     * Places {@link HostScore} into ascending order (lowest score to highest
     * score). Ties are broken based on an alpha sort of the index name.
     */
    static private class ASC implements Comparator<HostScore> {

        public int compare(HostScore arg0, HostScore arg1) {

            if (arg0.rawScore < arg1.rawScore) {

                return -1;

            } else if (arg0.rawScore > arg1.rawScore) {

                return 1;

            }

            return arg0.hostname.compareTo(arg1.hostname);

        }

    }

    /**
     * Places {@link HostScore} into descending order (highest score to lowest
     * score). Ties are broken based on an alpha sort of the index name.
     */
    static private class DESC implements Comparator<HostScore> {

        public int compare(HostScore arg0, HostScore arg1) {

            if (arg1.rawScore < arg0.rawScore) {

                return -1;

            } else if (arg1.rawScore > arg0.rawScore) {

                return 1;

            }

            return arg0.hostname.compareTo(arg1.hostname);

        }

    }

    /**
     * Normalizes a raw score in the context of totals for some data
     * service.
     * 
     * @param rawScore
     *            The raw score.
     * @param totalRawScore
     *            The raw score computed from the totals.
     * 
     * @return The normalized score.
     */
    static public double normalize(final double rawScore,
            final double totalRawScore) {

        if (totalRawScore == 0d) {

            return 0d;

        }

        return rawScore / totalRawScore;

    }

}
