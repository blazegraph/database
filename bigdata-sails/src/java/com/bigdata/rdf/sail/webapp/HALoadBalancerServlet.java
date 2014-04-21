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
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.proxy.ProxyServlet;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.ganglia.GangliaService;
import com.bigdata.ganglia.HostReportComparator;
import com.bigdata.ganglia.IGangliaMetricMessage;
import com.bigdata.ganglia.IHostReport;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.GangliaPlugIn;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.PlatformStatsPlugIn;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.journal.jini.ha.HAJournalServer;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.util.InnerCause;
import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * The HA Load Balancer servlet provides a transparent proxy for requests
 * arriving its configured URL pattern (the "external" interface for the load
 * balancer) to the root of the web application.
 * <p>
 * When successfully deployed, requests having prefix corresponding to the URL
 * pattern for the load balancer (typically, "/bigdata/LBS/*") are automatically
 * redirected to a joined service in the met quorum based on the configured load
 * balancer policy.
 * <p>
 * The use of the load balancer is entirely optional. If the load balancer is
 * not properly configured, then it will simply rewrite itself out of any
 * request and the request will be handled by the host to which it was directed
 * (no proxying).
 * <p>
 * Note: If the security rules permit, then clients MAY make requests directly
 * against a specific service.
 * <p>
 * The load balancer policies are "HA aware." They will always redirect update
 * requests to the quorum leader. Read requests will be directed to one of the
 * services that is joined with the met quorum.
 * <p>
 * 
 * <h3>Default Load Balancer Policy Configuration</h3>
 * <p>
 * The default policy will load balance read requests over the leader and
 * followers in a manner that reflects the CPU, IO Wait, and GC Time associated
 * with each service.
 * <p>
 * The {@link PlatformStatsPlugIn}Ê and {@link GangliaPlugIn} MUST be enabled
 * for the default load balancer policy to operate. It depends on those plugins
 * to maintain a model of the load on the HA replication cluster. The
 * GangliaPlugIn should be run only as a listener if you are are running the
 * real gmond process on the host. If you are not running gmond, then the
 * {@link GangliaPlugIn} should be configured as both a listener and a sender.
 * <p>
 * <ul>
 * <li>The {@link PlatformStatsPlugIn} must be enabled.</li>.
 * <li>The {@link GangliaPlugIn} must be enabled. The service does not need to
 * be enabled for {@link GangliaPlugIn.Options#GANGLIA_REPORT}, but it must be
 * enabled for {@link GangliaPlugIn.Options#GANGLIA_LISTEN}.
 * </ul>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/624"> HA Load Balancer </a>
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
        
        /**
         * FIXME The default must be something that we can override from the
         * test suite in order to test these different policies. I've added some
         * code to do this based on a System property, but the test suite does
         * not allow us to set arbitrary system properties on the child
         * processes so that code is not yet having the desired effect.
         */
//        String DEFAULT_POLICY = RoundRobinPolicy.class.getName();
        String DEFAULT_POLICY = NOPLBSPolicy.class.getName();
//        String DEFAULT_POLICY = GangliaLBSPolicy.class.getName();

    }
    
    public HALoadBalancerServlet() {
        super();
    }

//    /**
//     * This servlet request attribute is used to mark a request as either an
//     * update or a read-only operation.
//     */
//    protected static final String ATTR_LBS_UPDATE_REQUEST = "lbs-update-request";
    
    /**
     * If the LBS is not enabled, then it will strip its prefix from the URL
     * requestURI and do a servlet forward to the resulting requestURI. This
     * allows the webapp to start even if the LBS is not correctly configured.
     */
    private boolean enabled = false;
    private String prefix = null;
    private IHALoadBalancerPolicy policy;

    @Override
    public void init() throws ServletException {

        super.init();
        
        // Disabled by default.
        enabled = false;

        final ServletConfig servletConfig = getServletConfig();

        final ServletContext servletContext = servletConfig.getServletContext();

        final IIndexManager indexManager = BigdataServlet
                .getIndexManager(servletContext);

        if (!(indexManager instanceof HAJournal)) {
            // This is not an error, but the LBS is only for HA.
            log.warn("Not HA");
            return;
        }

        prefix = servletConfig.getInitParameter(InitParams.PREFIX);

        policy = newInstance(servletConfig, IHALoadBalancerPolicy.class,
                InitParams.POLICY, InitParams.DEFAULT_POLICY);

        try {

            // Attempt to provision the specified LBS policy.
            policy.init(servletConfig, indexManager);
            
        } catch (Throwable t) {
            
            /*
             * The specified LBS policy could not be provisioned.
             */

            if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                // Interrupted.
                return;
            }

            log.error("Could not setup policy: " + policy, t);

            try {
                policy.destroy();
            } catch (Throwable t2) {
                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                    // Interrupted.
                    return;
                }
                log.warn("Problem destroying policy: " + policy, t2);
            } finally {
                policy = null;
            }
            
            /*
             * Fall back onto a NOP policy. Each service will handle a
             * read-request itself. Write requests are proxied to the quorum
             * leader.
             */
  
            policy = new NOPLBSPolicy();

            log.warn("Falling back: policy=" + policy);

            // Initialize the fallback policy.
            policy.init(servletConfig, indexManager);

        }

        enabled = true;
        
        servletContext.setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                prefix);

        if (log.isInfoEnabled())
            log.info(servletConfig.getServletName() + " @ " + prefix
                    + " :: policy=" + policy);

    }

    @Override
    public void destroy() {
        
        enabled = false;

        prefix = null;
        
        if (policy != null) {
            policy.destroy();
            policy = null;
        }
        
        getServletContext().setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                null);

        super.destroy();
        
    }

    /**
     * Return the configured value of the named parameter. This method checks
     * the environment variables first for a fully qualified value for the
     * parameter using <code>HALoadBalancerServer</code><i>name</i>. If no value
     * is found for that variable, it checks the {@link ServletContext} for
     * <i>name</i>. If no value is found again, it returns the default value
     * specified by the caller. This makes it possible to configure the behavior
     * of the {@link HALoadBalancerServlet} using environment variables.
     * 
     * @param servletConfig
     *            The {@link ServletConfig}.
     * 
     * @param iface
     *            The interface that the type must implement.
     * @param name
     *            The name of the servlet init parameter.
     * @param def
     *            The default value for the servlet init parameter.
     * @return
     */
    private static String getConfigParam(final ServletConfig servletConfig,
            final String name, final String def) {
        
        // Look at environment variables for an override.
        String s = System.getProperty(HALoadBalancerServlet.class.getName()
                + "." + name);

        if (s == null || s.trim().length() == 0) {

            // Look at ServletConfig for the configured value.
            s = servletConfig.getInitParameter(name);

        }

        if (s == null || s.trim().length() == 0) {

            // Use the default value.
            s = def;

        }
        
        return s;

    }
    
    /**
     * Create an instance of some type based on the servlet init parameters.
     * <p>
     * Note: The configuration parameter MAY also be specified as <code>
     * com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.<i>name</i></code>.
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

        final String s = getConfigParam(servletConfig, name, def);

        final T t;
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
            /*
             * LBS is disabled. Strip LBS prefix from the requestURI and forward
             * the request to servlet on this host (NOP LBS).
             */
            forwardToThisService(request, response);
            return;
        }

        /*
         * Decide whether this is a read-only request or an update request.
         */
        final boolean isUpdate = isUpdateRequest(request);

//        // Set the request attribute.
//        request.setAttribute(ATTR_LBS_UPDATE_REQUEST, isUpdate);
        
        /*
         * Delegate to policy. This provides a single point during which the
         * policy can ensure that it is monitoring any necessary information and
         * also provides an opportunity to override the behavior completely. For
         * example, as an optimization, the policy can forward the request to a
         * servlet in this servlet container rather than proxying it to either
         * itself or another service.
         */
        if(policy.service(isUpdate, request, response)) {

            // Return immediately if the response was committed.
            return;
            
        }
        
        /*
         * TODO if rewriteURL() returns null, then the base class (ProxyServlet)
         * returns SC_FORBIDDEN. It should return something less ominous, like a
         * 404. With an explanation. Or a RETRY. Or just forward to the local
         * service and let it report an appropriate error message (e.g.,
         * NotReady).
         */
        super.service(request, response);
        
    }

    /**
     * Strip off the <code>/LBS</code> prefix from the requestURI and forward
     * the request to the servlet at the resulting requestURI. This forwarding
     * effectively disables the LBS but still allows requests which target the
     * LBS to succeed against the webapp on the same host.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     * 
     * @throws IOException
     * @throws ServletException
     */
    static protected void forwardToThisService(
            final HttpServletRequest request, //
            final HttpServletResponse response//
    ) throws IOException, ServletException {

        final String path = request.getRequestURI();

        // The prefix for the LBS servlet.
        final String prefix = (String) request.getServletContext()
                .getAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX);

        if (prefix == null) {
            // LBS is not running / destroyed.
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }

        if (!path.startsWith(prefix)) {
            // Request should not have reached the LBS.
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }

        // what remains after we strip off the LBS prefix.
        final String rest = path.substring(prefix.length());
        
        // build up path w/o LBS prefix.
        final StringBuilder uri = new StringBuilder();
        
        if (!rest.startsWith("/")) {
            /*
             * The new path must start with '/' and is relative to this
             * ServletContext.
             */
            uri.append("/");
        }

        // append the remainder of the original requestURI
        uri.append(rest);
        
//        // append the query parameters (if any).
//        final String query = request.getQueryString();
//        if (query != null)
//            uri.append("?").append(query);

        // The new path.
        final String newPath = uri.toString();
        
        /*
         * Forward the request to this servlet container now that we have
         * stripped off the prefix for the LBS.
         */

        if (log.isInfoEnabled())
            log.info("forward: " + path + " => " + newPath);

        request.getRequestDispatcher(newPath).forward(request, response);

    }

    /**
     * For update requests, rewrite the requestURL to the service that is the
     * quorum leader. For read requests, rewrite the requestURL to the service
     * having the least load.
     */
    @Override
    protected URI rewriteURI(final HttpServletRequest request) {

        final String path = request.getRequestURI();
        if (!path.startsWith(prefix))
            return null;

        final boolean isUpdate = isUpdateRequest(request);
        final String proxyTo;
        if(isUpdate) {
            // Proxy to leader.
            proxyTo = policy.getLeaderURL(request);
        } else {
            // Proxy to any joined service.
            proxyTo = policy.getReaderURL(request);
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
     * TODO This offers an opportunity to handle a rewrite failure. It could be
     * used to provide a default status code (e.g., 404 versus forbidden) or to
     * forward the request to this server rather than proxying to another
     * server.
     */
    @Override
    protected void onRewriteFailed(final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        response.sendError(HttpServletResponse.SC_FORBIDDEN);
        
    }

    /**
     * Return <code>true</code> iff this is an UPDATE request that must be
     * proxied to the quorum leader.  A SPARQL QUERY 
     */
    private boolean isUpdateRequest(final HttpServletRequest request) {

        final boolean isGet = request.getMethod().equalsIgnoreCase("GET");

        if (isGet) {

            // GET is never an UPDATE request.
            return false;
        
        }
        
        final String requestURI = request.getRequestURI();

        if (requestURI.endsWith("/sparql")) {

            /*
             * SPARQL end point.
             * 
             * @see QueryServlet#doPost()
             */

            if ( request.getParameter(QueryServlet.ATTR_QUERY) != null ||
                 RESTServlet.hasMimeType(request,
                            BigdataRDFServlet.MIME_SPARQL_QUERY)
                            ) {

                /*
                 * QUERY against SPARQL end point using POST for visibility, not
                 * mutability.
                 */

                return false; // idempotent query using POST.

            }

            if (request.getParameter(QueryServlet.ATTR_UUID) != null) {

                return false; // UUID request with caching disabled.

            } else if (request.getParameter(QueryServlet.ATTR_ESTCARD) != null) {

                return false; // ESTCARD with caching defeated.

            } else if (request.getParameter(QueryServlet.ATTR_CONTEXTS) != null) {

                // Request for all contexts in the database.
                return false;

            }

        }

        // Anything else must be proxied to the leader.
        return true;

    }

    /** Place into descending order by load_one. */
    public static class DefaultHostReportComparator extends
            HostReportComparator implements Comparator<IHostReport> {

        public DefaultHostReportComparator() {
            super("load_one", true/* asc */);
        }

    }

    /**
     * Abstract base class establishes a listener for quorum events, tracks the
     * services that are members of the quorum, and caches metadata about those
     * services (especially the requestURL at which they will respond).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         FIXME The {@link QuorumListener} is unregistered by
     *         {@link AbstractQuorum#terminate()}. This happens any time the
     *         {@link HAJournalServer} goes into the error state. When this
     *         occurs, we stop getting {@link QuorumEvent}s and the policy stops
     *         being responsive. We probably need to either NOT clear the quorum
     *         listener and/or add an event type that is sent when
     *         {@link Quorum#terminate()} is called.
     */
    abstract protected static class AbstractLBSPolicy implements
            IHALoadBalancerPolicy, QuorumListener {

        public interface InitParams {
            
        }

        /**
         * The {@link ServletContext#getContextPath()} is cached in
         * {@link #init(ServletConfig, IIndexManager)}.
         */
        private final AtomicReference<String> contextPath = new AtomicReference<String>();
        
        /**
         * A {@link WeakReference} to the {@link HAJournal} avoids pinning the
         * {@link HAJournal}.
         */
        protected final AtomicReference<WeakReference<HAJournal>> journalRef = new AtomicReference<WeakReference<HAJournal>>();

        /**
         * This is the table of known services. We can scan the table for a service
         * {@link UUID} and then forward a request to the pre-computed requestURL
         * associated with that {@link UUID}. If the requestURL is <code>null</code>
         * then we do not know how to reach that service and can not proxy the
         * request.
         */
        protected final AtomicReference<HAGlueScore[]> serviceTable = new AtomicReference<HAGlueScore[]>(
                null);

        /**
         * Return the cached reference to the {@link HAJournal}.
         * 
         * @return The reference or <code>null</code> iff the reference has been
         *         cleared or has not yet been set.
         */
        protected HAJournal getJournal() {

            final WeakReference<HAJournal> ref = journalRef.get();

            if (ref == null)
                return null;

            return ref.get();
            
        }
        
        @Override
        public void destroy() {

            contextPath.set(null);

            journalRef.set(null);
            
            serviceTable.set(null);

        }
        
        @Override
        public void init(final ServletConfig servletConfig,
                final IIndexManager indexManager) throws ServletException {

            final ServletContext servletContext = servletConfig
                    .getServletContext();

            contextPath.set(servletContext.getContextPath());

            final HAJournal journal = (HAJournal) BigdataServlet
                    .getIndexManager(servletContext);

            this.journalRef.set(new WeakReference<HAJournal>(journal));

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                    .getQuorum();

            quorum.addListener(this);
        
        }

        @Override
        public boolean service(final boolean isUpdate,
                final HttpServletRequest request,
                final HttpServletResponse response) throws ServletException,
                IOException {

            /*
             * Figure out whether the quorum is met and if this is the quorum
             * leader.
             */
            final HAJournal journal = getJournal();
            Quorum<HAGlue, QuorumService<HAGlue>> quorum = null;
            QuorumService<HAGlue> quorumService = null;
            long token = Quorum.NO_QUORUM; // assume no quorum.
            boolean isLeader = false; // assume false.
            boolean isQuorumMet = false; // assume false.
            if (journal != null) {
                quorum = journal.getQuorum();
                if (quorum != null) {
                    try {
                        // Note: This is the *local* HAGlueService.
                        quorumService = (QuorumService) quorum.getClient();
                        token = quorum.token();
                        isLeader = quorumService.isLeader(token);
                        isQuorumMet = token != Quorum.NO_QUORUM;
                    } catch (IllegalStateException ex) {
                        // Note: Not available (quorum.start() not
                        // called).
                    }
                }
            }

            if ((isLeader && isUpdate) || !isQuorumMet) {

                /*
                 * (1) If this service is the leader and the request is an
                 * UPDATE, then we forward the request to the local service. It
                 * will handle the UPDATE request.
                 * 
                 * (2) If the quorum is not met, then we forward the request to
                 * the local service. It will produce the appropriate error
                 * message.
                 * 
                 * FIXME (3) For read-only requests, have a configurable
                 * preference to forward the request to this service unless
                 * either (a) there is a clear load imbalance. This will help to
                 * reduce the latency of the request. If HAProxy is being used
                 * to load balance over the readers, then we should have a high
                 * threshold before we send the request somewhere else.
                 * 
                 * @see #forwardToThisService()
                 */
                forwardToThisService(request, response);
            
                // request was handled.
                return true;
                
            }
 
            /*
             * Hook the request to update the service/host tables if they are
             * not yet defined.
             */
            conditionallyUpdateServiceTable();

            // request was not handled.
            return false;
            
        }

        /**
         * {@inheritDoc}
         * <p>
         * This implementation rewrites the requestURL such that the request
         * will be proxied to the quorum leader.
         */
        @Override
        final public String getLeaderURL(final HttpServletRequest request) {

            final ServletContext servletContext = request.getServletContext();

            final HAJournal journal = (HAJournal) BigdataServlet
                    .getIndexManager(servletContext);

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                    .getQuorum();

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
         * {@inheritDoc}
         * <p>
         * The services table is updated if a services joins or leaves the
         * quorum.
         */
        @Override
        public void notify(final QuorumEvent e) {
            switch(e.getEventType()) {
            case SERVICE_JOIN:
            case SERVICE_LEAVE:
                updateServiceTable();
                break;
            }
        }
        
        /**
         * Conditionally update the {@link #serviceTable} iff it does not exist
         * or is empty.
         */
        protected void conditionallyUpdateServiceTable() {
            
            final HAGlueScore[] services = serviceTable.get();

            if (services == null || services.length == 0) {

                /*
                 * Ensure that the service table exists (more correctly, attempt
                 * to populate it, but we can only do that if the
                 * HAQuorumService is running.)
                 * 
                 * FIXME This should be robust even when the HAQuorumService is
                 * not running. We do not want to be unable to proxy to another
                 * service just because this one is going through an error
                 * state. Would it make more sense to have a 2nd Quorum object
                 * for this purpose - one that is not started and stopped by the
                 * HAJournalServer?
                 * 
                 * Note: Synchronization here is used to ensure only one thread
                 * runs this logic if the table does not exist and we get a
                 * barrage of requests.
                 */
                synchronized (serviceTable) {
                
                    updateServiceTable();
                    
                }
                
            }

        }
        
        /**
         * Update the per-service table.
         * 
         * @see #serviceTable
         */
        protected void updateServiceTable() {

            final HAJournal journal = getJournal();

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                    .getQuorum();

            final UUID[] joined = quorum.getJoined();

            final HAGlueScore[] serviceScores = new HAGlueScore[joined.length];

            for (int i = 0; i < joined.length; i++) {

                final UUID serviceId = joined[i];

                try {

                    /*
                     * TODO Scan the existing table before doing an RMI to the
                     * service. We only need to do the RMI for a new service,
                     * not one in the table.
                     * 
                     * TODO A services HashMap<UUID,HAGlueScore> would be much
                     * more efficient than a table. If we use a CHM, then we can
                     * do this purely asynchronously as the HAGlue services
                     * entire the set of joined services.
                     */
                    serviceScores[i] = new HAGlueScore(journal,
                            contextPath.get(), serviceId);

                } catch (RuntimeException ex) {

                    /*
                     * Ignore. Might not be an HAGlue instance.
                     */

                    if (log.isInfoEnabled())
                        log.info(ex, ex);

                    continue;

                }

            }

            if (log.isInfoEnabled())
                log.info("Updated servicesTable: #services="
                        + serviceScores.length);

            this.serviceTable.set(serviceScores);

        }

    }
    
    /**
     * This policy proxies all requests for update operations to the leader but
     * forwards read requests to the local service. Thus, it does not provide a
     * load balancing strategy, but it does allow update requests to be directed
     * to any service in an non-HA aware manner. This policy can be combined
     * with an external round-robin strategy to load balance the read-requests
     * over the cluster.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         TODO A service that is not joined with the met quorum can not
     *         answer a read-request. In order to be generally useful (and not
     *         just as a debugging policy), we need to proxy a read-request when
     *         this service is not joined with the met quorum. If there is no
     *         met quorum, then we can just forward the request to the local
     *         service and it will report the NoQuorum error.
     */
    public static class NOPLBSPolicy extends AbstractLBSPolicy {

        @Override
        public boolean service(final boolean isUpdate,
                final HttpServletRequest request,
                final HttpServletResponse response) throws IOException,
                ServletException {

            if (!isUpdate) {

                // Always handle read requests locally.
                forwardToThisService(request, response);
                
                // Request was handled.
                return true;
                
            }
            
            // Proxy update requests to the quorum leader.
            return super.service(isUpdate, request, response);

        }

        /**
         * Note: This method is not invoked.
         */
        @Override
        public String getReaderURL(final HttpServletRequest req) {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * Policy implements a round-robin over the services that are joined with
     * the met quorum.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class RoundRobinPolicy extends AbstractLBSPolicy {

        /**
         * {@inheritDoc}
         * <p>
         * This imposes a round-robin policy over the discovered services. If
         * the service is discovered and appears to be joined with the met
         * quorum, then the request can be proxied to that service.
         */
        @Override
        public String getReaderURL(final HttpServletRequest request) {

            final HAGlueScore[] serviceScores = this.serviceTable.get();

            if (serviceScores == null) {

                // Nothing discovered. Can't proxy.
                return null;

            }

            /*
             * Choose a service.
             * 
             * Note: This is a round robin over the services. Any service that
             * is joined with the met quorum can be selected as a target for the
             * read request.
             * 
             * Note: The round-robin is atomic with respect to each request. The
             * request obtains a starting point in the serviceScores[] and then
             * finds the next service in that array using a round-robin. The
             * [nextService] starting point is not updated until the round-robin
             * is complete - this is necessary in order to avoid skipping over
             * services that are being checked by a concurrent request.
             * 
             * The [nextService] is updated only after the round-robin decision
             * has been made. As soon as it has been updated, a new round-robin
             * decision will be made with respect to the new value for
             * [nextService] but any in-flight decisions will be made against
             * the value of [nextService] that they observed on entry.
             */
            
            // The starting offset for the round-robin.
            final long startIndex = nextService.longValue();

            // The selected service.
            HAGlueScore serviceScore = null;

            for (int i = 0; i < serviceScores.length; i++) {

                /*
                 * Find the next host index.
                 * 
                 * Note: We need to ensure that the hostIndex stays in the legal
                 * range, even with concurrent requests and when wrapping around
                 * MAX_VALUE.
                 */
                final int hostIndex = (int) Math
                        .abs(((i + startIndex) % serviceScores.length));

                serviceScore = serviceScores[hostIndex];

                if (serviceScore == null)
                    continue;

                if (serviceScore.hostname == null) {
                    // Can't use if no hostname.
                    continue;
                }

                if (serviceScore.requestURL == null) {
                    // Can't use if no requestURL.
                    continue;
                }

            }

            // Bump the nextService counter.
            nextService.incrementAndGet();
            
            if (serviceScore == null) {

                // No service. Can't proxy.
                return null;

            }

            return serviceScore.requestURL;

        }

        /**
         * Note: This could be a hot spot. We can have concurrent requests and
         * we need to increment this counter for each such request.
         */
        private final AtomicLong nextService = new AtomicLong(0L);

    }
    
    /**
     * Stochastically proxy the request to the services based on their load.
     * <p>
     * Note: This {@link IHALoadBalancerPolicy} has a dependency on the
     * {@link GangliaPlugIn}. The {@link GangliaPlugIn} must be setup to listen
     * to the Ganglia protocol and build up an in-memory model of the load on
     * each host. Ganglia must be reporting metrics for each host running an
     * {@link HAJournalServer} instance. This can be achieved either using the
     * <code>gmond</code> utility from the ganglia distribution or using the
     * {@link GangliaPlugIn}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class GangliaLBSPolicy extends AbstractLBSPolicy {

        public interface InitParams extends AbstractLBSPolicy.InitParams {

//            /**
//             * A {@link Comparator} that places {@link IHostReport}s into a
//             * total ordering from the host with the least load to the host with
//             * the greatest load (optional).
//             */
//            String COMPARATOR = "comparator";
//
//            String DEFAULT_COMPARATOR = DefaultHostReportComparator.class
//                    .getName();

            /**
             * The {@link IHostScoringRule} that will be used to score the
             * {@link IHostReport}s. The {@link IHostReport}s are obtained
             * periodically from the {@link GangliaPlugIn}. The reports reflect
             * the best local knowledge of the metrics on each of the hosts. The
             * hosts will each self-report their metrics periodically using the
             * ganglia protocol.
             * <p>
             * The purpose of the scoring rule is to compute a single workload
             * number based on those host metrics. The resulting scores are then
             * normalized. Load balancing decisions are made based on those
             * normalized scores.
             */
            String HOST_SCORING_RULE = "hostScoringRule";

            String DEFAULT_HOST_SCORING_RULE = DefaultHostScoringRule.class
                    .getName();

        }

        /**
         * Interface for scoring the load on a host.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        public interface IHostScoringRule {
            
            /**
             * Return a score for the given {@link IHostReport}.
             * 
             * @param hostReport
             *            The {@link IHostReport}.
             *            
             * @return The score.
             */
            public double getScore(final IHostReport hostReport);
            
        }
        
        /**
         * Returns ONE for each host (all hosts appear to have an equal
         * workload).
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        public static class NOPHostScoringRule implements IHostScoringRule {

            @Override
            public double getScore(final IHostReport hostReport) {

                return 1d;
                
            }

        }

        /**
         * Best effort computation of a workload score based on CPU Utilization,
         * IO Wait, and GC time.
         * <p>
         * Note: Not all platforms report all metrics. For example, OSX does not
         * report IO Wait, which is a key metric for the workload of a database.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * 
         *         FIXME GC time is a JVM metric. It will only get reported by
         *         the {@link GangliaPlugIn} if it is setup to self-report that
         *         data. And it may not report it correctly if there is more
         *         than one {@link HAJournalService} per host. It is also
         *         available from /counters and could be exposed as a JMX MBean.
         */
        public static class DefaultHostScoringRule implements IHostScoringRule {

            @Override
            public double getScore(final IHostReport hostReport) {
                
                final Map<String, IGangliaMetricMessage> metrics = hostReport
                        .getMetrics();

                /*
                 * TODO Use "load_one" if we can't get both "cpu_system" and
                 * "cpu_user".
                 */
//                final double cpu_system;
//                {
//
//                    final IGangliaMetricMessage m = metrics.get("cpu_system");
//
//                    if (m != null)
//                        cpu_system = m.getNumericValue().doubleValue();
//                    else
//                        cpu_system = .25d;
//                    
//                }
//                
//                final double cpu_user;
//                {
//
//                    final IGangliaMetricMessage m = metrics.get("cpu_user");
//
//                    if (m != null)
//                        cpu_user = m.getNumericValue().doubleValue();
//                    else
//                        cpu_user = .25d;
//
//                }
 
                final double cpu_idle;
                {

                    final IGangliaMetricMessage m = metrics.get("cpu_idle");

                    if (m != null)
                        cpu_idle = m.getNumericValue().doubleValue();
                    else
                        cpu_idle = .5d;

                }
                
                final double cpu_wio;
                {

                    final IGangliaMetricMessage m = metrics.get("cpu_wio");

                    if (m != null)
                        cpu_wio = m.getNumericValue().doubleValue();
                    else
                        cpu_wio = .05d;

                }

                final double hostScore = (1d + cpu_wio * 100d)
                        / (1d + cpu_idle);
                
                return hostScore;

            }

        }

        /**
         * Place into descending order by load_one.
         * <p>
         * Note: We do not rely on the ordering imposed by this comparator.
         * Instead, we filter the hosts for those that correspond to the joined
         * services in the met quorum, compute a score for each such host, and
         * then normalize those scores.
         */
        private final Comparator<IHostReport> comparator = new HostReportComparator(
                "load_one", false/* asc */);
        
        /**
         * The ganglia service - it must be configured at least as a listener.
         */
        private GangliaService gangliaService;

        /**
         * The set of metrics that we are requesting in the ganglia host
         * reports.
         */
        private String[] reportOn;

        /**
         * The {@link Future} of a task that periodically queries the ganglia
         * peer for its up to date host counters for each discovered host.
         */
        private ScheduledFuture<?> scheduledFuture;

        /**
         * The table of pre-scored hosts.
         * <P>
         * Note: There is an entry for all known hosts, but not all hosts are
         * running services that we care about. This means that we have to run
         * over the table, filtering for hosts that have services that we care
         * about.
         */
        private final AtomicReference<HostScore[]> hostTable = new AtomicReference<HostScore[]>(
                null);

        /**
         * The most recent score for this host.
         */
        private final AtomicReference<HostScore> thisHostScore = new AtomicReference<HostScore>();

        /**
         * The rule used to score the host reports.
         */
        private IHostScoringRule scoringRule;
        
//        @SuppressWarnings("unchecked")
        @Override
        public void init(final ServletConfig servletConfig,
                final IIndexManager indexManager) throws ServletException {

            super.init(servletConfig, indexManager);

//            comparator = newInstance(servletConfig, Comparator.class,
//                    InitParams.COMPARATOR, InitParams.DEFAULT_COMPARATOR);

            final HAJournal journal = (HAJournal) indexManager;

            if (journal.getPlatformStatisticsCollector() == null) {
                // LBS requires platform stats to load balance requests.
                throw new ServletException("LBS requires "
                        + PlatformStatsPlugIn.class.getName());
            }

            gangliaService = (GangliaService) journal.getGangliaService();

            if (gangliaService == null) {
                // LBS requires ganglia to load balance requests.
                throw new ServletException("LBS requires "
                        + GangliaPlugIn.class.getName());
            }

            reportOn = gangliaService.getDefaultHostReportOn();

            scoringRule = newInstance(servletConfig, IHostScoringRule.class,
                    InitParams.HOST_SCORING_RULE,
                    InitParams.DEFAULT_HOST_SCORING_RULE);

            /*
             * Setup a scheduled task to discover and rank the hosts on a
             * periodic basis.
             * 
             * FIXME Define an init parameter or query the gangliaService to
             * figure out how often it gets updates, or we can do this every 5
             * seconds or so.
             * 
             * Note: the ganglia updates are not synchronized across a cluster -
             * they just pour in ever N seconds from each host, but the hosts do
             * not begin to report on the same N second boundary. All you know
             * is that (on average) all hosts should have reported in within N
             * seconds.
             */
            final long initialDelay = 0L;
            final long delay = 5000L;
            
            scheduledFuture = journal.addScheduledTask(new Runnable() {
                @Override
                public void run() {
                    try {
                        updateHostTable();
                    } catch (RuntimeException ex) {
                        /*
                         * Note: If the task thows an exception it will not be
                         * rescheduled, therefore log @ ERROR rather than
                         * allowing the unchecked exception to be propagated.
                         */
                        log.error(ex, ex);
                    }
                }
            }, initialDelay, delay, TimeUnit.MILLISECONDS);

        }

        @Override
        public void destroy() {

//            comparator = null;

            reportOn = null;

            gangliaService = null;

            if (scheduledFuture != null) {

                scheduledFuture.cancel(true/* mayInterruptIfRunning */);

                scheduledFuture = null;

            }
            
            super.destroy();
            
        }

        /**
         * Extended to conditionally update the {@link #hostTable} iff it does
         * not exist or is empty.
         */
        @Override
        protected void conditionallyUpdateServiceTable() {
            
            super.conditionallyUpdateServiceTable();
            
            final HostScore[] hosts = hostTable.get();

            if (hosts == null || hosts.length == 0) {

                /*
                 * Synchronize so we do not do the same work for each concurrent
                 * request on a service start.
                 */

                synchronized(hostTable) {
                
                    // Ensure that the host table exists.
                    updateHostTable();
                    
                }

            }

        }

        /**
         * Overridden to also update the hosts table in case we add/remove a
         * service and the set of hosts that cover the member services is
         * changed as a result.
         */
        @Override
        protected void updateServiceTable() {
            
            super.updateServiceTable();
            
            updateHostTable();
            
        }
        
        /**
         * Update the per-host scoring table.
         * 
         * @see #hostTable
         * 
         *      TODO For scalability on clusters with a lot of ganglia chatter,
         *      we should only keep the data from those hosts that are of
         *      interest for a given HA replication cluster. The load on other
         *      hosts has no impact on our decision when load balancing within
         *      an HA replication cluster. This means checking each host in the
         *      loop below to see whether there is a service that is a member of
         *      the Quorum on that host and skipping the host when there is no
         *      such service. We should pre-filter the hosts before doing
         *      anything else so #hosts is not greater than #of quorum members.
         * 
         *      TODO We might need to compile a single array of data structures
         *      that link the service scores with the host scores so we can
         *      update them all atomically.
         */
        private void updateHostTable() {

            // Snapshot of the per service scores.
            final HAGlueScore[] serviceScores = serviceTable.get();

            if (serviceScores == null || serviceScores.length == 0) {

                /*
                 * If there are no joined services then we do not have any host
                 * scores for those services.
                 */
                hostTable.set(null);

                return;
                
            }

            // Obtain the host reports for those services.
            final IHostReport[] hostReport = getHostReportForKnownServices(serviceScores);

            /*
             * Compute the per-host scores and the total score across those
             * hosts.
             */

            final HostScore[] scores = new HostScore[hostReport.length];
            
            final double[] hostScores = new double[hostReport.length];

            double totalScore = 0d;
            
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
             * Note: This needs to be done only for those hosts that are
             * associated with the quorum members. If we do it for the other
             * hosts then the normalization is not meaningful. That is why we
             * first filter for only those hosts that are running services
             * associated with this quorum.
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
                    hostScore = new HostScore(hostname, hostScores[i],
                            totalScore);
                    
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

            this.hostTable.set(scores);

            if (thisHostScore != null) {
             
                this.thisHostScore.set(thisHostScore);
                
            }

        }

        /**
         * Return an {@link IHostReport} for each host that is known to be
         * running an {@link HAGlueScore} associated with the same quorum as
         * this service.
         * <p>
         * Note: If there is more than one service on the same host, then we
         * will have one record per host, not per service.
         * 
         * Note: The actual metrics that are available depend on the OS and on
         * whether you are running gmond or having the GangliaPlugIn do its own
         * reporting. The policy that ranks the host reports should be robust to
         * these variations.
         * 
         * @return A dense array of {@link IHostReport}s for our services.
         */
        private IHostReport[] getHostReportForKnownServices(
                final HAGlueScore[] serviceScores) {

            /*
             * TODO Might be less work to request for just the hosts for the
             * services that we are tracking, especially for larger clusters.
             */
            
            final IHostReport[] hostReport = gangliaService.getHostReport(//
                    reportOn,// metrics to be reported.
                    comparator// imposes order on the host reports (which we ignore). 
                    );

            final List<IHostReport> hostReportList = new LinkedList<IHostReport>();
            
            int nhostsWithService = 0;

            for (int i = 0; i < hostReport.length; i++) {

                final IHostReport theHostReport = hostReport[i];

                boolean foundServiceOnHost = false;
                
                for (int j = 0; j < serviceScores.length; j++) {

                    HAGlueScore theServiceScore = serviceScores[j];

                    if (theHostReport.getHostName().equals(
                            theServiceScore.hostname)) {

                        // Found service on this host.
                        foundServiceOnHost = true;
                        
                        break;
                        
                    }
                    
                }

                if(!foundServiceOnHost) {

                    // Ignore host if not running our service.
                    continue;
                    
                }

                hostReportList.add(theHostReport);
                
                nhostsWithService++; // one more host with at least one service.
                
            }

            log.warn("hostReport=" + hostReportList);
            
            // A dense list of host reports for our services.
            return hostReportList.toArray(new IHostReport[nhostsWithService]);

        }

        @Override
        public String getReaderURL(final HttpServletRequest req) {
            
            final HostScore[] scores = this.hostTable.get();
            
            if (scores == null || scores.length == 0) {

                // Can't do anything: TODO Should forward locally.
                return null;

            }
            
            /*
             * (1) If this host is lightly loaded and it is joined with the met
             * quorum, then just execute the query locally. This bias helps us
             * to avoid proxying when we can forward the query instead.
             * 
             * TODO How do we identify *this* host? The fully qualified host
             * name? Is this the same as the host name that is being reported by
             * ganglia? It would have to be. [ Pre-resolve this and then mark
             * the HostScore instance as local or not it is this host when it is
             * generated. Save off the reference to the local host score so we
             * can check it without a scan. ]
             */
            final HostScore thisHostScore = this.thisHostScore.get();

            if (thisHostScore != null && true) {

                if (thisHostScore.score < .5) { // TODO CONFIGURE THRESHOLD.
                    /*
                     * FIXME We should just forward the request to this host to
                     * avoid the overhead of proxying the request.
                     */
                }                

//                // Best effort search for the host score for this host.
//                HostScore thisHost = null;
//                for (int i = 0; i < scores.length; i++) {
//                    if (scores[i].hostname
//                            .equals(AbstractStatisticsCollector.fullyQualifiedHostName)) {
//                        thisHost = scores[i];
//                        break;
//                    }
//                }
                
            }

            /*
             * FIXME (2) Stochastically select the target host based on the
             * current load and proxy the request. We need to ignore any host
             * that is not joined with the met quorum....
             */
            
            throw new UnsupportedOperationException();

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

        @Override
        public String toString() {
            return getClass().getName() + "{serviceUUID=" + serviceUUID
                    + ", hostname=" + hostname + ", port=" + port
                    + ", requestURL=" + requestURL + "}";
        }

        public HAGlueScore(final IIndexManager indexManager,
                final String contextPath, final UUID serviceUUID) {

            if (serviceUUID == null)
                throw new IllegalArgumentException();

            this.serviceUUID = serviceUUID;
            
            final HAJournal journal = (HAJournal) indexManager;

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
        
        /**
         * <code>true</code> iff the host is this host.
         */
        private final boolean thisHost;
        
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

            return "HostScore{hostname=" + hostname + ", thisHost=" + thisHost
                    + ", rawScore=" + rawScore + ", score=" + score + ", rank="
                    + rank + ", drank=" + drank + "}";

        }

        public HostScore(final String hostname, final double rawScore,
                final double totalRawScore) {

            if (hostname == null)
                throw new IllegalArgumentException();

            if (hostname.trim().length() == 0)
                throw new IllegalArgumentException();
            
            this.hostname = hostname;

            this.rawScore = rawScore;

            this.thisHost = AbstractStatisticsCollector.fullyQualifiedHostName
                    .equals(hostname);
            
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

//    /**
//     * Places {@link HostScore} into ascending order (lowest score to highest
//     * score). Ties are broken based on an alpha sort of the index name.
//     */
//    static private class ASC implements Comparator<HostScore> {
//
//        @Override
//        public int compare(final HostScore arg0, final HostScore arg1) {
//
//            if (arg0.rawScore < arg1.rawScore) {
//
//                return -1;
//
//            } else if (arg0.rawScore > arg1.rawScore) {
//
//                return 1;
//
//            }
//
//            return arg0.hostname.compareTo(arg1.hostname);
//
//        }
//
//    }
//
//    /**
//     * Places {@link HostScore} into descending order (highest score to lowest
//     * score). Ties are broken based on an alpha sort of the index name.
//     */
//    static private class DESC implements Comparator<HostScore> {
//
//        @Override
//        public int compare(final HostScore arg0, final HostScore arg1) {
//
//            if (arg1.rawScore < arg0.rawScore) {
//
//                return -1;
//
//            } else if (arg1.rawScore > arg0.rawScore) {
//
//                return 1;
//
//            }
//
//            return arg0.hostname.compareTo(arg1.hostname);
//
//        }
//
//    }

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
