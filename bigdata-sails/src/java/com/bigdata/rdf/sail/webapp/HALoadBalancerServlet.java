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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.proxy.ProxyServlet;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.GangliaPlugIn;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.PlatformStatsPlugIn;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.rdf.sail.webapp.lbs.policy.NOPLBSPolicy;
import com.bigdata.util.InnerCause;
import com.bigdata.util.StackInfoReport;

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

        /*
         * Note: /bigdata/LBS is now a base prefix. There are fully qualified
         * prefix values of /bigdata/LBS/leader and /bigdata/LBS/read. This is
         * why the PREFIX init-param has been commented out.  It no longer maps
         * directly into a simple concept within the LBS servlet.
         */
//        /**
//         * The prefix at which the load balancer is deployed (its URL pattern,
//         * less any wildcard). This is typically
//         * 
//         * <pre>
//         * /bigdata/LBS
//         * </pre>
//         * 
//         * but the actual value depends on the servlet mappined established in
//         * <code>web.xml</code>.
//         */
//        String PREFIX = "prefix";
        
        /**
         * The fully qualified class name of the load balancer policy (optional
         * - the default is {@value #DEFAULT_POLICY}). This must be an instance
         * of {@link IHALoadBalancerPolicy} .
         */
        String POLICY = "policy";
        
        /**
         * The default {@link IHALoadBalancerPolicy} proxies all updates to the
         * quorum leader and forwards all other requests to the local service.
         */
        String DEFAULT_POLICY = NOPLBSPolicy.class.getName();

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
     * The initial prefix that will be stripped off by the load balancer.
     * <p>
     * Note: This is set by {@link #init()}. It must not be <code>null</code>.
     * The load balancer relies on the prefix to rewrite the requestURL when the
     * {@link IHALoadBalancerPolicy} is disabled in order to forward the request
     * to the local service.
     */
    private String prefix;

    /**
     * The configured {@link IHALoadBalancerPolicy} and <code>null</code> iff
     * the load balancer is disabled. If the LBS is not enabled, then it will
     * strip its prefix from the URL requestURI and do a servlet forward to the
     * resulting requestURI. This allows the webapp to start even if the LBS is
     * not correctly configured.
     * 
     * TODO Since we are allowing programatic change of the policy, it would be
     * a good idea to make that change atomic with respect to any specific
     * request and to make the destroy of the policy something that occurs once
     * any in flight request has been handled (there is more than one place
     * where the policy is checked in the code). The atomic change might be
     * accomplished by attaching the policy to the request as an attribute. The
     * destroy could be achieved by reference counts for the #of in flight
     * requests flowing through a policy. The request attribute and reference
     * count could be handled together through handshaking with the policy when
     * attaching it as a request attribute in
     * {@link #service(HttpServletRequest, HttpServletResponse)}.
     */
    private final AtomicReference<IHALoadBalancerPolicy> policyRef = new AtomicReference<IHALoadBalancerPolicy>();
    
    /**
     * Change the {@link IHALoadBalancerPolicy} associated with this instance of
     * this servlet. The new policy will be installed iff it can be initialized
     * successfully. The old policy will be destroyed iff the new policy is
     * successfully installed.
     * 
     * @param newValue
     *            The new value (required).
     */
    public void setPolicy(final IHALoadBalancerPolicy newValue) {

        if (newValue == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("newValue=" + newValue);

        final ServletConfig servletConfig = getServletConfig();

        final ServletContext servletContext = servletConfig.getServletContext();

        final IIndexManager indexManager = BigdataServlet
                .getIndexManager(servletContext);

        if (!(indexManager instanceof HAJournal)) {
            // This is not an error, but the LBS is only for HA.
            log.warn("Not HA");
            return;
        }

        try {

            // Attempt to provision the specified LBS policy.
            newValue.init(servletConfig, indexManager);

        } catch (Throwable t) {

            /*
             * The specified LBS policy could not be provisioned.
             */

            if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                // Interrupted.
                return;
            }

            log.error("Could not setup policy: " + newValue, t);

            try {
                newValue.destroy();
            } catch (Throwable t2) {
                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                    // Interrupted.
                    return;
                }
                log.warn("Problem destroying policy: " + newValue, t2);
            }
            
            // The new policy will not be installed.
            return;
        }

        // Install the new policy.
        final IHALoadBalancerPolicy oldValue = this.policyRef
                .getAndSet(newValue);

        if (oldValue != null && oldValue != newValue) {

            // TODO Should await a zero reference count on the policy.
            oldValue.destroy();
            
        }
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to setup the as-configured {@link IHALoadBalancerPolicy}.
     * 
     * @throws ServletException
     */
    @Override
    public void init() throws ServletException {

        super.init();
        
        final ServletConfig servletConfig = getServletConfig();

//        // Get the as-configured prefix to be stripped from requests.
//        prefix = servletConfig.getInitParameter(InitParams.PREFIX);
        prefix = BigdataStatics.getContextPath() + "/LBS";
        
        final ServletContext servletContext = servletConfig.getServletContext();

        final IIndexManager indexManager = BigdataServlet
                .getIndexManager(servletContext);

        if (!(indexManager instanceof HAJournal)) {
            // This is not an error, but the LBS is only for HA.
            log.warn("Not HA");
            return;
        }

        /*
         * Setup a fall back policy. This policy will strip off the configured
         * prefix from the requestURL and forward the request to the local
         * service.  If we can not establish the as-configured policy, then 
         * the servlet will run with this fall back policy.
         */

        {
            
            final IHALoadBalancerPolicy defaultPolicy = new NOPLBSPolicy();
            
            // Initialize the fallback policy.
            defaultPolicy.init(servletConfig, indexManager);

            policyRef.set(defaultPolicy);

        }

        // Get the as-configured policy.
        IHALoadBalancerPolicy policy = newInstance(servletConfig,
                IHALoadBalancerPolicy.class, InitParams.POLICY,
                InitParams.DEFAULT_POLICY);

        // Set the as-configured policy.
        setPolicy(policy);
        
        servletContext.setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                prefix);

        addServlet(servletContext, this/*servlet*/);
        
        if (log.isInfoEnabled())
            log.info(servletConfig.getServletName() + " @ " + prefix
                    + " :: policy=" + policy);

    }

    /*
     * Admin API helper classes.
     */
    
    private static void addServlet(final ServletContext servletContext,
            final HALoadBalancerServlet servlet) {

        if (servletContext == null)
            throw new IllegalArgumentException();

        if (servlet == null)
            throw new IllegalArgumentException();

        synchronized(servletContext) {

            @SuppressWarnings("unchecked")
            CopyOnWriteArraySet<HALoadBalancerServlet> servletSet = (CopyOnWriteArraySet<HALoadBalancerServlet>) servletContext
                    .getAttribute(BigdataServlet.ATTRIBUTE_LBS_INSTANCES);

            if (servletSet == null) {

                servletContext
                        .setAttribute(
                                BigdataServlet.ATTRIBUTE_LBS_INSTANCES,
                                servletSet = new CopyOnWriteArraySet<HALoadBalancerServlet>());

            }
            
            servletSet.add(servlet);
            
        }
        
    }

    private static void removeServlet(final ServletContext servletContext,
            final HALoadBalancerServlet servlet) {

        if (servletContext == null)
            throw new IllegalArgumentException();

        if (servlet == null)
            throw new IllegalArgumentException();

        synchronized (servletContext) {

            @SuppressWarnings("unchecked")
            final CopyOnWriteArraySet<HALoadBalancerServlet> servletSet = (CopyOnWriteArraySet<HALoadBalancerServlet>) servletContext
                    .getAttribute(BigdataServlet.ATTRIBUTE_LBS_INSTANCES);

            if (servletSet != null) {

                servletSet.remove(servlet);

            }
            
        }
        
    }

    private static HALoadBalancerServlet[] getServlets(
            final ServletContext servletContext) {

        if (servletContext == null)
            throw new IllegalArgumentException();

        synchronized (servletContext) {

            @SuppressWarnings("unchecked")
            final CopyOnWriteArraySet<HALoadBalancerServlet> servletSet = (CopyOnWriteArraySet<HALoadBalancerServlet>) servletContext
                    .getAttribute(BigdataServlet.ATTRIBUTE_LBS_INSTANCES);

            if (servletSet == null)
                return null;

            return servletSet.toArray(new HALoadBalancerServlet[servletSet
                    .size()]);

        }
        
    }

    public static void setPolicy(final ServletContext servletContext,
            final IHALoadBalancerPolicy policy) {

        final HALoadBalancerServlet[] servlets = getServlets(servletContext);

        if (servlets == null || servlets.length == 0) {
         
            // None running.
            return;

        }

        for (HALoadBalancerServlet servlet : servlets) {

            servlet.setPolicy(policy);

        }

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to destroy the as-configured {@link IHALoadBalancerPolicy}.
     */
    @Override
    public void destroy() {

        removeServlet(getServletContext(), this/* servlet */);

        final IHALoadBalancerPolicy policy = policyRef
                .getAndSet(null/* newValue */);

        if (policy != null) {

            policy.destroy();

        }

        prefix = null;

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
    public static String getConfigParam(final ServletConfig servletConfig,
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
    public static <T> T newInstance(final ServletConfig servletConfig,
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

        /*
         * Decide whether this is a read-only request or an update request.
         */
        final Boolean isLeaderRequest = isLeaderRequest(request);

        if (isLeaderRequest == null) {
            /*
             * Neither /LBS/leader -nor- /LBS/read.
             */
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        
        final IHALoadBalancerPolicy policy = policyRef.get();

        if (policy == null) {
            /*
             * LBS is disabled. Strip LBS prefix from the requestURI and forward
             * the request to servlet on this host (NOP LBS).
             */
            forwardToThisService(isLeaderRequest,request, response);
            return;
        }

        /*
         * Delegate to policy. This provides a single point during which the
         * policy can ensure that it is monitoring any necessary information and
         * also provides an opportunity to override the behavior completely. For
         * example, as an optimization, the policy can forward the request to a
         * servlet in this servlet container rather than proxying it to either
         * itself or another service.
         * 
         * FIXME This does too much work if the request is for the leader and
         * this service is not the leader. Look at it again under a debugger.
         */
        if (policy.service(isLeaderRequest, request, response)) {

            // Return immediately if the response was committed.
            return;
            
        }
        
        /*
         * Note: if rewriteURL() returns null, then the base class
         * (ProxyServlet) invokes onRewriteFailed() which is responsible for our
         * error handling policy.
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
    static public void forwardToThisService(//
            final boolean isLeaderRequest,//
            final HttpServletRequest request, //
            final HttpServletResponse response//
    ) throws IOException {

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

        // the full LBS prefix (includes /leader or /read).
        final String full_prefix = getFullPrefix(isLeaderRequest, prefix);

        // what remains after we strip off the full LBS prefix.
        final String rest = path.substring(full_prefix.length());

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

        // Get dispatched for the new requestURL path.
        final RequestDispatcher requestDispatcher = request
                .getRequestDispatcher(newPath);

        try {
            
            // forward to a local servlet.
            requestDispatcher.forward(request, response);
            
        } catch (ServletException e) {
            
            throw new IOException("Could not forward: requestURL=" + path, e);
            
        }

    }

    /**
     * Return the full prefix (including <code>/leader</code> or
     * <code>/read</code> as appropriate).
     * 
     * @param isLeaderRequest
     *            <code>true</code> iff this is a leader request.
     * @param prefix
     *            the base prefix (typically <code>/bigdata/LBS</code>)
     *            
     * @return The full prefix.
     */
    private static String getFullPrefix(final boolean isLeaderRequest,
            final String prefix) {

        final String full_prefix = isLeaderRequest ? prefix + "/leader"
                : prefix + "/read";

        return full_prefix;
        
    }

    /**
     * For update requests, rewrite the requestURL to the service that is the
     * quorum leader. For read requests, rewrite the requestURL to the service
     * having the least load.
     */
    @Override
    protected URI rewriteURI(final HttpServletRequest request) {

        final IHALoadBalancerPolicy policy = policyRef.get();

        if (policy == null) {
            // Could not rewrite.
            return null;
        }
        
        final String path = request.getRequestURI();
        if (!path.startsWith(prefix))
            return null;

        final Boolean isLeaderRequest = isLeaderRequest(request);
        if (isLeaderRequest == null) {
            // Neither /LBS/leader -nor- /LBS/read.
            return null;
        }
        final String proxyTo;
        if(isLeaderRequest) {
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
        // the full LBS prefix (includes /leader or /read).
        final String full_prefix = getFullPrefix(isLeaderRequest, prefix);
        final String rest = path.substring(full_prefix.length());
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

        if (log.isInfoEnabled())
            log.info("Could not rewrite: request=" + request,
                    new StackInfoReport());
        
        // Figure out if request is to leader or readers.
        final Boolean isLeaderRequest = isLeaderRequest(request);
        
        if (isLeaderRequest == null) {
         
            // Could not identify request based on known prefixes.
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Unknown prefix: requestURL=" + request.getRequestURL());
            return;
            
        }
        
        // Forward to this service (let it generate an error message).
        forwardToThisService(isLeaderRequest, request, response);
        
//        response.sendError(HttpServletResponse.SC_FORBIDDEN);
        
    }

    /**
     * Return <code>true</code> iff this is an UPDATE request that must be
     * proxied to the quorum leader.
     * <p>
     * Note: {@link HttpServletRequest#getParameter(String)} and friends consume
     * the entire input stream associated with the request and are NOT
     * compatable with asynchronous HTTP. This means that we MUST NOT examine
     * the URL query parameters when making a decision regarding whether the
     * request should be directed to the leader or load balanced over the leader
     * and followers. This code examines the <code>requestURI</code> and makes
     * the decision based solely on whether the request is directed to
     * <code>/LBS/leader</code> or <code>/LBS/read</code>
     * 
     * @return A {@link Boolean} indicating whether the request should be
     *         processed by the leader (True) or by any service that is joined
     *         with the met quorum (false). A <code>null</code> return indicates
     *         that the requestURI is not understood by this service.
     */
    private Boolean isLeaderRequest(final HttpServletRequest request) {

        final String requestURI = request.getRequestURI();

        final int indexLBS = requestURI.indexOf(prefix);

        if (indexLBS == -1) {
            // Not an LBS request
            return null;
        }

        final String rest = requestURI.substring(indexLBS + prefix.length());

        if (rest.startsWith("/leader")) {

            return Boolean.TRUE;

        }

        if (rest.startsWith("/read")) {

            return Boolean.FALSE;

        }

        // requestURI must specify either /leader or /read.
        return null;

//        final boolean isGet = request.getMethod().equalsIgnoreCase("GET");
//
//        if (isGet) {
//
//            // GET is never an UPDATE request.
//            return false;
//        
//        }
//        
//        final String requestURI = request.getRequestURI();
//
//        if (requestURI.endsWith("/sparql")) {
//
//            /*
//             * SPARQL end point.
//             * 
//             * @see QueryServlet#doPost()
//             */
//
//            if ( request.getParameter(QueryServlet.ATTR_QUERY) != null ||
//                 RESTServlet.hasMimeType(request,
//                            BigdataRDFServlet.MIME_SPARQL_QUERY)
//                            ) {
//
//                /*
//                 * QUERY against SPARQL end point using POST for visibility, not
//                 * mutability.
//                 */
//
//                return false; // idempotent query using POST.
//
//            }
//
//            if (request.getParameter(QueryServlet.ATTR_UUID) != null) {
//
//                return false; // UUID request with caching disabled.
//
//            } else if (request.getParameter(QueryServlet.ATTR_ESTCARD) != null) {
//
//                return false; // ESTCARD with caching defeated.
//
//            } else if (request.getParameter(QueryServlet.ATTR_CONTEXTS) != null) {
//
//                // Request for all contexts in the database.
//                return false;
//
//            }
//
//        }
//
//        // Anything else must be proxied to the leader.
//        return true;

    }

}
