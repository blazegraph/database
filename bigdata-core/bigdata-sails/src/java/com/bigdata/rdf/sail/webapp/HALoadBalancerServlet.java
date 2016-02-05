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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.AsyncContext;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.proxy.ProxyServlet;

import com.bigdata.BigdataStatics;
import com.bigdata.counters.CAT;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
//import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.rdf.sail.webapp.lbs.DefaultHARequestURIRewriter;
import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;
import com.bigdata.rdf.sail.webapp.lbs.IHAPolicyLifeCycle;
import com.bigdata.rdf.sail.webapp.lbs.IHARequestURIRewriter;
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/624"> HA Load Balancer </a>
 */
public class HALoadBalancerServlet extends ProxyServlet {

    private static final Logger log = Logger
            .getLogger(HALoadBalancerServlet.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface InitParams {

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

        /**
         * The fully qualified class name of an {@link IHARequestURIRewriter}
         * (optional - the default is {@value #DEFAULT_REWRITER}). This must be
         * an instance of {@link IHARequestURIRewriter}. This may be used to
         * impose application specific Request-URI rewrite semantics when a
         * request will be proxied to another service.
         */
        String REWRITER = "rewriter";

        String DEFAULT_REWRITER = DefaultHARequestURIRewriter.class.getName();

    }
    
    /*
     * Static declarations of some common exceptions to reduce overhead
     * associated with filling in the stack traces.
     */

    /**
     * There is no {@link IHALoadBalancerPolicy} currently declared.
     */
    private static final RuntimeException CAUSE_NO_LBS_POLICY = new RuntimeException(
            "No LBS policy");

    /**
     * The <code>Request-URI</code> does not map to either {@link #PATH_LEADER}
     * or {@link #PATH_READ}.
     */
    private static final RuntimeException CAUSE_BAD_REQUEST_URI = new RuntimeException(
            "Bad Request-URI");

    /**
     * There is no {@link IHARequestURIRewriter} currently declared.
     */
    private static final RuntimeException CAUSE_NO_REWRITER_POLICY = new RuntimeException(
            "No rewriter policy");

    /**
     * The destination could not be validated.
     * 
     * @see #validateDestination(String, int)
     */
    private static final RuntimeException CAUSE_DESTINATION_NOT_VALID = new RuntimeException(
            "Could not validate destination");

    public HALoadBalancerServlet() {

        super();
        
    }

    /**
     * The initial prefix and is formed as
     * 
     * <pre>
     * Context - Path / LBS
     * </pre>
     * <p>
     * Note: This is set by {@link #init()}. It must not be <code>null</code>.
     * The load balancer relies on the prefix to rewrite the Request-URI: (a)
     * when it is disabled (the request will be forwarded to a local service;
     * and (b) when the request is proxied to a remote service.
     */
    private String prefix;

    /**
     * The URI path component that follows the servlet context-path to identify
     * a request that will be handled by the load balancer component.
     */
    private static final String PATH_LBS = "/LBS";
    
    /**
     * The URI path component that follows the {@link #prefix} to identify a
     * request that will target the quorum leader.
     */
    private static final String PATH_LEADER = "/leader";
    
    /**
     * The URI path component that follows the {@link #prefix} to identify a
     * request that should be load balanced over the leader + followers.
     */
    private static final String PATH_READ = "/read";
    
    /**
     * The configured {@link IHALoadBalancerPolicy} and <code>null</code> iff
     * the load balancer is disabled. If the LBS is not enabled, then it will
     * strip its prefix from the URL requestURI and do a servlet forward to the
     * resulting requestURI. This allows the webapp to start even if the LBS is
     * not correctly configured.
     * 
     * @see InitParams#POLICY
     */
    private final AtomicReference<IHALoadBalancerPolicy> policyRef = new AtomicReference<IHALoadBalancerPolicy>();
    
    /**
     * The {@link IHARequestURIRewriter} that rewrites the original Request-URI
     * into a Request-URI for the target service to which the request will be
     * proxied.
     * 
     * @see InitParams#REWRITER
     */
    private final AtomicReference<IHARequestURIRewriter> rewriterRef = new AtomicReference<IHARequestURIRewriter>();
    
    /**
     * The number of requests that were forwarded to the local service.
     */
    private final CAT nforward = new CAT();
    
    /**
     * The number of requests that were proxied some service.
     */
    private final CAT nproxy = new CAT();
    
    /**
     * The number of requests for which {@link #rewriteURI(HttpServletRequest)}
     * trapped an error.
     * <p>
     * Note: Such requests wind up being forwarded to the local service.
     */
    private final CAT nerror = new CAT();

    /**
     * Change the {@link IHALoadBalancerPolicy} associated with this instance of
     * this servlet. The new policy will be installed iff it can be initialized
     * successfully. The old policy will be destroyed iff the new policy is
     * successfully installed.
     * 
     * @param newValue
     *            The new value (required).
     */
    public void setLBSPolicy(final IHALoadBalancerPolicy newValue) {

        if (newValue == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("newValue=" + newValue);
        
        setHAPolicy(newValue, policyRef);
       
    }
    
    /**
     * Return the current {@link IHALoadBalancerPolicy}.
     */
    public IHALoadBalancerPolicy getLBSPolicy() {
        
        return policyRef.get();
        
    }
    
    /**
     * Change the {@link IHARequestURIRewriter} associated with this instance of
     * this servlet. The new policy will be installed iff it can be initialized
     * successfully. The old policy will be destroyed iff the new policy is
     * successfully installed.
     * 
     * @param newValue
     *            The new value (required).
     */
    public void setRewriter(final IHARequestURIRewriter newValue) {

        if (newValue == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("newValue=" + newValue);

        setHAPolicy(newValue, rewriterRef);
        
    }

    /**
     * Change the {@link IHAPolicyLifeCycle} associated with this instance of
     * this servlet. The new policy will be installed iff it can be initialized
     * successfully. The old policy will be destroyed iff the new policy is
     * successfully installed.
     * 
     * @param newValue
     *            The new value (required).
     * @param ref
     *            The {@link AtomicReference} object that holds the current
     *            value of the policy.
     * 
     *            TODO Since we are allowing programatic change of the policy,
     *            it would be a good idea to make that change atomic with
     *            respect to any specific request and to make the destroy of the
     *            policy something that occurs once any in flight request has
     *            been handled (there is more than one place where the policy is
     *            checked in the code). The atomic change might be accomplished
     *            by attaching the policy to the request as an attribute. The
     *            destroy could be achieved by reference counts for the #of in
     *            flight requests flowing through a policy. The request
     *            attribute and reference count could be handled together
     *            through handshaking with the policy when attaching it as a
     *            request attribute in
     *            {@link #service(HttpServletRequest, HttpServletResponse)}.
     */
    private <T extends IHAPolicyLifeCycle> void setHAPolicy(final T newValue,
            final AtomicReference<T> ref) {
        
        final ServletConfig servletConfig = getServletConfig();

        final ServletContext servletContext = servletConfig.getServletContext();

        final IIndexManager indexManager = BigdataServlet
                .getIndexManager(servletContext);

        if (! ((AbstractJournal) indexManager).isHAJournal()) {
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
        final T oldValue = ref.getAndSet(newValue);

        if (oldValue != null && oldValue != newValue) {

            // TODO Should await a zero reference count on the policy.
            oldValue.destroy();
            
        }

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to setup the as-configured {@link IHALoadBalancerPolicy}.
     * <p>
     * Note: If the deployment is does not support HA replication (e.g., either
     * not HA or HA with replicationFactor:=1), then we still want to be able to
     * forward to the local service.
     * 
     * @throws ServletException
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/965" > Cannot run queries in
     *      LBS mode with HA1 setup </a>
     */
    @Override
    public void init() throws ServletException {

        super.init();
        
        final ServletConfig servletConfig = getServletConfig();

//        // Get the as-configured prefix to be stripped from requests.
//        prefix = servletConfig.getInitParameter(InitParams.PREFIX);
        prefix = BigdataStatics.getContextPath() + PATH_LBS;
        
        final ServletContext servletContext = servletConfig.getServletContext();

        final IIndexManager indexManager = BigdataServlet
                .getIndexManager(servletContext);

        if (((AbstractJournal) indexManager).isHAJournal()
                && ((AbstractJournal) indexManager).getQuorum() != null
                && ((AbstractJournal) indexManager).getQuorum()
                        .replicationFactor() > 1) {

            {
                // Get the as-configured policy.
                final IHALoadBalancerPolicy policy = newInstance(//
                        servletConfig, //
                        HALoadBalancerServlet.class,// owningClass
                        IHALoadBalancerPolicy.class,//
                        InitParams.POLICY, InitParams.DEFAULT_POLICY);

                // Set the as-configured policy.
                setLBSPolicy(policy);

            }
            {

                final IHARequestURIRewriter rewriter = newInstance(//
                        servletConfig,//
                        HALoadBalancerServlet.class, // owningClass
                        IHARequestURIRewriter.class,//
                        InitParams.REWRITER, InitParams.DEFAULT_REWRITER);

                setRewriter(rewriter);

            }

        }

        servletContext.setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                prefix);

        addServlet(servletContext, this/*servlet*/);
        
        if (log.isInfoEnabled())
            log.info(servletConfig.getServletName() + " @ " + prefix
                    + " :: policy=" + policyRef.get() + ", rewriter="
                    + rewriterRef.get());

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

    /**
     * Set the current {@link IHALoadBalancerPolicy} for all
     * {@link HALoadBalancerServlet} instances for the caller specified
     * {@link ServletContext}.
     * 
     * @param servletContext
     *            The {@link ServletContext}.
     * @param newValue
     *            The new {@link IHALoadBalancerPolicy}.
     *            
     * @throws IllegalArgumentException
     *             if the new policy is <code>null</code>.
     */
    public static void setLBSPolicy(final ServletContext servletContext,
            final IHALoadBalancerPolicy newValue) {

        if (newValue == null)
            throw new IllegalArgumentException();
        
        final HALoadBalancerServlet[] servlets = getServlets(servletContext);

        if (servlets == null || servlets.length == 0) {
         
            // None running.
            return;

        }

        for (HALoadBalancerServlet servlet : servlets) {

            servlet.setLBSPolicy(newValue);

        }

    }

    /**
     * Return the {@link IHALoadBalancerPolicy}s that are in force for the
     * active {@link HALoadBalancerServlet} instances.
     * 
     * @param servletContext
     *            The {@link ServletContext}.
     *            
     * @return The {@link IHALoadBalancerPolicy}[] -or- <code>null</code> if
     *         there are no {@link HALoadBalancerServlet}s.
     */
    public static String toString(final ServletContext servletContext) {

        final HALoadBalancerServlet[] servlets = getServlets(servletContext);

        if (servlets == null || servlets.length == 0) {
         
            // None running.
            return "Not running";

        }

        final StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < servlets.length; i++) {

            sb.append(servlets[i].toString());

        }
        
        return sb.toString();
        
    }

    @Override
    public String toString() {
        
        return super.toString()//
                + "{prefix=" + prefix//
                + ",policy=" + policyRef.get()//
                + ",rewriter=" + rewriterRef.get()//
                + ",nforward=" + nforward.estimate_get()//
                + ",nproxy=" + nproxy.estimate_get()//
                + ",nerror=" + nerror.estimate_get()//
                + "}"//
        ;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to destroy the as-configured {@link IHALoadBalancerPolicy}.
     */
    @Override
    public void destroy() {

        removeServlet(getServletContext(), this/* servlet */);

        {

            final IHALoadBalancerPolicy policy = policyRef
                    .getAndSet(null/* newValue */);

            if (policy != null) {

                policy.destroy();

            }

        }

        {

            final IHARequestURIRewriter rewriter = rewriterRef
                    .getAndSet(null/* newValue */);

            if (rewriter != null) {

                rewriter.destroy();

            }

        }

        prefix = null;

        getServletContext().setAttribute(BigdataServlet.ATTRIBUTE_LBS_PREFIX,
                null);

        super.destroy();
        
    }

    /**
     * Return the configured value of the named parameter. This method checks
     * the environment variables first for a fully qualified value for the
     * parameter using <i>owningClass</i>.<i>name</i> (or just <i>name</i> if
     * <i>owningClass</i> is <code>null</code>). If no value is found for that
     * variable, it checks the {@link ServletContext} for <i>name</i> (if the
     * owningClass:=HALoadBalancerServlet) and otherwise for
     * <i>owningClass.name</i>. If no value is found again, it returns the
     * default value specified by the caller. This makes it possible to
     * configure the behavior of the {@link HALoadBalancerServlet} using
     * environment variables.
     * 
     * @param servletConfig
     *            The {@link ServletConfig}.
     * @param owningClass
     *            The class that declares the init-param (required). This serves
     *            as a namespace when searching the environment variables. This
     *            is also used to impose a namespace when searching
     *            <code>web.xml</code> when
     *            <code>owningClass!=HALoadBalancerServlet</code>.
     * @param name
     *            The name of the servlet <code>init-param</code>.
     * @param def
     *            The default value for the servlet <code>init-param</code>.
     * @return
     */
    public static String getConfigParam(final ServletConfig servletConfig,
            final Class<?> owningClass, final String name, final String def) {

        // Look at environment variables for an override.
        String s = System.getProperty(owningClass.getName() + "." + name);

        if (s == null || s.trim().length() == 0) {

            // Look at ServletConfig for the configured value.
            if (owningClass == HALoadBalancerServlet.class) {

                /*
                 * The HALoadBalancerServlet does not use a namespace prefix for
                 * its web.xml declarations.
                 */
                s = servletConfig.getInitParameter(name);
                
            } else {
                
                /*
                 * The other policy objects DO use their owningClass as a
                 * namespace prefix. This is done to avoid collisions among the
                 * different policy classes.
                 */
                s = servletConfig.getInitParameter(owningClass.getName() + "."
                        + name);

            }

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
     * @param owningClass
     *            The class that declares the init-param. This serves as
     *            a namespace when searching the environment variables.
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
            final Class<?> owningClass,
            final Class<? extends T> iface, final String name, final String def)
            throws ServletException {

        final String s = getConfigParam(servletConfig, owningClass, name, def);

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
            forwardToLocalService(isLeaderRequest,request, response);
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
         * TODO This does too much work if the request is for the leader and
         * this service is not the leader. Look at it again under a debugger
         * and optimize the code paths.
         */
        if (policy.service(isLeaderRequest, this, request, response)) {

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
     * <p>
     * Note: If the deployment is does not support HA replication (e.g., either
     * not HA or HA with replicationFactor:=1), then we still want to be able to
     * forward to the local service.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     * 
     * @throws IOException
     * @throws ServletException
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/965" > Cannot run queries in
     *      LBS mode with HA1 setup </a>
     */
    public void forwardToLocalService(//
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

        // Get dispatched for the new requestURL path.
        final RequestDispatcher requestDispatcher = request
                .getRequestDispatcher(newPath);

        try {

            nforward.increment();
            
            if (log.isInfoEnabled())
                log.info("forward: " + path + " => " + newPath);

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
     * 
     *         TODO This may need to be configurable.
     */
    private String getFullPrefix(final boolean isLeaderRequest,
            final String prefix) {

        final String full_prefix = isLeaderRequest ? prefix + PATH_LEADER
                : prefix + PATH_READ;

        return full_prefix;

    }

    /**
     * Wrapper invokes {@link #doRewriteURI(HttpServletRequest)} and handles
     * any thrown exceptions.
     * 
     * @see #doRewriteURI(HttpServletRequest)
     */
    @Override
    final protected URI rewriteURI(final HttpServletRequest request) {

        try {

            final URI rewritten = doRewriteURI(request);

            if (rewritten != null) {

                // Track #of requests that are proxied.
                nproxy.increment();
                
            }
            
            return rewritten;
            
        } catch (Throwable t) {

            /*
             * Could not rewrite.
             */

            if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                throw new RuntimeException(t);
                
            }

            nerror.increment();
            
            if (log.isDebugEnabled()) {
                // full stack trace.
                log.warn(t, t);
            } else {
                // just the message.
                log.warn(t);
            }

            /*
             * Could not rewrite.
             * 
             * Return [null]. This will cause the onRewriteFailed() to be
             * invoked. That will do a local forward. If the request can not be
             * handled locally, then the local service will generate an error
             * message.
             * 
             * Note: This pattern means that we do not throw errors arising from
             * the decision to rewrite the request. They are logger (above) and
             * then the local forwarding logic is applied. This can mask some
             * errors since they will only appear in the log. However, this does
             * make the rewrite logic more robust.
             */
  
            return null;

        }

    }
    
    /**
     * Hook allows the servlet to rewrite the request.
     * 
     * For update requests, rewrite the requestURL to the service that is the
     * quorum leader. For read requests, rewrite the requestURL to the service
     * having the least load.
     * 
     * @param request
     *            The request.
     *            
     * @return Return the {@link URI} if the request should be proxied to
     *         another service -or- <code>null</code> if the request should be
     *         locally forwarded.
     */
    protected URI doRewriteURI(final HttpServletRequest request) {
        
        final IHALoadBalancerPolicy policy = policyRef.get();

        if (policy == null) {
            // No policy. Can not rewrite.
            throw CAUSE_NO_LBS_POLICY;
        }
        
        final String originalRequestURI = request.getRequestURI();
        
        if (!originalRequestURI.startsWith(prefix)) {
            // Request is not for this servlet.
            throw CAUSE_BAD_REQUEST_URI;
        }

        final Boolean isLeaderRequest = isLeaderRequest(request);
        
        if (isLeaderRequest == null) {
            // Neither /LBS/leader -nor- /LBS/read.
            throw CAUSE_BAD_REQUEST_URI;
        }

        final String proxyToRequestURI;
        {

            if (isLeaderRequest) {
                // Proxy to leader.
                proxyToRequestURI = policy.getLeaderURI(request);
            } else {
                // Proxy to any joined service.
                proxyToRequestURI = policy.getReaderURI(request);
            }

            if (log.isDebugEnabled())
                log.debug("proxyToRequestURI=" + proxyToRequestURI);

            if (proxyToRequestURI == null) {
                /*
                 * The LBS policy made a choice not to rewrite this request (not
                 * an error, but a deliberate choice).
                 */
                return null;
            }

        }

        // The full LBS prefix (includes /leader or /read).
        final String full_prefix = getFullPrefix(isLeaderRequest, prefix);

        // The configured Request-URL rewriter.
        final IHARequestURIRewriter rewriter = rewriterRef.get();

        if (rewriter == null) {
            throw CAUSE_NO_REWRITER_POLICY;
        }
        
        // Re-write requestURL.  
        final StringBuilder uri = rewriter.rewriteURI(//
                isLeaderRequest,// iff request for the leader
                full_prefix, //
                originalRequestURI,// old
                proxyToRequestURI, // new
                request // request
                );

        // Normalize the request.
        final URI rewrittenURI = URI.create(uri.toString()).normalize();

        if (!validateDestination(rewrittenURI.getHost(), rewrittenURI.getPort())) {
            throw CAUSE_DESTINATION_NOT_VALID;
        }
        
        if (log.isInfoEnabled())
            log.info("rewrote: " + originalRequestURI + " => " + rewrittenURI);
        
        return rewrittenURI;
        
    }

    /**
     * Note: This offers an opportunity to handle a failure where we were unable
     * to rewrite the request to some service, e.g., because the quorum is not
     * met. The implementation is overridden to forward the request to the local
     * service. The local service will then generate an appropriate HTTP error
     * response.
     */
    @Override
    protected void onRewriteFailed(final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        if (log.isInfoEnabled())
            log.info("Could not rewrite: request=" + request,
                    new StackInfoReport());
        
        // Figure out if request is to leader or reader.
        final Boolean isLeaderRequest = isLeaderRequest(request);
        
        if (isLeaderRequest == null) {
         
            // Could not identify request based on known prefixes (leader|read).
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Unknown prefix: requestURL=" + request.getRequestURL());
            return;
            
        }
        
        // Forward to this service (let it generate an error message).
        forwardToLocalService(isLeaderRequest, request, response);
        
//        response.sendError(HttpServletResponse.SC_FORBIDDEN);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to provide more information about the error. The
     * implementation is derived from the jetty 9.1.4 implementation of the
     * method in the base {@link ProxyServlet} class, but logs @ ERROR so we can
     * see more about the underlying problem.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/941" > HA LBS Gateway errors
     *      under heavy load </a>
     * 
     *      TODO jetty 9.2 provides a fully asynchronous proxy servlet. We will
     *      wind up replacing our base class with that implementation soon,
     *      probably for the 1.3.2 release. Until then, this will provide
     *      additional diagnoistic information about the root causes when there
     *      is a gateway error (proxying fails). If we can find some patterns to
     *      these failures, then it would be useful to recharacterize more of
     *      them to encourage the client to retry the request. Those semantics
     *      are not really available for 502 (Bad Gateway). They are more a
     *      appropriate for both 503 (Service Unavailable - temporary overload),
     *      and 504 (Gateway Timeout). 503 might be the best choice if there is
     *      not an explicit timeout and the root cause does not clearly indicate
     *      a durable problem with the target host.
     */
    @Override
    protected void onResponseFailure(//
            final HttpServletRequest request,//
            final HttpServletResponse response,//
            final Response proxyResponse,//
            final Throwable failure) {
        
        log.error(getRequestId(request) + " proxying failed: " + request, failure);
        if (!response.isCommitted())
        {
            if (failure instanceof TimeoutException)
                response.setStatus(HttpServletResponse.SC_GATEWAY_TIMEOUT);
            else
                response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
        }
        
        AsyncContext asyncContext = request.getAsyncContext();
        asyncContext.complete();
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

        if (rest.startsWith(PATH_LEADER)) {

            return Boolean.TRUE;

        }

        if (rest.startsWith(PATH_READ)) {

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
