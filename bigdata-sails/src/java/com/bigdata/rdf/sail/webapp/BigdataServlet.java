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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.rdf.sail.webapp.client.IMimeTypes;
import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Useful glue for implementing service actions, but does not directly implement
 * any service action/
 */
abstract public class BigdataServlet extends HttpServlet implements IMimeTypes {
	
	/**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger.getLogger(BigdataServlet.class); 

    /**
     * The name of the {@link ServletContext} attribute whose value is the
     * {@link BigdataRDFContext}.
     */
    static public final transient String ATTRIBUTE_RDF_CONTEXT = BigdataRDFContext.class
            .getName();
    
    /**
     * The name of the {@link ServletContext} attribute whose value is the
     * {@link IIndexManager}.
     */
    /*package*/ static final transient String ATTRIBUTE_INDEX_MANAGER = 
        IIndexManager.class.getName();

    /**
     * The {@link ServletContext} attribute whose value is the prefix for the
     * {@link HALoadBalancerServlet} iff it is running.
     * <p>
     * Note: Do NOT reference the <code>HALoadBalancerServlet</code> here. It
     * will drag in the jetty dependencies and that breaks the tomcat WAR
     * deployment.
     */
    static final String ATTRIBUTE_LBS_PREFIX = "com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.prefix";
    
    /**
     * The {@link ServletContext} attribute that is managed by the
     * {@link HALoadBalancerServlet} and which maintains a collection of the
     * active instances of that servlet. This is used to administer the
     * {@link IHALoadBalancerPolicy} associated with the load balancer servlet
     * instances.
     */
    static final String ATTRIBUTE_LBS_INSTANCES = "com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.instances";
    
//    /**
//     * The {@link ServletContext} attribute whose value is the
//     * {@link SparqlCache}.
//     */
//    /* package */static final transient String ATTRIBUTE_SPARQL_CACHE = SparqlCache.class.getName();

	/**
	 * The character set used for the response (not negotiated).
	 */
    static protected final String charset = "UTF-8";

    protected static final transient String GET = "GET";
    protected static final transient String POST = "POST";
    protected static final transient String PUT = "PUT";
    protected static final transient String DELETE = "DELETE";
    
	/**
	 * Some HTTP response status codes
	 */
	public static final transient int
        HTTP_OK = HttpServletResponse.SC_OK,
//        HTTP_ACCEPTED = HttpServletResponse.SC_ACCEPTED,
//		HTTP_REDIRECT = HttpServletResponse.SC_TEMPORARY_REDIRECT,
//		HTTP_FORBIDDEN = HttpServletResponse.SC_FORBIDDEN,
		HTTP_NOTFOUND = HttpServletResponse.SC_NOT_FOUND,
        HTTP_BADREQUEST = HttpServletResponse.SC_BAD_REQUEST,
        HTTP_METHOD_NOT_ALLOWED = HttpServletResponse.SC_METHOD_NOT_ALLOWED,
		HTTP_INTERNALERROR = HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        HTTP_NOTIMPLEMENTED = HttpServletResponse.SC_NOT_IMPLEMENTED;

    static <T> T getRequiredServletContextAttribute(
            final ServletContext servletContext, final String name) {

        if (servletContext == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();

        @SuppressWarnings("unchecked")
        final T v = (T) servletContext.getAttribute(name);

        if (v == null)
            throw new RuntimeException("Not set: " + name);

        return v;

    }

    static final SparqlEndpointConfig getConfig(
            final ServletContext servletContext) {

        return getBigdataRDFContext(servletContext).getConfig();

    }

    protected final BigdataRDFContext getBigdataRDFContext() {
        
        return getBigdataRDFContext(getServletContext());
        
    }

    static final BigdataRDFContext getBigdataRDFContext(
            final ServletContext servletContext) {

//        if (m_context == null) {
//
//            m_context = 
        return getRequiredServletContextAttribute(servletContext,
                ATTRIBUTE_RDF_CONTEXT);

//        }
//
//        return m_context;

    }

//    private volatile BigdataRDFContext m_context;

    /**
     * The backing {@link IIndexManager}.
     */
    protected IIndexManager getIndexManager() {
        
        return getIndexManager(getServletContext());
        
    }

    /**
     * The backing {@link IIndexManager}.
     */
    static public IIndexManager getIndexManager(final ServletContext servletContext) {

        return getRequiredServletContextAttribute(servletContext,
                ATTRIBUTE_INDEX_MANAGER);

    }

    /**
     * Submit a task and return a {@link Future} for that task. The task will be
     * run on the appropriate executor service depending on the nature of the
     * backing database and the view required by the task.
     * 
     * @param task
     *            The task.
     * 
     * @return The {@link Future} for that task.
     * 
     * @throws DatasetNotFoundException
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
     *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
     * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
     *      Concurrent unisolated operations against multiple KBs </a>
     */
    protected <T> Future<T> submitApiTask(final RestApiTask<T> task)
            throws DatasetNotFoundException {

        final IIndexManager indexManager = getIndexManager();

        return submitApiTask(indexManager, task);
        
    }
    
    /**
     * Submit a task and return a {@link Future} for that task. The task will be
     * run on the appropriate executor service depending on the nature of the
     * backing database and the view required by the task.
     * 
     * @param indexManager
     *            The {@link IndexManager}.
     * @param task
     *            The task.
     * 
     * @return The {@link Future} for that task.
     * 
     * @throws DatasetNotFoundException
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
     *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
     * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
     *      Concurrent unisolated operations against multiple KBs </a>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static protected <T> Future<T> submitApiTask(
            final IIndexManager indexManager, final RestApiTask<T> task)
            throws DatasetNotFoundException {

        final String namespace = task.getNamespace();
        
        final long timestamp = task.getTimestamp();
        
        if (!BigdataStatics.NSS_GROUP_COMMIT || indexManager instanceof IBigdataFederation
                || TimestampUtility.isReadOnly(timestamp)
                ) {

            /*
             * Execute the REST API task.
             * 
             * Note: For scale-out, the operation will be applied using
             * client-side global views of the indices.
             * 
             * Note: This can be used for operations on read-only views (even on
             * a Journal). This is helpful since we can avoid some overhead
             * associated the AbstractTask lock declarations.
             */
            // Wrap Callable.
            final FutureTask<T> ft = new FutureTask<T>(
                    new RestApiTaskForIndexManager(indexManager, task));

            if (true) {

                /*
                 * Caller runs (synchronous execution)
                 * 
                 * Note: By having the caller run the task here we avoid
                 * consuming another thread.
                 */
                ft.run();
                
            } else {
                
                /*
                 * Run on a normal executor service.
                 */
                indexManager.getExecutorService().submit(ft);
                
            }

            return ft;

        } else {

            /**
             * Run on the ConcurrencyManager of the Journal.
             * 
             * Mutation operations will be scheduled based on the pre-declared
             * locks and will have exclusive access to the resources guarded by
             * those locks when they run.
             * 
             * FIXME GROUP COMMIT: The {@link AbstractTask} was written to
             * require the exact set of resource lock declarations. However, for
             * the REST API, we want to operate on all indices associated with a
             * KB instance. This requires either:
             * <p>
             * (a) pre-resolving the names of those indices and passing them all
             * into the AbstractTask; or
             * <P>
             * (b) allowing the caller to only declare the namespace and then to
             * be granted access to all indices whose names are in that
             * namespace.
             * 
             * (b) is now possible with the fix to the Name2Addr prefix scan.
             */

            // Obtain the necessary locks for R/w access to KB indices.
            final String[] locks = getLocksForKB((Journal) indexManager,
                    namespace);

            final IConcurrencyManager cc = ((Journal) indexManager)
                    .getConcurrencyManager();
            
            // Submit task to ConcurrencyManager. Will acquire locks and run.
            return cc.submit(new RestApiTaskForJournal(cc, task.getTimestamp(),
                    locks, task));

        }

    }
    
    /**
     * Acquire the locks for the named indices associated with the specified KB.
     * 
     * @param indexManager
     *            The {@link Journal}.
     * @param namespace
     *            The namespace of the KB instance.
     * 
     * @return The locks for the named indices associated with that KB instance.
     * 
     * @throws DatasetNotFoundException
     * 
     *             FIXME GROUP COMMIT : [This should be replaced by the use of
     *             the namespace and hierarchical locking support in
     *             AbstractTask.] This could fail to discover a recently create
     *             KB between the time when the KB is created and when the group
     *             commit for that create becomes visible. This data race exists
     *             because we are using [lastCommitTime] rather than the
     *             UNISOLATED view of the GRS.
     *             <p>
     *             Note: This data race MIGHT be closed by the default locator
     *             cache. If it records the new KB properties when they are
     *             created, then they should be visible. If they are not
     *             visible, then we have a data race. (But if it records them
     *             before the group commit for the KB create, then the actual KB
     *             indices will not be durable until the that group commit...).
     *             <p>
     *             Note: The problem can obviously be resolved by using the
     *             UNISOLATED index to obtain the KB properties, but that would
     *             serialize ALL updates. What we need is a suitable caching
     *             mechanism that (a) ensures that newly create KB instances are
     *             visible; and (b) has high concurrency for read-only requests
     *             for the properties for those KB instances.
     */
    private static String[] getLocksForKB(final Journal indexManager,
            final String namespace) throws DatasetNotFoundException {

        final long timestamp = indexManager.getLastCommitTime();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null)
            throw new DatasetNotFoundException("Not found: namespace="
                    + namespace + ", timestamp="
                    + TimestampUtility.toString(timestamp));

        final Set<String> lockSet = new HashSet<String>();

        lockSet.addAll(tripleStore.getSPORelation().getIndexNames());

        lockSet.addAll(tripleStore.getLexiconRelation().getIndexNames());

        final String[] locks = lockSet.toArray(new String[lockSet.size()]);

        return locks;

    }
    
//    /**
//     * Return the {@link Quorum} -or- <code>null</code> if the
//     * {@link IIndexManager} is not participating in an HA {@link Quorum}.
//     */
//    protected Quorum<HAGlue, QuorumService<HAGlue>> getQuorum() {
//
//        final IIndexManager indexManager = getIndexManager();
//
//        if (indexManager instanceof Journal) {
//
//            return ((Journal) indexManager).getQuorum();
//
//        }
//
//        return null;
//	    
//	}

    /**
     * Return the {@link HAStatusEnum} -or- <code>null</code> if the
     * {@link IIndexManager} is not an {@link AbstractQuorum} or is not HA
     * enabled.
     */
    static public HAStatusEnum getHAStatus(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();
        
        if (indexManager instanceof AbstractJournal) {

            // Note: Invocation against local object (NOT RMI).
            return ((AbstractJournal) indexManager).getHAStatus();

        }

        return null;
        
    }
    
    /**
     * If the node is not writable, then commit a response and return
     * <code>false</code>. Otherwise return <code>true</code>.
     * 
     * @param req
     * @param resp
     * 
     * @return <code>true</code> iff the node is writable.
     * 
     * @throws IOException
     */
    static boolean isWritable(final ServletContext servletContext,
            final HttpServletRequest req, final HttpServletResponse resp)
            throws IOException {
        
        if (getConfig(servletContext).readOnly) {
            
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    "Not writable.");

            // Not writable.  Response has been committed.
            return false;
            
        }
        final HAStatusEnum haStatus = getHAStatus(getIndexManager(servletContext));
        if (haStatus == null) {
            // No quorum.
            return true;
        }
        switch (haStatus) {
        case Leader:
            return true;
        default:
            log.warn(haStatus.name());
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    haStatus.name());
            // Not writable.  Response has been committed.
            return false;
        }
                
	}
	
    /**
     * If the node is not readable, then commit a response and return
     * <code>false</code>. Otherwise return <code>true</code>.
     * 
     * @param req
     * @param resp
     * 
     * @return <code>true</code> iff the node is readable.
     * 
     * @throws IOException
     */
    static boolean isReadable(final ServletContext ctx,
            final HttpServletRequest req, final HttpServletResponse resp)
            throws IOException {

        final HAStatusEnum haStatus = getHAStatus(getIndexManager(ctx));
        if (haStatus == null) {
            // No quorum.
            return true;
        }
        switch (haStatus) {
        case Leader:
        case Follower:
            return true;
        default:
            // Not ready.
            log.warn(haStatus.name());
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    haStatus.name());
            // Response has been committed.
            return false;
        }

//        final long quorumToken = quorum.token();
//
//        if (!quorum.isQuorumMet()) {
//
//            /*
//             * The quorum is not met.
//             */
//
//            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
//                    "Quorum is not met.");
//
//            return false;
//
//        } else if (!quorum.getClient().isJoinedMember(quorumToken)) {
//
//            /*
//             * The quorum is met, but this service is not joined with the met
//             * quorum.
//             */
//
//            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
//                    "Service is not joined with met quorum.");
//
//            return false;
//
//        } else if (getIndexManager() instanceof AbstractJournal
//                && ((AbstractJournal) getIndexManager()).getHAReady() == Quorum.NO_QUORUM) {
//
//            /*
//             * The service is not "HA Ready".
//             * 
//             * Note: There is a lag between when a service joins with a met
//             * quorum and when it has completed some internal asynchronous
//             * processing to become "HA Ready". This handles that gap for the
//             * HAJournalServer.
//             */
//            
//            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
//                    "Service is not HA Ready.");
//
//            return false;
//
//        } else {
//            
//            /*
//             * There is a quorum. The quorum is met. This service is part of the
//             * met quorum.
//             */
//
//            return true;
//
//        }

    }
    
//	/**
//	 * The {@link SparqlCache}.
//	 */
//    protected SparqlCache getSparqlCache() {
//        
//        return getRequiredServletContextAttribute(ATTRIBUTE_SPARQL_CACHE);
//        
//    }

    /**
     * Return the serviceURI(s) for this service (one or more).
     * 
     * @param req
     *            The request.
     *            
     * @return The known serviceURIs for this service.
     */
    static public String[] getServiceURIs(final ServletContext servletContext,
            final HttpServletRequest req) {

        // One or more.
        final List<String> serviceURIs = new LinkedList<String>();

        /*
         * Figure out the service end point.
         * 
         * Note: This is just the requestURL as reported. This makes is
         * possible to support virtual hosting and similar http proxy
         * patterns since the SPARQL end point is just the URL at which the
         * service is responding.
         */
        final String uri;
        {
            
            final StringBuffer sb = req.getRequestURL();

            final int indexOf = sb.indexOf("?");

            if (indexOf == -1) {
                uri = sb.toString();
            } else {
                uri = sb.substring(0, indexOf);
            }
            serviceURIs.add(uri);

        }

        /**
         * If the load balancer servlet is registered, then get its effective
         * service URI. This will be a load balanced version of the serviceURI
         * that we obtained above. We are trying to go from
         * 
         * http://localhost:8080/bigdata/sparql
         * 
         * to
         * 
         * http://localhost:8080/bigdata/LBS/sparql
         * 
         * where LBS is the prefix of the load balancer servlet.
         */
        {
            final String prefix = (String) servletContext.getAttribute(
                    ATTRIBUTE_LBS_PREFIX);

            if (prefix != null) {
                
                // locate the // in the protocol.
                final int doubleSlash = uri.indexOf("//");
                
                // skip past that and locate the next /
                final int nextSlash = uri
                        .indexOf('/', doubleSlash + 2/* fromIndex */);

                // The ContextPath for the webapp. This should be the next thing
                // in the [uri].
                final String contextPath = servletContext.getContextPath();

                // The index of the end of the ContextPath.
                final int endContextPath = nextSlash
                        + contextPath.length();

                // everything up to the *start* of the ContextPath
                final String baseURL = uri.substring(0/* beginIndex */,
                        nextSlash/* endIndex */);

                final String s = baseURL // base URL
                        + prefix // LBS prefix (includes ContextPath)
                        + (prefix.endsWith("/") ? "" : "/")
                        + uri.substring(endContextPath + 1) // remainder of requestURL.
                        ;

                serviceURIs.add(s);
                
            }
            
        }

        return serviceURIs.toArray(new String[serviceURIs.size()]);
        
    }

    static public void buildResponse(final HttpServletResponse resp,
            final int status, final String mimeType) throws IOException {

        resp.setStatus(status);

        resp.setContentType(mimeType);

    }

    static public void buildResponse(final HttpServletResponse resp, final int status,
            final String mimeType, final String content) throws IOException {

        buildResponse(resp, status, mimeType);

        final Writer w = resp.getWriter();

        w.write(content);

        w.flush();

    }

    static protected void buildResponse(final HttpServletResponse resp,
            final int status, final String mimeType, final InputStream content)
            throws IOException {

        buildResponse(resp, status, mimeType);

        final OutputStream os = resp.getOutputStream();

        copyStream(content, os);

        os.flush();

    }

    /**
     * Copy the input stream to the output stream.
     * 
     * @param content
     *            The input stream.
     * @param outstr
     *            The output stream.
     *            
     * @throws IOException
     */
    static protected void copyStream(final InputStream content,
            final OutputStream outstr) throws IOException {

        final byte[] buf = new byte[1024];

        while (true) {
        
            final int rdlen = content.read(buf);
            
            if (rdlen <= 0) {
            
                break;
                
            }
            
            outstr.write(buf, 0, rdlen);
            
        }

    }

    /**
     * Conditionally wrap the input stream, causing the data to be logged as
     * characters at DEBUG. Whether or not the input stream is wrapped depends
     * on the current {@link #log} level.
     * 
     * @param instr
     *            The input stream.
     * 
     * @return The wrapped input stream.
     * 
     * @throws IOException
     */
    protected InputStream debugStream(final InputStream instr)
            throws IOException {

        if (!log.isDebugEnabled()) {

            return instr;

        }

        final ByteArrayOutputStream outstr = new ByteArrayOutputStream();

        final byte[] buf = new byte[1024];
        int rdlen = 0;
        while (rdlen >= 0) {
            rdlen = instr.read(buf);
            if (rdlen > 0) {
                outstr.write(buf, 0, rdlen);
            }
        }

        final InputStreamReader rdr = new InputStreamReader(
                new ByteArrayInputStream(outstr.toByteArray()));
        final char[] chars = new char[outstr.size()];
        rdr.read(chars);
        log.debug("debugStream, START");
        log.debug(chars);
        log.debug("debugStream, END");

        return new ByteArrayInputStream(outstr.toByteArray());
        
    }

}
