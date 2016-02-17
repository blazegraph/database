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
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;

import com.bigdata.ha.HAStatusEnum;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.TaskAndFutureTask;
import com.bigdata.rdf.sail.webapp.client.EncodeDecodeValue;
import com.bigdata.rdf.sail.webapp.client.IMimeTypes;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.NV;

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
     * HALoadBalancerServlet (DO NOT LINK JAVADOC) iff it is running.
     * <p>
     * Note: Do NOT reference the <code>HALoadBalancerServlet</code> or anything
     * in the <code>com.bigdata.rdf.sail.webapp.lbs</code> package here. It will
     * drag in the jetty dependencies and that breaks the tomcat WAR deployment.
     */
    static final String ATTRIBUTE_LBS_PREFIX = "com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.prefix";
    
    /**
     * The {@link ServletContext} attribute that is managed by the
     * HALoadBalancerServlet (DO NOT LINK JAVADOC) and which maintains a
     * collection of the active instances of that servlet. This is used to
     * administer the IHALoadBalancerPolicy associated with the load balancer
     * servlet instances.
     * <p>
     * Note: Do NOT reference the <code>HALoadBalancerServlet</code> or anything
     * in the <code>com.bigdata.rdf.sail.webapp.lbs</code> package here. It will
     * drag in the jetty dependencies and that breaks the tomcat WAR deployment.
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
    * Submit a task, await its {@link Future}, flush and commit the servlet
    * resdponse, and then a <strong>completed</strong> {@link Future} for that
    * task. The task will be run on the appropriate executor service depending
    * on the nature of the backing database and the view required by the task.
    * <p>
    * <strong>The servlet API MUST NOT close the output stream from within the a
    * submitted mutation task. Closing the output stream within the mutation
    * task permits the client to conclude that the operation was finished before
    * the group commit actually occurs which breaks the visibility guarantee of
    * an ACID commit (the change is not yet visible).</strong>
    * <p>
    * This arises because the {@link AbstractApiTask} implementation has invoked
    * conn.commit() and hence believes that it was successful, but the
    * {@link AbstractTask} itself has not yet been through a checkpoint and the
    * write set for the commit group has not yet been melded into a stable
    * commit point. Instead, the caller MUST allow the servlet container to
    * close the output stream once the submitted task has completed successfully
    * (at which point the group commit will be stable). This provides the
    * necessary and correct visibility barrier for the updates.
    * <p>
    * <strong>CAUTION: Non-success outcomes MUST throw exceptions!</strong> Once
    * the flow of control enters an {@link AbstractRestApiTask} the task MUST
    * throw out a typed exception that conveys the necessary information to the
    * launderThrowable() code which can then turn it into an appropriate HTTP
    * response. If the task does not throw an exception then it is presumed to
    * be successful and it will join the next group commit. Failure to follow
    * this caution can cause partial write sets to be made durable, thus
    * breaking the ACID semantics of the API.
    * 
    * @param task
    *           The task.
    * 
    * @return The {@link Future} for that task.
    * 
    * @throws DatasetNotFoundException
    * @throws ExecutionException
    * @throws InterruptedException
    * @throws IOException
    * 
    * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
    *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
    * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
    *      Concurrent unisolated operations against multiple KBs </a>
    * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
    *      operations should be cancelable from both REST API and workbench
    *      </a>
    */
   protected <T> FutureTask<T> submitApiTask(final AbstractRestApiTask<T> task)
         throws DatasetNotFoundException, InterruptedException,
         ExecutionException, IOException {

        if (task == null)
            throw new IllegalArgumentException();
       
        final IIndexManager indexManager = getIndexManager();

        /*
         * ::CAUTION::
         * 
         * MUTATION TASKS MUST NOT FLUSH OR CLOSE THE HTTP OUTPUT STREAM !!!
         * 
         * THIS MUST BE DONE *AFTER* THE GROUP COMMIT POINT.
         * 
         * THE GROUP COMMIT POINT OCCURS *AFTER* THE TASK IS DONE.
         */

        final BigdataRDFContext context = getBigdataRDFContext();
        
        try {

            // Submit task. Will run.
            final FutureTask<T> ft = AbstractApiTask.submitApiTask(
                    indexManager, task);

            // register task.
            context.addTask(task, ft);
            
            // Await Future.
            ft.get();

            /*
             * IFF successful, flush and close the response.
             * 
             * Note: This *commits* the http response to the client. The client
             * will see this as the commit point on the database and will expect
             * to have any mutation visible in subsequent reads.
             */
            task.flushAndClose();

            /*
             * TODO Modify to return T rather than Future<T> if we are always
             * doing the get() here? (If we do this then we also need to hook
             * the FutureTask to remove itself from the set of known running
             * tasks.)
             */
            return ft;

        } finally {
            
            context.removeTask(task.uuid);

        }

    }
   
    /**
     * Return the {@link HAStatusEnum} -or- <code>null</code> if the
     * {@link IIndexManager} is not an {@link AbstractQuorum} or is not HA
     * enabled.
     */
    static HAStatusEnum getHAStatus(final IIndexManager indexManager) {

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
            
            buildAndCommitResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
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
            buildAndCommitResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
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
            buildAndCommitResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    haStatus.name());
            // Response has been committed.
            return false;
        }

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
         * http://localhost:9999/bigdata/sparql
         * 
         * to
         * 
         * http://localhost:9999/bigdata/LBS/sparql
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

   /**
    * Generate and commit a response having the indicated http status code, mime
    * type, and content.
    * <p>
    * This flushes the response to the client immediately. Therefore this method
    * MUST NOT be invoked before the group commit point! All code paths in the
    * REST API that have actually performed a mutation (vs simply reporting a
    * client or server error before entering into their mutation code path) MUST
    * use {@link #submitApiTask(AbstractRestApiTask)}.
    * <p>
    * Note: It is NOT safe to invoke this method once you are inside an
    * {@link AbstractRestApiTask} EVEN if the purpose is to report a client or
    * server error. The task MUST throw out a typed exception that conveys the
    * necessary information to the launderThrowable() code which can then turn
    * it into an appropriate HTTP response. If the task does not throw an
    * exception then it is presumed to be successful and it will join the next
    * group commit!
    * 
    * @param resp
    * @param status
    *           The http status code.
    * @param mimeType
    *           The MIME type of the response.
    * @param content
    *           The content (optional).
    * @param headers
    *           Zero or more headers.
    * 
    * @throws IOException
    */
   static public void buildAndCommitResponse(final HttpServletResponse resp,
         final int status, final String mimeType, final String content,
         final NV... headers) throws IOException {

      resp.setStatus(status);

      resp.setContentType(mimeType);

      for (NV nv : headers) {
         
         resp.setHeader(nv.getName(), nv.getValue());
     
      }

      final Writer w = resp.getWriter();

      if (content != null)
         w.write(content);

      /*
       * Note: This commits the response! This method MUST NOT be used within an
       * AbstractRestApiTask that results in a mutation as that would permit the
       * client to believe that the mutation was committed when in fact it is
       * NOT committed until the AbstractTask running that AbstractRestApiTask
       * is melded into a group commit, which occurs *after* the user code for
       * task is done executing.
       */
      w.flush();

   }

   /**
    * Return the effective boolean value of a request parameter. The text
    * <code>"true"</code> is recognized. All other values are interpreted as
    * <code>false</code>.
    * 
    * @param req
    *           The request.
    * @param name
    *           The name of the request parameter.
    * @param defaultValue
    *           The default value.
    * 
    * @return The effective value.
    */
   protected boolean getBooleanValue(final HttpServletRequest req,
         final String name, final boolean defaultValue) {

      final String s = req.getParameter(name);

      if (s == null)
         return defaultValue;

      // Note: returns true iff "true" and otherwise returns false.
      final boolean b = Boolean.valueOf(s);

      return b;
      
   }

   /**
    * Decode an array of named graph contexts from a request.
    * 
    * @param req
    *           The request.
    * 
    * @param name
    *           The name of the request parameter to be decoded.
    * 
    * @return An array of decoded resources and never <code>null</code>. If the
    *         request parameter does not appear in the request then this method
    *         returns <code>Resource[0]</code>.
    * 
    * @see BD#NULL_GRAPH
    * @see EncodeDecodeValue#decodeContexts(String[])
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   protected static Resource[] decodeContexts(final HttpServletRequest req,
         final String name) {

      /*
       * Note: return value is [null] if the parameter does not appear for the
       * request.
       */
      final String[] values = req.getParameterValues(name);

      if (values == null) {

         // This needs to be treated as an empty[].
         return EMPTY_RESOURCE_ARRAY;
         
      }

      /*
       * Otherwise one or more parameter values.  Decode into a Resource[].
       */
      final Resource[] contexts = EncodeDecodeValue.decodeContexts(values);

      return contexts;

   }

   /** An empty Resource[] for decode. */
   static private final Resource[] EMPTY_RESOURCE_ARRAY = new Resource[0];
   
   static protected String readFully(final Reader reader) throws IOException {

      final char[] arr = new char[8 * 1024]; // 8K at a time
      final StringBuffer buf = new StringBuffer();
      int numChars;

      while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
         buf.append(arr, 0, numChars);
      }

      return buf.toString();
   }

}
