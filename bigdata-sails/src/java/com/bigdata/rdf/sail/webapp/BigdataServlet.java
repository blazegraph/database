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
import java.io.PrintWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAStatusEnum;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.rdf.sail.webapp.client.IMimeTypes;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.rdf.task.IApiTask;

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
	 * Submit a task and return a {@link Future} for that task. The task will be
	 * run on the appropriate executor service depending on the nature of the
	 * backing database and the view required by the task.
	 * <p>
	 * <strong>The servlet API MUST NOT close the output stream from within the
	 * a submitted mutation task. Closing the output stream within the mutation
	 * task permits the client to conclude that the operation was finished
	 * before the group commit actually occurs which breaks the visibility
	 * guarantee of an ACID commit (the change is not yet visible).</strong>
	 * <p>
	 * This arises because the {@link AbstractApiTask} implementation has
	 * invoked conn.commit() and hence believes that it was successful, but the
	 * {@link AbstractTask} itself has not yet been through a checkpoint and the
	 * write set for the commit group has not yet been melded into a stable
	 * commit point. Instead, the caller MUST allow the servlet container to
	 * close the output stream once the submitted task has completed
	 * successfully (at which point the group commit will be stable). This
	 * provides the necessary and correct visibility barrier for the updates.
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
    protected <T> Future<T> submitApiTask(final AbstractRestApiTask<T> task)
            throws DatasetNotFoundException {

        final IIndexManager indexManager = getIndexManager();

		/*
		 * CAUTION: DO NOT SUBMIT MUTATION TASKS THAT CLOSE THE HTTP OUTPUT
		 * STREAM !!!
		 */

		return AbstractApiTask.submitApiTask(indexManager, new WrapperTask<T>(
				task));

    }

    /**
	 * Class ensures flush() and close() of the {@link ServletOutputStream} or
	 * {@link PrintWriter} associated with the {@link HttpServletResponse}.
	 * 
	 * @author bryan
	 * 
	 * @param <T>
	 */
	private static class WrapperTask<T> implements IApiTask<T> {

		private final AbstractRestApiTask<T> delegate;

		WrapperTask(final AbstractRestApiTask<T> delegate) {

			if (delegate == null)
				throw new IllegalArgumentException();
    		
    		this.delegate = delegate;
    		
    	}
    	
		/**
		 * Hook method is invoked after this task is executed. This may be used by
		 * mutation tasks that need to coordinate some action with the successful
		 * completion of the task.
		 * 
		 * @throws IOException 
		 */
	    protected void afterCall() throws Exception {
	       
	    	// Flush and close the output stream / writer.
	    	delegate.flushAndClose();

	    }

	    @Override
		public T call() throws Exception {

	    	final T ret = delegate.call();
			try {
				afterCall();
			} catch (Throwable t) {
				// Log an ignore.
				log.warn(t, t);
			}
			
			return ret;
		}

		@Override
		public String getNamespace() {
			return delegate.getNamespace();
		}

		@Override
		public long getTimestamp() {
			return delegate.getTimestamp();
		}

		@Override
		public void setIndexManager(IIndexManager indexManager) {

			delegate.setIndexManager(indexManager);
			
		}

		@Override
		public void clearIndexManager() {
			delegate.clearIndexManager();
		}
    	
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

   /**
    * Generate and commit a response having the indicated http status code, mime
    * type, and content.
    * 
    * @param resp
    * @param status
    *           The http status code.
    * @param mimeType
    *           The MIME type of the response.
    * @param content
    *           The content
    * @throws IOException
    * 
    *            FIXME GROUP_COMMIT: This flushes the response to the client
    *            immediately. Therefore this method MUST NOT be invoked before
    *            the group commit point! All code paths in the REST API that
    *            have actually performed a mutation (vs simply reporting a
    *            client or server error before entering into their mutation code
    *            path) MUST handle that flush from within the
    *            {@link AbstractRestApiTask}.
    */
   static public void buildAndCommitResponse(final HttpServletResponse resp,
         final int status, final String mimeType, final String content)
         throws IOException {

      resp.setStatus(status);

      resp.setContentType(mimeType);

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

}
