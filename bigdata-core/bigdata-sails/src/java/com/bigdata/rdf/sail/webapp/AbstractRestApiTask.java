/*

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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractTask;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.NV;

/**
 * Abstract base class for REST API methods. This class is compatible with a
 * job-oriented concurrency control pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
 *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
 * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
 *      Concurrent unisolated operations against multiple KBs </a>
 */
public abstract class AbstractRestApiTask<T> extends AbstractApiTask<T> {

    private static final Logger log = Logger.getLogger(AbstractRestApiTask.class);

    /**
     * The {@link UUID} associated with this task. This is used to CANCEL a
     * task.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
     *      operations should be cancelable from both REST API and workbench
     *      </a>
     */
    protected final UUID uuid;
    
    /** The {@link HttpServletRequest}. */
    protected final HttpServletRequest req;
    
    /** The {@link HttpServletResponse}. */
    protected final HttpServletResponse resp;
    
    /**
     * The {@link ServletOutputStream} iff requested by the task.
     */
    private ServletOutputStream os;

    /**
     * The {@link PrintWriter} iff requested by the task.
     */
    private PrintWriter writer;

   /**
    * Used to coordinate access to the {@link ServletOutputStream} versus the
    * {@link PrintWriter} (visibility).
    */
    private final Lock lock = new ReentrantLock();
    
	/**
	 * Return the {@link ServletOutputStream} associated with the request (and
	 * stash a copy).
	 * 
	 * @throws IOException
	 * @throws IllegalStateException
	 *             per the servlet API if the writer has been requested already.
	 */
    public OutputStream getOutputStream() throws IOException {

      lock.lock();
      try {

         return new FilterOutputStream(this.os = resp.getOutputStream()) {
            @Override
            public void flush() {
               throw new UnsupportedOperationException();
            }
            @Override
            public void close() {
               throw new UnsupportedOperationException();
            }
         };

      } finally {
         lock.unlock();
		}
    	
    }

	/**
	 * Return the {@link PrintWriter} associated with the request (and stash a
	 * copy).
	 * 
	 * @throws IOException
	 * @throws IllegalStateException
	 *             per the servlet API if the {@link ServletOutputStream} has
	 *             been requested already.
	 */
	public PrintWriter getWriter() throws IOException {

      lock.lock();
      try {

         return new PrintWriter(this.writer = resp.getWriter()) {
            @Override
            public void flush() {
               throw new UnsupportedOperationException();
            }
            @Override
            public void close() {
               throw new UnsupportedOperationException();
            }
         };

      } finally {
         lock.unlock();
      }

	}

	/**
	 * Flush and close the {@link ServletOutputStream} or {@link PrintWriter}
	 * depending on which was obtained.
	 * 
	 * @throws IOException
	 */
	public void flushAndClose() throws IOException {

		lock.lock();
		try {
		   if(log.isInfoEnabled())
            log.info("FLUSH-AND-CLOSE: " + toString());
			if (os != null) {
			   final ServletOutputStream tmp = resp.getOutputStream();
				tmp.flush();
				tmp.close();
			} else if (writer != null) {
			   final PrintWriter tmp = resp.getWriter();
				tmp.flush();
				tmp.close();
			}

		} finally {
		   lock.unlock();
		}

	}

    /**
     * @param req
     *            The {@link HttpServletRequest}.
     * @param resp
     *            The {@link HttpServletResponse}.
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp of the view of that KB instance.
     */
    protected AbstractRestApiTask(final HttpServletRequest req,
            final HttpServletResponse resp, final String namespace,
            final long timestamp) {
        this(req, resp, namespace,timestamp, false/*isGSRRequired*/);
    }

    /**
     * @param req
     *            The {@link HttpServletRequest}.
     * @param resp
     *            The {@link HttpServletResponse}.
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp of the view of that KB instance.
     * @param isGSRRequired
     *            <code>true</code> iff the task requires a lock on the GRS
     *            index.
     */
    protected AbstractRestApiTask(final HttpServletRequest req,
            final HttpServletResponse resp, final String namespace,
            final long timestamp, final boolean isGRSRequired) {

        super(namespace, timestamp, isGRSRequired);
        
        this.req = req;
        
        this.resp = resp;

        // Extract the UUID of the request (if given).
        final String s = req.getParameter(QueryHints.QUERYID);
        
        // The given UUID or a randomly assigned one.
        this.uuid = s == null ? UUID.randomUUID() : UUID.fromString(s);
    }

    @Override
    public String toString() {

      return getClass().getSimpleName() + "{namespace=" + getNamespace()
            + ",timestamp=" + getTimestamp() + ", isGRSRequired="
            + isGRSRequired() + "}";

    }

    /**
	 * Reports the mutation count and elapsed operation time as specified by the
	 * REST API for mutation operations.
	 * <p>
	 * Note: When GROUP_COMMIT (#566) is enabled the http output stream MUST NOT
	 * be closed by this method. Doing so would permit the client to conclude
	 * that the operation was finished before the group commit actually occurs.
	 * This situation arises because the AbstractApiTask implementation has
	 * invoked conn.commit() and hence believes that it was successful, but the
	 * {@link AbstractTask} itself has not yet been through a checkpoint and the
	 * write set for the commit group has not yet been melded into a stable
	 * commit point. By NOT closing the output stream here we defer the signal
	 * to the client until control is returned to the servlet container layer,
	 * which does not occur until the task has finished executing and the group
	 * commit is done. The servlet container then closes the output stream and
	 * the client notices that the mutation operation is done and that its
	 * applied mutation is now visible to a client reading against that commit
	 * point. (Mind you, there might be other mutations in the same group commit
	 * or even in subsequent commits so the client does not have a strong
	 * guarantee that their mutation is visible in a post-commit read unless
	 * they are controlling all writers.)
	 * <p>
	 * Note: The other code path for signaling to the client that a mutation is
	 * done is SPARQL UPDATE. See that code for a similar caution.
	 * 
	 * @param nmodified
	 *            The number of modified triples/quads.
	 * @param elapsed
	 *            The elapsed time for the operation.
	 * 
	 * @throws IOException
	 */
    protected void reportModifiedCount(final long nmodified, final long elapsed)
            throws IOException {

      if (log.isInfoEnabled())
         log.info("task=" + this + ", nmodified=" + nmodified);

       final StringWriter stringWriter = new StringWriter();
       
       final XMLBuilder t = new XMLBuilder(stringWriter);

       t.root("data").attr("modified", nmodified)
               .attr("milliseconds", elapsed).close();

       resp.setStatus(HttpServletResponse.SC_OK);

       resp.setContentType(BigdataServlet.MIME_APPLICATION_XML);

//       buildResponse(resp, HTTP_OK, MIME_APPLICATION_XML, w.toString());

       final Writer writer = resp.getWriter();

       writer.write(stringWriter.toString());

      /*
       * Note: GROUP COMMIT: This would flush the response to the client. This
       * MUST NOT be invoked before the group commit point! See flushAndClose().
       */
//       w.flush(); 

    }
    
    /**
    * Generate a response having the indicated http status code, mime type, and
    * content.
    * 
    * @param status
    *           The http status code.
    * @param mimeType
    *           The MIME type of the response.
    * @param content
    *           The content
    * @param headers
    *           Zero or more headers to be included in the response.
    * 
    * @throws IOException
    * 
    * @throws AssertionError
    *            if the caller attempts to use this method for a non-success
    *            (2xx) outcome. The correct pattern is to throw an
    *            {@link HttpOperationException} instead once you are inside of
    *            an {@link AbstractRestApiTask}.
    */
   protected void buildResponse(final int status, final String mimeType,
         final String content,final NV...headers) throws IOException {

      if (status < 200 || status >= 300)
         throw new AssertionError(
               "This method must not be used for non-success outcomes");
      
      if (log.isInfoEnabled())
         log.info("task=" + this + ", status=" + status + ", mimeType="
               + mimeType + ", content=[" + content + "]");

       resp.setStatus(status);

       resp.setContentType(mimeType);
       
       for (NV nv : headers) {
  
          resp.setHeader(nv.getName(), nv.getValue());
      
       }

       final Writer w = resp.getWriter();

       if (content != null)
          w.write(content);

       /*
        * Note: GROUP COMMIT: This would flush the response to the client. This
        * MUST NOT be invoked before the group commit point! See flushAndClose().
        */
//        w.flush(); 

    }
   
}
