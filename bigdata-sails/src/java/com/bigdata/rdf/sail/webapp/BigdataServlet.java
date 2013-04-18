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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.IMimeTypes;

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
     * {@link IIndexManager}.
     */
    /*package*/ static final transient String ATTRIBUTE_INDEX_MANAGER = 
        IIndexManager.class.getName();

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

    protected <T> T getRequiredServletContextAttribute(final String name) {

        @SuppressWarnings("unchecked")
        final T v = (T) getServletContext().getAttribute(name);

        if (v == null)
            throw new RuntimeException("Not set: " + name);

        return v;

    }

    /**
     * The backing {@link IIndexManager}.
     */
	protected IIndexManager getIndexManager() {
	
	    return getRequiredServletContextAttribute(ATTRIBUTE_INDEX_MANAGER);
	    
	}

    /**
     * Return the {@link Quorum} -or- <code>null</code> if the
     * {@link IIndexManager} is not participating in an HA {@link Quorum}.
     */
    protected Quorum<HAGlue, QuorumService<HAGlue>> getQuorum() {

        final IIndexManager indexManager = getIndexManager();

        if (indexManager instanceof Journal) {

            return ((Journal) indexManager).getQuorum();

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
    protected boolean isWritable(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

        if (quorum == null) {
         
            // No quorum.
            return true;
            
        }

        /*
         * Note: This is a summary of what we need to look at to proxy a
         * request.
         * 
         * Note: We need to proxy PUT, POST, DELETE requests to the quorum
         * leader.
         * 
         * Note: If an NSS service is NOT joined with a met quorum, but there is
         * a met quorum, then we should just proxy the request to the met
         * quorum. This includes both reads and writes.
         */
        
//        req.getMethod();
//        final Enumeration<String> names = req.getHeaderNames();
//        while(names.hasMoreElements()) {
//            req.getHeaders(names.nextElement());
//        }
//        req.getInputStream();

        // Note: Response also has status code plus everything above.
        
        // Note: Invocation against local HAGlue object (NOT RMI).
        final HAStatusEnum haStatus = getQuorum().getClient().getService()
                .getHAStatus();

        switch (haStatus) {
        case Leader:
            return true;
        default:
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    "Not quorum leader.");
            // Not writable.  Response has been committed.
            return false;
        }
                
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
    protected boolean isReadable(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

        if (quorum == null) {
         
            // No quorum.
            return true;

        }

        // Note: Invocation against local HAGlue object (NOT RMI).
        final HAStatusEnum haStatus = getQuorum().getClient().getService()
                .getHAStatus();

        switch (haStatus) {
        case Leader:
        case Follower:
            return true;
        default:
            // Not ready.
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    "Not HA Ready.");
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
