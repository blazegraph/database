/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Apr 13, 2011
 */

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.IAtomicStore;

/**
 * Abstract base class for {@link Servlet}s which interact with the bigdata RDF
 * data and/or SPARQL query layers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BigdataRDFServlet extends BigdataServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger.getLogger(BigdataRDFServlet.class);

    /**
     * A SPARQL results set in XML.
     * 
     * @see http://www.w3.org/TR/rdf-sparql-XMLres/
     */
    static protected final transient String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";

    /**
     * A SPARQL results set in JSON.
     * 
     * @see http://www.w3.org/TR/rdf-sparql-json-res/
     */
    static protected final transient String MIME_SPARQL_RESULTS_JSON = "application/sparql-results+json";

    /**
     * RDF/XML.
     */
    static protected final transient String MIME_RDF_XML = "application/rdf+xml"; 

    /**
     * 
     */
    public BigdataRDFServlet() {
        
    }

    final protected SparqlEndpointConfig getConfig() {
        
        return getBigdataRDFContext().getConfig();
        
    }

    final protected BigdataRDFContext getBigdataRDFContext() {

        if (m_context == null) {

            m_context = getRequiredServletContextAttribute(BigdataRDFContext.class
                    .getName());

        }

        return m_context;

    }

    private volatile BigdataRDFContext m_context;

    /**
     * Write the stack trace onto the output stream. This will show up in the
     * client's response. This code path should be used iff we have already
     * begun writing the response. Otherwise, an HTTP error status should be
     * used instead.
     * 
     * @param t
     *            The thrown error.
     * @param os
     *            The stream on which the response will be written.
     * @param queryStr
     *            The query string (if available).
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     */
    protected static RuntimeException launderThrowable(final Throwable t,
            final HttpServletResponse resp, final String queryStr)
            throws Exception {
        try {
            // log an error for the service.
            log.error(t, t);
        } finally {
            // ignore any problems here.
        }
    	if (resp != null) {
            if (!resp.isCommitted()) {
                resp.setStatus(HTTP_INTERNALERROR);
                resp.setContentType(MIME_TEXT_PLAIN);
            }
    	    OutputStream os = null;
    		try {
    		    os = resp.getOutputStream();
                final PrintWriter w = new PrintWriter(os);
                if (queryStr != null) {
                    /*
                     * Write the query onto the output stream.
                     */
                    w.write(queryStr);
                    w.write("\n");
                }
                /*
                 * Write the stack trace onto the output stream.
                 */
                t.printStackTrace(w);
                w.flush();
    			// flush the output stream.
    			os.flush();
    		} finally {
    			// ignore any problems here.
            }
            if (os != null) {
                try {
                    // ensure output stream is closed.
                    os.close();
                } catch (Throwable t2) {
                    // ignore any problems here.
                }
            }
    	}
    	if (t instanceof RuntimeException) {
    		return (RuntimeException) t;
    	} else if (t instanceof Error) {
    		throw (Error) t;
    	} else if (t instanceof Exception) {
    		throw (Exception) t;
    	} else
    		throw new RuntimeException(t);
    }

    /**
     * Return the timestamp which will be used to execute the query. The uri
     * query parameter <code>timestamp</code> may be used to communicate the
     * desired commit time against which the query will be issued. If that uri
     * query parameter is not given then the default configured commit time will
     * be used. Applications may create protocols for sharing interesting commit
     * times as reported by {@link IAtomicStore#commit()} or by a distributed
     * data loader (for scale-out).
     * 
     * @todo the configured timestamp should only be used for the default
     *       namespace (or it should be configured for each graph explicitly, or
     *       we should bundle the (namespace,timestamp) together as a single
     *       object).
     */
    protected long getTimestamp(final String uri,
            final HttpServletRequest req) {
        
        final String timestamp = req.getParameter("timestamp");
        
        if (timestamp == null) {
            
            return getConfig().timestamp;
            
        }

        return Long.valueOf(timestamp);

    }
    
    /**
     * Return the namespace which will be used to execute the query. The
     * namespace is represented by the first component of the URI. If there is
     * no namespace, then return the configured default namespace.
     * 
     * @param uri
     *            The URI path string.
     * 
     * @return The namespace.
     */
    protected String getNamespace(final String uri) {

//        // locate the "//" after the protocol.
//        final int index = uri.indexOf("//");
        
        int snmsp = uri.indexOf("/namespace/");

        if (snmsp == -1) {
            // use the default namespace.
            return getConfig().namespace;
        }

        // locate the next "/" in the URI path.
        final int beginIndex = uri.indexOf('/', snmsp + 1/* fromIndex */);

        // locate the next "/" in the URI path.
        int endIndex = uri.indexOf('/', beginIndex + 1/* fromIndex */);

        if (endIndex == -1) {
            // use the rest of the URI.
            endIndex = uri.length();
        }

        // return the namespace.
        return uri.substring(beginIndex + 1, endIndex);

    }

    /**
     * Factory for the {@link PipedInputStream}.
     */
    protected PipedInputStream newPipedInputStream(final PipedOutputStream os)
            throws IOException {

        return new PipedInputStream(os);

    }

    /**
     * Report a mutation count and elapsed time back to the user agent.
     * 
     * @param resp
     *            The response.
     * @param nmodified
     *            The mutation count.
     * @param elapsed
     *            The elapsed time (milliseconds).
     * 
     * @throws IOException
     */
    protected void reportModifiedCount(final HttpServletResponse resp,
            final long nmodified, final long elapsed) throws IOException {

        final XMLBuilder t = new XMLBuilder();

        t.root("data").attr("modified", nmodified)
                .attr("milliseconds", elapsed).close();

        buildResponse(resp, HTTP_OK, MIME_APPLICATION_XML, t.toString());

    }

}
