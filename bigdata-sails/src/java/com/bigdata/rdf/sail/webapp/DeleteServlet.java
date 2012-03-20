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
import java.io.InputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.SailException;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;

/**
 * Handler for DELETE by query (DELETE verb) and DELETE by data (POST).
 * 
 * @author martyncutcher
 */
public class DeleteServlet extends BigdataRDFServlet {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(DeleteServlet.class);

    public DeleteServlet() {

    }

	@Override
	protected void doDelete(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {

        final String queryStr = req.getParameter("query");

        if (queryStr != null) {
            
            doDeleteWithQuery(req, resp);
            
        } else {
            
            doDeleteWithAccessPath(req, resp);
            
//        } else {
//  
//        	resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        	
        }
           
    }

    /**
     * Delete all statements materialized by a DESCRIBE or CONSTRUCT query.
     * <p>
     * Note: To avoid materializing the statements, this runs the query against
     * the last commit time and uses a pipe to connect the query directly to the
     * process deleting the statements. This is done while it is holding the
     * unisolated connection which prevents concurrent modifications. Therefore
     * the entire SELECT + DELETE operation is ACID.
     */
    private void doDeleteWithQuery(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final long begin = System.currentTimeMillis();
        
        final String baseURI = req.getRequestURL().toString();

        final String namespace = getNamespace(req);

        final String queryStr = req.getParameter("query");

        if (queryStr == null)
            throw new UnsupportedOperationException();

        if (log.isInfoEnabled())
            log.info("delete with query: " + queryStr);

        try {

            /*
             * Note: pipe is drained by this thread to consume the query
             * results, which are the statements to be deleted.
             */
            final PipedOutputStream os = new PipedOutputStream();
            final InputStream is = newPipedInputStream(os);
            try {

                // Use this format for the query results.
                final RDFFormat format = RDFFormat.NTRIPLES;
                
                final AbstractQueryTask queryTask = getBigdataRDFContext()
                        .getQueryTask(namespace, ITx.READ_COMMITTED, queryStr,
                                format.getDefaultMIMEType(),
                                req, os, false/*update*/);

                switch (queryTask.queryType) {
                case DESCRIBE:
                case CONSTRUCT:
                    break;
                default:
                    buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                            "Must be DESCRIBE or CONSTRUCT query.");
                    return;
                }

                final AtomicLong nmodified = new AtomicLong(0L);

                BigdataSailRepositoryConnection conn = null;
                try {

                    conn = getBigdataRDFContext().getUnisolatedConnection(
                            namespace);

                    final RDFParserFactory factory = RDFParserRegistry
                            .getInstance().get(format);

                    final RDFParser rdfParser = factory.getParser();

                    rdfParser.setValueFactory(conn.getTripleStore()
                            .getValueFactory());

                    rdfParser.setVerifyData(false);

                    rdfParser.setStopAtFirstError(true);

                    rdfParser
                            .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                    rdfParser.setRDFHandler(new RemoveStatementHandler(conn
                            .getSailConnection(), nmodified));

                    // Wrap as Future.
                    final FutureTask<Void> ft = new FutureTask<Void>(queryTask);
                    
                    // Submit query for evaluation.
                    getBigdataRDFContext().queryService.execute(ft);
                    
                    // Run parser : visited statements will be deleted.
                    rdfParser.parse(is, baseURI);

                    // Await the Future (of the Query)
                    ft.get();
                    
                    // Commit the mutation.
                    conn.commit();

                    final long elapsed = System.currentTimeMillis() - begin;
                    
                    reportModifiedCount(resp, nmodified.get(), elapsed);

                } catch(Throwable t) {
                    
                    if(conn != null)
                        conn.rollback();
                    
                    throw new RuntimeException(t);
                    
                } finally {

                    if (conn != null)
                        conn.close();

                }

            } catch (Throwable t) {

                throw BigdataRDFServlet.launderThrowable(t, resp, queryStr);

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);

        }

    }

    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final String contentType = req.getContentType();

        final String queryStr = req.getParameter("query");

        if (queryStr != null) {

            doDeleteWithQuery(req, resp);

        } else if (contentType != null) {

            doDeleteWithBody(req, resp);

        } else {

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }

    }

    /**
     * DELETE request with a request body containing the statements to be
     * removed.
     */
    private void doDeleteWithBody(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final long begin = System.currentTimeMillis();

        final String baseURI = req.getRequestURL().toString();
        
        final String namespace = getNamespace(req);

        final String contentType = req.getContentType();

        if (contentType == null)
            throw new UnsupportedOperationException();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        try {

            /*
             * There is a request body, so let's try and parse it.
             */

            final RDFFormat format = RDFFormat.forMIMEType(contentType);

            if (format == null) {

                buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Content-Type not recognized as RDF: " + contentType);

                return;

            }

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(format);

            if (rdfParserFactory == null) {

                buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                        "Parser factory not found: Content-Type=" + contentType
                                + ", format=" + format);

                return;

            }

            final RDFParser rdfParser = rdfParserFactory.getParser();

            final AtomicLong nmodified = new AtomicLong(0L);

            BigdataSailRepositoryConnection conn = null;
            try {

                conn = getBigdataRDFContext()
                        .getUnisolatedConnection(namespace);

                rdfParser.setValueFactory(conn.getTripleStore()
                        .getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new RemoveStatementHandler(conn
                        .getSailConnection(), nmodified));

                /*
                 * Run the parser, which will cause statements to be deleted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                final long elapsed = System.currentTimeMillis() - begin;

                reportModifiedCount(resp, nmodified.get(), elapsed);

            } catch(Throwable t) {
                
                if (conn != null)
                    conn.rollback();

                throw new RuntimeException(t);
                
            } finally {

                if (conn != null)
                    conn.close();

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);
            
        }

    }
    
    /**
     * Helper class removes statements from the sail as they are visited by a parser.
     */
    static class RemoveStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        
        public RemoveStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified) {

            this.conn = conn;
            
            this.nmodified = nmodified;
            
        }

        public void handleStatement(final Statement stmt)
                throws RDFHandlerException {

            try {

                final Resource context = stmt.getContext();
            	
                conn.removeStatements(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        (Resource[]) (context == null ? nullArray
                                : new Resource[] { context })//
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            nmodified.incrementAndGet();

        }

    }
    
    /**
     * Delete all statements described by an access path.
     */
    private void doDeleteWithAccessPath(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final long begin = System.currentTimeMillis();
        
        final String namespace = getNamespace(req);

        final Resource s;
        final URI p;
        final Value o;
        final Resource c;
        try {
            s = EncodeDecodeValue.decodeResource(req.getParameter("s"));
            p = EncodeDecodeValue.decodeURI(req.getParameter("p"));
            o = EncodeDecodeValue.decodeValue(req.getParameter("o"));
            c = EncodeDecodeValue.decodeResource(req.getParameter("c"));
        } catch (IllegalArgumentException ex) {
            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    ex.getLocalizedMessage());
            return;
        }
        
        if (log.isInfoEnabled())
            log.info("delete with access path: (s=" + s + ", p=" + p + ", o="
                    + o + ", c=" + c + ")");

        try {

            try {

                BigdataSailRepositoryConnection conn = null;
                try {

                    conn = getBigdataRDFContext().getUnisolatedConnection(
                            namespace);

                    // Remove all statements matching that access path.
                    final long nmodified = conn.getSailConnection()
                            .getBigdataSail().getDatabase()
                            .removeStatements(s, p, o, c);
                    
                    // Commit the mutation.
                    conn.commit();

                    final long elapsed = System.currentTimeMillis() - begin;
                    
                    reportModifiedCount(resp, nmodified, elapsed);

                } catch(Throwable t) {
                    
                    if(conn != null)
                        conn.rollback();
                    
                    throw new RuntimeException(t);
                    
                } finally {

                    if (conn != null)
                        conn.close();

                }

            } catch (Throwable t) {

                throw BigdataRDFServlet.launderThrowable(t, resp, "");

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);

        }

    }

    static private transient final Resource[] nullArray = new Resource[]{};
    
}
