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
import java.util.Arrays;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
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
import com.bigdata.rdf.sail.webapp.client.EncodeDecodeValue;
import com.bigdata.rdf.sail.webapp.client.MiniMime;

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

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

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
     * 
     * FIXME GROUP COMMIT : Again, a pattern where a query is run to produce
     * solutions that are then deleted from the database. Can we rewrite this to
     * be a SPARQL UPDATE? (DELETE WHERE). Note that the ACID semantics of this
     * operation would be broken by group commit since other tasks could have
     * updated the KB since the lastCommitTime and been checkpointed and hence
     * be visible to an unisolated operation without there being an intervening
     * commit point. FIXME Review for #1036
     */
    private void doDeleteWithQuery(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
		final String baseURI = req.getRequestURL().toString();

		final String namespace = getNamespace(req);

		final String queryStr = req.getParameter("query");

		if (queryStr == null)
			throw new UnsupportedOperationException();

		if (log.isInfoEnabled())
			log.info("delete with query: " + queryStr);

		try {

			submitApiTask(
					new DeleteWithQueryTask(req, resp, namespace,
							ITx.UNISOLATED, //
							queryStr,//
							baseURI//
					)).get();

		} catch (Throwable t) {

			launderThrowable(t, resp, "UPDATE-WITH-QUERY" + ": queryStr="
					+ queryStr + ", baseURI=" + baseURI);

		}
		
    }

    private static class DeleteWithQueryTask extends AbstractRestApiTask<Void> {

    	private final String queryStr;
        private final String baseURI;

        /**
         * 
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         * @param baseURI
         *            The base URI for the operation.
         */
        public DeleteWithQueryTask(final HttpServletRequest req,
                final HttpServletResponse resp,
                final String namespace, final long timestamp,
                final String queryStr,//
                final String baseURI
                ) {
            super(req, resp, namespace, timestamp);
            this.queryStr = queryStr;
            this.baseURI = baseURI;
        }
        
        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public Void call() throws Exception {

            final long begin = System.currentTimeMillis();
            
            final AtomicLong nmodified = new AtomicLong(0L);

			BigdataSailRepositoryConnection conn = null;
			boolean success = false;
			try {

				conn = getUnisolatedConnection();

				{

					if (log.isInfoEnabled())
						log.info("delete with query: " + queryStr);

					final BigdataRDFContext context = BigdataServlet
							.getBigdataRDFContext(req.getServletContext());

					/*
					 * Note: pipe is drained by this thread to consume the query
					 * results, which are the statements to be deleted.
					 */
					final PipedOutputStream os = new PipedOutputStream();

					// The read-only connection for the query.
					BigdataSailRepositoryConnection roconn = null;
					try {
						
						roconn = getQueryConnection();

						// Use this format for the query results.
						final RDFFormat format = RDFFormat.NTRIPLES;

						final AbstractQueryTask queryTask = context
								.getQueryTask(roconn, namespace,
										ITx.READ_COMMITTED, queryStr,
										format.getDefaultMIMEType(), req, resp,
										os);

						switch (queryTask.queryType) {
						case DESCRIBE:
						case CONSTRUCT:
							break;
						default:
							throw new MalformedQueryException(
									"Must be DESCRIBE or CONSTRUCT query");
						}

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
						final FutureTask<Void> ft = new FutureTask<Void>(
								queryTask);

						// Submit query for evaluation.
						context.queryService.execute(ft);

						// Reads on the statements produced by the query.
						final InputStream is = newPipedInputStream(os);

						// Run parser : visited statements will be deleted.
						rdfParser.parse(is, baseURI);

						// Await the Future (of the Query)
						ft.get();

					} finally {

						if (roconn != null) {
							// close the read-only connection for the query.
							roconn.rollback();
						}

					}

				}

				conn.commit();

				success = true;

				final long elapsed = System.currentTimeMillis() - begin;

				reportModifiedCount(nmodified.get(), elapsed);

				return null;

			} finally {

				if (conn != null) {

					if (!success)
						conn.rollback();

					conn.close();

				}

			}

		}

    } // class DeleteWithQueryTask

    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

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

        final String baseURI = req.getRequestURL().toString();

        final String contentType = req.getContentType();

        if (contentType == null)
            throw new UnsupportedOperationException();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        /**
         * There is a request body, so let's try and parse it.
         * 
         * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/620">
         * UpdateServlet fails to parse MIMEType when doing conneg. </a>
         */

        final RDFFormat format = RDFFormat.forMIMEType(new MiniMime(
                contentType).getMimeType());

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

        /*
         * Allow the caller to specify the default contexts.
         */
        final Resource[] defaultContext;
        {
            final String[] s = req.getParameterValues("context-uri");
            if (s != null && s.length > 0) {
                try {
                    defaultContext = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContext = new Resource[0];
            }
        }

        try {

            submitApiTask(
                    new DeleteWithBodyTask(req, resp, getNamespace(req),
                            ITx.UNISOLATED, baseURI, defaultContext,
                            rdfParserFactory)).get();

        } catch (Throwable t) {

            throw BigdataRDFServlet.launderThrowable(t, resp,
                    "DELETE-WITH-BODY: baseURI=" + baseURI + ", context-uri="
                            + Arrays.toString(defaultContext));

        }

    }
    
    private static class DeleteWithBodyTask extends AbstractRestApiTask<Void> {

        private final String baseURI;
        private final Resource[] defaultContext;
        private final RDFParserFactory rdfParserFactory;

        /**
         * 
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         * @param baseURI
         *            The base URI for the operation.
         * @param defaultContext
         *            The context(s) for triples without an explicit named graph
         *            when the KB instance is operating in a quads mode.
         * @param rdfParserFactory
         *            The factory for the {@link RDFParser}. This should have
         *            been chosen based on the caller's knowledge of the
         *            appropriate content type.
         */
        public DeleteWithBodyTask(final HttpServletRequest req,
                final HttpServletResponse resp,
                final String namespace, final long timestamp,
                final String baseURI, final Resource[] defaultContext,
                final RDFParserFactory rdfParserFactory) {
            super(req, resp, namespace, timestamp);
            this.baseURI = baseURI;
            this.defaultContext = defaultContext;
            this.rdfParserFactory = rdfParserFactory;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public Void call() throws Exception {

            final long begin = System.currentTimeMillis();
            
            BigdataSailRepositoryConnection conn = null;
            boolean success = false;
            try {

                conn = getUnisolatedConnection();

                final RDFParser rdfParser = rdfParserFactory.getParser();

                final AtomicLong nmodified = new AtomicLong(0L);

                rdfParser.setValueFactory(conn.getTripleStore()
                        .getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new RemoveStatementHandler(conn
                        .getSailConnection(), nmodified, defaultContext));

                /*
                 * Run the parser, which will cause statements to be deleted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                success = true;
                
                final long elapsed = System.currentTimeMillis() - begin;

                reportModifiedCount(nmodified.get(), elapsed);

                return null;
                
            } finally {

                if (conn != null) {

                    if (!success)
                        conn.rollback();

                    conn.close();

                }
                
            }

        }
        
    }

    /**
     * Helper class removes statements from the sail as they are visited by a parser.
     */
    static class RemoveStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        private final Resource[] defaultContext;
        
        public RemoveStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified, final Resource... defaultContext) {
            this.conn = conn;
            this.nmodified = nmodified;
            final boolean quads = conn.getTripleStore().isQuads();
            if (quads && defaultContext != null) {
                // The context may only be specified for quads.
                this.defaultContext = defaultContext; //new Resource[] { defaultContext };
            } else {
                this.defaultContext = new Resource[0];
            }
        }

        @Override
        public void handleStatement(final Statement stmt)
                throws RDFHandlerException {

        	final Resource[] c = (Resource[]) 
        			(stmt.getContext() == null 
        			?  defaultContext
                    : new Resource[] { stmt.getContext() }); 
        	
            try {

                conn.removeStatements(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        c
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            if (c.length >= 2) {
                // removed from more than one context
                nmodified.addAndGet(c.length);
            } else {
                nmodified.incrementAndGet();
            }

        }

    }
    
    /**
     * Delete all statements described by an access path.
     */
    private void doDeleteWithAccessPath(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final String namespace = getNamespace(req);

        final Resource s;
        final URI p;
        final Value o;
        final Resource[] c;
        try {
            s = EncodeDecodeValue.decodeResource(req.getParameter("s"));
            p = EncodeDecodeValue.decodeURI(req.getParameter("p"));
            o = EncodeDecodeValue.decodeValue(req.getParameter("o"));
//            c = EncodeDecodeValue.decodeResource(req.getParameter("c"));
        	c = EncodeDecodeValue.decodeResources(req.getParameterValues("c"));
        } catch (IllegalArgumentException ex) {
            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    ex.getLocalizedMessage());
            return;
        }
        
        if (log.isInfoEnabled())
            log.info("DELETE-WITH-ACCESS-PATH: (s=" + s + ", p=" + p + ", o="
                    + o + ", c=" + c + ")");

        try {

            submitApiTask(
                    new DeleteWithAccessPathTask(req, resp, namespace,
                            ITx.UNISOLATED, s, p, o, c)).get();

        } catch (Throwable t) {

            throw BigdataRDFServlet.launderThrowable(t, resp,
                    "DELETE-WITH-ACCESS-PATH: (s=" + s + ",p=" + p + ",o=" + o
                            + ",c=" + c + ")");

        }

    }

//    static private transient final Resource[] nullArray = new Resource[]{};
    
    private static class DeleteWithAccessPathTask extends AbstractRestApiTask<Void> {

        private Resource s;
        private URI p;
        private final Value o;
        private final Resource[] c;

        /**
         * 
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         * @param baseURI
         *            The base URI for the operation.
         * @param defaultContext
         *            The context(s) for triples without an explicit named graph
         *            when the KB instance is operating in a quads mode.
         * @param rdfParserFactory
         *            The factory for the {@link RDFParser}. This should have
         *            been chosen based on the caller's knowledge of the
         *            appropriate content type.
         */
        public DeleteWithAccessPathTask(final HttpServletRequest req,
                final HttpServletResponse resp, //
                final String namespace, final long timestamp,//
                final Resource s, final URI p, final Value o, final Resource[] c) {
            super(req, resp, namespace, timestamp);
            this.s = s;
            this.p = p;
            this.o = o;
            this.c = c;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        @Override
        public Void call() throws Exception {

            final long begin = System.currentTimeMillis();

            BigdataSailRepositoryConnection conn = null;
            boolean success = false;
            try {

                conn = getUnisolatedConnection();

                // Remove all statements matching that access path.
                // final long nmodified = conn.getSailConnection()
                // .getBigdataSail().getDatabase()
                // .removeStatements(s, p, o, c);

                // Remove all statements matching that access path.
                long nmodified = 0;
                if (c != null && c.length > 0) {
                    for (Resource r : c) {
                        nmodified += conn.getSailConnection().getBigdataSail()
                                .getDatabase().removeStatements(s, p, o, r);
                    }
                } else {
                    nmodified += conn.getSailConnection().getBigdataSail()
                            .getDatabase().removeStatements(s, p, o, null);
                }

                // Commit the mutation.
                conn.commit();

                success = true;

                final long elapsed = System.currentTimeMillis() - begin;

                reportModifiedCount(nmodified, elapsed);

                return null;

            } finally {

                if (conn != null) {

                    if (!success)
                        conn.rollback();

                    conn.close();

                }

            }

        }
        
    }

}
