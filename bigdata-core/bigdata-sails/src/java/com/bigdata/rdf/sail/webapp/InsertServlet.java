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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
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
import com.bigdata.rdf.sail.webapp.client.MiniMime;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUpdate;

/**
 * Handler for INSERT operations.
 * 
 * @author martyncutcher
 */
public class InsertServlet extends BigdataRDFServlet {
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(InsertServlet.class); 

    public InsertServlet() {
        
    }

	/**
	 * <p>
	 * Perform an HTTP-POST, which corresponds to the basic CRUD operation
	 * "create" according to the generic interaction semantics of HTTP REST. The
	 * operation will be executed against the target namespace per the URI.
	 * </p>
	 * 
	 * <pre>
	 * POST [/namespace/NAMESPACE]
	 * ...
	 * Content-Type: 
	 * ...
	 * 
	 * BODY
	 * </pre>
	 * <p>
	 * Where <code>BODY</code> is the new RDF content using the representation
	 * indicated by the <code>Content-Type</code>.
	 * </p>
	 * <p>
	 * -OR-
	 * </p>
	 * 
	 * <pre>
	 * POST [/namespace/NAMESPACE] ?uri=URL
	 * </pre>
	 * <p>
	 * Where <code>URI</code> identifies a resource whose RDF content will be
	 * inserted into the database. The <code>uri</code> query parameter may
	 * occur multiple times. All identified resources will be loaded within a
	 * single native transaction. Bigdata provides snapshot isolation so you can
	 * continue to execute queries against the last commit point while this
	 * operation is executed.
	 * </p>
	 */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        if (req.getParameter(BigdataRDFContext.URI) != null) {
            doPostWithURIs(req, resp);
            return;
        } else {
            doPostWithBody(req, resp);
            return;
        }
	}

	/**
	 * POST with request body containing statements to be inserted.
	 * 
	 * @param req
	 *            The request.
	 * 
	 * @return The response.
	 * 
	 * @throws Exception
	 */
	private void doPostWithBody(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {
	    
        final String baseURI = req.getRequestURL().toString();
        
        final String contentType = req.getContentType();

        if (contentType == null)
            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not specified.");
        
        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        /**
         * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/620">
         * UpdateServlet fails to parse MIMEType when doing conneg. </a>
         */

		final String mimeTypeStr = new MiniMime(contentType).getMimeType();

		final RDFFormat format = RDFFormat.forMIMEType(mimeTypeStr);

        if (format == null) {

            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as RDF: " + contentType);

            return;

        }

        if (log.isInfoEnabled())
            log.info("RDFFormat=" + format);
        
        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(format);

        if (rdfParserFactory == null) {

            buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                    "Parser factory not found: Content-Type=" + contentType
                            + ", format=" + format);

            return;

        }

        /*
         * Allow the caller to specify the default contexts.
         */
        final Resource[] defaultContext;
        {
            final String[] s = req.getParameterValues(BigdataRDFContext.CONTEXT_URI);
            if (s != null && s.length > 0) {
                try {
                	defaultContext = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContext = new Resource[0];
            }
        }

        try {
            
            submitApiTask(
                    new InsertWithBodyTask(req, resp, getNamespace(req),
                            ITx.UNISOLATED, baseURI, defaultContext,
                            rdfParserFactory)).get();
            
        } catch (Throwable t) {

         BigdataRDFServlet.launderThrowable(t, resp,
               "INSERT-WITH-BODY: baseURI=" + baseURI + ", Content-Type="
                     + contentType + ", " + BigdataRDFContext.CONTEXT_URI + "="
                     + Arrays.toString(defaultContext));

        }

    }

    /**
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * 
	 * TODO #1056 (Add ability to set RIO options to REST API and workbench)
	 */
    private static class InsertWithBodyTask extends AbstractRestApiTask<Void> {

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
        public InsertWithBodyTask(final HttpServletRequest req,
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

            final AtomicLong nmodified = new AtomicLong(0L);

            BigdataSailRepositoryConnection conn = null;
            boolean success = false;
            try {

                conn = getConnection();

                /**
                 * There is a request body, so let's try and parse it.
                 * 
                 * FIXME This does not handle .gz or .zip files. We handle this
                 * in the
                 * 
                 * @see <a href="http://trac.blazegraph.com/ticket/991" >REST API:
                 *      INSERT does not handle .gz</a>
                 */

                final RDFParser rdfParser = rdfParserFactory.getParser();

                rdfParser.setValueFactory(conn.getTripleStore()
                        .getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new AddStatementHandler(conn
                        .getSailConnection(), nmodified, defaultContext));

                /*
                 * Run the parser, which will cause statements to be inserted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                success = true;

                final long elapsed = System.currentTimeMillis() - begin;

                reportModifiedCount(nmodified.get(), elapsed);

                return (Void) null;

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
	 * POST with URIs of resources to be inserted (loads the referenced
	 * resources).
	 * 
	 * @param req
	 *            The request.
	 * 
	 * @return The response.
	 * 
	 * @throws Exception
	 */
	private void doPostWithURIs(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {
	    
		final String namespace = getNamespace(req);

		final String[] uris = req.getParameterValues(BigdataRDFContext.URI);

      if (uris == null || uris.length == 0) {

         buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
               MIME_TEXT_PLAIN,
               "Parameter must be specified one or more times: '"
                     + BigdataRDFContext.URI + "'");

         return;

      }

        if (log.isInfoEnabled())
            log.info("URIs: " + Arrays.toString(uris));

        // Before we do anything, make sure we have valid URLs.
        final Vector<URL> urls = new Vector<URL>(uris.length);
 
        for (String uri : uris) {
        
            urls.add(new URL(uri));
            
        }

        /*
         * Allow the caller to specify the default contexts.
         */
        final Resource[] defaultContext;
        {
            final String[] s = req.getParameterValues(BigdataRDFContext.CONTEXT_URI);
            if (s != null && s.length > 0) {
                try {
                	defaultContext = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContext = new Resource[0];
            }
        }

        try {

            submitApiTask(
                    new InsertWithURLsTask(req, resp, namespace,
                            ITx.UNISOLATED, defaultContext, urls)).get();

        } catch (Throwable t) {

         launderThrowable(
               t,
               resp,
               BigdataRDFContext.URI + "=" + urls + ", "
                     + BigdataRDFContext.CONTEXT_URI + "="
                     + Arrays.toString(defaultContext));

        }

    }
	
    private static class InsertWithURLsTask extends AbstractRestApiTask<Void> {

        private final Vector<URL> urls;
        private final Resource[] defaultContext;

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
         * @param urls
         *            The {@link URL}s whose contents will be parsed and loaded
         *            into the target KB.
         */
        public InsertWithURLsTask(final HttpServletRequest req,
                final HttpServletResponse resp, final String namespace,
                final long timestamp, final Resource[] defaultContext,
                final Vector<URL> urls) {
            super(req, resp, namespace, timestamp);
            this.urls = urls;
            this.defaultContext = defaultContext;
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

                conn = getConnection();

                final AtomicLong nmodified = new AtomicLong(0L);

                for (URL url : urls) {

                        // Use the default context if one was given and otherwise
                        // the URI from which the data are being read.
//                        final Resource defactoContext = defaultContext == null ? new URIImpl(
//                                url.toExternalForm()) : defaultContext;
                    final Resource[] defactoContext = defaultContext.length == 0 ? new Resource[] { new URIImpl(
                            url.toExternalForm()) } : defaultContext;

                    URLConnection hconn = null;
                    try {

                        hconn = url.openConnection();
                        if (hconn instanceof HttpURLConnection) {
                            ((HttpURLConnection) hconn).setRequestMethod("GET");
                        }
                        hconn.setDoInput(true);
                        hconn.setDoOutput(false);
                        hconn.setReadTimeout(0);// no timeout? http param?

                        /**
                         * There is a request body, so let's try and parse it.
                         * 
                         * @see <a href=
                         *      "https://sourceforge.net/apps/trac/bigdata/ticket/620"
                         *      > UpdateServlet fails to parse MIMEType when
                         *      doing conneg. </a>
                         * 
                         *      FIXME This does not handle .gz or .zip files. We
                         *      handle this in the
                         * 
                         * @see <a href="http://trac.blazegraph.com/ticket/991"
                         *      >REST API: INSERT does not handle .gz</a>
                         */

                        final String contentType = hconn.getContentType();

                        RDFFormat format = RDFFormat.forMIMEType(new MiniMime(
                                contentType).getMimeType());
                        
                        final String fileName = url.getPath();

                        if (format == null) {

                            /*
                             * Try to get the RDFFormat from the URL's file
                             * path.
                             */
                        	//BLZG-1929
                            format = AST2BOpUpdate.rdfFormatForFile (fileName);

                        }

                        if (format == null) {

                            throw new HttpOperationException(HTTP_BADREQUEST,
                                    MIME_TEXT_PLAIN,
                                    "Content-Type not recognized as RDF: "
                                            + contentType);

                        }

                        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                                .getInstance().get(format);

                        if (rdfParserFactory == null) {
                        
                           throw new HttpOperationException(HTTP_INTERNALERROR,
                                    MIME_TEXT_PLAIN,
                                    "Parser not found: Content-Type="
                                            + contentType);

                        }

                        final RDFParser rdfParser = rdfParserFactory
                                .getParser();

                        rdfParser.setValueFactory(conn.getTripleStore()
                                .getValueFactory());

                        rdfParser.setVerifyData(true);

                        rdfParser.setStopAtFirstError(true);

                        rdfParser
                                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                        rdfParser
                                .setRDFHandler(new AddStatementHandler(conn
                                        .getSailConnection(), nmodified,
                                        defactoContext));

                        /*
                         * Run the parser, which will cause statements to be
                         * inserted.
                         */
                        
                        InputStream is = hconn.getInputStream();
                        
                       if(fileName.endsWith(".gz")) {
                    	   
                    	   is = new GZIPInputStream(hconn.getInputStream());
                    	   
                       } 

                        rdfParser.parse(is,
                                url.toExternalForm()/* baseURL */);

                    } finally {

                        if (hconn instanceof HttpURLConnection) {
                            /*
                             * Disconnect, but only after we have loaded all the
                             * URLs. Disconnect is optional for java.net. It is
                             * a hint that you will not be accessing more
                             * resources on the connected host. By disconnecting
                             * only after all resources have been loaded we are
                             * basically assuming that people are more likely to
                             * load from a single host.
                             */
                            ((HttpURLConnection) hconn).disconnect();
                        }

                    }

                } // next URI.

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
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    static class AddStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        private final Resource[] defaultContext;

        /**
         * 
         * @param conn
         * @param nmodified
         * @param defaultContexts
         * 			Only used if the statements themselves do not have a context.
         */
        public AddStatementHandler(final BigdataSailConnection conn,
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

                conn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        c
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            if (c.length >= 2) {
                // added to more than one context
                nmodified.addAndGet(c.length);
            } else {
                nmodified.incrementAndGet();
            }

        }

    }

}
