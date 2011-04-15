package com.bigdata.rdf.sail.webapp;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

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
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
		try {
			if (req.getParameter("uri") != null) {
				doPostWithURIs(req, resp);
				return;
			} else {
				doPostWithBody(req, resp);
				return;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
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
			final HttpServletResponse resp) throws Exception {

	    final long begin = System.currentTimeMillis();
	    
        final String baseURI = req.getRequestURL().toString();
        
        final String namespace = getNamespace(req);

        final String contentType = req.getContentType();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        final RDFFormat format = RDFFormat.forMIMEType(contentType);

        if (format == null) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as RDF: " + contentType);

            return;

        }

        if (log.isInfoEnabled())
            log.info("RDFFormat=" + format);
        
        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(format);

        if (rdfParserFactory == null) {

            buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                    "Parser factory not found: Content-Type="
                            + contentType + ", format=" + format);
        	
        	return;

        }

        try {
            
            final AtomicLong nmodified = new AtomicLong(0L);

            BigdataSailRepositoryConnection conn = null;
            try {

                conn = getBigdataRDFContext()
                        .getUnisolatedConnection(namespace);

                /*
                 * There is a request body, so let's try and parse it.
                 */

                final RDFParser rdfParser = rdfParserFactory.getParser();

                rdfParser.setValueFactory(conn.getTripleStore()
                        .getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new AddStatementHandler(conn
                        .getSailConnection(), nmodified));

                /*
                 * Run the parser, which will cause statements to be inserted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                final long elapsed = System.currentTimeMillis() - begin;
                
                reportModifiedCount(resp, nmodified.get(), elapsed);
                
                return;

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
			final HttpServletResponse resp) throws Exception {

	    final long begin = System.currentTimeMillis();
	    
		final String namespace = getNamespace(req);

		final String[] uris = req.getParameterValues("uri");

		if (uris == null)
			throw new UnsupportedOperationException();

		if (uris.length == 0) {

            reportModifiedCount(resp, 0L/* nmodified */, System
                    .currentTimeMillis()
                    - begin);

        	return;
        	
        }

        if (log.isInfoEnabled())
            log.info("URIs: " + Arrays.toString(uris));

        // Before we do anything, make sure we have valid URLs.
        final Vector<URL> urls = new Vector<URL>(uris.length);
 
        for (String uri : uris) {
        
            urls.add(new URL(uri));
            
        }

        try {

            final AtomicLong nmodified = new AtomicLong(0L);

            BigdataSailRepositoryConnection conn = null;
            try {

                conn = getBigdataRDFContext().getUnisolatedConnection(
                        namespace);

                for (URL url : urls) {

                    URLConnection hconn = null;
                    try {

                        hconn = url.openConnection();
                        if (hconn instanceof HttpURLConnection) {
                            ((HttpURLConnection) hconn).setRequestMethod("GET");
                        }
                        hconn.setDoInput(true);
                        hconn.setDoOutput(false);
                        hconn.setReadTimeout(0);// no timeout? http param?

                        /*
                         * There is a request body, so let's try and parse it.
                         */

                        final String contentType = hconn.getContentType();

                        final RDFFormat format = RDFFormat
                                .forMIMEType(contentType);

                        if (format == null) {
                        	buildResponse(resp, HTTP_BADREQUEST,
                                    MIME_TEXT_PLAIN,
                                    "Content-Type not recognized as RDF: "
                                            + contentType);
                        	
                        	return;
                        }

                        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                                .getInstance().get(format);

                        if (rdfParserFactory == null) {
                        	buildResponse(resp, HTTP_INTERNALERROR,
                                    MIME_TEXT_PLAIN,
                                    "Parser not found: Content-Type="
                                            + contentType);
                        	
                        	return;
                        }

                        final RDFParser rdfParser = rdfParserFactory
                                .getParser();

                        rdfParser.setValueFactory(conn.getTripleStore()
                                .getValueFactory());

                        rdfParser.setVerifyData(true);

                        rdfParser.setStopAtFirstError(true);

                        rdfParser
                                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                        rdfParser.setRDFHandler(new AddStatementHandler(conn
                                .getSailConnection(), nmodified));

                        /*
                         * Run the parser, which will cause statements to be
                         * inserted.
                         */

                        rdfParser.parse(hconn.getInputStream(), url
                                .toExternalForm()/* baseURL */);

                    } finally {

                        if (hconn instanceof HttpURLConnection) {
                            /*
                             * Disconnect, but only after we have loaded all the
                             * URLs. Disconnect is optional for java.net. It is a
                             * hint that you will not be accessing more resources on
                             * the connected host. By disconnecting only after all
                             * resources have been loaded we are basically assuming
                             * that people are more likely to load from a single
                             * host.
                             */
                            ((HttpURLConnection) hconn).disconnect();
                        }

                    }
                    
                    } // next URI.

                // Commit the mutation.
                conn.commit();
                
                final long elapsed = System.currentTimeMillis();

                reportModifiedCount(resp, nmodified.get(), elapsed);

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
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    private static class AddStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        
        public AddStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified) {
            this.conn = conn;
            this.nmodified = nmodified;
        }

        public void handleStatement(Statement stmt) throws RDFHandlerException {

            try {

                conn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        (Resource[]) (stmt.getContext() == null ? new Resource[] { }
                                : new Resource[] { stmt.getContext() })//
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            nmodified.incrementAndGet();

        }

    }

}
