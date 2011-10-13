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
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.DeleteServlet.RemoveStatementHandler;
import com.bigdata.rdf.sail.webapp.InsertServlet.AddStatementHandler;

/**
 * Handler for UPDATE operations (PUT).
 * 
 * @author martyncutcher
 */
public class UpdateServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(UpdateServlet.class);

    public UpdateServlet() {

    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        final String queryStr = req.getParameter("query");

        final String contentType = req.getContentType();

        if(contentType == null) {
            
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        
        }   
        
        if(queryStr == null) {
            
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        
        }   
        
        doUpdateWithQuery(req, resp);
        
    }

    /**
     * Delete all statements materialized by a DESCRIBE or CONSTRUCT query and
     * then insert all statements in the request body.
     * <p>
     * Note: To avoid materializing the statements, this runs the query against
     * the last commit time and uses a pipe to connect the query directly to the
     * process deleting the statements. This is done while it is holding the
     * unisolated connection which prevents concurrent modifications. Therefore
     * the entire SELECT + DELETE operation is ACID.
     */
    private void doUpdateWithQuery(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final long begin = System.currentTimeMillis();
        
        final String baseURI = req.getRequestURL().toString();

        final String namespace = getNamespace(req);

        final String queryStr = req.getParameter("query");

        if (queryStr == null)
            throw new UnsupportedOperationException();

        final String contentType = req.getContentType();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        final RDFFormat requestBodyFormat = RDFFormat.forMIMEType(contentType);

        if (requestBodyFormat == null) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as RDF: " + contentType);

            return;

        }

        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(requestBodyFormat);

        if (rdfParserFactory == null) {

            buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                    "Parser factory not found: Content-Type="
                            + contentType + ", format=" + requestBodyFormat);
            
            return;

        }

        /*
         * Allow the caller to specify the default context.
         */
        final Resource defaultContext;
        {
            final String s = req.getParameter("context-uri");
            if (s != null) {
                try {
                    defaultContext = new URIImpl(s);
                } catch (IllegalArgumentException ex) {
                    buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContext = null;
            }
        }

        if (log.isInfoEnabled())
            log.info("update with query: " + queryStr);

        try {

            /*
             * Note: pipe is drained by this thread to consume the query
             * results, which are the statements to be deleted.
             */
            final PipedOutputStream os = new PipedOutputStream();
            final InputStream is = newPipedInputStream(os);
            try {

                // Use this format for the query results.
                final RDFFormat deleteQueryFormat = RDFFormat.NTRIPLES;
                
                final AbstractQueryTask queryTask = getBigdataRDFContext()
                        .getQueryTask(namespace, ITx.READ_COMMITTED, queryStr,
                                deleteQueryFormat.getDefaultMIMEType(),
                                req, os);

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

                    // Run DELETE
                    {

                        final RDFParserFactory factory = RDFParserRegistry
                                .getInstance().get(deleteQueryFormat);

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
                        getBigdataRDFContext().queryService.execute(ft);

                        // Run parser : visited statements will be deleted.
                        rdfParser.parse(is, baseURI);

                        // Await the Future (of the Query)
                        ft.get();
                        
                    }

                    // Run INSERT
                    {
                        
                        /*
                         * There is a request body, so let's try and parse it.
                         */

                        final RDFParser rdfParser = rdfParserFactory
                                .getParser();

                        rdfParser.setValueFactory(conn.getTripleStore()
                                .getValueFactory());

                        rdfParser.setVerifyData(true);

                        rdfParser.setStopAtFirstError(true);

                        rdfParser
                                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                        rdfParser.setRDFHandler(new AddStatementHandler(conn
                                .getSailConnection(), nmodified, defaultContext));

                        /*
                         * Run the parser, which will cause statements to be
                         * inserted.
                         */
                        rdfParser.parse(req.getInputStream(), baseURI);

                    }

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

}
