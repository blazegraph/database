package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.sail.SailException;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.store.AbstractTripleStore;

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
    
	static private final transient Logger log = Logger.getLogger(DeleteServlet.class); 

    public DeleteServlet() {
        
//        getContext().registerServlet(this);

    }

   /**
     * <pre>
     * DELETE [/namespace/NAMESPACE] ?query=...
     * </pre>
     * <p>
     * Where <code>query</code> is a CONSTRUCT or DESCRIBE query. Statements are
     * materialized using the query from the addressed namespace are deleted
     * from that namespace.
     * </p>
     */
	@Override
	protected void doDelete(final HttpServletRequest req,
			final HttpServletResponse resp) {

        final String queryStr = req.getRequestURI();

        if (queryStr != null) {
            
            doDeleteWithQuery(req, resp);
            
        } else {
  
        	resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        	
        }
           
    }

     /**
      * Delete all statements materialized by a DESCRIBE or CONSTRUCT query.
      * <p>
      * Note: To avoid materializing the statements, this runs the query against
      * the last commit time. This is done while it is holding the unisolated
      * connection which prevents concurrent modifications. Therefore the entire
      * SELECT + DELETE operation is ACID.
      */
     private void doDeleteWithQuery(final HttpServletRequest req, final HttpServletResponse resp) {
         
         final String baseURI = "";// @todo baseURI query parameter?
         
         final String namespace = getNamespace(req.getRequestURI());
         
         final String queryStr = req.getParameter("query");

         if(queryStr == null)
             throw new UnsupportedOperationException();
                 
         if (log.isInfoEnabled())
             log.info("delete with query: "+queryStr);
         
         try {

             // resolve the default namespace.
             final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                     .getResourceLocator().locate(namespace, ITx.UNISOLATED);

             if (tripleStore == null) {
             	buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                         "Not found: namespace=" + namespace);
             	return;
             }

             /*
              * Note: pipe is drained by this thread to consume the query
              * results, which are the statements to be deleted.
              */
             final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             try {

                final AbstractQueryTask queryTask = getBigdataRDFContext()
                        .getQueryTask(namespace, ITx.READ_COMMITTED, queryStr,
                                req, bos);

                 switch (queryTask.queryType) {
                 case DESCRIBE:
                 case CONSTRUCT:
                     break;
                 default:
                 	buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                             "Must be DESCRIBE or CONSTRUCT query.");
                 	return;
                 }
                 
                 // invoke query, writing statements into temporary OS
                 queryTask.call();

                 final AtomicLong nmodified = new AtomicLong(0L);

                 // Wrap with SAIL.
                 final BigdataSail sail = new BigdataSail(tripleStore);
                 BigdataSailConnection conn = null;
                 try {

                     sail.initialize();
                     
                     // get the unisolated connection.
                     conn = sail.getConnection();

                     final RDFXMLParser rdfParser = new RDFXMLParser(
                             tripleStore.getValueFactory());

                     rdfParser.setVerifyData(false);
                     
                     rdfParser.setStopAtFirstError(true);
                     
                     rdfParser
                             .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                     rdfParser.setRDFHandler(new RemoveStatementHandler(conn, nmodified));

                     /*
                      * Run the parser, which will cause statements to be
                      * deleted.
                      */
                     rdfParser.parse(new ByteArrayInputStream(bos.toByteArray()), baseURI);

                     // Commit the mutation.
                     conn.commit();

                 } finally {

                     if (conn != null)
                         conn.close();

//                     sail.shutDown();

                 }

                 buildResponse(resp, HTTP_OK, MIME_TEXT_PLAIN, nmodified.get()
                         + " statements modified.");

             } catch (Throwable t) {

                 throw BigdataRDFServlet.launderThrowable(t, resp.getOutputStream(), queryStr);

             }

         } catch (Exception ex) {

             // Will be rendered as an INTERNAL_ERROR.
             throw new RuntimeException(ex);

         }

     }

	/**
	 * <pre>
	 * POST [/namespace/NAMESPACE]
	 * ...
	 * Content-Type
	 * ...
	 * 
	 * BODY
	 * 
	 * </pre>
	 * <p>
	 * BODY contains RDF statements according to the specified Content-Type.
	 * Statements parsed from the BODY are deleted from the addressed namespace.
	 * </p>
	 * <p>
	 * Note: Most client APIs do not permit a message body to be sent with a
	 * DELETE request.
	 */
     @Override
     protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) {

         final String contentType = req.getContentType();
         
         final String queryStr = req.getRequestURI();

         if (contentType != null) {
         
             doDeleteWithBody(req, resp);
             
         } else if (queryStr != null) {
             
             doDeleteWithQuery(req, resp);
             
         } else {
         	resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
         }
            
     }

    /**
     * DELETE request with a request body containing the statements to be
     * removed.
     */
    private void doDeleteWithBody(final HttpServletRequest req, final HttpServletResponse resp) {

        final String baseURI = "";// @todo baseURI query parameter?
        
        final String namespace = getNamespace(req.getRequestURI());

        final String contentType = req.getContentType();

        if (contentType == null)
            throw new UnsupportedOperationException();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        try {

            // resolve the default namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null) {
            	buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Not found: namespace=" + namespace);
            	return;
            }

            final AtomicLong nmodified = new AtomicLong(0L);

            // Wrap with SAIL.
            final BigdataSail sail = new BigdataSail(tripleStore);
            BigdataSailConnection conn = null;
            try {

                sail.initialize();
                conn = sail.getConnection();

                /*
                 * There is a request body, so let's try and parse it.
                 */

                final RDFFormat format = RDFFormat.forMIMEType(contentType);

                if (format == null) {
                	buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                            "Content-Type not recognized as RDF: "
                                    + contentType);
                	
                	return;
                }

                final RDFParserFactory rdfParserFactory = RDFParserRegistry
                        .getInstance().get(format);

                if (rdfParserFactory == null) {
                	buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            "Parser not found: Content-Type=" + contentType);
                	return;
                }

                final RDFParser rdfParser = rdfParserFactory.getParser();

                rdfParser.setValueFactory(tripleStore.getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new RemoveStatementHandler(conn,
                        nmodified));

                /*
                 * Run the parser, which will cause statements to be deleted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                buildResponse(resp, HTTP_OK, MIME_TEXT_PLAIN, nmodified.get()
                        + " statements modified.");

            } finally {

                if (conn != null)
                    conn.close();

//                sail.shutDown();

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);
            
        }

    }
    
    /**
     * Helper class removes statements from the sail as they are visited by a parser.
     */
    private static class RemoveStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        
        public RemoveStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified) {
            this.conn = conn;
            this.nmodified = nmodified;
        }

        Resource[] nullArray = new Resource[]{};
        public void handleStatement(Statement stmt) throws RDFHandlerException {

            try {
            	Resource context = stmt.getContext();
            	
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
    
}
