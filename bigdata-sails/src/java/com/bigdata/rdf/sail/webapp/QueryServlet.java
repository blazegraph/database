package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.concurrent.FutureTask;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;

/**
 * SPARQL query handler for GET or POST verbs.
 * 
 * @author martyncutcher
 * @author thompsonbry
 */
public class QueryServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(QueryServlet.class); 

    public QueryServlet() {

    }

    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        doQuery(req, resp);

    }

    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        doQuery(req, resp);
        
    }

    /**
     * Run a SPARQL query.
     * 
     * FIXME Does not handle default-graph-uri or named-graph-uri query
     * parameters.
     */
    private void doQuery(final HttpServletRequest req,
                final HttpServletResponse resp) throws IOException {

    	final String namespace = getNamespace(req.getRequestURI());

		final long timestamp = getTimestamp(req.getRequestURI(), req);

		final String queryStr = req.getParameter("query");

		if(queryStr == null) {

		    buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN, "Not found: query");
		    
		    return;
		    
		}

        /*
         * Setup task to execute the query. The task is executed on a thread
         * pool. This bounds the possible concurrency of query execution (as
         * opposed to queries accepted for eventual execution).
         * 
         * Note: If the client closes the connection, then the response's
         * InputStream will be closed and the task will terminate rather than
         * running on in the background with a disconnected client.
         */
		try {

            final AbstractQueryTask queryTask = getBigdataRDFContext()
                    .getQueryTask(namespace, timestamp, queryStr, req,
                            resp.getOutputStream());

            final FutureTask<Void> ft = new FutureTask<Void>(queryTask);

            if (log.isTraceEnabled())
                log.trace("Will run query: " + queryStr);

            /*
             * Setup the response headers.
             */

            resp.setStatus(HTTP_OK);

            // Figure out the filename extension for the response.
            
            final String ext;
            final String charset;
            
            if(queryTask.format != null) {

                /*
                 * If some RDFormat was negotiated, then construct the filename
                 * for the attachment using the default extension for that
                 * format and the queryId.
                 */
                
                ext = queryTask.format.getDefaultFileExtension();
                
                charset = queryTask.format.getCharset().name();

            } else {

                if(queryTask.mimeType.equals(MIME_SPARQL_RESULTS_XML)) {

                    // See http://www.w3.org/TR/rdf-sparql-XMLres/

                    ext = "srx"; // Sparql Result Set.
                    
                } else if(queryTask.mimeType.equals(MIME_SPARQL_RESULTS_JSON)) {

                    // See http://www.w3.org/TR/rdf-sparql-json-res/
                    
                    ext = "srj";
                    
                } else {
                    
                    ext = "xxx";
                    
                }

                charset = QueryServlet.charset;
                
            }
            
            resp.setContentType(queryTask.mimeType);
            
            resp.setCharacterEncoding(charset);

            resp.setHeader("Content-disposition", "attachment; filename=query"
                    + queryTask.queryId + "." + ext);

            if(TimestampUtility.isCommitTime(queryTask.timestamp)) {

                /*
                 * A read against a commit time or a read-only tx. Such results
                 * SHOULD be cached because the data from which the response was
                 * constructed have snapshot isolation. (Note: It is possible
                 * that the commit point against which the query reads will be
                 * aged out of database and that the query would therefore fail
                 * if it were retried. This can happen with the RWStore or in
                 * scale-out.)
                 * 
                 * Note: READ_COMMITTED requests SHOULD NOT be cached. Such
                 * requests will read against then current committed state of
                 * the database each time they are processed.
                 * 
                 * Note: UNISOLATED queries SHOULD NOT be cached. Such
                 * operations will read on (and write on) the then current state
                 * of the unisolated indices on the database each time they are
                 * processed. The results of such operations could be different
                 * with each request.
                 * 
                 * Note: Full read-write transaction requests SHOULD NOT be
                 * cached unless they are queries and the transaction scope is
                 * limited to the request (rather than running across multiple
                 * requests).
                 */

                resp.addHeader("Cache-Control", "public");
                
                // to disable caching.
                // r.addHeader("Cache-Control", "no-cache");

            }
            
            // Begin executing the query (asynchronous)
            getBigdataRDFContext().queryService.execute(ft);
            
            // wait for the Future.
            ft.get();

		} catch (Throwable e) {
			try {
				throw BigdataRDFServlet.launderThrowable(e, resp, queryStr);
			} catch (Exception e1) {
				throw new RuntimeException(e);
			}
		}
	
	}

}

