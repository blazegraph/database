package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.concurrent.FutureTask;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

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
             * Note: This is run on an ExecutorService with a configured thread
             * pool size so we can avoid running too many queries concurrently.
             */

            // Setup the response.
            // TODO Move charset choice into conneg logic.
            buildResponse(resp, HTTP_OK, queryTask.mimeType + "; charset='" + charset + "'");
			
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

