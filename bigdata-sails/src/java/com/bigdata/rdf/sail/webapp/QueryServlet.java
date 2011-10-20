package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryLog;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.util.HTMLUtility;
import com.bigdata.util.InnerCause;

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

        if (req.getParameter("query") != null) {
            doQuery(req, resp);
        } else if (req.getParameter("ESTCARD") != null) {
            doEstCard(req, resp);
        } else {
            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN);
            return;
        }
        
    }

    /**
     * Return <code>true</code> if the <code>Content-disposition</code> header
     * should be set to indicate that the response body should be handled as an
     * attachment rather than presented inline. This is just a hint to the user
     * agent. How the user agent handles this hint is up to it.
     * 
     * @param mimeType
     *            The mime type.
     *            
     * @return <code>true</code> if it should be handled as an attachment.
     */
    private boolean isAttachment(final String mimeType) {
        if(mimeType.equals(MIME_TEXT_PLAIN)) {
            return false;
        } else if(mimeType.equals(MIME_SPARQL_RESULTS_XML)) {
            return false;
        } else if(mimeType.equals(MIME_SPARQL_RESULTS_JSON)) {
            return false;
        } else if(mimeType.equals(MIME_APPLICATION_XML)) {
            return false;
        }
        return true;
    }
    
    /**
     * Run a SPARQL query.
     */
    private void doQuery(final HttpServletRequest req,
                final HttpServletResponse resp) throws IOException {

    	final String namespace = getNamespace(req);

		final long timestamp = getTimestamp(req);

		// The SPARQL query.
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

			final OutputStream os = resp.getOutputStream();
			
			final BigdataRDFContext context = getBigdataRDFContext();
			
			final boolean explain = req.getParameter(BigdataRDFContext.EXPLAIN) != null;

			final AbstractQueryTask queryTask;
            try {
                /*
                 * Attempt to construct a task which we can use to evaluate the
                 * query.
                 */
                queryTask = context.getQueryTask(namespace, timestamp,
                        queryStr, null/*acceptOverride*/, req, os);
            } catch (MalformedQueryException ex) {
                /*
                 * Send back a BAD REQUEST (400) along with the text of the
                 * syntax error message.
                 */
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ex
                        .getLocalizedMessage());
                return;
            }

            final FutureTask<Void> ft = new FutureTask<Void>(queryTask);

            if (log.isTraceEnabled())
                log.trace("Will run query: " + queryStr);

            /*
             * Setup the response headers.
             */

            resp.setStatus(HTTP_OK);

            resp.setContentType(queryTask.mimeType);

            if (queryTask.charset != null) {

                // Note: Binary encodings do not specify charset.
                resp.setCharacterEncoding(queryTask.charset.name());
                
            }

            if (isAttachment(queryTask.mimeType)) {
                /*
                 * Mark this as an attachment (rather than inline). This is just
                 * a hint to the user agent. How the user agent handles this
                 * hint is up to it.
                 */
                resp.setHeader("Content-disposition",
                        "attachment; filename=query" + queryTask.queryId + "."
                                + queryTask.fileExt);
            }

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

			if (explain) {
				final Writer w = new OutputStreamWriter(os, queryTask.charset);
				try {
					// Send an explanation instead of the query results.
					explainQuery(queryStr, queryTask, ft, w);
				} finally {
					w.flush();
					w.close();
					os.flush();
					os.close();
				}
			} else {
				// Wait for the Future.
				ft.get();
			}

		} catch (Throwable e) {
			try {
				throw BigdataRDFServlet.launderThrowable(e, resp, queryStr);
			} catch (Exception e1) {
				throw new RuntimeException(e);
			}
		}
	
	}

	/**
	 * Sends an explanation for the query rather than the query results. The
	 * query is still run, but the query statistics are reported instead of the
	 * query results.
	 * 
	 * @param queryStr
	 * @param queryTask
	 * @param ft
	 * @param os
	 * @throws Exception
	 */
	private void explainQuery(final String queryStr,
			final AbstractQueryTask queryTask, final FutureTask<Void> ft,
			final Writer w) throws Exception {
		
		/*
		 * Spin until either we have the UUID of the IRunningQuery or the Future
		 * of the query is done.
		 */
		if(log.isDebugEnabled())
			log.debug("Will build explanation");
		UUID queryId2 = null;
		IRunningQuery q = null;
		while (!ft.isDone() && queryId2 == null) {
			try {
				// Wait a bit for queryId2 to be assigned.
				ft.get(1/* timeout */, TimeUnit.MILLISECONDS);
			} catch(TimeoutException ex) {
				// Ignore.
			}
			if (queryTask.queryId2 != null) {
				// Got it.
				queryId2 = queryTask.queryId2;
				break;
			}
		}

		if(ft.isDone()) {
			/*
			 * If the query is done, the check for an error before we build up
			 * the explanation document.
			 */
			ft.get();
			
			/*
			 * No error and the Future is done. The UUID of the IRunningQuery
			 * MUST have been assigned. If we do not have it yet, then check
			 * once more. If it is not set then that is an error.
			 */
			if (queryTask.queryId2 != null) {
				// Check once more. 
				queryId2 = queryTask.queryId2;
				if (queryId2 == null) {
					/*
					 * This should have been assigned unless the query failed
					 * during the setup.
					 */
					throw new AssertionError();
				}
			}
		}
		assert queryId2 != null;

		/*
		 * Build the explanation.
		 * 
		 * Note: The query may still be executing while we do this.
		 */
		final HTMLBuilder doc = new HTMLBuilder(queryTask.charset.name(), w);
		{

			XMLBuilder.Node current = doc.root("html");
			{
				current = current.node("head");
				current.node("meta").attr("http-equiv", "Content-Type")
						.attr("content", "text/html;charset=utf-8").close();
				current.node("title").text("bigdata&#174;").close();
				current = current.close();// close the head.
			}
			current = current.node("body");

			// TODO Redundant if using native SPARQL evaluation.
			current.node("h2", "SPARQL").node("p",
					HTMLUtility.escapeForXHTML(queryTask.queryStr));

			current.node("h2", "Parsed Query").node("pre",
					HTMLUtility.escapeForXHTML(queryTask.sailQuery.toString()));
			
			/*
			 * Spin until we get the IRunningQuery reference or the query is
			 * done, in which case we won't get it.
			 */
			if (queryId2 != null) {
				if(log.isDebugEnabled())
					log.debug("Resolving IRunningQuery: queryId2=" + queryId2);
				final IIndexManager indexManager = getBigdataRDFContext()
						.getIndexManager();
				final QueryEngine queryEngine = QueryEngineFactory
						.getQueryController(indexManager);
				while (!ft.isDone() && q == null) {
					try {
						// Wait a bit for the IRunningQuery to *start*.
						ft.get(1/* timeout */, TimeUnit.MILLISECONDS);
					} catch(TimeoutException ex) {
						// Ignore.
					}
					// Resolve the IRunningQuery.
					try {
						q = queryEngine.getRunningQuery(queryId2);
					} catch (RuntimeException ex) {
						if (InnerCause.isInnerCause(ex, InterruptedException.class)) {
							// Ignore. Query terminated normally, but we don't have
							// it.
						} else {
							// Ignore. Query has error, but we will get err from
							// Future.
						}
					}
				}
				if (q != null)
					if(log.isDebugEnabled())
						log.debug("Resolved IRunningQuery: query=" + q);
			}
			
			// wait for the Future (will toss any exceptions).
			ft.get();

			{
				current.node("h2",
						"Query Evaluation Statistics").node("p");
				if (q != null) {

					current.node("h2", "BOP Plan").node(
							"pre",
							HTMLUtility.escapeForXHTML(BOpUtility
									.toString(q.getQuery())));

					/*
					 * Format query statistics as a table.
					 * 
					 * Note: This is writing on the Writer so it goes directly
					 * into the HTML document we are building for the client.
					 */
					QueryLog.getTableXHTML(queryStr, q, w,
							false/* summaryOnly */, 0/* maxBopLength */);

//					// Add into the HTML document.
//					statsNode.text(w.toString());
				} else {
					/*
					 * This can happen if we fail to get the IRunningQuery
					 * reference before the query terminates.  E.g., if the
					 * query runs too quickly there is a data race and the
					 * reference may not be available anymore.
					 */
					current
							.text("Not available.");
				}
			}
			doc.closeAll(current);
		}

//		/*
//		 * Send the response.
//		 * 
//		 * TODO It would be better to stream this rather than buffer it in
//		 * RAM. That also opens up the opportunity for real-time updates for
//		 * long-running (analytic) queries, incremental information from the
//		 * runtime query optimizer, etc.
//		 */
//		if(log.isDebugEnabled())
//			log.debug("Sending explanation.");
//		os.write(doc.toString().getBytes("UTF-8"));
//		if(log.isDebugEnabled())
//			log.debug("Sent explanation.");

    }
    
	/**
	 * Estimate the cardinality of an access path (fast range count).
	 * @param req
	 * @param resp
	 */
    private void doEstCard(final HttpServletRequest req,
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
            log.info("ESTCARD: access path: (s=" + s + ", p=" + p + ", o="
                    + o + ", c=" + c + ")");

        try {

            try {

                BigdataSailRepositoryConnection conn = null;
                try {

                    final long timestamp = getTimestamp(req);

                    conn = getBigdataRDFContext().getQueryConnection(
                            namespace, timestamp);

                    // Range count all statements matching that access path.
                    final long rangeCount = conn.getSailConnection()
                            .getBigdataSail().getDatabase()
                            .getAccessPath(s, p, o, c)
                            .rangeCount(false/* exact */);
                    
                    final long elapsed = System.currentTimeMillis() - begin;
                    
                    reportRangeCount(resp, rangeCount, elapsed);

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

}
