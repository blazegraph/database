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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryLog;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rdf.sail.BigdataSailQuery;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.sparql.ast.SimpleNode;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.RunningQuery;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.util.InnerCause;

/**
 * SPARQL Query (GET/POST) and SPARQL UPDATE handler (POST).
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

//    /**
//     * The name of the request attribute for the {@link AbstractQueryTask}.
//     */
//    static private final transient String ATTR_QUERY_TASK = "QueryTask";
    
    public QueryServlet() {

    }

    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (req.getParameter("update") != null) {
            
            // SPARQL 1.1 UPDATE.
            doUpdate(req, resp);
            
        } else {
            
            // SPARQL Query.
            doQuery(req, resp);
            
        }

    }

    /**
     * Handles query, ESTCARD, and SHARDS.
     */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (req.getParameter("query") != null) {
            
            doQuery(req, resp);
            
        } else if (req.getParameter("ESTCARD") != null) {
            
            doEstCard(req, resp);
            
        } else if (req.getParameter("SHARDS") != null) {
            
            doShardReport(req, resp);
            
        } else {
            
            doServiceDescription(req, resp);

            return;
            
        }
        
    }

    /**
     * Generate a SPARQL 1.1 Service Description for the addressed triple store
     * or quad store.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/500
     */
    private void doServiceDescription(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
        final String namespace = getNamespace(req);

        final long timestamp = getTimestamp(req);

        final AbstractTripleStore tripleStore = getBigdataRDFContext()
                .getTripleStore(namespace, timestamp);
        
        if (tripleStore == null) {
            /*
             * There is no such triple/quad store instance.
             */
            buildResponse(resp, HTTP_NOTFOUND, MIME_TEXT_PLAIN);
            return;
        }

        final Graph g = SD.describeService(tripleStore);

        /*
         * Add the service end point.
         * 
         * TODO Report alternative end points?
         */
        {
            final StringBuffer sb = req.getRequestURL();

            final int indexOf = sb.indexOf("?");

            final String serviceURI;
            if (indexOf == -1) {
                serviceURI = sb.toString();
            } else {
                serviceURI = sb.substring(0, indexOf);
            }

            g.add(SD.Service, SD.endpoint,
                    g.getValueFactory().createURI(serviceURI));

        }
        
        /*
         * CONNEG for the MIME type.
         * 
         * Note: An attempt to CONNEG for a MIME type which can not be used with
         * a give type of query will result in a response using a default MIME
         * Type for that query.
         * 
         * TODO We could also do RDFa embedded in XHTML.
         */
        {

            final String acceptStr = req.getHeader("Accept");

            final ConnegUtil util = new ConnegUtil(acceptStr);

            // The best RDFFormat for that Accept header. 
            RDFFormat format = util.getRDFFormat();
            
            if (format == null)
                format = RDFFormat.RDFXML;

            resp.setStatus(HTTP_OK);

            resp.setContentType(format.getDefaultMIMEType());

            final OutputStream os = resp.getOutputStream();
            try {
                final RDFWriter writer = RDFWriterRegistry.getInstance()
                        .get(format).getWriter(os);
                writer.startRDF();
                final Iterator<Statement> itr = g.iterator();
                while (itr.hasNext()) {
                    final Statement stmt = itr.next();
                    writer.handleStatement(stmt);
                }
                writer.endRDF();
                os.flush();
            } catch (RDFHandlerException e) {
                log.error(e, e);
                throw launderThrowable(e, resp, "");
            } finally {
                os.close();
            }
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
     * Handles SPARQL UPDATE.
     * 
     * <pre>
     * update (required)
     * using-graph-uri (0 or more)
     * using-named-graph-uri (0 or more) 
     * </pre>
     * 
     * @param req
     * @param resp
     * @throws IOException
     */
    private void doUpdate(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final String namespace = getNamespace(req);

        final long timestamp = ITx.UNISOLATED;//getTimestamp(req);

        // The SPARQL query.
        final String updateStr = req.getParameter("update");

        if (updateStr == null) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Not found: update");

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

            // final boolean explain =
            // req.getParameter(BigdataRDFContext.EXPLAIN) != null;

            final AbstractQueryTask queryTask;
            try {
                /*
                 * Attempt to construct a task which we can use to evaluate the
                 * query.
                 */
                queryTask = context
                        .getQueryTask(namespace, timestamp, updateStr,
                                null/* acceptOverride */, req, os, true/* update */);
            } catch (MalformedQueryException ex) {
                /*
                 * Send back a BAD REQUEST (400) along with the text of the
                 * syntax error message.
                 */
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        ex.getLocalizedMessage());
                return;
            }

            final FutureTask<Void> ft = new FutureTask<Void>(queryTask);

            if (log.isTraceEnabled())
                log.trace("Will run update: " + updateStr);

            /*
             * Setup the response headers.
             */

            resp.setStatus(HTTP_OK);
            resp.setContentType(BigdataServlet.MIME_TEXT_PLAIN);
            
            // No caching for UPDATE.
            resp.addHeader("Cache-Control", "no-cache");

//            if (queryTask.explain) {
//                resp.setContentType(BigdataServlet.MIME_TEXT_HTML);
//                final Writer w = new OutputStreamWriter(os, queryTask.charset);
//                try {
//                    // Begin executing the query (asynchronous)
//                    getBigdataRDFContext().queryService.execute(ft);
//                    // Send an explanation instead of the query results.
//                    explainQuery(queryStr, queryTask, ft, w);
//                } finally {
//                    w.flush();
//                    w.close();
//                    os.flush();
//                    os.close();
//                }
//
//            } else {
//                resp.setContentType(queryTask.mimeType);
//
//                if (queryTask.charset != null) {
//
//                    // Note: Binary encodings do not specify charset.
//                    resp.setCharacterEncoding(queryTask.charset.name());
//
//                }
//
//                if (isAttachment(queryTask.mimeType)) {
//                    /*
//                     * Mark this as an attachment (rather than inline). This is
//                     * just a hint to the user agent. How the user agent handles
//                     * this hint is up to it.
//                     */
//                    resp.setHeader("Content-disposition",
//                            "attachment; filename=query" + queryTask.queryId
//                                    + "." + queryTask.fileExt);
//                }
//
//                if (TimestampUtility.isCommitTime(queryTask.timestamp)) {
//
//                    /*
//                     * A read against a commit time or a read-only tx. Such
//                     * results SHOULD be cached because the data from which the
//                     * response was constructed have snapshot isolation. (Note:
//                     * It is possible that the commit point against which the
//                     * query reads will be aged out of database and that the
//                     * query would therefore fail if it were retried. This can
//                     * happen with the RWStore or in scale-out.)
//                     * 
//                     * Note: READ_COMMITTED requests SHOULD NOT be cached. Such
//                     * requests will read against then current committed state
//                     * of the database each time they are processed.
//                     * 
//                     * Note: UNISOLATED queries SHOULD NOT be cached. Such
//                     * operations will read on (and write on) the then current
//                     * state of the unisolated indices on the database each time
//                     * they are processed. The results of such operations could
//                     * be different with each request.
//                     * 
//                     * Note: Full read-write transaction requests SHOULD NOT be
//                     * cached unless they are queries and the transaction scope
//                     * is limited to the request (rather than running across
//                     * multiple requests).
//                     */
//
//                    resp.addHeader("Cache-Control", "public");
//
//                    // to disable caching.
//                    // r.addHeader("Cache-Control", "no-cache");
//
//                }

                // Begin executing the query (asynchronous)
                getBigdataRDFContext().queryService.execute(ft);

                // Wait for the Future.
                ft.get();
//
//            }

        } catch (Throwable e) {
            try {
                throw BigdataRDFServlet.launderThrowable(e, resp, updateStr);
            } catch (Exception e1) {
                throw new RuntimeException(e);
            }
        }
        
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

        if (queryStr == null) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Not found: query");

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

            // final boolean explain =
            // req.getParameter(BigdataRDFContext.EXPLAIN) != null;

            final AbstractQueryTask queryTask;
            try {
                /*
                 * Attempt to construct a task which we can use to evaluate the
                 * query.
                 */
                queryTask = context
                        .getQueryTask(namespace, timestamp, queryStr,
                                null/* acceptOverride */, req, os, false/* update */);
            } catch (MalformedQueryException ex) {
                /*
                 * Send back a BAD REQUEST (400) along with the text of the
                 * syntax error message.
                 */
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        ex.getLocalizedMessage());
                return;
            }

//            /*
//             * Test the cache.
//             */
//            {
//
//                req.setAttribute(ATTR_QUERY_TASK, queryTask);
//                
//                doCache(req, resp);
//
//                if (resp.isCommitted()) {
//                    // Answered by the cache.
//                    return;
//                }
//                
//            }
            
            final FutureTask<Void> ft = new FutureTask<Void>(queryTask);

            if (log.isTraceEnabled())
                log.trace("Will run query: " + queryStr);

            /*
             * Setup the response headers.
             */

            resp.setStatus(HTTP_OK);

            if (queryTask.explain) {
                resp.setContentType(BigdataServlet.MIME_TEXT_HTML);
                final Writer w = new OutputStreamWriter(os, queryTask.charset);
                try {
                    // Begin executing the query (asynchronous)
                    getBigdataRDFContext().queryService.execute(ft);
                    // Send an explanation instead of the query results.
                    explainQuery(queryStr, queryTask, ft, w);
                } finally {
                    w.flush();
                    w.close();
                    os.flush();
                    os.close();
                }

            } else {
                resp.setContentType(queryTask.mimeType);

                if (queryTask.charset != null) {

                    // Note: Binary encodings do not specify charset.
                    resp.setCharacterEncoding(queryTask.charset.name());

                }

                if (isAttachment(queryTask.mimeType)) {
                    /*
                     * Mark this as an attachment (rather than inline). This is
                     * just a hint to the user agent. How the user agent handles
                     * this hint is up to it.
                     */
                    resp.setHeader("Content-disposition",
                            "attachment; filename=query" + queryTask.queryId
                                    + "." + queryTask.fileExt);
                }

                if (TimestampUtility.isCommitTime(queryTask.timestamp)) {

                    /*
                     * A read against a commit time or a read-only tx. Such
                     * results SHOULD be cached because the data from which the
                     * response was constructed have snapshot isolation. (Note:
                     * It is possible that the commit point against which the
                     * query reads will be aged out of database and that the
                     * query would therefore fail if it were retried. This can
                     * happen with the RWStore or in scale-out.)
                     * 
                     * Note: READ_COMMITTED requests SHOULD NOT be cached. Such
                     * requests will read against then current committed state
                     * of the database each time they are processed.
                     * 
                     * Note: UNISOLATED queries SHOULD NOT be cached. Such
                     * operations will read on (and write on) the then current
                     * state of the unisolated indices on the database each time
                     * they are processed. The results of such operations could
                     * be different with each request.
                     * 
                     * Note: Full read-write transaction requests SHOULD NOT be
                     * cached unless they are queries and the transaction scope
                     * is limited to the request (rather than running across
                     * multiple requests).
                     */

                    resp.addHeader("Cache-Control", "public");

                    // to disable caching.
                    // r.addHeader("Cache-Control", "no-cache");

                }

                // Begin executing the query (asynchronous)
                getBigdataRDFContext().queryService.execute(ft);

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
     * 
     *             TODO The complexity here is due to the lack of a tight
     *             coupling between the {@link RunningQuery}, the
     *             {@link BigdataSailQuery}, and the {@link IRunningQuery}. It
     *             was not possible to obtain that tight coupling with the 1.0.x
     *             releases of bigdata due to the integration with the Sail.
     *             This led to the practice of setting the query {@link UUID} so
     *             we could resolve it once the query was executing using
     *             {@link QueryEngine#getRunningQuery(UUID)}. This also required
     *             the spin loops in explainQuery() since we had to wait for the
     *             {@link IRunningQuery} to become available. This is also the
     *             reason why we can fail to report parts of the explanation for
     *             fast queries.
     *             <p>
     *             This issue could be revisited now. Probably the right way to
     *             do this is by defining our own evaluate() method on the
     *             {@link BigdataSailQuery} which would provide either an object
     *             to be monitored or an interface for a query listener. Either
     *             approach could be used to ensure that we always have the
     *             {@link IRunningQuery} for an {@link AbstractQueryTask} which
     *             was submitted for evaluation.
     *             <p>
     *             I have not moved on this issue because the RTO integration
     *             will change things again. Right now, we have one
     *             {@link IRunningQuery} per top-level submitted query (plus one
     *             for each named subquery). With the RTO integration, there
     *             could be more {@link IRunningQuery}s issued and we will also
     *             want to paint the statics which it uncovers in its rounds.
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

        if (ft.isDone()) {
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
         * 
         * Note: The document that we are building writes onto the http
         * response. Therefore, the response MIGHT be committed anytime after we
         * start producing this document.
         * 
         * Note: If the query fails after this point, this method will wind up
         * writing the stack trace into the response page.
         */
		final HTMLBuilder doc = new HTMLBuilder(queryTask.charset.name(), w);
		{

			XMLBuilder.Node current = doc.root("html");
			{
				current = current.node("head");
                current.node("meta")
                        .attr("http-equiv", "Content-Type")
                        .attr("content",
                                "text/html;charset=" + queryTask.charset.name())
                        .close();
				current.node("title").textNoEncode("bigdata&#174;").close();
				current = current.close();// close the head.
			}

			// open the body
			current = current.node("body");

			current.node("h1", "Query");

            final ASTContainer astContainer = queryTask.astContainer;

            /*
             * These things are available as soon as the parser runs, so we can
             * paint them onto the page now.
             * 
             * Note: The code is written defensively even though all of this
             * information should be available
             */
            {

                final String queryString = astContainer.getQueryString();

                if (queryString != null) {

                    current.node("h2", "SPARQL");
                    
                    current.node("pre", queryString);

                }

                final SimpleNode parseTree = ((SimpleNode) astContainer
                        .getParseTree());

                if (parseTree != null) {

                    current.node("h2", "Parse Tree");
                    
                    current.node("pre", parseTree.dump(""));

                }

                final QueryRoot originalAST = astContainer.getOriginalAST();

                if (originalAST != null) {

                    current.node("h2", "Original AST");
                    
                    current.node("pre", originalAST.toString());

                }
                
            }
            
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

            /*
             * Once the IRunningQuery is available, we know that the query has
             * been optimized and can paint the final (optimized) AST and the
             * query plan onto the page.
             * 
             * Note: The code is written defensively even though all of this
             * information should be available
             */
            if (q != null) {

                final QueryRoot optimizedAST = astContainer.getOptimizedAST();

                if (optimizedAST != null) {

                    current.node("h2", "Optimized AST");

                    current.node("pre", optimizedAST.toString());

                }

                final PipelineOp queryPlan = astContainer.getQueryPlan();

                if (queryPlan != null) {

                    current.node("h2", "Query Plan");
                    
                    current.node("pre", BOpUtility.toString(queryPlan));

                }

            }

            try {
                
                /*
                 * Wait for the Future. If the query fails, then note the
                 * exception but do NOT rethrow it. The exception will get
                 * painted into the page.
                 */

                ft.get();
                
                /*
                 * Note: An InterruptedException here is NOT caught. It means
                 * that this Thread was interrupted rather than the Query.
                 */
                
            } catch (ExecutionException ex) {

                // Some error.
                final Throwable cause = ex.getCause();

                // Format the stack trace.
                final StringWriter sw = new StringWriter();

                cause.printStackTrace(new PrintWriter(sw));

                final String s = sw.getBuffer().toString();

                // And write it into the page.
                current.node("pre").text(s).close();

                // Fall through and paint the query stats table(s).
                
            }

			current.node("h2", "Query Evaluation Statistics");
			
            if (q == null) {
            
                /*
                 * This can happen if we fail to get the IRunningQuery reference
                 * before the query terminates. E.g., if the query runs too
                 * quickly there is a data race and the reference may not be
                 * available anymore.
                 */
                
                current.node("p",
                        "Statistics are not available (query already terminated).");
                
            } else {

                // An array of the declared child queries.
                final IRunningQuery[] children = ((AbstractRunningQuery) q)
                        .getChildren();

                final long elapsedMillis = q.getElapsed();

                final BOpStats stats = q.getStats().get(
                        q.getQuery().getId());

                final String solutionsOut = stats == null ? NA : Long
                        .toString(stats.unitsOut.get());

                final String chunksOut = stats == null ? NA : Long
                        .toString(stats.chunksOut.get());

                current.node("p")//
                        .text("solutions=" + solutionsOut)//
                        .text(", chunks=" + chunksOut)//
                        .text(", subqueries=" + children.length)//
                        .text(", elapsed=" + elapsedMillis + "ms")//
                        .text(q.isCancelled()?", CANCELLED.":".")
                        .close();
                
                /*
                 * Format query statistics as a table.
                 * 
                 * Note: This is writing on the Writer so it goes directly into
                 * the HTML document we are building for the client.
                 */

                QueryLog.getTableXHTML(queryStr, q, children, w,
                        false/* summaryOnly */, 0/* maxBopLength */);

            }

            doc.closeAll(current);
            
        }

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

    /**
     * Private API reports the shards against which the access path would
     * read.
     * 
     * @param req
     * @param resp
     */
    private void doShardReport(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!getBigdataRDFContext().isScaleOut()) {
            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Not scale-out");
            return;
        }
        
        final long begin = System.currentTimeMillis();
        
        final String namespace = getNamespace(req);

		final boolean doRangeCount = true;
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
            log.info("SHARDS: access path: (s=" + s + ", p=" + p + ", o="
                    + o + ", c=" + c + ")");

        try {

            try {

                BigdataSailRepositoryConnection conn = null;
                try {

                    final long timestamp = getTimestamp(req);

                    conn = getBigdataRDFContext().getQueryConnection(
                            namespace, timestamp);

                    final AccessPath<?> accessPath = (AccessPath<?>) conn
                            .getSailConnection().getBigdataSail().getDatabase()
                            .getAccessPath(s, p, o, c);
                    
                    final ClientIndexView ndx = (ClientIndexView) accessPath
                            .getIndex();
                    
                    final String charset = "utf-8";// TODO from request.

                    resp.setContentType(BigdataServlet.MIME_TEXT_HTML);
                    resp.setCharacterEncoding(charset);
                    final Writer w = resp.getWriter();
                    try {

                        final HTMLBuilder doc = new HTMLBuilder(charset, w);
                        
                        XMLBuilder.Node current = doc.root("html");
                        {
                            current = current.node("head");
                            current.node("meta")
                                    .attr("http-equiv", "Content-Type")
                                    .attr("content",
                                            "text/html;charset=utf-8")
                                    .close();
                            current.node("title")
                                    .textNoEncode("bigdata&#174;").close();
                            current = current.close();// close the head.
                        }

                        // open the body
                        current = current.node("body");

                        final IBigdataFederation<?> fed = (IBigdataFederation<?>) getBigdataRDFContext()
                                .getIndexManager();
                        
                        final Iterator<PartitionLocator> itr = ndx.locatorScan(
                                timestamp, accessPath.getFromKey(),
                                accessPath.getToKey(), false/* reverseScan */);

                        int nlocators = 0;

                        // The distinct hosts on which the shards are located.
                        final Map<String,AtomicInteger> hosts = new TreeMap<String,AtomicInteger>();
                        
                        // The host+locators in key order.
                        final StringBuilder sb = new StringBuilder();
                        
                        while (itr.hasNext()) {

                            final PartitionLocator loc = itr.next();

                            final IDataService ds = fed.getDataService(loc
                                    .getDataServiceUUID());
                            
							final String hostname = ds == null ? "N/A" : ds
									.getHostname();

							AtomicInteger nshards = hosts.get(hostname);

							if (nshards == null) {

								hosts.put(hostname,
										nshards = new AtomicInteger());
							
							}
							
							nshards.incrementAndGet();
							
							sb.append("\nhost=" + hostname);
							sb.append(", locator=" + loc);
							
							nlocators++;

                        } // while(itr.hasNext())

                        // elapsed locator scan time
						final long begin2 = System.currentTimeMillis();
						final long elapsed = begin2 - begin;
						
						// fast range count (requires visiting shards)
						final long rangeCount = doRangeCount ? accessPath
								.rangeCount(false/* exact */) : -1;

						// elapsed time for the fast range count
						final long elapsed2 = System.currentTimeMillis()
								- begin2;

						current = current.node("H2", "summary");
						{
							current.node("p", "index="
									+ ndx.getIndexMetadata().getName()
									+ ", locators=" + nlocators + ", hosts="
									+ hosts.size() + ", elapsed=" + elapsed
									+ "ms");
							if (doRangeCount) {
								current.node("p", "rangeCount=" + rangeCount
										+ ", elapsed=" + elapsed2 + "ms");
							}
							current = current.close();
						}

						// host + locators in key order.
						current.node("H2","shards").node("pre", sb.toString()).close();
						
						// hosts + #shards in host name order
						{

							sb.setLength(0); // clear buffer.

							for (Map.Entry<String, AtomicInteger> e : hosts
									.entrySet()) {

								sb.append("\nhost=" + e.getKey());
								
								sb.append(", #shards=" + e.getValue());
								
							}

							current.node("H2","hosts").node("pre", sb.toString()).close();

						}

                        doc.closeAll(current);
                        
                    } finally {
                        w.flush();
                        w.close();
                    }

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

//    /**
//     * Test the SPARQL cache.
//     */
//    private void doCache(final HttpServletRequest req,
//            final HttpServletResponse resp) throws IOException {
//
//        // The query task.
//        final AbstractQueryTask queryTask = (AbstractQueryTask) req
//                .getAttribute(ATTR_QUERY_TASK);
//
//        if (queryTask == null)
//            return;
//
//        final SparqlCache cache = getSparqlCache();
//
//        if (cache == null)
//            return;
//
//        final CacheHit hit = cache.get(queryTask);
//        
//        if(hit == null)
//            return;
//
//        /*
//         * Send the response.
//         * 
//         * TODO If the cached response entity is the wrong content type (not
//         * accepted by the Accept header), then de-serialize, convert, and
//         * re-serialize. The cache could do that on its end or we could do it
//         * here.
//         * 
//         * Ideally the SparqlCache class can be reused outside of the NSS
//         * context, e.g., for embedded applications which do not use the NSS at
//         * all. This also supports a deeper integration into the query planner.
//         * Both of which suggest that we should handle the conneg problems here.
//         */
//        resp.setStatus(HTTP_OK);
//        resp.setContentType(hit.contentType);
//        resp.setContentLength(hit.contentLength);
//        resp.setDateHeader("Last-Modified", hit.lastModified);
//        final OutputStream os = resp.getOutputStream();
//        try {
//            os.write(hit.data);
//            os.flush();
//        } finally {
//            os.close();
//        }
//        
//    }

}
