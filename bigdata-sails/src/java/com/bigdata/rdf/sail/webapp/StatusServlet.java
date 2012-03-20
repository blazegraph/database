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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryLog;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.sparql.ast.SimpleNode;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.RunningQuery;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.InnerCause;

/**
 * A status page for the service.
 * 
 * @author thompsonbry
 * @author martyncutcher
 */
public class StatusServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(StatusServlet.class);

    /**
     * The name of a request parameter used to request metadata about the
     * default namespace.
     */
    private static final String SHOW_KB_INFO = "showKBInfo";
    
    /**
     * The name of a request parameter used to request a list of the namespaces
     * which could be served.
     */
    private static final String SHOW_NAMESPACES = "showNamespaces";
    
    /**
     * The name of a request parameter used to request a display of the
     * currently running queries. Legal values for this request parameter are
     * either {@value #DETAILS} or no value.
     * 
     * @see #DETAILS
     * @see #QUERY_ID
     */
    private static final String SHOW_QUERIES = "showQueries";

    /**
     * @see #SHOW_QUERIES
     */
    private static final String DETAILS = "details"; 
    
    /**
     * The name of a request parameter whose value is the {@link UUID} of a
     * top-level query.
     */
    private static final String QUERY_ID = "queryId";

    /**
     * The name of a request parameter used to cancel a running query. At least
     * one {@link #QUERY_ID} must also be specified. Queries specified by their
     * {@link #QUERY_ID} will be cancelled if they are still running.
     * 
     * @see #QUERY_ID
     */
    static final String CANCEL_QUERY = "cancelQuery";
    
    /**
     * Handles CANCEL requests (terminate a running query).
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final boolean cancelQuery = req.getParameter(CANCEL_QUERY) != null;

        if (cancelQuery) {

            doCancelQuery(req, resp, getIndexManager());

            // Fall through.
            
        }
            
        /*
         * The other actions are all "safe" (idempotent).
         */
        doGet(req, resp);
        
        return;
            
    }
    
    /**
     * Cancel a running query.
     * 
     * <pre>
     * queryId=<UUID>
     * </pre>
     * 
     * Note: This DOES NOT build a response unless there is an error. The caller
     * needs to build a suitable response. This is done to support a use case
     * where the status page is repainted as well as a remote "cancel" command.
     * 
     * @param req
     * @param resp
     * @param indexManager
     * 
     * @throws IOException
     */
    static void doCancelQuery(final HttpServletRequest req,
            final HttpServletResponse resp, final IIndexManager indexManager)
            throws IOException {

        final String[] a = req.getParameterValues(QUERY_ID);

        if (a == null || a.length == 0) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN);

            return;
            
        }
        
        final Set<UUID> queryIds = new LinkedHashSet<UUID>();
        
        for(String s : a) {
            
            queryIds.add(UUID.fromString(s));
            
        }

        final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory
                .getQueryController(indexManager);
        
        for(UUID queryId : queryIds) {
            
            final IRunningQuery q;
            try {
                q = queryEngine.getRunningQuery(queryId);
            } catch (RuntimeException ex) {
                // ignore. (typically the query has already terminated).
                if (log.isInfoEnabled())
                    log.info("No such query: " + queryId);
                continue;
            }

            if( q == null ) {

                if (log.isInfoEnabled())
                    log.info("No such query: " + queryId);

            }
            
            if (q.cancel(true/* mayInterruptIfRunning */)) {

                // TODO Could paint the page with this information.
                if (log.isInfoEnabled())
                    log.info("Cancelled query: " + queryId);

            }

        }

    }
    
    /**
     * <p>
     * A status page. Options include:
     * <dl>
     * <dt>showQueries</dt>
     * <dd>List SPARQL queries accepted by the SPARQL end point which are
     * currently executing on the {@link QueryEngine}. The queries are listed in
     * order of decreasing elapsed time. You can also specify
     * <code>showQueries=details</code> to get a detailed breakdown of the query
     * execution.</dd>
     * <code>queryId=&lt;UUID&gt;</code> to specify the query(s) of interest.
     * This parameter may appear zero or more times. When give, the response
     * will include information only about the specified queries.</dd>
     * <dt>showKBInfo</dt>
     * <dd>Show some information about the {@link AbstractTripleStore} instance
     * being served by this SPARQL end point.</dd>
     * <dt>showNamespaces</dt>
     * <dd>List the namespaces for the registered {@link AbstractTripleStore}s.</dd>
     * </dl>
     * </p>
     * 
     * @todo This status page combines information about the addressed KB and
     *       the backing store. Those items should be split out onto different
     *       status requests. One should be at a URI for the database. The other
     *       should be at the URI of the SPARQL end point.
     */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        // IRunningQuery objects currently running on the query controller.
        final boolean showQueries = req.getParameter(SHOW_QUERIES) != null;

		boolean showQueryDetails = false;
		if (showQueries) {
			for (String tmp : req.getParameterValues(SHOW_QUERIES)) {
				if (tmp.equals(DETAILS))
					showQueryDetails = true;
			}
		}

		/*
		 * The maximum inline length of BOp#toString() visible on the page. The
		 * entire thing is accessible via the title attribute (a flyover). Use
		 * ZERO (0) to see everything.
		 */
		int maxBopLength = 0;
		if (req.getParameter("maxBopLength") != null) {
			maxBopLength = Integer.valueOf(req.getParameter("maxBopLength"));
			if (maxBopLength < 0)
				maxBopLength = 0;
		}

        // Information about the KB (stats, properties).
        final boolean showKBInfo = req.getParameter(SHOW_KB_INFO) != null;

        // bigdata namespaces known to the index manager.
        final boolean showNamespaces = req.getParameter(SHOW_NAMESPACES) != null;

        resp.setContentType(MIME_TEXT_HTML);
        final Writer w = new OutputStreamWriter(resp.getOutputStream(), UTF8);
        try {

            final HTMLBuilder doc = new HTMLBuilder(UTF8, w);

            XMLBuilder.Node current = doc.root("html");
            {
                current = current.node("head");
                current.node("meta").attr("http-equiv", "Content-Type")
                        .attr("content", "text/html;charset=utf-8").close();
                current.node("title").textNoEncode("bigdata&#174;").close();
                current = current.close();// close the head.
            }
            
            // open the body
            current = current.node("body");

            current.node("br", "Accepted query count="
                    + getBigdataRDFContext().getQueryIdFactory().get());

            current.node("br", "Running query count="
                    + getBigdataRDFContext().getQueries().size());

            // Offer a link to the "showQueries" page.
            {

                final String showQueriesURL = req.getRequestURL().append("?")
                        .append(SHOW_QUERIES).toString();
                
                final String showQueriesDetailsURL = req.getRequestURL()
                        .append("?").append(SHOW_QUERIES).append("=")
                        .append(DETAILS).toString();

                current.node("p").text("Show ")
                        //
                        .node("a").attr("href", showQueriesURL).text("queries")
                        .close()//
                        .text(", ")//
                        .node("a").attr("href", showQueriesDetailsURL)//
                        .text("query details").close()//
                        .text(".");

            }

            if (showNamespaces) {

                final List<String> namespaces = getBigdataRDFContext()
                        .getNamespaces();

                current.node("h3", "Namespaces: ");

                for (String s : namespaces) {

                    current.node("p", s);

                }

            }

            if (showKBInfo) {

                // General information on the connected kb.
                current.node("pre", getBigdataRDFContext().getKBInfo(
                                getNamespace(req), getTimestamp(req))
                                .toString());

            }

            /*
             * Performance counters for the QueryEngine.
             */
            {

                final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory
                        .getQueryController(getIndexManager());

                final CounterSet counterSet = queryEngine.getCounters();
                
                if (getBigdataRDFContext().getSampleTask() != null) {

                    /*
                     * Performance counters for the NSS queries.
                     * 
                     * Note: This is NSS specific, rather than per-QueryEngine.
                     * For example, DataServices on a federation embed a
                     * QueryEngine instance, but it does not expose a SPARQL end
                     * point and will not have a queryService against which
                     * SPARQL queries can be submitted. The purpose of the
                     * per-DS QueryEngine instances is support distributed query
                     * evaluation.
                     */
                    counterSet.makePath("queryService").attach(
                            getBigdataRDFContext().getSampleTask()
                                    .getCounters());

                }

//                @SuppressWarnings("rawtypes")
//                final Iterator<ICounter> itr = counterSet
//                        .getCounters(null/* filter */);
//                
//                while(itr.hasNext()) {
//
//                    final ICounter<?> c = itr.next();
//
//                    final Object value = c.getInstrument().getValue();
//
//                    // The full path to the metric name.
//                    final String path = c.getPath();
//                 
//                    current.node("br", path + "=" + value);
//
//                }

                current.node("pre", counterSet.toString());
                
            }
            
            if (!showQueries) {
                // Nothing more to do.
                return;
            }

            /*
             * The set of queryIds for which information was explicitly
             * requested. If empty, then information will be provided for all
             * running queries.
             */
            final Set<UUID> requestedQueryIds = new HashSet<UUID>();
            {

                final String[] a = req.getParameterValues(QUERY_ID);
                
                if (a != null && a.length > 0) {

                    for(String s : a) {
                        
                        final UUID queryId = UUID.fromString(s);
                        
                        requestedQueryIds.add(queryId);
                        
                    }
                    
                }
                
            }
            
            // Marker timestamp used to report the age of queries.
            final long now = System.nanoTime();

            /*
             * Map providing a cross walk from the QueryEngine's
             * IRunningQuery.getQueryId() to NanoSparqlServer's
             * RunningQuery.queryId.
             */
            final Map<UUID/* IRunningQuery.queryId */, RunningQuery> crosswalkMap = new LinkedHashMap<UUID, RunningQuery>();

            /*
             * Map providing the accepted RunningQuery objects in descending
             * order by their elapsed run time.
             */
            final TreeMap<Long/* elapsed */, RunningQuery> acceptedQueryAge = newQueryMap();

            {

                final Iterator<RunningQuery> itr = getBigdataRDFContext()
                        .getQueries().values().iterator();

                while (itr.hasNext()) {

                    final RunningQuery query = itr.next();

                    crosswalkMap.put(query.queryId2, query);

                    final long age = now - query.begin;

                    acceptedQueryAge.put(age, query);

                }

            }

            /*
             * Show the queries which are currently executing (actually running
             * on the QueryEngine).
             */

            final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory
                    .getQueryController(getIndexManager());

            final UUID[] queryIds = queryEngine.getRunningQueries();

            // final long now = System.nanoTime();

            /*
             * Map providing the QueryEngine's IRunningQuery objects in order by
             * descending elapsed evaluation time.
             */
            final TreeMap<Long, IRunningQuery> runningQueryAge = newQueryMap();

            for (UUID queryId : queryIds) {

                final IRunningQuery query;
                try {

                    query = queryEngine.getRunningQuery(queryId);

                    if (query == null) {

                        // Already terminated.
                        continue;

                    }

                } catch (RuntimeException e) {

                    if (InnerCause.isInnerCause(e, InterruptedException.class)) {

                        // Already terminated.
                        continue;

                    }

                    throw new RuntimeException(e);

                }

                runningQueryAge.put(query.getElapsed(), query);

            }

            /*
             * Now, paint the page for each query (or for each queryId that was
             * requested).
             */
            {
                
                final Iterator<Map.Entry<Long/* age */, IRunningQuery>> itr = runningQueryAge
                        .entrySet().iterator();

                while (itr.hasNext()) {

                    final Map.Entry<Long/* age */, IRunningQuery> e = itr
                            .next();

                    final long age = e.getKey();

                    final IRunningQuery q = e.getValue();

                    if (q.isDone() && q.getCause() != null) {
                        // Already terminated (normal completion).
                        continue;
                    }
                    
                    final UUID queryId = q.getQueryId();

                    if (!requestedQueryIds.isEmpty()
                            && !requestedQueryIds.contains(queryId)) {
                        // Information was not requested for this query.
                        continue;
                    }

                    // Lookup the NanoSparqlServer's RunningQuery object.
                    final RunningQuery acceptedQuery = crosswalkMap
                            .get(queryId);

                    if (acceptedQuery == null) {

                        /*
                         * A query running on the query engine which is not a
                         * query accepted by the NanoSparqlServer is typically a
                         * sub-query being evaluated as part of the query plan
                         * for the top-level query.
                         * 
                         * Since we nomw model the parent/child relationship and
                         * display the data for the child query, we want to skip
                         * anything which is not recognizable as a top-level
                         * query submitted to the NanoSparqlServer.
                         * 
                         * TODO This does leave open the possibility that a
                         * query directly submitted against the database from an
                         * application which embeds bigdata will not be reported
                         * here. One way to handle that is to make a collection
                         * of all queries which were skipped here, to remove all
                         * queries from that collection which were identified as
                         * subqueries below, and then to paint anything which
                         * remains and which has not yet been terminated.
                         */
                        continue;
                    }

                    // An array of the declared child queries.
                    final IRunningQuery[] children = ((AbstractRunningQuery) q)
                            .getChildren();

                    final long elapsedMillis = q.getElapsed();

                    current.node("h1", "Query");
                    {
                        /*
                         * TODO Could provide an "EXPLAIN" link. That would
                         * block until the query completes and then give you the
                         * final state of the query.
                         */
                        // FORM for CANCEL action.
                        current = current.node("FORM").attr("method", "POST")
                                .attr("action", "");

                        final String detailsURL = req.getRequestURL()
                                .append("?").append(SHOW_QUERIES).append("=")
                                .append(DETAILS).append("&").append(QUERY_ID)
                                .append("=").append(queryId.toString())
                                .toString();

                        final BOpStats stats = q.getStats().get(
                                q.getQuery().getId());

                        final String solutionsOut = stats == null ? NA : Long
                                .toString(stats.unitsOut.get());

                        final String chunksOut = stats == null ? NA : Long
                                .toString(stats.chunksOut.get());

                        current.node("p")//
                                .text("solutions=" + solutionsOut)//
                                .text(", chunks=" + chunksOut)//
                                .text(", children=" + children.length)//
                                .text(", elapsed=" + elapsedMillis + "ms")//
                                .text(", ").node("a").attr("href", detailsURL)
                                .text("details").close()//
                                .close();

                        // open <p>
                        current = current.node("p");
                        // Pass the queryId.
                        current.node("INPUT").attr("type", "hidden")
                                .attr("name", "queryId").attr("value", queryId)
                                .close();
                        current.node("INPUT").attr("type", "submit")
                                .attr("name", CANCEL_QUERY).attr("value", "Cancel")
                                .close();
                        current = current.close(); // close <p>

                        current = current.close(); // close <FORM>

                    }

                    final String queryString;

                    if (acceptedQuery != null) {

                        /*
                         * A top-level query submitted to the NanoSparqlServer.
                         */

                        final ASTContainer astContainer = acceptedQuery.queryTask.astContainer;

                        queryString = astContainer.getQueryString();

                        if (queryString != null) {

                            current.node("h2", "SPARQL");
                            
                            current.node("pre", queryString);

                        }

                        if (showQueryDetails) {

                            final SimpleNode parseTree = ((SimpleNode) astContainer
                                    .getParseTree());

                            if (parseTree != null) {

                                current.node("h2", "Parse Tree");

                                current.node("pre", parseTree.dump(""));

                            }

                            final QueryRoot originalAST = astContainer
                                    .getOriginalAST();

                            if (originalAST != null) {

                                current.node("h2", "Original AST");

                                current.node("pre", originalAST.toString());

                            }

                            final QueryRoot optimizedAST = astContainer
                                    .getOptimizedAST();

                            if (optimizedAST != null) {

                                current.node("h2", "Optimized AST");

                                current.node("pre", optimizedAST.toString());

                            }

                            final PipelineOp queryPlan = astContainer
                                    .getQueryPlan();

                            if (queryPlan != null) {

                                current.node("h2", "Query Plan");

                                current.node("pre",
                                        BOpUtility.toString(queryPlan));

                            }

                        }

                    } else {

                        /*
                         * Typically a sub-query for some top-level query, but
                         * this could also be something submitted via a
                         * different mechanism to run on the QueryEngine.
                         */

                        queryString = "N/A";

                    }

                    if (showQueryDetails) {

                        current.node("h2", "Query Evaluation Statistics");

                        // Format as a table, writing onto the response.
                        QueryLog.getTableXHTML(queryString, q, children, w,
                                !showQueryDetails, maxBopLength);

                    }

                } // next IRunningQuery.

            } // end of block in which we handle the running queries.

            doc.closeAll(current);

        } finally {

            w.flush();
            w.close();

        }

    }

    /**
     * Return a {@link Map} whose natural order puts the entries into descending
     * order based on their {@link Long} keys. This is used with keys which
     * represent query durations to present the longest running queries first.
     * 
     * @param <T>
     *            The generic type of the map values.
     * @return The map.
     */
    private <T> TreeMap<Long, T> newQueryMap() {
        return new TreeMap<Long, T>(new Comparator<Long>() {
            /**
             * Comparator puts the entries into descending order by the query
             * execution time (longest running queries are first).
             */
            public int compare(final Long o1, final Long o2) {
                if (o1.longValue() < o2.longValue()) return 1;
                if (o1.longValue() > o2.longValue()) return -1;
                return 0;
            }
        });
    }

}
