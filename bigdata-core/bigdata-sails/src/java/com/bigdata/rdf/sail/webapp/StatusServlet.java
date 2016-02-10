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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.BigdataStatics;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryLog;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.counters.CounterSet;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.QueryCancellationHelper;
import com.bigdata.rdf.sail.model.JsonHelper;
import com.bigdata.rdf.sail.sparql.ast.SimpleNode;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.RunningQuery;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.TaskAndFutureTask;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.UpdateTask;
import com.bigdata.rdf.sail.webapp.QueryServlet.SparqlQueryTask;
import com.bigdata.rdf.sail.webapp.QueryServlet.SparqlUpdateTask;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.ClassPathUtil;
import com.bigdata.util.InnerCause;

/**
 * A status page for the service.
 * 
 * TODO The KB addressed by the request should also be displayed as metadata
 * associated with the request. We should make this a restriction that can be
 * placed onto the status page and make it easy to see the status for different
 * KBs.
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
     * The name of a request parameter used to request a list of the namespaces
     * which could be served.
     */
    private static final String SHOW_NAMESPACES = "showNamespaces";

    /**
     * Request a low-level dump of the journal.
     * 
     * @see DumpJournal
     */
    private static final String DUMP_JOURNAL = "dumpJournal";

    /**
     * Request a low-level dump of the pages in the indices for the journal. The
     * {@link #DUMP_JOURNAL} option MUST also be specified.
     * 
     * @see DumpJournal
     */
    private static final String DUMP_PAGES = "dumpPages";

    /**
     * Restrict a low-level dump of the journal to only the indices having the
     * specified namespace prefix. The {@link #DUMP_JOURNAL} option MUST also be
     * specified.
     * 
     * @see DumpJournal
     */
    private static final String DUMP_NAMESPACE = "dumpNamespace";

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
     * top-level query. See also {@link QueryHints#QUERYID} which is the same
     * value.
     */
    private static final String QUERY_ID = "queryId";

    /**
     * The name of a request parameter used to cancel a running query (or any
     * other kind of REST API operation). At least one {@link #QUERY_ID} must
     * also be specified. Queries specified by their {@link #QUERY_ID} will be
     * cancelled if they are still running.
     * 
     * @see #QUERY_ID
     * @see QueryHints#QUERYID
     */
    protected static final String CANCEL_QUERY = "cancelQuery";

    /**
     * Request a snapshot of the journal (HA only). The snapshot will be written
     * into the configured directory on the server. If a snapshot is already
     * being taken then this is a NOP.
     */
    static final String SNAPSHOT = "snapshot";

    /**
     * Request to generate the digest for the journals, HALog files, and
     * snapshot files. This is only a debugging tool. In particular, the digests
     * on the journal are only valid if there are no concurrent writes on the
     * journal and the journal has been through either a commit or an abort
     * protocol.
     * <p>
     * The value is a {@link DigestEnum} and defaults to
     * {@link DigestEnum#Journal} when {@link #DIGESTS} is specified without an
     * explicit {@link DigestEnum} value.
     */
    static final String DIGESTS = "digests";
    
    static final DigestEnum DEFAULT_DIGESTS = DigestEnum.Journal;

    static enum DigestEnum {
        None, Journal, HALogs, Snapshots, All;
    }

    /**
	 * URL request parameter to trigger a thread dump. The thread dump is
	 * written onto the http response. This is intended to provide an aid when
	 * analyzing either node-local or distributed deadlocks.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1082" > Add ability to dump
	 *      threads to status page </a>
	 */
    static final String THREAD_DUMP = "threadDump";

    /**
     * Special HA status request designed for clients that poll to determine the
     * status of an HAJournalServer. This option is exclusive of other
     * parameters.
     */
    static final String HA = "HA";

    /**
     * Request basic server health information.
     */
    static final String HEALTH = "health";
    
    /**
     * Request information on the mapgraph-runtime.
     */
    static final String MAPGRAPH = "mapgraph";
    
    /**
     * Handles CANCEL requests (terminate a running query).
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final boolean cancelQuery = req.getParameter(CANCEL_QUERY) != null;

        if (cancelQuery) {

            doCancelQuery(req, resp, getIndexManager(), getBigdataRDFContext());

            // Fall through so we will also deliver the status page.

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
     * queryId=&lt;UUID&gt;
     * </pre>
     * 
     * Note: This DOES NOT build a response unless there is an error. The caller
     * needs to build a suitable response. This is done to support a use case
     * where the status page is repainted as well as a remote "cancel" command.
     * 
     * <p>
     * 
     * CAUTION: The implementation of this request MUST NOT cause itself to be
     * registered as a task using {@link #submitApiTask(AbstractRestApiTask)}.
     * Doing so makes the CANCEL request itself subject to cancellation. This
     * would be a HUGE problem since the name of the query parameter ("queryId")
     * for the operations to be cancelled by a CANCEL request is the same name
     * that is used to associate a request with a UUID. In effect, this would
     * cause CANCEL to always CANCEL itself!
     * 
     * @param req
     * @param resp
     * @param indexManager
     * 
     * @throws IOException
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/899"> REST API Query
     *      Cancellation </a>
     */
    static void doCancelQuery(final HttpServletRequest req,
            final HttpServletResponse resp, final IIndexManager indexManager,
            final BigdataRDFContext context)
            throws IOException {

        final String[] a = req.getParameterValues(QUERY_ID);

        if (a == null || a.length == 0) {
   
            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                  "Required parameter not found: " + QUERY_ID);

            return;

        }

        final Set<UUID> queryIds = new LinkedHashSet<UUID>();

        for (String s : a) {

            queryIds.add(UUID.fromString(s));

        }

        final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory.getInstance()
                .getQueryController(indexManager);
        
        //See BLZG-1464  
        //QueryCancellationHelper.cancelQueries(queryIds, queryEngine);

        for (UUID queryId : queryIds) {

            if (!QueryCancellationHelper.tryCancelQuery(queryEngine, queryId)) {
                if (!tryCancelUpdate(context, queryId)) {
                    if (!tryCancelTask(context, queryId)) {
                        queryEngine.addPendingCancel(queryId);
                        if (log.isInfoEnabled()) {
                            log.info("No such QUERY, UPDATE, or task: "
                                    + queryId);
                        }
                    }
                }
            }
            
        }

        /*
         * DO NOT COMMIT RESPONSE. CALLER MUST COMMIT
         * 
         * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
         * operations should be cancelable from both REST API and workbench </a>
         */
        
    }

    /**
     * Attempt to cancel a running SPARQL UPDATE request.
     * @param context
     * @param queryId
     * @return
     */
    static private boolean tryCancelUpdate(final BigdataRDFContext context,
            final UUID queryId) {

        final RunningQuery query = context.getQueryById(queryId);

        if (query != null) {

            if (query.queryTask instanceof UpdateTask) {

                final Future<Void> f = ((UpdateTask) query.queryTask).updateFuture;

                if (f != null) {

                    if (f.cancel(true/* mayInterruptIfRunning */)) {

                        return true;

                    }

                }

            }

        }

        // Either not found or found but not running when cancelled.
        return false;

   }
    

    /**
     * Attempt to cancel a task that is neither a SPARQL QUERY nor a SPARQL UPDATE.
     * @param context
     * @param queryId
     * @return
     * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
     *      operations should be cancelable from both REST API and workbench
     *      </a>
     */
    static private boolean tryCancelTask(final BigdataRDFContext context,
            final UUID queryId) {

        final TaskAndFutureTask<?> tmp = context.getTaskById(queryId);

        if (tmp != null) {

            final Future<?> f = tmp.ft;

            if (f != null) {

                if (f.cancel(true/* mayInterruptIfRunning */)) {

                    return true;

                }

            }

        }

        // Either not found or found but not running when cancelled.
        return false;

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
     * <dt>dumpJournal</dt>
     * <dd>Provides low-level information about the backing {@link Journal} (if
     * any).</dd>
     * </dl>
     * </p>
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
     *      operations should be cancelable from both REST API and workbench
     *      </a>
     *      
     * @todo This status page combines information about the addressed KB and
     *       the backing store. Those items should be split out onto different
     *       status requests. One should be at a URI for the database. The other
     *       should be at the URI of the SPARQL end point.
     */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

		if (req.getParameter(THREAD_DUMP) != null) {

			/*
			 * Write out a thread dump as an aid to the diagnosis of deadlocks.
			 * 
			 * Note: This code path should not obtain any locks. This is
			 * necessary in order for the code to run even when the server is in
			 * a deadlock.
			 */
			doThreadDump(req, resp);
			return;

    	}
    	
        if (req.getParameter(HA) != null
                && getIndexManager() instanceof AbstractJournal
        		&& ((AbstractJournal) getIndexManager()).getQuorum() != null) { // for HA1

            new HAStatusServletUtilProxy.HAStatusServletUtilFactory().getInstance(getIndexManager()).doHAStatus(req, resp);

            return;
        }

        if (req.getParameter(HEALTH) != null) {

            new HAStatusServletUtilProxy.HAStatusServletUtilFactory().getInstance(getIndexManager()).doHealthStatus(req,
                    resp);

            return;
        }

        if (req.getParameter(MAPGRAPH) != null) {

            final IServletDelegate delegate = ClassPathUtil.classForName(//
                        "com.blazegraph.gpu.webapp.MapgraphStatusServletDelegate", // preferredClassName,
                        ServletDelegateBase.class, // defaultClass,
                        IServletDelegate.class, // sharedInterface,
                        getClass().getClassLoader() // classLoader
                );

            delegate.doGet(req, resp);

            return;
        }

		final String acceptHeader = ConnegUtil
				.getMimeTypeForQueryParameterServiceRequest(
						req.getParameter(BigdataRDFServlet.OUTPUT_FORMAT_QUERY_PARAMETER),
						req.getHeader(ConnectOptions.ACCEPT_HEADER));

      if(BigdataRDFServlet.MIME_JSON.equals(acceptHeader))
    	  doGetJsonResponse(req, resp);
      else {
    	  doGetHtmlResponse(req, resp);
      }
        
    }
    
    /**
     * 
     * Internal method to process the Servlet request returning the results in JSON form.  Currently only 
     * supports the listing of running queries.
     * 
     * TODO:  This is an initial version and should be refactored to support HTML, XML, JSON, and RDF.  
     * {@link http://jira.blazegraph.com/browse/BLZG-1316}
     * 
     * @param req
     * @param resp
     * @throws IOException
     */
	private void doGetJsonResponse(HttpServletRequest req,
			HttpServletResponse resp) throws IOException {

		resp.setContentType(MIME_JSON);
		final Writer w = new OutputStreamWriter(resp.getOutputStream(), UTF8);

		final Set<UUID> requestedQueryIds = getRequestedQueryIds(req);

		// Map from IRunningQuery.queryId => RunningQuery (for SPARQL QUERY
		// requests).
		final Map<UUID/* IRunningQuery.queryId */, RunningQuery> crosswalkMap = getQueryCrosswalkMap();

		final List<com.bigdata.rdf.sail.model.RunningQuery> modelRunningQueries = new ArrayList<com.bigdata.rdf.sail.model.RunningQuery>();

		/*
		 * Show the queries that are currently executing (actually running on
		 * the QueryEngine).
		 */

		final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory.getInstance()
				.getQueryController(getIndexManager());

		final UUID[] queryIds = queryEngine.getRunningQueries();

		final TreeMap<Long, IRunningQuery> runningQueryAge = orderRunningQueries(
				queryIds, crosswalkMap, queryEngine);

		/*
		 * Now, paint the page for each query (or for each queryId that was
		 * requested).
		 * 
		 * Note: This is only SPARQL QUERY requests.
		 */
		{

			Iterator<RunningQuery> rSparqlQueries = getRunningSparqlQueries(
					requestedQueryIds, runningQueryAge, crosswalkMap)
					.iterator();

			while (rSparqlQueries.hasNext()) {

				final RunningQuery r = rSparqlQueries.next();

				modelRunningQueries.add(r.getModelRunningQuery());

			}

		}

		/*
		 * 
		 * This is for Update requests
		 */
		{
			Iterator<RunningQuery> rUpdateQueries = getPendingUpdates(
					requestedQueryIds).iterator();

			while (rUpdateQueries.hasNext()) {

				final RunningQuery r = rUpdateQueries.next();

				modelRunningQueries.add(r.getModelRunningQuery());

			}
		}

		// Build and send the JSON Response
		JsonHelper.writeRunningQueryList(w, modelRunningQueries);

	}
    
    /**
     * Internal method to process the Servlet request returning the results in HTML form.
     * 
     * TODO:  This is an initial version and should be refactored to support HTML, XML, JSON, and RDF.  
     * {@link http://jira.blazegraph.com/browse/BLZG-1316}
     * 
     * @param req
     * @param resp
     * @throws IOException
     */
    private void doGetHtmlResponse(HttpServletRequest req, HttpServletResponse resp) throws IOException {

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

        // bigdata namespaces known to the index manager.
        final boolean showNamespaces = req.getParameter(SHOW_NAMESPACES) != null;
        
        resp.setContentType(MIME_TEXT_HTML);
        final Writer w = new OutputStreamWriter(resp.getOutputStream(), UTF8);
        try {

            final HTMLBuilder doc = new HTMLBuilder(UTF8, w);

            XMLBuilder.Node current = doc.root("html");

            BigdataRDFContext.addHtmlHeader(current, charset);
            
            // Dump Journal?
            final boolean dumpJournal = req.getParameter(DUMP_JOURNAL) != null;

            if (dumpJournal && getIndexManager() instanceof AbstractJournal) {

                current.node("h1", "Dump Journal").node("p", "Running...");

                // final XMLBuilder.Node section = current.node("pre");
                // flush writer before writing on PrintStream.
                doc.getWriter().flush();

                // dump onto the response.
                final PrintWriter out = new PrintWriter(resp.getOutputStream(),
                        true/* autoFlush */);

                out.print("<pre id=\"journal-dump\">\n");

                final DumpJournal dump = new DumpJournal(
                        (Journal) getIndexManager());

                final List<String> namespaces;

                // Add in any specified namespace(s) (defaults to all).
                {

                    final String[] a = req.getParameterValues(DUMP_NAMESPACE);

                    if (a == null) {

                        namespaces = Collections.emptyList();

                    } else {

                        namespaces = new LinkedList<String>();

                        for (String namespace : a) {

                            namespaces.add(namespace);

                        }

                    }

                }

                final boolean dumpHistory = false;

                final boolean dumpPages = req.getParameter(DUMP_PAGES) != null;

                final boolean dumpIndices = false;

                final boolean dumpTuples = false;

                dump.dumpJournal(out, namespaces, dumpHistory, dumpPages,
                        dumpIndices, dumpTuples);

                out.print("\n</pre>");

                // flush PrintStream before resuming writes on Writer.
                out.flush();

                // close section.
                // section.close();

            }

            // final boolean showQuorum = req.getParameter(SHOW_QUORUM) != null;

            if (getIndexManager() instanceof AbstractJournal) {

                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = ((AbstractJournal) getIndexManager())
                        .getQuorum();

                if (quorum != null) {//&& quorum.isHighlyAvailable()) {

                	new HAStatusServletUtilProxy.HAStatusServletUtilFactory().getInstance(getIndexManager()).doGet(req, resp,
                            current);

                }

            }
            
			{ // Report the build version (when available). See #1089
				String buildVer = Banner.getBuildInfo().get(Banner.BuildInfoMeta.buildVersion);
				if (buildVer == null )
					buildVer = "N/A";
				current.node("p").text("Build Version=").node("span")
						.attr("id", "buildVersion").text(buildVer).close()
						.close();
			}

			{ // Report the git commit when available.  See BLZG-1688
				String gitCommit = Banner.getBuildInfo().get(Banner.BuildInfoMeta.gitCommit);
				if (gitCommit == null )
					gitCommit = "N/A";
				current.node("p").text("Build Git Commit=").node("span")
						.attr("id", "gitCommit").text(gitCommit).close()
						.close();
			}

			{ // Report the git branch when available.  See BLZG-1688
				String gitBranch = Banner.getBuildInfo().get(Banner.BuildInfoMeta.gitBranch);
				if (gitBranch == null )
					gitBranch = "N/A";
				current.node("p").text("Build Git Branch=").node("span")
						.attr("id", "gitBranch").text(gitBranch).close()
						.close();
			}


            current.node("p").text("Accepted query count=")
               .node("span").attr("id", "accepted-query-count")
               .text("" +getBigdataRDFContext().getQueryIdFactory().get())
               .close()
            .close();

            current.node("p").text("Running query count=")
               .node("span").attr("id", "running-query-count")
               .text("" + getBigdataRDFContext().getQueries().size()).close()
            .close();

            // Offer a link to the "showQueries" page.
            {

                final String showQueriesURL = req.getRequestURL().append("?")
                        .append(SHOW_QUERIES).toString();

                final String showQueriesDetailsURL = req.getRequestURL()
                        .append("?").append(SHOW_QUERIES).append("=").append(
                                DETAILS).toString();

                current.node("p").text("Show ")
                        //
                        .node("a").attr("href", showQueriesURL)
                        .attr("id", "show-queries").text("queries").close()
                        .text(", ")//
                        .node("a").attr("href", showQueriesDetailsURL)
                        .attr("id", "show-query-details").text("query details")
                        .close()//
                        .text(".").close();

            }

			if (showNamespaces) {
			    
                final List<String> namespaces = getBigdataRDFContext()
                        .getNamespaces(getTimestamp(req));

                current.node("h3", "Namespaces: ");
                
                XMLBuilder.Node ul = current.node("ul").attr("id", "namespaces");

                for (String s : namespaces) {

                    ul.node("li", s);

                }
                
                ul.close();

			}

            /*
             * Performance counters for the QueryEngine.
             */
            {

                final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory
                        .getInstance().getQueryController(getIndexManager());

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

                // @SuppressWarnings("rawtypes")
                // final Iterator<ICounter> itr = counterSet
                // .getCounters(null/* filter */);
                //                
                // while(itr.hasNext()) {
                //
                // final ICounter<?> c = itr.next();
                //
                // final Object value = c.getInstrument().getValue();
                //
                // // The full path to the metric name.
                // final String path = c.getPath();
                //                 
                // current.node("br", path + "=" + value);
                //
                // }

                current.node("p").attr("id",  "counter-set")
                .text(counterSet.toString()).close();

            }

            if (!showQueries) {
                // Nothing more to do.
                doc.closeAll(current);
                return;
            }

            final Set<UUID> requestedQueryIds = getRequestedQueryIds(req);

            /**
             * Obtain a cross walk from the {@link QueryEngine}'s
             * {@link IRunningQuery#getQueryId()} to {@link NanoSparqlServer}'s
             * {@link RunningQuery#queryId}.
             * 
             * Note: This does NOT include the SPARQL UPDATE requests because
             * they are not executed directly by the QueryEngine and hence are
             * not assigned {@link IRunningQuery}s.
             */
            
            // Map from IRunningQuery.queryId => RunningQuery (for SPARQL QUERY requests).
            final Map<UUID/* IRunningQuery.queryId */, RunningQuery> crosswalkMap = getQueryCrosswalkMap();

            /*
             * Show the queries that are currently executing (actually running
             * on the QueryEngine).
             */

            final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory.getInstance()
                    .getQueryController(getIndexManager());

            final UUID[] queryIds = queryEngine.getRunningQueries();

			final TreeMap<Long, IRunningQuery> runningQueryAge = orderRunningQueries(
					queryIds, crosswalkMap, queryEngine);

            /*
             * Now, paint the page for each query (or for each queryId that was
             * requested).
             * 
             * Note: This is only SPARQL QUERY requests.
             */
            {
            	Iterator<RunningQuery> rSparqlQueries = getRunningSparqlQueries(
    					requestedQueryIds, runningQueryAge, crosswalkMap)
    					.iterator();
    			
    			while(rSparqlQueries.hasNext()) {
    				
    				final RunningQuery acceptedQuery = rSparqlQueries.next();

					// Paint the query.
					current = showQuery(
							req,
							resp,
							w,
							current,
							queryEngine.getRunningQuery(acceptedQuery.queryId2),
							acceptedQuery, showQueryDetails, maxBopLength);
    			}

            } // end of block in which we handle the running queries.

            /*
             * Now handle any SPARQL UPDATE requests.
             * 
             * Note: Since these are not directly run on the QueryEngine, we
             * proceed from the set of active RunningQuery objects maintained by
             * the NSS.  If we find anything there, it is presumed to still be
             * executing (it it is done, it will be removed very soon after the
             * UPDATE commits).
             */
			{
				Iterator<RunningQuery> rUpdateQueries = getPendingUpdates(
						requestedQueryIds).iterator();

				while (rUpdateQueries.hasNext()) {

					final RunningQuery acceptedQuery = rUpdateQueries.next();

					showUpdateRequest(req, resp, current, acceptedQuery,
							showQueryDetails);
				}

			} // SPARQL UPDATE requests
            
            /*
             * Now handle any other kinds of REST API requests.
             * 
             * Note: We have to explicitly exclude the SPARQL QUERY and SPARQL
             * UPDATE requests here since they were captured above.
             * 
             * TODO Refactor to handle all three kinds of requests using a
             * common approach.
             * 
             * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
             * operations should be cancelable from both REST API and workbench
             * </a>
             */
			{

				Iterator<TaskAndFutureTask<?>> otherTasks = getOtherTasks(
				requestedQueryIds).iterator();

				while (otherTasks.hasNext()) {

					final TaskAndFutureTask<?> task = otherTasks.next();
					showTaskRequest(req, resp, current, task, showQueryDetails);

				} // Next Task

			} // Other REST API requests.
            
            doc.closeAll(current);

        } catch(Throwable t) {
        
            log.error(t,t);
            
        } finally {

            w.flush();
            w.close();

        }

    }

    private XMLBuilder.Node showUpdateRequest(final HttpServletRequest req,
            final HttpServletResponse resp, //final Writer w,
            XMLBuilder.Node current,// final IRunningQuery q,
            final RunningQuery acceptedQuery, final boolean showQueryDetails)
            throws IOException {

        // The UUID for this UPDATE request.
        final UUID queryId = acceptedQuery.queryId2;

        // The UpdateTask.
        final UpdateTask updateTask = (UpdateTask) acceptedQuery.queryTask;

        final long elapsedMillis = updateTask.getElapsedExecutionMillis();
        
        final long mutationCount = updateTask.getMutationCount();

        current.node("h1", "Update");
        {

            // Open <FORM>
            current = current.node("FORM").attr("method", "POST")
                    .attr("action", "");

            final String detailsURL = req.getRequestURL().append(
                    "?").append(SHOW_QUERIES).append("=").append(
                    DETAILS).append("&").append(QUERY_ID).append(
                    "=").append(queryId.toString()).toString();
            
            // Open <p>.
            current.node("p")
            .attr("class", "update")
            //
            .text("elapsed=").node("span")
               .attr("class", "elapsed").text("" + elapsedMillis).close()
            .text("ms")
            // TODO HERE
            // See BLZG-1661
            .text(", ").text("mutationCount=").node("span")
            .attr("class", "mutationCount").text("" + mutationCount).close()
            .text(", ").node("a").attr("href", detailsURL)
            .attr("class", "details-url")
            .text("details").close()//
            .close();

            // open <p>
            current = current.node("p");
            // Pass the queryId.
            current.node("INPUT").attr("type", "hidden").attr(
                    "name", "queryId").attr("value", queryId)
                    .close();
            current.node("INPUT").attr("type", "submit").attr(
                    "name", CANCEL_QUERY).attr("value", "Cancel")
                    .close();
            current = current.close(); // close <p>

            current = current.close(); // close <FORM>

        }

        {

            /*
             * A top-level query submitted to the NanoSparqlServer.
             */

            final ASTContainer astContainer = acceptedQuery.queryTask.astContainer;

            final String queryString = astContainer.getQueryString();

            if (queryString != null) {

                current.node("h2", "SPARQL");

                current.node("p").attr("class", "query-string")
                .text(queryString).close();

            }

            if (showQueryDetails) {

                final SimpleNode parseTree = ((SimpleNode) astContainer
                        .getParseTree());

                if (parseTree != null) {

                    current.node("h2", "Parse Tree");

                    current.node("p").attr("class", "parse-tree")
                    .text(parseTree.dump("")).close();

                }

                final UpdateRoot originalAST = astContainer
                        .getOriginalUpdateAST();

                if (originalAST != null) {

                    current.node("h2", "Original AST");

                    current.node("p").attr("class", "original-ast")
                    .text(originalAST.toString()).close();

                }
                
                /*
                 * Note: The UPDATE request is optimized piece by piece, and
                 * those pieces are often rewrites. Thus, the optimized AST is
                 * not available here. Likewise, neither is the query plan.
                 * However, when a UPDATE operation is rewritten to include a
                 * QUERY that is executed (e.g., INSERT/DELETE/WHERE), then that
                 * QUERY will be an IRunningQuery visible on the QueryEngine.
                 * 
                 * @see AST2BOpUpdate
                 */

            }

        }

        return current;

    }

    /**
     * Display metadata about a currently executing task.
     * 
     * @param req
     * @param resp
     * @param current
     * @param task
     * @param showQueryDetails
     * @return
     * @throws IOException
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
     *      operations should be cancelable from both REST API and workbench
     *      </a>
     */
    private <T> XMLBuilder.Node showTaskRequest(final HttpServletRequest req,
            final HttpServletResponse resp, //final Writer w,
            XMLBuilder.Node current,// final IRunningQuery q,
            final TaskAndFutureTask<T> task, final boolean showQueryDetails)
            throws IOException {

        // The UUID for this REST API request.
        final UUID queryId = task.task.uuid;

        // The time since the task began executing (until it stops).
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(task
                .getElapsedNanos());

        current.node("h1", "Task");
        {

            // Open <FORM>
            current = current.node("FORM").attr("method", "POST")
                    .attr("action", "");

            final String detailsURL = req.getRequestURL().append(
                    "?").append(SHOW_QUERIES).append("=").append(
                    DETAILS).append("&").append(QUERY_ID).append(
                    "=").append(queryId.toString()).toString();
            
            // Open <p>.
            current.node("p")
            .attr("class", "task")
            //
            .text("elapsed=").node("span")
               .attr("class", "elapsed").text("" + elapsedMillis).close()
            .text("ms")
            //
            .text(", ").node("a").attr("href", detailsURL)
            .attr("class", "details-url")
            .text("details").close()//
            .close();

            // open <p>
            current = current.node("p");
            // Pass the queryId.
            current.node("INPUT").attr("type", "hidden").attr(
                    "name", "queryId").attr("value", queryId)
                    .close();
            current.node("INPUT").attr("type", "submit").attr(
                    "name", CANCEL_QUERY).attr("value", "Cancel")
                    .close();
            current = current.close(); // close <p>

            current = current.close(); // close <FORM>

        }

        {

            /*
             * Format the task into the response.
             * 
             * TODO Improve our reporting here through an explicit API for this
             * information. This currently conveys the type of task (Class), the
             * namespace, and the timestamp associated with the task. It
             * *should* be a nice rendering of the request.
             */

            final String queryString = task.task.toString();

            if (queryString != null) {

                current.node("h2", "TASK");

                current.node("p").attr("class", "query-string")
                        .text(task.task.toString()).close();

            }

        }

        return current;

    }
    
    /**
     * Paint a single query.
     * 
     * @param req
     * @param resp
     * @param w
     * @param current
     * @param q
     * @param acceptedQuery
     * @param showQueryDetails
     * @param maxBopLength
     * @return
     * @throws IOException
     */
    private XMLBuilder.Node showQuery(final HttpServletRequest req,
            final HttpServletResponse resp, final Writer w,
            XMLBuilder.Node current, final IRunningQuery q,
            final RunningQuery acceptedQuery, final boolean showQueryDetails,
            final int maxBopLength) throws IOException {

        // The UUID assigned to the IRunningQuery.
        final UUID queryId = q.getQueryId();
        
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

            final String detailsURL = req.getRequestURL().append(
                    "?").append(SHOW_QUERIES).append("=").append(
                    DETAILS).append("&").append(QUERY_ID).append(
                    "=").append(queryId.toString()).toString();

            final BOpStats stats = q.getStats().get(
                    q.getQuery().getId());

            final String solutionsOut = stats == null ? NA : Long
                    .toString(stats.unitsOut.get());

            final String chunksOut = stats == null ? NA : Long
                    .toString(stats.chunksOut.get());

            current.node("p")
                    //
                    .text("solutions=").node("span").attr("class", "solutions")
                       .text(""+ solutionsOut).close()
                    //
                    .text(", chunks=").node("span").attr("class", "chunks")
                       .text(""+ chunksOut).close()
                    //
                    .text(", children=").node("span").attr("class", "children")
                       .text("" + children.length).close()
                    //
                    .text(", elapsed=").node("span").attr("class", "elapsed")
                       .text("" + elapsedMillis).close()
                    .text("ms, ")
                    //
                    .node("a").attr("href", detailsURL)
                    .attr("class",  "details-url")
                    .text("details").close()//
                    .close();

            // open <p>
            current = current.node("p");
            // Pass the queryId.
            current.node("INPUT").attr("type", "hidden").attr(
                    "name", "queryId").attr("value", queryId)
                    .close();
            current.node("INPUT").attr("type", "submit").attr(
                    "name", CANCEL_QUERY).attr("value", "Cancel")
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

                current.node("p").attr("class", "query-string").text(queryString)
                .close();

            }

            if (showQueryDetails) {

                final SimpleNode parseTree = ((SimpleNode) astContainer
                        .getParseTree());

                if (parseTree != null) {

                    current.node("h2", "Parse Tree");

                    current.node("p").attr("class", "parse-tree")
                    .text(parseTree.dump("")).close();

                }

                final QueryRoot originalAST = astContainer
                        .getOriginalAST();

                if (originalAST != null) {

                    current.node("h2", "Original AST");

                    current.node("p").attr("class", "original-ast")
                    .text(originalAST.toString()).close();

                }

                final QueryRoot optimizedAST = astContainer
                        .getOptimizedAST();

                if (optimizedAST != null) {

                    current.node("h2", "Optimized AST");

                    current.node("p").attr("class", "optimized-ast")
                    .text(optimizedAST.toString()).close();

                }

                final PipelineOp queryPlan = astContainer
                        .getQueryPlan();

                if (queryPlan != null) {

                    current.node("h2", "Query Plan");

                    current.node("p").attr("class", "query-plan")
                    .text(BOpUtility.toString(queryPlan)).close();

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

            // iff scale-out.
            final boolean clusterStats = q.getFederation() != null;
            // detailed explain not enabled on this code path.
            final boolean detailedStats = false;
            // no mutation for query.
            final boolean mutationStats = false;
            
            // Format as a table, writing onto the response.
            QueryLog.getTableXHTML(queryString, q, children, w,
                    !showQueryDetails, maxBopLength, clusterStats,
                    detailedStats, mutationStats);

        }

        return current;
        
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
        	@Override
            public int compare(final Long o1, final Long o2) {
                if (o1.longValue() < o2.longValue())
                    return 1;
                if (o1.longValue() > o2.longValue())
                    return -1;
                return 0;
            }
        });
    }

    /**
     * Map providing a cross walk from the {@link QueryEngine}'s
     * {@link IRunningQuery#getQueryId()} to {@link NanoSparqlServer}'s
     * {@link RunningQuery#queryId}.
     * <p>
     * Note: The obtained {@link RunningQuery} objects are NOT the
     * {@link IRunningQuery} objects of the {@link QueryEngine}. They are
     * metadata wrappers for the NSS {@link AbstractQueryTask} objects. However,
     * because we are starting with the {@link QueryEngine}'s
     * {@link IRunningQuery} {@link UUID}s, this only captures the SPARQL QUERY
     * requests and not the SPARQL UPDATE requests (since the latter do not
     * execute on the {@link QueryEngine} .
     * 
     * @return The mapping from {@link IRunningQuery#getQueryId()} to
     *         {@link RunningQuery}.
     */
    private Map<UUID/* IRunningQuery.queryId */, RunningQuery> getQueryCrosswalkMap() {

        final Map<UUID/* IRunningQuery.queryId */, RunningQuery> crosswalkMap = new LinkedHashMap<UUID, RunningQuery>();

//        // Marker timestamp used to report the age of queries.
//        final long now = System.nanoTime();

//        /*
//         * Map providing the accepted RunningQuery objects in descending order
//         * by their elapsed run time.
//         */
//        final TreeMap<Long/* elapsed */, RunningQuery> acceptedQueryAge = newQueryMap();

        {

            final Iterator<RunningQuery> itr = getBigdataRDFContext()
                    .getQueries().values().iterator();

            while (itr.hasNext()) {

                final RunningQuery query = itr.next();

//                if (query.queryTask instanceof UpdateTask) {
//
//                    // A SPARQL UPDATE
//                    updateTasks.add(query);
//
//                } else {

                // A SPARQL Query.
                crosswalkMap.put(query.queryId2, query);

//                }

//                final long age = now - query.begin;
//
//                acceptedQueryAge.put(age, query);

            }

        }

        return crosswalkMap;

    }

	/**
	 * Write a thread dump onto the http response as an aid to diagnose both
	 * node-local and distributed deadlocks.
	 * <p>
	 * Note: This code should not obtain any locks. This is necessary in order
	 * for the code to run even when the server is in a deadlock.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1082" > Add ability to dump
	 *      threads to status page </a>
	 */
	private static void doThreadDump(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {

		resp.setStatus(HTTP_OK);

		// Do not cache the response.
		// TODO Alternatively "max-age=1" for max-age in seconds.
		resp.addHeader("Cache-Control", "no-cache");

		// Plain text response.
		resp.setContentType(MIME_TEXT_PLAIN);

		final PrintWriter w = resp.getWriter();

		try {

			BigdataStatics.threadDump(w);

			w.flush();
			
		} catch (Throwable t) {

			launderThrowable(t, resp, "");

		} finally {

			w.close();

		}

	}
	
	private Set<UUID> getRequestedQueryIds(HttpServletRequest req) {

		/*
		 * The set of queryIds for which information was explicitly requested.
		 * If empty, then information will be provided for all running queries.
		 */
		final Set<UUID> requestedQueryIds = new HashSet<UUID>();
		{

			final String[] a = req.getParameterValues(QUERY_ID);

			if (a != null && a.length > 0) {

				for (String s : a) {

					final UUID queryId = UUID.fromString(s);

					requestedQueryIds.add(queryId);

				}

			}

		}
		
		return requestedQueryIds;
	}
	
	/**
	 * Map providing the QueryEngine's IRunningQuery objects in order by
	 * descending elapsed evaluation time (longest running queries are listed
	 * first). This provides a stable ordering and help people to focus on the
	 * problem queries.
	 * 
	 * @return TreeMap<Long, IRunningQuery>
	 */
	private TreeMap<Long, IRunningQuery> orderRunningQueries(
			final UUID[] queryIds,
			final Map<UUID/* IRunningQuery.queryId */, RunningQuery> crosswalkMap,
			final QueryEngine queryEngine) {

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

			final Long elapsedTime = new Long(query.getElapsed());
			runningQueryAge.put(elapsedTime, query);

		}

		return runningQueryAge;
	}
	
	/**
	 * Return a collection of running SPARQL QUERY REQUESTS requested).
	 * 
	 * Note: This is only SPARQL QUERY requests.
	 * 
	 * @return Collection<RunningQuery>
	 */
	private Collection<RunningQuery> getRunningSparqlQueries(
			final Set<UUID> requestedQueryIds,
			final TreeMap<Long, IRunningQuery> runningQueryAge,
			final Map<UUID/* IRunningQuery.queryId */, RunningQuery> crosswalkMap) {

			final Iterator<Map.Entry<Long/* age */, IRunningQuery>> itr = runningQueryAge
					.entrySet().iterator();

			final LinkedList<RunningQuery> runningSparqlQueries = new LinkedList<RunningQuery>();

			while (itr.hasNext()) {

				final Map.Entry<Long/* age */, IRunningQuery> e = itr.next();

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
				final RunningQuery acceptedQuery = crosswalkMap.get(queryId);

				if (acceptedQuery == null) {

					/*
					 * A query running on the query engine which is not a query
					 * accepted by the NanoSparqlServer is typically a sub-query
					 * being evaluated as part of the query plan for the
					 * top-level query.
					 * 
					 * Since we now model the parent/child relationship and
					 * display the data for the child query, we want to skip
					 * anything which is not recognizable as a top-level query
					 * submitted to the NanoSparqlServer.
					 * 
					 * TODO This does leave open the possibility that a query
					 * directly submitted against the database from an
					 * application which embeds bigdata will not be reported
					 * here. One way to handle that is to make a collection of
					 * all queries which were skipped here, to remove all
					 * queries from that collection which were identified as
					 * subqueries below, and then to paint anything which
					 * remains and which has not yet been terminated.
					 */
					continue;
				}

				// Add the query to the list

				runningSparqlQueries.add(acceptedQuery);

			} // next IRunningQuery.

		return runningSparqlQueries;
	}
	
	/**
	 *
	 * Convenience method to return a collection of update requests that may be running.
	 * 
	 * @param requestedQueryIds
	 * @return
	 */
	private Collection<RunningQuery> getPendingUpdates(
			final Set<UUID> requestedQueryIds) {
	
		final LinkedList<RunningQuery> pendingUpdates = new LinkedList<RunningQuery>();
		
		final Iterator<RunningQuery> itr = getBigdataRDFContext().getQueries()
				.values().iterator();

		while (itr.hasNext()) {

			final RunningQuery acceptedQuery = itr.next();

			if (!(acceptedQuery.queryTask instanceof UpdateTask)) {

				// Not an UPDATE request.
				continue;

			}

			// The UUID for this UPDATE request.
			final UUID queryId = acceptedQuery.queryId2;

			if (queryId == null) {

				/*
				 * Note: The UUID is not assigned until the UPDATE request
				 * begins to execute.
				 */
				continue;

			}

			if (!requestedQueryIds.isEmpty()
					&& !requestedQueryIds.contains(queryId)) {
				// Information was not requested for this UPDATE.
				continue;
			}

			if (acceptedQuery.queryTask.updateFuture == null) {

				// Request has not yet been queued for execution.
				continue;

			} else {

				final Future<Void> f = acceptedQuery.queryTask.updateFuture;

				if (f.isDone()) {
					try {
						f.get();
						// Already terminated (normal completion).
						continue;
					} catch (InterruptedException ex) {
						// Propagate interrupt.
						Thread.currentThread().interrupt();
					} catch (ExecutionException ex) {
						// Already terminated (failure).
						continue;
					}
				}

			}
			
			pendingUpdates.add(acceptedQuery);

		} // Next request
		
		return pendingUpdates;
	}
	
	private Collection<TaskAndFutureTask<?>> getOtherTasks(final Set<UUID> requestedQueryIds) {
		
		final LinkedList<TaskAndFutureTask<?>> otherTasks = new LinkedList<TaskAndFutureTask<?>>();
		
		final Iterator<TaskAndFutureTask<?>> itr = getBigdataRDFContext()
                .getTasks().values().iterator();

        while (itr.hasNext()) {

            final TaskAndFutureTask<?> task = itr.next();

            if (task.task instanceof SparqlUpdateTask
                    || task.task instanceof SparqlQueryTask) {

                // Already handled
                continue;

            }

            // The UUID for this REST API request.
            final UUID queryId = task.task.uuid;

            if (!requestedQueryIds.isEmpty()
                    && !requestedQueryIds.contains(queryId)) {
                // Information was not requested for this task.
                continue;
            }

            final Future<?> f = task.ft;
            if (f != null) {
                if (f.isDone()) {
                    try {
                        f.get();
                        // Already terminated (normal completion).
                        continue;
                    } catch (InterruptedException ex) {
                        // Propagate interrupt.
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException ex) {
                        // Already terminated (failure).
                        continue;
                    }
                }
            }
            
            otherTasks.add(task);
        }
        
        return otherTasks;
	}
	
}
