package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryLog;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.RunningQuery;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.HTMLUtility;
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

//    static private final transient Logger log = Logger
//            .getLogger(StatusServlet.class);

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
        final boolean showQueries = req.getParameter("showQueries") != null;

		boolean showQueryDetails = false;
		if (showQueries) {
			for (String tmp : req.getParameterValues("showQueries")) {
				if (tmp.equals("details"))
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
        final boolean showKBInfo = req.getParameter("showKBInfo") != null;

        // bigdata namespaces known to the index manager.
        final boolean showNamespaces = req.getParameter("showNamespaces") != null;

        resp.setContentType(MIME_TEXT_HTML);
        final Writer w = new OutputStreamWriter(resp.getOutputStream(), "UTF-8");
        try {
        
        final HTMLBuilder doc = new HTMLBuilder("UTF-8", w);

		XMLBuilder.Node current = doc.root("html");
		{
			current = current.node("head");
			current.node("meta").attr("http-equiv", "Content-Type").attr(
					"content", "text/html;charset=utf-8").close();
			current.node("title").text("bigdata&#174;").close();
			current = current.close();// close the head.
		}
		current = current.node("body");

        current.node("p", "Accepted query count="
                + getBigdataRDFContext().getQueryIdFactory().get());

        current.node("p", "Running query count="
                + getBigdataRDFContext().getQueries().size());

        if (showNamespaces) {

            final List<String> namespaces = getBigdataRDFContext()
                    .getNamespaces();

            current.node("h3", "Namespaces: ");

            for (String s : namespaces) {

                current.node("p", HTMLUtility.escapeForXHTML(s));

            }

        }

        if (showKBInfo) {

            // General information on the connected kb.
            current.node("pre", HTMLUtility
                    .escapeForXHTML(getBigdataRDFContext().getKBInfo(
                            getNamespace(req), getTimestamp(req)).toString()));

        }

        if (getBigdataRDFContext().getSampleTask() != null) {

            // Performance counters for the NSS queries.
            current.node("pre", HTMLUtility
                    .escapeForXHTML(getBigdataRDFContext().getSampleTask()
                            .getCounters().toString()));

        }
        
		if (!showQueries) {
			// Nothing more to do.
			return;
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
		 * Map providing the accepted RunningQuery objects in descending order
		 * by their elapsed run time.
		 */
        final TreeMap<Long/*elapsed*/, RunningQuery> acceptedQueryAge = newQueryMap();

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
		 * Show the queries which are currently executing (actually running on
		 * the QueryEngine).
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

		{

			final Iterator<IRunningQuery> itr = runningQueryAge.values()
					.iterator();

//			final StringWriter w = new StringWriter(Bytes.kilobyte32 * 8);

			while (itr.hasNext()) {

				final IRunningQuery query = itr.next();

				if (query.isDone() && query.getCause() != null) {
					// Already terminated (normal completion).
					continue;
				}

				// Lookup the NanoSparqlServer's RunningQuery object.
				final RunningQuery acceptedQuery = crosswalkMap.get(query
						.getQueryId());

				final String queryStr;

				if (acceptedQuery != null) {

					final AbstractQueryTask queryTask = acceptedQuery.queryTask;
					
					queryStr = acceptedQuery.queryTask.queryStr;

//					// TODO redundant if using native SPARQL evaluation.
//					current.node("h2", "SPARQL").node("p",
//							HTMLUtility.escapeForXHTML(queryTask.queryStr));

					current.node("h2", "Query").node(
							"pre",
							HTMLUtility.escapeForXHTML(queryTask.sailQuery
									.toString()));

					current.node("h2", "BOP Plan").node(
							"pre",
							HTMLUtility.escapeForXHTML(BOpUtility
									.toString(query.getQuery())));
					
				} else {
				
					queryStr = "N/A";
					
				}

				current.node("h2", "Query Evaluation Statistics").node("p").close();

				// Format as a table, writing onto the response.
				QueryLog.getTableXHTML(queryStr, query, w,
						!showQueryDetails, maxBopLength);

//				// Extract as String
//				final String s = w.getBuffer().toString();
//
//				// Add into the HTML document.
//				current.text(s);
//
//				// Clear the buffer.
//				w.getBuffer().setLength(0);

			} // next IRunningQuery.

		}

        doc.closeAll(current);
        
        } finally {
        	
        	w.flush();
        	w.close();
        	
        }

//        buildResponse(resp, HTTP_OK, MIME_TEXT_HTML, doc.toString());

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
                if(o1.longValue()<o2.longValue()) return 1;
                if(o1.longValue()>o2.longValue()) return -1;
                return 0;
            }
        });
    }

}
