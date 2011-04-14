package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.RunningQuery;
import com.bigdata.rdf.store.AbstractTripleStore;

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

//    @Override
//    public void init() throws ServletException {
//
//        super.init();
//
//    }

    /**
     * <p>
     * A status page. Options include:
     * <dl>
     * <dt>showQueries</dt>
     * <dd>List SPARQL queries accepted by the SPARQL end point. The queries are
     * listed in order of decreasing elapsed time.</dd>
     * <dt>showRunningQueries</dt>
     * <dd>List SPARQL queries accepted by the SPARQL end point which are
     * currently executing on the {@link QueryEngine}. The queries are listed in
     * order of decreasing elapsed time.</dd>
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

        // SPARQL queries accepted by the SPARQL end point.
        final boolean showQueries = req.getParameter("showQueries") != null;

        // IRunningQuery objects currently running on the query controller.
        final boolean showRunningQueries = req
                .getParameter("showRunningQueries") != null;

        // Information about the KB (stats, properties).
        final boolean showKBInfo = req.getParameter("showKBInfo") != null;

        // bigdata namespaces known to the index manager.
        final boolean showNamespaces = req.getParameter("showNamespaces") != null;

        final HTMLBuilder doc = new HTMLBuilder();

        XMLBuilder.Node current = doc.root("html").node("body");

        current.node("p", "Accepted query count="
                + getBigdataRDFContext().getQueryIdFactory().get());

        current.node("p", "Running query count="
                + getBigdataRDFContext().getQueries().size());

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
                    getNamespace(req.getRequestURI()),
                    getTimestamp(req.getRequestURI(), req)).toString());

        }

        if (getBigdataRDFContext().getSampleTask() != null) {

            // Performance counters for the NSS queries.
            current.node("pre", getBigdataRDFContext().getSampleTask()
                    .getCounters().toString());

        }

        if (showQueries) {

            /*
             * Show the queries which are currently executing (accepted by the
             * NanoSparqlServer).
             */

            final long now = System.nanoTime();

            final TreeMap<Long, RunningQuery> ages = newQueryMap();

            {

                final Iterator<RunningQuery> itr = getBigdataRDFContext()
                        .getQueries().values().iterator();

                while (itr.hasNext()) {

                    final RunningQuery query = itr.next();

                    final long age = now - query.begin;

                    ages.put(age, query);

                }

            }

            {

                final Iterator<RunningQuery> itr = ages.values().iterator();

                while (itr.hasNext()) {

                    final RunningQuery query = (RunningQuery) itr.next();

                    final long age = now - query.begin;

                    current = current.node(
                            "p",
                            "age="
                                    + java.util.concurrent.TimeUnit.NANOSECONDS
                                            .toMillis(age) + "ms, queryId="
                                    + query.queryId + "\n").node("p",
                            query.query + "\n");

                }

            }

        }

        if (showRunningQueries) {

            /*
             * Show the queries which are currently executing (actually running
             * on the QueryEngine).
             */

            final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory
                    .getQueryController(getIndexManager());

            final UUID[] queryIds = queryEngine.getRunningQueries();

            // final long now = System.nanoTime();

            final TreeMap<Long, IRunningQuery> ages = newQueryMap();

            for (UUID queryId : queryIds) {

                final IRunningQuery query = queryEngine
                        .getRunningQuery(queryId);

                if (query == null) {
                    // Already terminated.
                    continue;
                }

                ages.put(query.getElapsed(), query);

            }

            {

                final Iterator<IRunningQuery> itr = ages.values().iterator();

                while (itr.hasNext()) {

                    final IRunningQuery query = itr.next();

                    if (query.isDone() && query.getCause() != null) {
                        // Already terminated (normal completion).
                        continue;
                    }

                    /*
                     * @todo The runstate and stats could be formatted into an
                     * HTML table ala QueryLog or RunState.
                     */
                    current = current.node("p",
                            "age=" + query.getElapsed() + "ms").node("p",
                            "queryId=" + query.getQueryId()).node("p",
                            query.toString()).node("p",
                            BOpUtility.toString(query.getQuery()));

                }

            }

        }

        doc.closeAll(current);

        buildResponse(resp, HTTP_OK, MIME_TEXT_HTML, doc.toString());

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
