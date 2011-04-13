package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.RunningQuery;

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

    static private final transient Logger log = Logger.getLogger(StatusServlet.class); 

    @Override
    public void init() throws ServletException {

        super.init();
        
    }
    
    /**
     * <p>
     * A status page is available:
     * </p>
     * 
     * <pre>
     * GET /status
     * </pre>
	 */
	@Override
	protected void doGet(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {

		try {
			// SPARQL queries accepted by the SPARQL end point.
			final boolean showQueries = req.getParameter("showQueries") != null;

			// IRunningQuery objects currently running on the query controller.
			final boolean showRunningQueries = req.getParameter("showRunningQueries") != null;

			// Information about the KB (stats, properties).
			final boolean showKBInfo = req.getParameter("showKBInfo") != null;

			// bigdata namespaces known to the index manager.
			final boolean showNamespaces = req.getParameter("showNamespaces") != null;

			final XMLBuilder.HTML doc = new XMLBuilder.HTML();

			XMLBuilder.Node current = doc.root("html").node("body");

			current.node("p", "Accepted query count=" + getQueryIdFactory().get());

			current.node("p", "Running query count=" + getQueries().size());

			if (showNamespaces) {

				final List<String> namespaces = getNamespaces();

				current.node("h3", "Namespaces: ");

				for (String s : namespaces) {

					current.node("p", s);

				}

			}

			if (showKBInfo) {

				// General information on the connected kb.
				current
						.node("pre", getKBInfo(getNamespace(req.getRequestURI()),
								getTimestamp(req.getRequestURI(), req)).toString());

			}

			if (getSampleTask() != null) {

				// Performance counters for the NSS queries.
				current.node("pre", getSampleTask().getCounters().toString());

			}

			if (showQueries) {

				/*
				 * Show the queries which are currently executing (accepted by
				 * the NanoSparqlServer).
				 */

				final long now = System.nanoTime();

				final TreeMap<Long, RunningQuery> ages = getQueryMap();

				{

					final Iterator<RunningQuery> itr = getQueries().values().iterator();

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

						current = current
							.node("p", "age=" + java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(age)
								+ "ms, queryId=" + query.queryId + "\n")
							.node("p", query.query + "\n");

					}

				}

			}

			if (showRunningQueries) {

				/*
				 * Show the queries which are currently executing (actually
				 * running on the QueryEngine).
				 */

				final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory.getQueryController(getIndexManager());

				final UUID[] queryIds = queryEngine.getRunningQueries();

				// final long now = System.nanoTime();

				final TreeMap<Long, IRunningQuery> ages = getQueryMap();

				for (UUID queryId : queryIds) {

					final IRunningQuery query = queryEngine.getRunningQuery(queryId);

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
						 * @todo The runstate and stats could be formatted into
						 * an HTML table ala QueryLog or RunState.
						 */
						current = current
							.node("p", "age=" + query.getElapsed() + "ms")
							.node("p", "queryId=" + query.getQueryId())
							.node("p", query.toString())
							.node("p", BOpUtility.toString(query.getQuery()));

					}

				}

			}

			doc.closeAll(current);

			buildResponse(resp, HTTP_OK, MIME_TEXT_HTML, doc.toString());

		} catch (IOException e) {
		
		    throw new RuntimeException(e);
		    
		}

	}

}
