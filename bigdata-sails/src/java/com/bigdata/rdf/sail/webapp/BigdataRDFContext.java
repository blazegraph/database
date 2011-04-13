package com.bigdata.rdf.sail.webapp;

import info.aduna.xml.XMLWriter;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

/**
 * 
 * @author Martyn Cutcher
 * @author thompsonbry@users.sourceforge.net
 */
public class BigdataRDFContext extends BigdataBaseContext {

    static private final transient Logger log = Logger
            .getLogger(BigdataRDFContext.class);

	private final SparqlEndpointConfig m_config;
	private final QueryParser m_engine;
	
	private final ScheduledFuture<?> m_queueStatsFuture;
	private final ThreadPoolExecutorBaseStatisticsTask m_queueSampleTask;

    /**
     * The currently executing queries (does not include queries where a client
     * has established a connection but the query is not running because the
     * {@link #queryService} is blocking).
     */
    private final ConcurrentHashMap<Long/* queryId */, RunningQuery> m_queries = new ConcurrentHashMap<Long, RunningQuery>();
    
    /**
     * Factory for the query identifiers.
     */
    private final AtomicLong m_queryIdFactory = new AtomicLong();
    
    final public Map<Long, RunningQuery> getQueries() {

        return m_queries;
        
    }
    
    final public AtomicLong getQueryIdFactory() {
    
        return m_queryIdFactory;
        
    }
    
    public BigdataRDFContext(final SparqlEndpointConfig config,
            final IIndexManager indexManager) {

        super(indexManager);
        
        if(config == null)
            throw new IllegalArgumentException();
        
		if (config.namespace == null)
			throw new IllegalArgumentException();

		m_config = config;

		// used to parse qeries.
		m_engine = new SPARQLParserFactory().getParser();

		if (indexManager.getCollectQueueStatistics()) {

			final long initialDelay = 0; // initial delay in ms.
			final long delay = 1000; // delay in ms.
			final TimeUnit unit = TimeUnit.MILLISECONDS;

			// FIXME add mechanism for stats sampling
			// queueSampleTask = new ThreadPoolExecutorBaseStatisticsTask(
			// (ThreadPoolExecutor) queryService);
			//			
			// queueStatsFuture = indexManager.addScheduledTask(queueSampleTask,
			// initialDelay, delay, unit);

			m_queueSampleTask = null;

			m_queueStatsFuture = null;

		} else {

			m_queueSampleTask = null;

			m_queueStatsFuture = null;

		}

	}

    /*
     * FIXME Provide shutdown semantics for the statistics collection on the
     * SPARQL end point and the thread pool for processing SPARQL queries.
     */
    public void shutdownNow() {

        if(log.isInfoEnabled())
            log.info("Normal shutdown.");
        
        // Stop collecting queue statistics.
		if (m_queueStatsFuture != null)
			m_queueStatsFuture.cancel(true/* mayInterruptIfRunning */);

    }

    public SparqlEndpointConfig getConfig() {
		
	    return m_config;
	    
	}

	public ThreadPoolExecutorBaseStatisticsTask getSampleTask() {

	    return m_queueSampleTask;
	    
	}

	/**
     * Abstract base class for running queries handles the timing, pipe,
     * reporting, obtains the connection, and provides the finally {} semantics
     * for each type of query task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public abstract class AbstractQueryTask implements Callable<Void> {
        
        /** The namespace against which the query will be run. */
        private final String namespace;

        /**
         * The timestamp of the view for that namespace against which the query
         * will be run.
         */
        private final long timestamp;

        /** The SPARQL query string. */
        protected final String queryStr;

        /**
         * A symbolic constant indicating the type of query.
         */
        protected final BigdataRDFServlet.QueryType queryType;
        
        /**
         * The negotiated MIME type to be used for the query response.
         */
        protected final String mimeType;
        
        /** A pipe used to incrementally deliver the results to the client. */
        private final OutputStream os;

        /**
         * Sesame has an option for a base URI during query evaluation. This
         * provides a symbolic place holder for that URI in case we ever provide
         * a hook to set it.
         */
        protected final String baseURI = null;

        /**
         * The queryId as assigned by the SPARQL end point (rather than the
         * {@link QueryEngine}).
         */
        protected final Long queryId;
        
        /**
         * The queryId used by the {@link QueryEngine}.
         */
        protected final UUID queryId2;
        
        /**
         * 
         * @param namespace
         *            The namespace against which the query will be run.
         * @param timestamp
         *            The timestamp of the view for that namespace against which
         *            the query will be run.
         * @param queryStr
         *            The SPARQL query string.
         * @param os
         *            A pipe used to incrementally deliver the results to the
         *            client.
         */
        protected AbstractQueryTask(final String namespace,
                final long timestamp, final String queryStr,
                final BigdataRDFServlet.QueryType queryType,
                final String mimeType,
                final OutputStream os) {

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.queryStr = queryStr;
            this.queryType = queryType;
            this.mimeType = mimeType;
            this.os = os;
            this.queryId = Long.valueOf(m_queryIdFactory.incrementAndGet());
            this.queryId2 = UUID.randomUUID();

        }

        /**
         * Execute the query.
         * 
         * @param cxn
         *            The connection.
         * @param os
         *            Where the write the query results.
         * 
         * @throws Exception
         */
        abstract protected void doQuery(BigdataSailRepositoryConnection cxn,
                OutputStream os) throws Exception;

        final public Void call() throws Exception {
            final long begin = System.nanoTime();
			BigdataSailRepositoryConnection cxn = null;
            try {
                cxn = getQueryConnection(namespace, timestamp);
                m_queries.put(queryId, new RunningQuery(queryId.longValue(),queryId2,
                        queryStr, begin));
                if(log.isTraceEnabled())
                    log.trace("Query running...");
//                try {
                	doQuery(cxn, os);
//                } catch(Throwable t) {
//                	/*
//                	 * Log the query and the exception together.
//                	 */
//					log.error(t.getLocalizedMessage() + ":\n" + queryStr, t);
//                }
                	if(log.isTraceEnabled())
                	    log.trace("Query done - flushing results.");
                os.flush();
                os.close();
                if(log.isTraceEnabled())
                    log.trace("Query done - output stream closed.");
                return null;
            } catch (Throwable t) {
                // launder and rethrow the exception.
                throw BigdataRDFServlet.launderThrowable(t, os, queryStr);
            } finally {
                m_queries.remove(queryId);
                try {
                    os.close();
                } catch (Throwable t) {
                    log.error(t, t);
                }
                try {
                    if (cxn != null)
                        cxn.close();
                } catch (Throwable t) {
                    log.error(t, t);
                }
            }
        }

    }

	/**
	 * Executes a tuple query.
	 */
	private class TupleQueryTask extends AbstractQueryTask {

        public TupleQueryTask(final String namespace, final long timestamp,
                final String queryStr, final BigdataRDFServlet.QueryType queryType,
                final String mimeType, final OutputStream os) {

			super(namespace, timestamp, queryStr, queryType, mimeType, os);

		}

		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

			final BigdataSailTupleQuery query = cxn.prepareTupleQuery(
					QueryLanguage.SPARQL, queryStr, baseURI);
			
			if (true) {
				StringWriter strw = new StringWriter();
				
				query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(strw)));
				
				OutputStreamWriter outstr = new OutputStreamWriter(os);
				String res = strw.toString();
				outstr.write(res);
				outstr.flush();
				outstr.close();
			} else {
				query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(os)));
			}
		}

	}

	/**
	 * Executes a graph query.
	 */
    private class GraphQueryTask extends AbstractQueryTask {

        public GraphQueryTask(final String namespace, final long timestamp,
                final String queryStr, final BigdataRDFServlet.QueryType queryType,
                final String mimeType, final OutputStream os) {

            super(namespace, timestamp, queryStr, queryType, mimeType, os);

        }

		@Override
		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

			final BigdataSailGraphQuery query = cxn.prepareGraphQuery(
					QueryLanguage.SPARQL, queryStr, baseURI);

           query.evaluate(new RDFXMLWriter(os));

        }

	}
    
	/**
	 * Return the task which will execute the query.
	 * 
	 * @param queryStr
	 *            The query.
	 * @param os
	 *            Where the task will write its output.
	 *            
	 * @return The task.
	 * 
	 * @throws MalformedQueryException 
	 */
    public AbstractQueryTask getQueryTask(final String namespace,
            final long timestamp, final String queryStr,
            final HttpServletRequest req,
            final OutputStream os) throws MalformedQueryException {
    	
		/*
		 * Parse the query so we can figure out how it will need to be executed.
		 * 
		 * Note: This will fail a query on its syntax. However, the logic used
		 * in the tasks to execute a query will not fail a bad query for some
		 * reason which I have not figured out yet. Therefore, we are in the
		 * position of having to parse the query here and then again when it is
		 * executed.
		 */
        final ParsedQuery q = m_engine.parseQuery(queryStr, null/*baseURI*/);
        
        if(log.isDebugEnabled())
            log.debug(q.toString());
        
		final BigdataRDFServlet.QueryType queryType = BigdataRDFServlet.QueryType
				.fromQuery(queryStr);

		final String mimeType;
		switch (queryType) {
		case ASK:
			/*
			 * FIXME handle ASK.
			 */
			break;
		case DESCRIBE:
		case CONSTRUCT:
            // FIXME Conneg for the mime type for construct/describe!
            mimeType = BigdataRDFServlet.MIME_RDF_XML;
            return new GraphQueryTask(namespace, timestamp, queryStr,
                    queryType, mimeType, os);
        case SELECT:
            mimeType = BigdataRDFServlet.MIME_SPARQL_RESULTS_XML;
            return new TupleQueryTask(namespace, timestamp, queryStr,
                    queryType, mimeType, os);
        }

		throw new RuntimeException("Unknown query type: " + queryType);

	}

	/**
     * Metadata about running queries.
     */
	static class RunningQuery {

		/**
		 * The unique identifier for this query as assigned by the SPARQL 
		 * end point (rather than the {@link QueryEngine}).
		 */
		final long queryId;

		/**
		 * The unique identifier for this query for the {@link QueryEngine}.
		 * 
		 * @see QueryEngine#getRunningQuery(UUID)
		 */
		final UUID queryId2;

		/** The query. */
		final String query;
		
		/** The timestamp when the query was accepted (ns). */
		final long begin;

		public RunningQuery(final long queryId, final UUID queryId2,
				final String query, final long begin) {

			this.queryId = queryId;

			this.queryId2 = queryId2;
			
			this.query = query;

			this.begin = begin;

		}

	}

    /**
     * Return a read-only transaction which will read from the commit point
     * associated with the given timestamp.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            The timestamp.
     * 
     * @throws RepositoryException
     * 
     * @todo enforce historical query by making sure timestamps conform (we do
     *       not want to allow read/write tx queries unless update semantics are
     *       introduced ala SPARQL 1.1).
     * 
     * @todo Use a distributed read-only tx for queries (it would be nice if a
     *       tx used 2PL to specify which namespaces it could touch).
     */
    public BigdataSailRepositoryConnection getQueryConnection(
            final String namespace, final long timestamp)
            throws RepositoryException {
        
        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null) {

            throw new RuntimeException("Not found: namespace=" + namespace
                    + ", timestamp=" + TimestampUtility.toString(timestamp));

        }

        /*
         * Since the kb exists, wrap it as a sail.
         * 
         * @todo cache? close when not in use any more?
         */
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        return (BigdataSailRepositoryConnection) repo
                .getReadOnlyConnection(timestamp);

    }

}
