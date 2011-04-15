package com.bigdata.rdf.sail.webapp;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultWriter;
import org.openrdf.query.resultio.BooleanQueryResultWriterRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailBooleanQuery;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.RelationSchema;
import com.bigdata.rwstore.RWStore;
import com.bigdata.sparse.ITPS;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

/**
 * Class encapsulates state shared by {@link QueryServlet}(s) for the same
 * {@link IIndexManager}.
 * 
 * @author Martyn Cutcher
 * @author thompsonbry@users.sourceforge.net
 */
public class BigdataRDFContext extends BigdataBaseContext {

    static private final transient Logger log = Logger
            .getLogger(BigdataRDFContext.class);

	private final SparqlEndpointConfig m_config;
	private final QueryParser m_engine;

    /**
     * A thread pool for running accepted queries against the
     * {@link QueryEngine}.
     */
    /*package*/final ExecutorService queryService;
	
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

		// used to parse queries.
		m_engine = new SPARQLParserFactory().getParser();

        if (config.queryThreadPoolSize == 0) {

            queryService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".queryService"));

        } else {

            queryService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    config.queryThreadPoolSize, new DaemonThreadFactory(
                            getClass().getName() + ".queryService"));

        }

		if (indexManager.getCollectQueueStatistics()) {

			final long initialDelay = 0; // initial delay in ms.
			final long delay = 1000; // delay in ms.
			final TimeUnit unit = TimeUnit.MILLISECONDS;

            m_queueSampleTask = new ThreadPoolExecutorBaseStatisticsTask(
                    (ThreadPoolExecutor) queryService);

            m_queueStatsFuture = indexManager.addScheduledTask(
                    m_queueSampleTask, initialDelay, delay, unit);

		} else {

			m_queueSampleTask = null;

			m_queueStatsFuture = null;

		}

	}

//    /**
//     * Normal shutdown waits until all accepted queries are done. 
//     */
//    void shutdown() {
//        
//        if(log.isInfoEnabled())
//            log.info("Normal shutdown.");
//
//        // Stop collecting queue statistics.
//        if (m_queueStatsFuture != null)
//            m_queueStatsFuture.cancel(true/* mayInterruptIfRunning */);
//
//        // Stop servicing new requests. 
//        queryService.shutdown();
//        try {
//            queryService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
//        } catch (InterruptedException ex) {
//            throw new RuntimeException(ex);
//        }
//        
//    }

    /**
     * Immediate shutdown interrupts any running queries.
     * 
     * FIXME Must abort any open transactions. This does not matter for the
     * standalone database, but it will make a difference in scale-out. The
     * transaction identifiers could be obtained from the {@link #queries} map.
     * 
     * FIXME This must also abort any running updates. Those are currently
     * running in thread handling the {@link HttpServletRequest}, however it
     * probably makes sense to execute them on a bounded thread pool as well.
     */
    void shutdownNow() {

        if(log.isInfoEnabled())
            log.info("Immediate shutdown.");
        
        // Stop collecting queue statistics.
        if (m_queueStatsFuture != null)
            m_queueStatsFuture.cancel(true/* mayInterruptIfRunning */);

        // Interrupt all running queries.
        queryService.shutdownNow();
        
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
        public final long timestamp;

        /** The SPARQL query string. */
        protected final String queryStr;

        /**
         * A symbolic constant indicating the type of query.
         */
        protected final QueryType queryType;
        
        /**
         * The negotiated MIME type to be used for the query response.
         */
        protected final String mimeType;

        /**
         * The character encoding to use with the negotiated {@link #mimeType}
         * -or- <code>null</code> (it will be <code>null</code> for a binary
         * encoding).
         */
        protected final Charset charset;
        
        /**
         * The file extension (without the leading ".") to use with the
         * negotiated {@link #mimeType}.
         */
        protected final String fileExt;
        
        /** The request. */
        private final HttpServletRequest req;
        
        /** Where to write the response. */
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
         * @param mimeType
         *            The MIME type to be used for the response. The caller must
         *            verify that the MIME Type is appropriate for the query
         *            type.
         * @param charset
         *            The character encoding to use with the negotiated MIME
         *            type (this is <code>null</code> for binary encodings).
         * @param fileExt
         *            The file extension (without the leading ".") to use with
         *            that MIME Type.
         * @param req
         *            The request.
         * @param os
         *            Where to write the data for the query result.
         */
        protected AbstractQueryTask(//
                final String namespace,//
                final long timestamp, //
                final String queryStr,//
                final QueryType queryType,//
                final String mimeType,//
                final Charset charset,//
                final String fileExt,//
                final HttpServletRequest req,//
                final OutputStream os//
        ) {

            if (namespace == null)
                throw new IllegalArgumentException();
            if (queryStr == null)
                throw new IllegalArgumentException();
            if (queryType == null)
                throw new IllegalArgumentException();
            if (mimeType == null)
                throw new IllegalArgumentException();
//            if (charset == null) // Note: null for binary encodings.
//                throw new IllegalArgumentException();
            if (fileExt == null)
                throw new IllegalArgumentException();
            if (req == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.queryStr = queryStr;
            this.queryType = queryType;
            this.mimeType = mimeType;
            this.charset = charset;
            this.fileExt = fileExt;
            this.req = req;
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
                m_queries.put(queryId, new RunningQuery(queryId.longValue(),
                        queryId2, queryStr, begin));
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
//            } catch (Throwable t) {
//                // launder and rethrow the exception.
//                throw BigdataRDFServlet.launderThrowable(t, resp, queryStr);
            } finally {
                m_queries.remove(queryId);
//                if (os != null) {
//                    try {
//                        os.close();
//                    } catch (Throwable t) {
//                        log.error(t, t);
//                    }
//                }
                if (cxn != null) {
                    try {
                        cxn.close();
                        if(log.isTraceEnabled())
                            log.trace("Connection closed.");
                    } catch (Throwable t) {
                        log.error(t, t);
                    }
                }
            }
        } // call()

    } // class AbstractQueryTask

    /**
     * Executes a ASK query.
     */
    private class AskQueryTask extends AbstractQueryTask {

        public AskQueryTask(final String namespace, final long timestamp,
                final String queryStr, final QueryType queryType,
                final BooleanQueryResultFormat format,
                final HttpServletRequest req, final OutputStream os) {

            super(namespace, timestamp, queryStr, queryType, format
                    .getDefaultMIMEType(), format.getCharset(), format
                    .getDefaultFileExtension(), req, os);

        }

        protected void doQuery(final BigdataSailRepositoryConnection cxn,
                final OutputStream os) throws Exception {

            final BigdataSailBooleanQuery query = cxn.prepareBooleanQuery(
                    QueryLanguage.SPARQL, queryStr, baseURI);
        
            // Note: getQueryTask() verifies that format will be non-null.
            final BooleanQueryResultFormat format = BooleanQueryResultWriterRegistry
                    .getInstance().getFileFormatForMIMEType(mimeType);

            final BooleanQueryResultWriter w = BooleanQueryResultWriterRegistry
                    .getInstance().get(format).getWriter(os);
            
            final boolean result = query.evaluate();
            
            w.write(result);

        }

    }

	/**
	 * Executes a tuple query.
	 */
	private class TupleQueryTask extends AbstractQueryTask {

        public TupleQueryTask(final String namespace, final long timestamp,
                final String queryStr, final QueryType queryType,
                final TupleQueryResultFormat format,
                final HttpServletRequest req,
                final OutputStream os) {

            super(namespace, timestamp, queryStr, queryType, format
                    .getDefaultMIMEType(), format.getCharset(), format
                    .getDefaultFileExtension(), req, os);

		}

		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

			final BigdataSailTupleQuery query = cxn.prepareTupleQuery(
					QueryLanguage.SPARQL, queryStr, baseURI);

            // Note: getQueryTask() verifies that format will be non-null.
            final TupleQueryResultFormat format = TupleQueryResultWriterRegistry
                    .getInstance().getFileFormatForMIMEType(mimeType);

            final TupleQueryResultWriter w = TupleQueryResultWriterRegistry
                    .getInstance().get(format).getWriter(os);

			query.evaluate(w);

		}

	}

	/**
	 * Executes a graph query.
	 */
    private class GraphQueryTask extends AbstractQueryTask {

        public GraphQueryTask(final String namespace, final long timestamp,
                final String queryStr, final QueryType queryType,
                final RDFFormat format, 
                final HttpServletRequest req,
                final OutputStream os) {

            super(namespace, timestamp, queryStr, queryType, format
                    .getDefaultMIMEType(), format.getCharset(), format
                    .getDefaultFileExtension(), req, os);

        }

		@Override
		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

			final BigdataSailGraphQuery query = cxn.prepareGraphQuery(
					QueryLanguage.SPARQL, queryStr, baseURI);

            /*
             * FIXME An error thrown here (such as if format is null and we do
             * not check it) will cause the response to hang, at least for the
             * test suite. Look into this further and make the error handling
             * bullet proof!
             * 
             * This may be related to queryId2. That should be imposed on the
             * IRunningQuery via a query hint such that the QueryEngine assigns
             * that UUID to the query. We can then correlate the queryId to the
             * IRunningQuery, which is important for some of the status pages.
             * This will also let us INTERRUPT the IRunningQuery if there is an
             * error during evaluation, which might be necessary. For example,
             * if the client dies while the query is running.  Look at the old
             * NSS code and see what it was doing and whether this was logic was
             * lost of simply never implemented.
             * 
             * However, I do not see how that would explain the failure of the
             * ft.get() method to return.
             */
//			if(true)
//			    throw new RuntimeException();

            // Note: getQueryTask() verifies that format will be non-null.
            final RDFFormat format = RDFWriterRegistry.getInstance()
                    .getFileFormatForMIMEType(mimeType);

            final RDFWriter w = RDFWriterRegistry.getInstance().get(format)
                    .getWriter(os);

			query.evaluate(w);

        }

	}

    /**
     * Return the task which will execute the query.
     * <p>
     * Note: The {@link OutputStream} is passed in rather than the
     * {@link HttpServletResponse} in order to permit operations such as
     * "DELETE WITH QUERY" where this method is used in a context which writes
     * onto an internal pipe rather than onto the {@link HttpServletResponse}.
     * 
     * @param queryStr
     *            The query.
     * @param req
     *            The request.
     * @param os
     *            Where to write the results.
     * 
     * @return The task.
     * 
     * @throws MalformedQueryException
     */
    public AbstractQueryTask getQueryTask(//
            final String namespace,//
            final long timestamp,//
            final String queryStr,//
            final HttpServletRequest req,//
            final OutputStream os) throws MalformedQueryException {

        /*
         * Parse the query so we can figure out how it will need to be executed.
         * 
         * FIXME Parse the query once. [This will fail a query on its syntax.
         * However, the logic used in the tasks to execute a query will not fail
         * a bad query for some reason which I have not figured out yet.
         * Therefore, we are in the position of having to parse the query here
         * and then again when it is executed.]
         */
        final ParsedQuery q = m_engine.parseQuery(queryStr, null/*baseURI*/);
        
        if(log.isDebugEnabled())
            log.debug(q.toString());
        
        final QueryType queryType = QueryType.fromQuery(queryStr);

        /*
         * CONNEG for the MIME type.
         * 
         * Note: An attempt to CONNEG for a MIME type which can not be used with
         * a give type of query will result in a response using a default MIME
         * Type for that query.
         * 
         * TODO This is a hack which will obey an Accept header IF the header
         * contains a single well-formed MIME Type. Complex accept headers will
         * not be matched and quality parameters (q=...) are ignored. (Sesame
         * has some stuff related to generating Accept headers in their
         * RDFFormat which could bear some more looking into in this regard.)
         */
        final String acceptStr = req.getHeader("Accept");

        switch (queryType) {
        case ASK: {

            final BooleanQueryResultFormat format = acceptStr == null ? null
                    : BooleanQueryResultFormat.forMIMEType(acceptStr,
                            BooleanQueryResultFormat.SPARQL);

            return new AskQueryTask(namespace, timestamp, queryStr, queryType,
                    format, req, os);

        }
        case DESCRIBE:
        case CONSTRUCT: {

            final RDFFormat format = RDFFormat.forMIMEType(acceptStr,
                    RDFFormat.RDFXML);

            return new GraphQueryTask(namespace, timestamp, queryStr,
                    queryType, format, req, os);

        }
        case SELECT: {

            final TupleQueryResultFormat format = TupleQueryResultFormat
                    .forMIMEType(acceptStr, TupleQueryResultFormat.SPARQL);

            return new TupleQueryTask(namespace, timestamp, queryStr,
                    queryType, format, req, os);

        }
        } // switch(queryType)

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

        if (timestamp == ITx.UNISOLATED)
            throw new IllegalArgumentException("UNISOLATED reads disallowed.");
        
        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null) {

            throw new RuntimeException("Not found: namespace=" + namespace
                    + ", timestamp=" + TimestampUtility.toString(timestamp));

        }

        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        return (BigdataSailRepositoryConnection) repo
                .getReadOnlyConnection(timestamp);

    }

    /**
     * Return an UNISOLATED connection.
     * 
     * @param namespace
     *            The namespace.
     * 
     * @return The UNISOLATED connection.
     * 
     * @throws SailException
     * 
     * @throws RepositoryException
     */
    public BigdataSailRepositoryConnection getUnisolatedConnection(
            final String namespace) throws SailException, RepositoryException {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("Not found: namespace=" + namespace);

        }

        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        final BigdataSailRepositoryConnection conn = (BigdataSailRepositoryConnection) repo
                .getUnisolatedConnection();

        conn.setAutoCommit(false);

        return conn;

    }

    /**
     * Return various interesting metadata about the KB state.
     * 
     * @todo The range counts can take some time if the cluster is heavily
     *       loaded since they must query each shard for the primary statement
     *       index and the TERM2ID index.
     */
    protected StringBuilder getKBInfo(final String namespace,
            final long timestamp) {

        final StringBuilder sb = new StringBuilder();

        BigdataSailRepositoryConnection conn = null;

        try {

            conn = getQueryConnection(namespace, timestamp);
            
            final AbstractTripleStore tripleStore = conn.getTripleStore();

            sb.append("class\t = " + tripleStore.getClass().getName() + "\n");

            sb
                    .append("indexManager\t = "
                            + tripleStore.getIndexManager().getClass()
                                    .getName() + "\n");

            sb.append("namespace\t = " + tripleStore.getNamespace() + "\n");

            sb.append("timestamp\t = "
                    + TimestampUtility.toString(tripleStore.getTimestamp())
                    + "\n");

            sb.append("statementCount\t = " + tripleStore.getStatementCount()
                    + "\n");

            sb.append("termCount\t = " + tripleStore.getTermCount() + "\n");

            sb.append("uriCount\t = " + tripleStore.getURICount() + "\n");

            sb.append("literalCount\t = " + tripleStore.getLiteralCount() + "\n");

            /*
             * Note: The blank node count is only available when using the told
             * bnodes mode.
             */
            sb
                    .append("bnodeCount\t = "
                            + (tripleStore.getLexiconRelation()
                                    .isStoreBlankNodes() ? ""
                                    + tripleStore.getBNodeCount() : "N/A")
                            + "\n");

            sb.append(IndexMetadata.Options.BTREE_BRANCHING_FACTOR
                    + "="
                    + tripleStore.getSPORelation().getPrimaryIndex()
                            .getIndexMetadata().getBranchingFactor() + "\n");

            sb.append(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY
                    + "="
                    + tripleStore.getSPORelation().getPrimaryIndex()
                            .getIndexMetadata()
                            .getWriteRetentionQueueCapacity() + "\n");

            sb.append(BigdataSail.Options.STAR_JOINS + "="
                    + conn.getRepository().getSail().isStarJoins() + "\n");

            sb.append("-- All properties.--\n");
            
            // get the triple store's properties from the global row store.
            final Map<String, Object> properties = getIndexManager()
                    .getGlobalRowStore().read(RelationSchema.INSTANCE,
                            namespace);

            // write them out,
            for (String key : properties.keySet()) {
                sb.append(key + "=" + properties.get(key)+"\n");
            }

            /*
             * And show some properties which can be inherited from
             * AbstractResource. These have been mainly phased out in favor of
             * BOP annotations, but there are a few places where they are still
             * in use.
             */
            
            sb.append("-- Interesting AbstractResource effective properties --\n");
            
            sb.append(AbstractResource.Options.CHUNK_CAPACITY + "="
                    + tripleStore.getChunkCapacity() + "\n");

            sb.append(AbstractResource.Options.CHUNK_OF_CHUNKS_CAPACITY + "="
                    + tripleStore.getChunkOfChunksCapacity() + "\n");

            sb.append(AbstractResource.Options.CHUNK_TIMEOUT + "="
                    + tripleStore.getChunkTimeout() + "\n");

            sb.append(AbstractResource.Options.FULLY_BUFFERED_READ_THRESHOLD + "="
                    + tripleStore.getFullyBufferedReadThreshold() + "\n");

            sb.append(AbstractResource.Options.MAX_PARALLEL_SUBQUERIES + "="
                    + tripleStore.getMaxParallelSubqueries() + "\n");

            /*
             * And show some interesting effective properties for the KB, SPO
             * relation, and lexicon relation.
             */
            sb.append("-- Interesting KB effective properties --\n");
            
            sb
                    .append(AbstractTripleStore.Options.TERM_CACHE_CAPACITY
                            + "="
                            + tripleStore
                                    .getLexiconRelation()
                                    .getProperties()
                                    .getProperty(
                                            AbstractTripleStore.Options.TERM_CACHE_CAPACITY,
                                            AbstractTripleStore.Options.DEFAULT_TERM_CACHE_CAPACITY) + "\n");

            /*
             * And show several interesting properties with their effective
             * defaults.
             */

            sb.append("-- Interesting Effective BOP Annotations --\n");

            sb.append(BufferAnnotations.CHUNK_CAPACITY
                    + "="
                    + tripleStore.getProperties().getProperty(
                            BufferAnnotations.CHUNK_CAPACITY,
                            "" + BufferAnnotations.DEFAULT_CHUNK_CAPACITY)
                    + "\n");

            sb
                    .append(BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY
                            + "="
                            + tripleStore
                                    .getProperties()
                                    .getProperty(
                                            BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                                            ""
                                                    + BufferAnnotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY)
                            + "\n");

            sb.append(BufferAnnotations.CHUNK_TIMEOUT
                    + "="
                    + tripleStore.getProperties().getProperty(
                            BufferAnnotations.CHUNK_TIMEOUT,
                            "" + BufferAnnotations.DEFAULT_CHUNK_TIMEOUT)
                    + "\n");

            sb.append(PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS
                    + "="
                    + tripleStore.getProperties().getProperty(
                            PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS,
                            "" + PipelineJoin.Annotations.DEFAULT_MAX_PARALLEL_CHUNKS) + "\n");

            sb
                    .append(IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD
                            + "="
                            + tripleStore
                                    .getProperties()
                                    .getProperty(
                                            IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD,
                                            ""
                                                    + IPredicate.Annotations.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD)
                            + "\n");

            // sb.append(tripleStore.predicateUsage());

            if (tripleStore.getIndexManager() instanceof Journal) {

                final Journal journal = (Journal) tripleStore.getIndexManager();
                
                final IBufferStrategy strategy = journal.getBufferStrategy();
                
                if (strategy instanceof RWStrategy) {
                
                    final RWStore store = ((RWStrategy) strategy).getRWStore();
                    
                    store.showAllocators(sb);
                    
                }
                
            }

        } catch (Throwable t) {

            log.warn(t.getMessage(), t);

        } finally {
            
            if(conn != null) {
                try {
                    conn.close();
                } catch (RepositoryException e) {
                    log.error(e, e);
                }
                
            }
            
        }

        return sb;

    }

    /**
     * Return a list of the namespaces for the {@link AbstractTripleStore}s
     * registered against the bigdata instance.
     */
    /*package*/ List<String> getNamespaces() {
    
        // the triple store namespaces.
        final List<String> namespaces = new LinkedList<String>();

        // scan the relation schema in the global row store.
        final Iterator<ITPS> itr = (Iterator<ITPS>) getIndexManager()
                .getGlobalRowStore().rangeIterator(RelationSchema.INSTANCE);

        while (itr.hasNext()) {

            // A timestamped property value set is a logical row with
            // timestamped property values.
            final ITPS tps = itr.next();

            // If you want to see what is in the TPS, uncomment this.
//          System.err.println(tps.toString());
            
            // The namespace is the primary key of the logical row for the
            // relation schema.
            final String namespace = (String) tps.getPrimaryKey();

            // Get the name of the implementation class
            // (AbstractTripleStore, SPORelation, LexiconRelation, etc.)
            final String className = (String) tps.get(RelationSchema.CLASS)
                    .getValue();

            try {
                final Class<?> cls = Class.forName(className);
                if (AbstractTripleStore.class.isAssignableFrom(cls)) {
                    // this is a triple store (vs something else).
                    namespaces.add(namespace);
                }
            } catch (ClassNotFoundException e) {
                log.error(e,e);
            }

        }

        return namespaces;

    }
    
}
