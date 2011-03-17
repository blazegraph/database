/*

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
/*
 * Created on May 29, 2010
 */
package com.bigdata.rdf.sail.bench;

import info.aduna.xml.XMLWriter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.bench.NanoSparqlClient.QueryType;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.RelationSchema;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.sparse.ITPS;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.bigdata.util.httpd.NanoHTTPD;

/**
 * A flyweight SPARQL endpoint using HTTP.
 * 
 * @author thompsonbry@users.sourceforge.net
 * 
 * @todo Allow configuration options for the sparql endpoint either as URI
 *       parameters, in the property file, as request headers, or as query hints
 *       using the PREFIX mechanism.
 * 
 * @todo Isn't there a baseURI option for SPARQL end points?
 * 
 * @todo Add an "?explain" URL query parameter and show the execution plan and
 *       costs (or make this a navigable option from the set of running queries
 *       to drill into their running costs and offer an opportunity to kill them
 *       as well).
 * 
 * @todo Add command to kill a running query, e.g., from the view of the long
 *       running queries.
 * 
 * @todo Report other performance counters using {@link CounterSetHTTPD}
 * 
 * @todo Simple update protocol.
 * 
 * @todo If the addressed instance uses full transactions, then mutation should
 *       also use a full transaction.
 * 
 * @todo Remote command to bulk load data from a remote or local resource (it's
 *       pretty much up to people handling deployment to secure access to
 *       queries, update requests, and bulk load requests).
 * 
 * @todo Remote command to advance the read-behind point. This will let people
 *       bulk load a bunch of stuff before advancing queries to read from the
 *       new consistent commit point.
 * 
 * @todo Review the settings for the {@link RDFParser} instances, e.g.,
 *       verifyData, preserveBNodeIds, etc. Perhaps we should use the same
 *       defaults as the {@link DataLoader}?
 * 
 * @todo It is possible that we could have concurrent requests which each get
 *       the unisolated connection. This could cause two problems: (1) we could
 *       exhaust our request pool, which would cause the server to block; and
 *       (2) I need to verify that the exclusive semaphore logic for the
 *       unisolated sail connection works with cross thread access. Someone had
 *       pointed out a bizarre hole in this....
 */
public class NanoSparqlServer extends AbstractHTTPD {

	/**
	 * The logger for the concrete {@link NanoSparqlServer} class.  The {@link NanoHTTPD}
	 * class has its own logger.
	 */
	static private final Logger log = Logger.getLogger(NanoSparqlServer.class); 
	
	/**
	 * A SPARQL results set in XML.
	 */
	static public final String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";

	/**
	 * RDF/XML.
	 */
	static public final String MIME_RDF_XML = "application/rdf+xml";

	/**
	 * The character set used for the response (not negotiated).
	 */
    static private final String charset = "UTF-8";
   
    /**
     * The configuration object.
     */
    private final Config config;
    
    /**
     * Provides access to the bigdata database.
     */
    private final IIndexManager indexManager;
    
	/**
	 * @todo use to decide ASK, DESCRIBE, CONSTRUCT, SELECT, EXPLAIN, etc.
	 */
	private final QueryParser engine;
	
    /**
     * Runs a pool of threads for handling requests.
     */
    private final ExecutorService queryService;

    private final LinkedBlockingQueue<byte[]> pipeBufferPool;
    
    /**
     * Metadata about running queries.
     */
	private static class RunningQuery {

		/**
		 * The unique identifier for this query for the {@link NanoSparqlServer}.
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
     * The currently executing queries (does not include queries where a client
     * has established a connection but the query is not running because the
     * {@link #queryService} is blocking).
     */
    private final ConcurrentHashMap<Long/* queryId */, RunningQuery> queries = new ConcurrentHashMap<Long, RunningQuery>();
    
    /**
     * Factory for the query identifiers.
     */
    private final AtomicLong queryIdFactory = new AtomicLong();

	/**
	 * 
	 * @param config
	 *            The configuration for the server.
	 * @param indexManager
	 *            The database instance that the server will operate against.
	 * 
	 * @throws IOException
	 * @throws SailException
	 * @throws RepositoryException
	 */
	public NanoSparqlServer(final Config config,
			final IIndexManager indexManager) throws IOException,
			SailException, RepositoryException {

		super(config.port);

        if (config.namespace == null)
            throw new IllegalArgumentException();
		
		if(indexManager == null)
		    throw new IllegalArgumentException();
		
		this.config = config;
		
		this.indexManager = indexManager;
		
		// used to parse qeries.
        engine = new SPARQLParserFactory().getParser();

        if (config.queryThreadPoolSize == 0) {

            queryService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".queryService"));

            // no buffer pool since the #of requests is unbounded.
			pipeBufferPool = null;

		} else {

			queryService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
					config.queryThreadPoolSize, new DaemonThreadFactory(
							getClass().getName() + ".queryService"));

			// create a buffer pool which is reused for each request.
			pipeBufferPool = new LinkedBlockingQueue<byte[]>(
					config.queryThreadPoolSize);
			
			for (int i = 0; i < config.queryThreadPoolSize; i++) {

				pipeBufferPool.add(new byte[config.bufferCapacity]);

			}

		}

		if (indexManager.getCollectQueueStatistics()) {

			final long initialDelay = 0; // initial delay in ms.
			final long delay = 1000; // delay in ms.
			final TimeUnit unit = TimeUnit.MILLISECONDS;

			queueSampleTask = new ThreadPoolExecutorBaseStatisticsTask(
					(ThreadPoolExecutor) queryService);
			
			queueStatsFuture = indexManager.addScheduledTask(queueSampleTask,
					initialDelay, delay, unit);

		} else {
		
			queueSampleTask = null;
			
			queueStatsFuture = null;
			
		}

	}
	private final ScheduledFuture<?> queueStatsFuture;
	private final ThreadPoolExecutorBaseStatisticsTask queueSampleTask;

	/**
	 * Return a list of the registered {@link AbstractTripleStore}s.
	 */
	protected List<String> getNamespaces() {
	
		// the triple store namespaces.
		final List<String> namespaces = new LinkedList<String>();

		// scan the relation schema in the global row store.
		final Iterator<ITPS> itr = (Iterator<ITPS>) indexManager
				.getGlobalRowStore().rangeIterator(RelationSchema.INSTANCE);

		while (itr.hasNext()) {

			// A timestamped property value set is a logical row with
			// timestamped property values.
			final ITPS tps = itr.next();

			// If you want to see what is in the TPS, uncomment this.
//			System.err.println(tps.toString());
			
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
			final Map<String, Object> properties = indexManager
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
     * {@inheritDoc}
     * <p>
     * Overridden to wait until all running queries are complete before 
     */
    @Override
    public void shutdown() {
        if(log.isInfoEnabled())
            log.info("Normal shutdown.");
        // Stop collecting queue statistics.
		if (queueStatsFuture != null)
			queueStatsFuture.cancel(true/* mayInterruptIfRunning */);
        // Tell NanoHTTP to stop accepting new requests.
        super.shutdown();
        // Stop servicing new requests. 
        queryService.shutdown();
        try {
            queryService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        /*
         * Note: This is using the atomic boolean as a lock in addition to
         * relying on its visibility guarantee.
         */
		synchronized (alive) {
			alive.set(false);
			alive.notifyAll();
		}
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to interrupt all running requests.
     * 
     * FIXME Must abort any open transactions. This does not matter for the
     * standalone database, but it will make a difference in scale-out. The
     * transaction identifiers could be obtained from the {@link #queries}
     * map.
     */
    @Override
    public void shutdownNow() {
        if(log.isInfoEnabled())
            log.info("Normal shutdown.");
        // Stop collecting queue statistics.
		if (queueStatsFuture != null)
			queueStatsFuture.cancel(true/* mayInterruptIfRunning */);
        // Immediately stop accepting connections and interrupt open requests.
        super.shutdownNow();
        // Interrupt all running queries.
    	queryService.shutdownNow();
        /*
         * Note: This is using the atomic boolean as a lock in addition to
         * relying on its visibility guarantee.
         */
        synchronized (alive) {
			alive.set(false);
			alive.notifyAll();
		}
    }

    /**
     * Note: This uses an atomic boolean in order to give us a synchronization
     * object whose state also serves as a condition variable. findbugs objects,
     * but this is a deliberate usage.
     */
    private final AtomicBoolean alive = new AtomicBoolean(true);

    /**
     * <p>
     * Perform an HTTP-POST, which corresponds to the basic CRUD operation
     * "create" according to the generic interaction semantics of HTTP REST. The
     * operation will be executed against the target namespace per the URI.
     * </p>
     * 
     * <pre>
     * POST [/namespace/NAMESPACE]
     * ...
     * Content-Type: 
     * ...
     * 
     * BODY
     * </pre>
     * <p>
     * Where <code>BODY</code> is the new RDF content using the representation
     * indicated by the <code>Content-Type</code>.
     * </p>
     * <p>
     * -OR-
     * </p>
     * 
     * <pre>
     * POST [/namespace/NAMESPACE] ?uri=URL
     * </pre>
     * <p>
     * Where <code>URI</code> identifies a resource whose RDF content will be
     * inserted into the database. The <code>uri</code> query parameter may
     * occur multiple times. All identified resources will be loaded within a
     * single native transaction. Bigdata provides snapshot isolation so you can
     * continue to execute queries against the last commit point while this
     * operation is executed.
     * </p>
     * 
     * <p>
     * You can shutdown the server using:
     * </p>
     * 
     * <pre>
     * POST /stop
     * </pre>
     * 
     * <p>
     * A status page is available:
     * </p>
     * 
     * <pre>
     * POST /status
     * </pre>
     */
	@Override
	public Response doPost(final Request req) throws Exception {

        final String uri = req.uri;
        
		if("/stop".equals(uri)) {

		    return doStop(req);

		}

        if("/status".equals(uri)) {
            
            return doStatus(req);

        }

        final String queryStr = getQueryStr(req.params);
        
        if (queryStr != null) {

            return doQuery(req);
            
        }

        final String contentType = req.getContentType();

        if (contentType != null) {

            return doPostWithBody(req);

        }
 
        if (req.params.get("uri") != null) {

            return doPostWithURIs(req);
            
        }
        
        return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN, uri);

	}

    /**
     * POST with request body containing statements to be inserted.
     * 
     * @param req
     *            The request.
     *
     * @return The response.
     * 
     * @throws Exception
     */
    private Response doPostWithBody(final Request req) throws Exception {
        
        final String baseURI = "";// @todo baseURI query parameter?
        
        final String namespace = getNamespace(req.uri);

        final String contentType = req.getContentType();

        if (contentType == null)
            throw new UnsupportedOperationException();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        final RDFFormat format = RDFFormat.forMIMEType(contentType);

        if (format == null) {
            return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as RDF: "
                            + contentType);
        }

        if (log.isInfoEnabled())
            log.info("RDFFormat=" + format);
        
        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(format);

        if (rdfParserFactory == null) {
            return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                    "Parser not found: Content-Type=" + contentType);
        }

        try {

            // resolve the default namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null)
                return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Not found: namespace=" + namespace);

            final AtomicLong nmodified = new AtomicLong(0L);

            // Wrap with SAIL.
            final BigdataSail sail = new BigdataSail(tripleStore);
            BigdataSailConnection conn = null;
            try {

                sail.initialize();
                conn = sail.getConnection();

                /*
                 * There is a request body, so let's try and parse it.
                 */

                final RDFParser rdfParser = rdfParserFactory.getParser();

                rdfParser.setValueFactory(tripleStore.getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new AddStatementHandler(conn,nmodified));

                /*
                 * Run the parser, which will cause statements to be inserted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                return new Response(HTTP_OK, MIME_TEXT_PLAIN, nmodified.get()
                        + " statements modified.");

            } finally {

                if (conn != null)
                    conn.close();

//                sail.shutDown();
                
            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);
            
        }

    }

    /**
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    private static class AddStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        
        public AddStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified) {
            this.conn = conn;
            this.nmodified = nmodified;
        }

        public void handleStatement(Statement stmt) throws RDFHandlerException {

            try {

                conn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        (Resource[]) (stmt.getContext() == null ? null
                                : new Resource[] { stmt.getContext() })//
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            nmodified.incrementAndGet();

        }

    }
    
    /**
     * Helper class removes statements from the sail as they are visited by a parser.
     */
    private static class RemoveStatementHandler extends RDFHandlerBase {

        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        
        public RemoveStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified) {
            this.conn = conn;
            this.nmodified = nmodified;
        }

        public void handleStatement(Statement stmt) throws RDFHandlerException {

            try {

                conn.removeStatements(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        (Resource[]) (stmt.getContext() == null ? null
                                : new Resource[] { stmt.getContext() })//
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            nmodified.incrementAndGet();

        }

    }
    
    /**
     * POST with URIs of resources to be inserted.
     * 
     * @param req
     *            The request.
     *
     * @return The response.
     * 
     * @throws Exception
     */
    private Response doPostWithURIs(final Request req) throws Exception {
        
        final String namespace = getNamespace(req.uri);

        final String contentType = req.getContentType();

        final Vector<String> uris = req.params.get("uri");
        
        if (uris == null)
            throw new UnsupportedOperationException();

        if (uris.isEmpty())
            return new Response(HTTP_OK, MIME_TEXT_PLAIN,
                    "0 statements modified");

        if (log.isInfoEnabled())
            log.info("URIs: " + uris);

        // Before we do anything, make sure we have valid URLs.
        final Vector<URL> urls = new Vector<URL>(uris.size());
        for (String uri : uris) {
            urls.add(new URL(uri));
        }

        try {

            // resolve the default namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null)
                return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Not found: namespace=" + namespace);

            final AtomicLong nmodified = new AtomicLong(0L);

            // Wrap with SAIL.
            final BigdataSail sail = new BigdataSail(tripleStore);
            BigdataSailConnection conn = null;
            try {

                conn = sail.getConnection();

                for (URL url : urls) {

                    HttpURLConnection hconn = null;
                    try {

                        hconn = (HttpURLConnection) url.openConnection();
                        hconn.setRequestMethod(NanoHTTPD.GET);
                        hconn.setReadTimeout(0);// no timeout? http param?

                        /*
                         * There is a request body, so let's try and parse it.
                         */

                        final RDFFormat format = RDFFormat
                                .forMIMEType(contentType);

                        if (format == null) {
                            return new Response(HTTP_BADREQUEST,
                                    MIME_TEXT_PLAIN,
                                    "Content-Type not recognized as RDF: "
                                            + contentType);
                        }

                        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                                .getInstance().get(format);

                        if (rdfParserFactory == null) {
                            return new Response(HTTP_INTERNALERROR,
                                    MIME_TEXT_PLAIN,
                                    "Parser not found: Content-Type="
                                            + contentType);
                        }

                        final RDFParser rdfParser = rdfParserFactory
                                .getParser();

                        rdfParser
                                .setValueFactory(tripleStore.getValueFactory());

                        rdfParser.setVerifyData(true);

                        rdfParser.setStopAtFirstError(true);

                        rdfParser
                                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                        rdfParser.setRDFHandler(new AddStatementHandler(conn, nmodified));

                        /*
                         * Run the parser, which will cause statements to be
                         * inserted.
                         */
                        
                        rdfParser.parse(req.getInputStream(), url
                                .toExternalForm()/* baseURL */);

                    } finally {
                        
                        if (hconn != null)
                            hconn.disconnect();

                    } // next URI.

                }

                // Commit the mutation.
                conn.commit();

                return new Response(HTTP_OK, MIME_TEXT_PLAIN, nmodified.get()
                        + " statements modified.");

            } finally {

                if (conn != null)
                    conn.close();

//                sail.shutDown();

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);
            
        }

    }
	
    /**
     * Halt the server.
     * 
     * @param req
     *            The request.
     *            
     * @return The response.
     * 
     * @throws Exception
     */
	private Response doStop(final Request req) throws Exception {
	    
        /*
         * Create a new thread to run shutdown since we do not want this to
         * block on the queryService.
         */
        final Thread t = new Thread(new Runnable() {

            public void run() {
            
                log.warn("Will shutdown.");
                
                try {
            
                    /*
                     * Sleep for a bit so the Response will be delivered
                     * before we shutdown the server.
                     */
                    
                    Thread.sleep(100/* ms */);

                } catch (InterruptedException ex) {
                
                    // ignore
                    
                }

                // Shutdown the server.
                shutdown();
                
            }
            
        });

        t.setDaemon(true);
        
        // Start the shutdown thread.
        t.start();
        
//      // Shutdown.
//      shutdown();

        /*
         * Note: Client might not see this response since the shutdown thread
         * may have already terminated the httpd service.
         */
        return new Response(HTTP_OK, MIME_TEXT_PLAIN, "Shutting down.");

	}
	
    /**
     * Accepts SPARQL queries.
     * 
     * <pre>
     * GET [/namespace/NAMESPACE] ?query=QUERY
     * </pre>
     * 
     * Where <code>QUERY</code> is the SPARQL query.
     */
	@Override
	public Response doGet(final Request req) throws Exception {

		final String uri = req.uri;
		
        if("/status".equals(uri)) {
            
            return doStatus(req);

        }

        final String queryStr = getQueryStr(req.params);
        
        if (queryStr != null) {

            return doQuery(req);
            
        }

		return new Response(HTTP_NOTFOUND, MIME_TEXT_PLAIN, uri);
		
	}

//    /**
//     * TODO Perform an HTTP-PUT, which corresponds to the basic CRUD operation
//     * "update" according to the generic interaction semantics of HTTP REST.
//     * 
//     */
//	@Override
//    public Response doPut(final Request req) {
//
//        return new Response(HTTP_NOTFOUND, MIME_TEXT_PLAIN, req.uri);
//
//    }

    /**
     * REST DELETE. There are two forms for this operation.
     * 
     * <pre>
     * DELETE [/namespace/NAMESPACE]
     * ...
     * Content-Type
     * ...
     * 
     * BODY
     * 
     * </pre>
     * <p>
     * BODY contains RDF statements according to the specified Content-Type.
     * Statements parsed from the BODY are deleted from the addressed namespace.
     * </p>
     * <p>
     * -OR-
     * </p>
     * 
     * <pre>
     * DELETE [/namespace/NAMESPACE] ?query=...
     * </pre>
     * <p>
     * Where <code>query</code> is a CONSTRUCT or DESCRIBE query. Statements are
     * materialized using the query from the addressed namespace are deleted
     * from that namespace.
     * </p>
     */
    @Override
    public Response doDelete(final Request req) {

        final String contentType = req.getContentType();
        
        final String queryStr = getQueryStr(req.params);

        if(contentType != null) {
        
            return doDeleteWithBody(req);
            
        } else if (queryStr != null) {
            
            return doDeleteWithQuery(req);
            
        }
            
        return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN, "");
           
    }

    /**
     * Delete all statements materialized by a DESCRIBE or CONSTRUCT query.
     * <p>
     * Note: To avoid materializing the statements, this runs the query against
     * the last commit time. This is done while it is holding the unisolated
     * connection which prevents concurrent modifications. Therefore the entire
     * SELECT + DELETE operation is ACID.
     */
    private Response doDeleteWithQuery(final Request req) {
        
        final String baseURI = "";// @todo baseURI query parameter?
        
        final String namespace = getNamespace(req.uri);
        
        final String queryStr = getQueryStr(req.params);

        if(queryStr == null)
            throw new UnsupportedOperationException();
                
        if (log.isInfoEnabled())
            log.info("delete with query: "+queryStr);
        
        try {

            // resolve the default namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null)
                return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Not found: namespace=" + namespace);

            /*
             * Note: pipe is drained by this thread to consume the query
             * results, which are the statements to be deleted.
             */
            final PipedOutputStream os = new PipedOutputStream();
            final InputStream is = newPipedInputStream(os);
            try {

                final AbstractQueryTask queryTask = getQueryTask(namespace,
                        ITx.READ_COMMITTED, queryStr, req.params, req.headers,
                        os);

                switch (queryTask.queryType) {
                case DESCRIBE:
                case CONSTRUCT:
                    break;
                default:
                    return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                            "Must be DESCRIBE or CONSTRUCT query.");
                }

                final AtomicLong nmodified = new AtomicLong(0L);

                // Wrap with SAIL.
                final BigdataSail sail = new BigdataSail(tripleStore);
                BigdataSailConnection conn = null;
                try {

                    sail.initialize();
                    
                    // get the unisolated connection.
                    conn = sail.getConnection();

                    final RDFXMLParser rdfParser = new RDFXMLParser(
                            tripleStore.getValueFactory());

                    rdfParser.setVerifyData(false);
                    
                    rdfParser.setStopAtFirstError(true);
                    
                    rdfParser
                            .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                    rdfParser.setRDFHandler(new RemoveStatementHandler(conn, nmodified));

                    /*
                     * Run the parser, which will cause statements to be
                     * deleted.
                     */
                    rdfParser.parse(is, baseURI);

                    // Commit the mutation.
                    conn.commit();

                } finally {

                    if (conn != null)
                        conn.close();

//                    sail.shutDown();

                }

                return new Response(HTTP_OK, MIME_TEXT_PLAIN, nmodified.get()
                        + " statements modified.");

            } catch (Throwable t) {

                throw launderThrowable(t, os, queryStr);

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);

        }

    }

    /**
     * DELETE request with a request body containing the statements to be
     * removed.
     */
    private Response doDeleteWithBody(final Request req) {

        final String baseURI = "";// @todo baseURI query parameter?
        
        final String namespace = getNamespace(req.uri);

        final String contentType = req.getContentType();

        if (contentType == null)
            throw new UnsupportedOperationException();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        try {

            // resolve the default namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null)
                return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Not found: namespace=" + namespace);

            final AtomicLong nmodified = new AtomicLong(0L);

            // Wrap with SAIL.
            final BigdataSail sail = new BigdataSail(tripleStore);
            BigdataSailConnection conn = null;
            try {

                sail.initialize();
                conn = sail.getConnection();

                /*
                 * There is a request body, so let's try and parse it.
                 */

                final RDFFormat format = RDFFormat.forMIMEType(contentType);

                if (format == null) {
                    return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                            "Content-Type not recognized as RDF: "
                                    + contentType);
                }

                final RDFParserFactory rdfParserFactory = RDFParserRegistry
                        .getInstance().get(format);

                if (rdfParserFactory == null) {
                    return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            "Parser not found: Content-Type=" + contentType);
                }

                final RDFParser rdfParser = rdfParserFactory.getParser();

                rdfParser.setValueFactory(tripleStore.getValueFactory());

                rdfParser.setVerifyData(true);

                rdfParser.setStopAtFirstError(true);

                rdfParser
                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

                rdfParser.setRDFHandler(new RemoveStatementHandler(conn,
                        nmodified));

                /*
                 * Run the parser, which will cause statements to be deleted.
                 */
                rdfParser.parse(req.getInputStream(), baseURI);

                // Commit the mutation.
                conn.commit();

                return new Response(HTTP_OK, MIME_TEXT_PLAIN, nmodified.get()
                        + " statements modified.");

            } finally {

                if (conn != null)
                    conn.close();

//                sail.shutDown();

            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);
            
        }

    }

    /**
     * Return the namespace which will be used to execute the query. The
     * namespace is represented by the first component of the URI. If there is
     * no namespace, then return the configured default namespace.
     * 
     * @param uri
     *            The URI path string.
     * 
     * @return The namespace.
     */
    private String getNamespace(final String uri) {

//        // locate the "//" after the protocol.
//        final int index = uri.indexOf("//");

        if(!uri.startsWith("/namespace/")) {
            // use the default namespace.
            return config.namespace;
        }

        // locate the next "/" in the URI path.
        final int beginIndex = uri.indexOf('/', 1/* fromIndex */);

        // locate the next "/" in the URI path.
        int endIndex = uri.indexOf('/', beginIndex + 1/* fromIndex */);

        if (endIndex == -1) {
            // use the rest of the URI.
            endIndex = uri.length();
        }

        // return the namespace.
        return uri.substring(beginIndex + 1, endIndex);

    }

	/**
	 * Return the timestamp which will be used to execute the query. The uri
	 * query parameter <code>timestamp</code> may be used to communicate the
	 * desired commit time against which the query will be issued. If that uri
	 * query parameter is not given then the default configured commit time will
	 * be used. Applications may create protocols for sharing interesting commit
	 * times as reported by {@link IAtomicStore#commit()} or by a distributed
	 * data loader (for scale-out).
	 * 
	 * @todo the configured timestamp should only be used for the default
	 *       namespace (or it should be configured for each graph explicitly, or
	 *       we should bundle the (namespace,timestamp) together as a single
	 *       object).
	 */
	private long getTimestamp(final String uri,
			final LinkedHashMap<String, Vector<String>> params) {

		final Vector<String> tmp = params.get("timestamp");

		if (tmp == null || tmp.size() == 0 || tmp.get(0) == null) {

			return config.timestamp;

		}

		final String val = tmp.get(0);

		return Long.valueOf(val);

    }

	/**
	 * Respond to a status request.
	 * 
	 * @param uri
	 * @param method
	 * @param header
	 * @param params
	 * @return
	 * @throws Exception
	 * 
	 * @todo add statistics for top-N queries based on query template
	 *       identifiers, which can be communicated using query hints. See //
	 *       wait for the subquery.
	 */
	private Response doStatus(final Request req) throws Exception {

		// SPARQL queries accepted by the SPARQL end point.
        final boolean showQueries = req.params.get("showQueries") != null;

        // IRunningQuery objects currently running on the query controller.
        final boolean showRunningQueries = req.params.get("showRunningQueries") != null;

        // Information about the KB (stats, properties).
        final boolean showKBInfo = req.params.get("showKBInfo") != null;

        // bigdata namespaces known to the index manager.
        final boolean showNamespaces = req.params.get("showNamespaces") != null;

        final StringBuilder sb = new StringBuilder();

        sb.append("Accepted query count=" + queryIdFactory.get() + "\n");

		sb.append("Running query count=" + queries.size() + "\n");

		if (showNamespaces) {

			final List<String> namespaces = getNamespaces();
			
			sb.append("Namespaces: ");

			for (String s : namespaces) {

				sb.append(s);

			}
			
			sb.append("\n");
			
        }
        
        if (showKBInfo) {

            // General information on the connected kb.
            sb.append(getKBInfo(getNamespace(req.uri), getTimestamp(req.uri,
                    req.params)));

        }
        
        if(queueSampleTask != null) {
        	
        	// Performance counters for the NSS queries.
        	sb.append(queueSampleTask.getCounters().toString());
        	
        }
		
//		if (indexManager instanceof IJournal) {
//
//	        /*
//	         * Stuff which is specific to a local/embedded database.
//	         */
//
//			final AbstractJournal jnl = (AbstractJournal) indexManager;
//
//            sb.append("file\t= " + jnl.getFile() + "\n");
//
//            sb.append("BufferMode\t= "
//                    + jnl.getBufferStrategy().getBufferMode() + "\n");
//
//            sb.append("nextOffset\t= " + jnl.getRootBlockView().getNextOffset()
//                    + "\n");
//
//			if (LRUNexus.INSTANCE != null) {
//
//				sb.append(LRUNexus.Options.CLASS + "="
//						+ LRUNexus.INSTANCE.toString().getClass() + "\n");
//
//				sb.append(LRUNexus.INSTANCE.toString() + "\n");
//
//			} else {
//				
//				sb.append("LRUNexus is disabled.");
//				
//			}
//
//			// show the disk access details.
//			sb.append(jnl.getBufferStrategy().getCounters().toString()+"\n");
//
//		}
		
		if(showQueries) {
		    
		    /*
		     * Show the queries which are currently executing (accepted by the NanoSparqlServer).
		     */

			sb.append("\n");
			
			final long now = System.nanoTime();
			
			final TreeMap<Long, RunningQuery> ages = new TreeMap<Long, RunningQuery>(new Comparator<Long>() {
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
			
			{

				final Iterator<RunningQuery> itr = queries.values().iterator();

				while (itr.hasNext()) {

					final RunningQuery query = itr.next();

					final long age = now - query.begin;

					ages.put(age, query);

				}

			}

			{

				final Iterator<RunningQuery> itr = ages.values().iterator();

				while (itr.hasNext()) {

					final RunningQuery query = itr.next();

					final long age = now - query.begin;

                    sb.append("age="
                            + java.util.concurrent.TimeUnit.NANOSECONDS
                                    .toMillis(age) + "ms, queryId="
                            + query.queryId + "\n");
                    sb.append(query.query + "\n");

				}

			}
			
		}
		
		if(showRunningQueries) {
			
			/*
			 * Show the queries which are currently executing (actually running
			 * on the QueryEngine).
			 */
			
			sb.append("\n");
			
			final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory
					.getQueryController(indexManager);
			
			final UUID[] queryIds = queryEngine.getRunningQueries();
			
//			final long now = System.nanoTime();
			
			final TreeMap<Long, IRunningQuery> ages = new TreeMap<Long, IRunningQuery>(new Comparator<Long>() {
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

			for(UUID queryId : queryIds) {
				
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
					sb.append("age=" + query.getElapsed() + "ms\n");
					sb.append("queryId=" + query.getQueryId() + "\n");
					sb.append(query.toString());
					sb.append("\n");
					sb.append(BOpUtility.toString(query.getQuery()));
					sb.append("\n");
					sb.append("\n");
					
//					final long age = query.getElapsed();
//					sb.append("age="
//							+ java.util.concurrent.TimeUnit.NANOSECONDS
//									.toMillis(age) + "ms, queryId="
//							+ query.getQueryId() + "\nquery="
//							+ BOpUtility.toString(query.getQuery()) + "\n");

				}

			}

		}
		
		return new Response(HTTP_OK, MIME_TEXT_PLAIN, sb.toString());

	}
	
	/**
	 * Answer a SPARQL query.
	 * 
	 * @param uri
	 * @param method
	 * @param header
	 * @param params
	 * @return
	 * @throws Exception
	 */
	public Response doQuery(final Request req) throws Exception {

        final String namespace = getNamespace(req.uri);

		final long timestamp = getTimestamp(req.uri, req.params);

        final String queryStr = getQueryStr(req.params);

        if (queryStr == null)
            return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Specify query using ?query=....");

		/*
		 * Setup pipes. The [os] will be passed into the task that executes the
		 * query. The [is] will be passed into the Response. The task is
		 * executed on a thread pool.
		 * 
		 * Note: If the client closes the connection, then the InputStream
		 * passed into the Response will be closed and the task will terminate
		 * rather than running on in the background with a disconnected client.
		 */
        final PipedOutputStream os = new PipedOutputStream();
        final InputStream is = newPipedInputStream(os);
        try {

            final AbstractQueryTask queryTask = getQueryTask(namespace, timestamp,
                    queryStr, req.params, req.headers, os);
            
            final FutureTask<Void> ft = new FutureTask<Void>(queryTask);
            
            // Setup the response.
            // TODO Move charset choice into conneg logic.
            final Response r = new Response(HTTP_OK, queryTask.mimeType
                    + "; charset='" + charset + "'", is);

            if (log.isTraceEnabled())
                log.trace("Will run query: " + queryStr);
            
            // Begin executing the query (asynchronous).
            queryService.execute(ft);

			/*
			 * Sets the cache behavior.
			 */
			// r.addHeader("Cache-Control",
			// "max-age=60, must-revalidate, public");
			// to disable caching.
			// r.addHeader("Cache-Control", "no-cache");

			return r;

		} catch (Throwable e) {

			throw launderThrowable(e, os, queryStr);

		}

	}

    /**
     * Return the query string.
     * 
     * @param params
     * 
     * @return The query string -or- <code>null</code> if none was specified.
     */
    private String getQueryStr(final Map<String, Vector<String>> params) {

        final String queryStr;

        final Vector<String> tmp = params.get("query");

        if (tmp == null || tmp.isEmpty() || tmp.get(0) == null) {
            queryStr = null;
        } else {
            queryStr = tmp.get(0);

            if (log.isDebugEnabled())
                log.debug("query: " + queryStr);

        }

        return queryStr;

    }
	
	/**
	 * Class reuses the a pool of buffers for each pipe. This is a significant
	 * performance win.
	 * 
	 * @see NanoSparqlServer#pipeBufferPool
	 */
	private class MyPipedInputStream extends PipedInputStream {
		
		MyPipedInputStream(final PipedOutputStream os) throws IOException,
				InterruptedException {
		
			super(os, 1/* size */);
			
			// override the buffer.
			this.buffer = pipeBufferPool.take();
			
		}

		public void close() throws IOException {
	
			super.close();
			
			// return the buffer to the pool.
			pipeBufferPool.add(buffer);
			
		}
		
	}

	/**
	 * Factory for the {@link PipedInputStream} which supports reuse of the
	 * buffer instances. The buffer pool is only used when there is an upper
	 * bound on the number of queries which will be concurrently evaluated.
	 * 
	 * @see Config#queryThreadPoolSize
	 * @see Config#bufferCapacity
	 * @see NanoSparqlServer#pipeBufferPool
	 * 
	 * @throws InterruptedException
	 */
	private PipedInputStream newPipedInputStream(final PipedOutputStream os)
			throws IOException, InterruptedException {

		if (pipeBufferPool != null) {

			return new MyPipedInputStream(os);

		}
		
		return new PipedInputStream(os, config.bufferCapacity);
		
	}

    /**
     * Write the stack trace onto the output stream. This will show up in the
     * client's response. This code path should be used iff we have already
     * begun writing the response. Otherwise, an HTTP error status should be
     * used instead.
     * 
     * @param t
     *            The thrown error.
     * @param os
     *            The stream on which the response will be written.
     * @param queryStr
     *            The query string (if available).
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     */
    static private RuntimeException launderThrowable(final Throwable t,
            final OutputStream os, final String queryStr) throws Exception {
        try {
            // log an error for the service.
            log.error(t, t);
        } finally {
            // ignore any problems here.
        }
		if (os != null) {
			try {
                final PrintWriter w = new PrintWriter(os);
                if (queryStr != null) {
                    /*
                     * Write the query onto the output stream.
                     */
                    w.write(queryStr);
                    w.write("\n");
                }
                /*
                 * Write the stack trace onto the output stream.
                 */
                t.printStackTrace(w);
                w.flush();
				// flush the output stream.
				os.flush();
			} finally {
				// ignore any problems here.
			}
			try {
				// ensure output stream is closed.
				os.close();
			} catch (Throwable t2) {
				// ignore any problems here.
			}
		}
		if (t instanceof RuntimeException) {
			return (RuntimeException) t;
		} else if (t instanceof Error) {
			throw (Error) t;
		} else if (t instanceof Exception) {
			throw (Exception) t;
		} else
			throw new RuntimeException(t);
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
    private AbstractQueryTask getQueryTask(final String namespace,
            final long timestamp, final String queryStr,
            final Map<String,Vector<String>> params,
            final Map<String,String> headers,
            final PipedOutputStream os) throws MalformedQueryException {
    	
		/*
		 * Parse the query so we can figure out how it will need to be executed.
		 * 
		 * Note: This will fail a query on its syntax. However, the logic used
		 * in the tasks to execute a query will not fail a bad query for some
		 * reason which I have not figured out yet. Therefore, we are in the
		 * position of having to parse the query here and then again when it is
		 * executed.
		 */
        final ParsedQuery q = engine.parseQuery(queryStr, null/*baseURI*/);
        
        if(log.isDebugEnabled())
            log.debug(q.toString());
        
		final NanoSparqlClient.QueryType queryType = NanoSparqlClient.QueryType
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
            mimeType = MIME_RDF_XML;
            return new GraphQueryTask(namespace, timestamp, queryStr,
                    queryType, mimeType, os);
        case SELECT:
            mimeType = MIME_SPARQL_RESULTS_XML;
            return new TupleQueryTask(namespace, timestamp, queryStr,
                    queryType, mimeType, os);
        }

		throw new RuntimeException("Unknown query type: " + queryType);

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
    protected BigdataSailRepositoryConnection getQueryConnection(
            final String namespace, final long timestamp)
            throws RepositoryException {
        
        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
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

    /**
     * Abstract base class for running queries handles the timing, pipe,
     * reporting, obtains the connection, and provides the finally {} semantics
     * for each type of query task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    abstract private class AbstractQueryTask implements Callable<Void> {
        
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
        protected final QueryType queryType;
        
        /**
         * The negotiated MIME type to be used for the query response.
         */
        protected final String mimeType;
        
        /** A pipe used to incrementally deliver the results to the client. */
        private final PipedOutputStream os;

        /**
         * Sesame has an option for a base URI during query evaluation. This
         * provides a symbolic place holder for that URI in case we ever provide
         * a hook to set it.
         */
        protected final String baseURI = null;
        
        /**
         * The queryId used by the {@link NanoSparqlServer}.
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
                final QueryType queryType,
                final String mimeType,
                final PipedOutputStream os) {

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.queryStr = queryStr;
            this.queryType = queryType;
            this.mimeType = mimeType;
            this.os = os;
            this.queryId = Long.valueOf(queryIdFactory.incrementAndGet());
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
                queries.put(queryId, new RunningQuery(queryId.longValue(),queryId2,
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
                throw launderThrowable(t, os, queryStr);
            } finally {
                queries.remove(queryId);
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
                final String queryStr, final QueryType queryType,
                final String mimeType, final PipedOutputStream os) {

			super(namespace, timestamp, queryStr, queryType, mimeType, os);

		}

		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

			final BigdataSailTupleQuery query = cxn.prepareTupleQuery(
					QueryLanguage.SPARQL, queryStr, baseURI);
			
			query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(os)));

		}

	}

	/**
	 * Executes a graph query.
	 */
    private class GraphQueryTask extends AbstractQueryTask {

        public GraphQueryTask(final String namespace, final long timestamp,
                final String queryStr, final QueryType queryType,
                final String mimeType, final PipedOutputStream os) {

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
     * Send a STOP message to the service
     * 
     * @param port
     *            The port for that service.
     * 
     * @throws IOException
     * 
     * @todo This winds up warning <code>
     * java.net.SocketTimeoutException: Read timed out
     * </code> even though the shutdown request was
     *       accepted and processed by the server. I'm not sure why.
     */
    public static void sendStop(final int port) throws IOException {

        final URL url = new URL("http://localhost:" + port + "/stop");
        HttpURLConnection conn = null;
        try {

            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoInput(true); // true to read from the server.
            conn.setDoOutput(true); // true to write to the server.
            conn.setUseCaches(false);
            conn.setReadTimeout(2000/* ms */);
            conn.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            conn.setRequestProperty("Content-Length", "" + Integer.toString(0));
            conn.setRequestProperty("Content-Language", "en-US");

            // Send request
            conn.getOutputStream().close();

            // connect.
			try {

				conn.connect();

				final int rc = conn.getResponseCode();
				
				if (rc < 200 || rc >= 300) {
					
					log.error(conn.getResponseMessage());
					
				}

				System.out.println(conn.getResponseMessage());

			} catch (IOException ex) {

				log.warn(ex);
				
			}

		} finally {

			// clean up the connection resources
			if (conn != null)
				conn.disconnect();

		}

	}
	
    /**
     * Configuration object.
     */
    public static class Config {

    	/**
    	 * When true, suppress various things otherwise written on stdout.
    	 */
    	public boolean quiet = false;
    	
		/**
		 * The port on which the server will answer requests -or- ZERO to
		 * use any open port.
		 */
		public int port = 80;
		
		/**
		 * The default namespace.
		 */
		public String namespace;
    	
		/**
		 * The default timestamp used to query the default namespace. The server
		 * will obtain a read only transaction which reads from the commit point
		 * associated with this timestamp.  
		 */
		public long timestamp = ITx.UNISOLATED;
		
    	/**
		 * The #of threads to use to handle SPARQL queries -or- ZERO (0) for an
		 * unbounded pool.
		 */
    	public int queryThreadPoolSize = 8;

		/**
		 * The capacity of the buffers for the pipe connecting the running query
		 * to the HTTP response.
		 */
		public int bufferCapacity = Bytes.kilobyte32 * 1;
    	
    	public Config() {
    	}
    	
    }

    /**
     * Print the optional message on stderr, print the usage information on stderr,
     * and then force the program to exit with the given status code. 
     * @param status The status code.
     * @param msg The optional message
     */
	private static void usage(final int status, final String msg) {
		
		if (msg != null) {

			System.err.println(msg);
			
		}

		System.err.println("[options] port namespace (propertyFile|configFile)");
		
		System.err.println("-OR-");
		
		System.err.println("port -stop");
		
		System.exit(status);
		
	}

    /**
     * Run an httpd service exposing a SPARQL endpoint. The service will respond
     * to the following URL paths:
     * <dl>
     * <dt>http://localhost:port/</dt>
     * <dd>The SPARQL end point for the default namespace as specified by the
     * <code>namespace</code> command line argument.</dd>
     * <dt>http://localhost:port/namespace/NAMESPACE</dt>
     * <dd>where <code>NAMESPACE</code> is the namespace of some triple store or
     * quad store, may be used to address ANY triple or quads store in the
     * bigdata instance.</dd>
     * <dt>http://localhost:port/status</dt>
     * <dd>A status page.</dd>
     * </dl>
     * 
     * @param args
     *            USAGE:<br/>
     *            To stop the server:<br/>
     *            <code>port -stop</code><br/>
     *            To start the server:<br/>
     *            <code>(options) <i>namespace</i> (propertyFile|configFile) )</code>
     *            <p>
     *            <i>Where:</i>
     *            <dl>
     *            <dt>port</dt>
     *            <dd>The port on which the service will respond -or-
     *            <code>0</code> to use any open port.</dd>
     *            <dt>namespace</dt>
     *            <dd>The namespace of the default SPARQL endpoint (the
     *            namespace will be <code>kb</code> if none was specified when
     *            the triple/quad store was created).</dd>
     *            <dt>propertyFile</dt>
     *            <dd>A java properties file for a standalone {@link Journal}.</dd>
     *            <dt>configFile</dt>
     *            <dd>A jini configuration file for a bigdata federation.</dd>
     *            </dl>
     *            and <i>options</i> are any of:
     *            <dl>
     *            <dt>-q</dt>
     *            <dd>Suppress messages on stdout.</dd>
     *            <dt>-nthreads</dt>
     *            <dd>The #of threads which will be used to answer SPARQL
     *            queries (default 8).</dd>
     *            <dt>-forceOverflow</dt>
     *            <dd>Force a compacting merge of all shards on all data
     *            services in a bigdata federation (this option should only be
     *            used for benchmarking purposes).</dd>
     *            <dt>readLock</dt>
     *            <dd>The commit time against which the server will assert a
     *            read lock by holding open a read-only transaction against that
     *            commit point. When given, queries will default to read against
     *            this commit point. Otherwise queries will default to read
     *            against the most recent commit point on the database.
     *            Regardless, each query will be issued against a read-only
     *            transaction.</dt>
     *            <dt>bufferCapacity [#bytes]</dt>
     *            <dd>Specify the capacity of the buffers used to decouple the
     *            query evaluation from the consumption of the HTTP response by
     *            the clinet. The capacity may be specified in bytes or
     *            kilobytes, e.g., <code>5k</code>.</dd>
     *            </dl>
     *            </p>
     */
	public static void main(final String[] args) {
		final Config config = new Config();
		boolean forceOverflow = false;
		Journal jnl = null;
		JiniClient<?> jiniClient = null;
		NanoSparqlServer server = null;
		ITransactionService txs = null;
		Long readLock = null; 
		try {
			/*
			 * First, handle the [port -stop] command, where "port" is the port
			 * number of the service. This case is a bit different because the
			 * "-stop" option appears after the "port" argument.
			 */
			if (args.length == 2) {
				if("-stop".equals(args[1])) {
					final int port;
					try {
						port = Integer.valueOf(args[0]);
					} catch (NumberFormatException ex) {
						usage(1/* status */, "Could not parse as port# : '"
								+ args[0] + "'");
						// keep the compiler happy wrt [port] being bound.
						throw new AssertionError();
					}
					// Send stop to server.
					sendStop(port);
					// Normal exit.
					System.exit(0);
				} else {
					usage(1/* status */, null/* msg */);
				}
			}

			/*
			 * Now that we have that case out of the way, handle all arguments
			 * starting with "-". These should appear before any non-option
			 * arguments to the program.
			 */
			int i = 0;
			while (i < args.length) {
				final String arg = args[i];
				if (arg.startsWith("-")) {
					if (arg.equals("-q")) {
						config.quiet = true;
					} else if (arg.equals("-forceOverflow")) {
						forceOverflow = true;
					} else if (arg.equals("-nthreads")) {
						final String s = args[++i];
						config.queryThreadPoolSize = Integer.valueOf(s);
						if (config.queryThreadPoolSize < 0) {
							usage(1/* status */,
									"-nthreads must be non-negative, not: " + s);
						}
					} else if (arg.equals("-bufferCapacity")) {
						final String s = args[++i];
						final long tmp = BytesUtil.getByteCount(s);
						if (tmp < 1) {
							usage(1/* status */,
									"-bufferCapacity must be non-negative, not: "
											+ s);
						}
						if (tmp > Bytes.kilobyte32 * 100) {
							usage(1/* status */,
									"-bufferCapacity must be less than 100kb, not: "
											+ s);
						}
						config.bufferCapacity = (int) tmp;
					} else if (arg.equals("-readLock")) {
						final String s = args[++i];
						readLock = Long.valueOf(s);
						if (!TimestampUtility
								.isCommitTime(readLock.longValue())) {
							usage(1/* status */,
									"Read lock must be commit time: "
											+ readLock);
						}
					} else {
						usage(1/* status */, "Unknown argument: " + arg);
					}
				} else {
					break;
				}
				i++;
			}

			/*
			 * Finally, there should be exactly THREE (3) command line arguments
			 * remaining. These are the [port], the [namespace] and the
			 * [propertyFile] (journal) or [configFile] (scale-out).
			 */
			final int nremaining = args.length - i;
			if (nremaining != 3) {
				/*
				 * There are either too many or too few arguments remaining.
				 */
				usage(1/* status */, nremaining < 3 ? "Too few arguments."
						: "Too many arguments");
			}
			/*
			 * http service port.
			 */
			{
				final String s = args[i++];
				try {
					config.port = Integer.valueOf(s);
				} catch (NumberFormatException ex) {
					usage(1/* status */, "Could not parse as port# : '" + s
							+ "'");
				}
			}
			/*
			 * Namespace.
			 */
			config.namespace = args[i++];
			/*
			 * Property file.
			 */
			final String propertyFile = args[i++];
			final File file = new File(propertyFile);
			if (!file.exists()) {
				throw new RuntimeException("Could not find file: " + file);
			}
			boolean isJini = false;
			if (propertyFile.endsWith(".config")) {
				// scale-out.
				isJini = true;
			} else if (propertyFile.endsWith(".properties")) {
				// local journal.
				isJini = false;
			} else {
				/*
				 * Note: This is a hack, but we are recognizing the jini
				 * configuration file with a .config extension and the journal
				 * properties file with a .properties extension.
				 */
				usage(1/* status */,
						"File should have '.config' or '.properties' extension: "
								+ file);
			}
			if (!config.quiet) {
				System.out.println("port: " + config.port);
				System.out.println("namespace: " + config.namespace);
				System.out.println("file: " + file.getAbsoluteFile());
			}

			/*
			 * Connect to the database.
			 */
			final IIndexManager indexManager;
			{

				if (isJini) {

					/*
					 * A bigdata federation.
					 */

					jiniClient = new JiniClient(new String[] { propertyFile });

					indexManager = jiniClient.connect();

				} else {

					/*
					 * Note: we only need to specify the FILE when re-opening a
					 * journal containing a pre-existing KB.
					 */
					final Properties properties = new Properties();
					{
						// Read the properties from the file.
						final InputStream is = new BufferedInputStream(
								new FileInputStream(propertyFile));
						try {
							properties.load(is);
						} finally {
							is.close();
						}
						if (System.getProperty(BigdataSail.Options.FILE) != null) {
							// Override/set from the environment.
							properties
									.setProperty(
											BigdataSail.Options.FILE,
											System
													.getProperty(BigdataSail.Options.FILE));
						}
					}

					indexManager = jnl = new Journal(properties);

				}

			}

            txs = (indexManager instanceof Journal ? ((Journal) indexManager)
					.getTransactionManager().getTransactionService()
					: ((IBigdataFederation<?>) indexManager)
							.getTransactionService());

			if (readLock != null) {

				/*
				 * Obtain a read-only transaction which will assert a read lock
				 * for the specified commit time. The database WILL NOT release
				 * storage associated with the specified commit point while this
				 * server is running. Queries will read against the specified
				 * commit time by default, but this may be overridden on a query
				 * by query basis.
				 */
				config.timestamp = txs.newTx(readLock);

				if (!config.quiet) {

					System.out.println("Holding read lock: readLock="
							+ readLock + ", tx: " + config.timestamp);

				}

			} else {

				/*
				 * The default for queries is to read against then most recent
				 * commit time as of the moment when the request is accepted.
				 */

				config.timestamp = ITx.READ_COMMITTED;

			}

			// start the server.
			server = new NanoSparqlServer(config, indexManager);

            /*
             * Install a shutdown hook (normal kill will trigger this hook).
             */
            {
                
                final NanoSparqlServer tmp = server;
                
                Runtime.getRuntime().addShutdownHook(new Thread() {

                    public void run() {

                        tmp.shutdownNow();

                        log.warn("Caught signal, shutting down: " + new Date());

                    }

                });

            }

			if (!config.quiet) {

                log.warn("Service is running: port=" + config.port);
            	
            }

			if (log.isInfoEnabled()) {
				/*
				 * Log some information about the default kb (#of statements,
				 * etc).
				 */
				log.info("\n"
						+ server.getKBInfo(config.namespace, config.timestamp));
			}

			if (forceOverflow && indexManager instanceof IBigdataFederation<?>) {

				if (!config.quiet)
					System.out
							.println("Forcing compacting merge of all data services: "
									+ new Date());

				((AbstractDistributedFederation<?>) indexManager)
						.forceOverflow(true/* compactingMerge */, false/* truncateJournal */);

				if (!config.quiet)
					System.out
							.println("Did compacting merge of all data services: "
									+ new Date());
            	
            }
            
			/*
			 * Wait until the server is terminated.
			 */
			synchronized (server.alive) {

				while (server.alive.get()) {

					server.alive.wait();

				}

				if (!config.quiet)
					System.out.println("Service is down.");

			}

		} catch (Throwable ex) {
			ex.printStackTrace();
        } finally {
            if (txs != null && readLock != null) {
                try {
                    txs.abort(config.timestamp);
                } catch (IOException e) {
                    log.error("Could not release transaction: tx="
                            + config.timestamp, e);
                }
		    }
			if (server != null)
				server.shutdownNow();
			if (jnl != null) {
				jnl.close();
			}
			if (jiniClient != null) {
				jiniClient.disconnect(true/* immediateShutdown */);
			}
		}

	}

}
