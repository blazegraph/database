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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailException;

import com.bigdata.LRUNexus;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
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
import com.bigdata.rdf.sail.bench.NanoSparqlClient.QueryType;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.RelationSchema;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.sparse.ITPS;
import com.bigdata.util.concurrent.DaemonThreadFactory;
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

 * @todo Allow timestamp for server reads as protocol parameter (URL query
 *       parameter or header).
 * 
 * @todo Add an "?explain" URL query parameter and show the execution plan and
 *       costs (or make this a navigable option from the set of running queries
 *       to drill into their running costs and offer an opportunity to kill them
 *       as well).
 * 
 * @todo Add command to kill a running query.
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
	static private final String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";

	/**
	 * RDF/XML.
	 */
	static private final String MIME_RDF_XML = "application/rdf+xml";

	/**
	 * The character set used for the response (not negotiated).
	 */
    static private final String charset = "UTF-8";
    
//    /**
//     * The target Sail.
//     */
//    private final BigdataSail sail;
//
//    /**
//	 * The target repository.
//	 */
//	private final BigdataSailRepository repo;

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

    /**
     * Metadata about running queries.
     */
    private static class RunningQuery {
    	/** The unique identifier for this query. */
    	final long queryId;
    	/** The query. */
    	final String query;
    	/** The timestamp when the query was accepted (ns). */
    	final long begin;
        public RunningQuery(long queryId, String query, long begin) {
    		this.queryId = queryId;
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

        } else {

            queryService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    config.queryThreadPoolSize, new DaemonThreadFactory
                    (getClass().getName()+".queryService"));

        }

	}

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
				final Class cls = Class.forName(className);
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
    
    synchronized public void shutdown() {
        System.err.println("Normal shutdown.");
        queryService.shutdown();
        try {
            System.err.println("Awaiting termination of running queries.");
            queryService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    	super.shutdown();
		synchronized (alive) {
			alive.set(false);
			alive.notifyAll();
		}
    }

    /**
     * FIXME Must abort any open transactions.
     */
    synchronized public void shutdownNow() {
        System.err.println("Immediate shutdown");
        // interrupt all running queries.
    	queryService.shutdownNow();
        /*
         * Wait a moment for any running queries to close their query
         * connections.
         */
        try {
            Thread.sleep(1000/* ms */);
        } catch (InterruptedException ex) {
            // ignore 
        }
    	super.shutdown();
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

//    @Override
//    public Response serve(final String uri, final String method,
//            final Properties header,
//            final LinkedHashMap<String, Vector<String>> parms) {
//    
//        if ("STOP".equalsIgnoreCase(method)) {
//
//        	queryService.execute(new Runnable(){
//        		public void run() {
//        			shutdown();
//        		}
//        	});
//        	
//            return new Response(HTTP_OK, MIME_TEXT_PLAIN, "Shutting down.");
//            
//        }
//        
//    	return super.serve(uri, method, header, parms);
//    	
//    }

	@Override
	public Response doPost(final String uri, final String method,
			final Properties header,
			final LinkedHashMap<String, Vector<String>> params) throws Exception {

		if (log.isDebugEnabled()) {
			log.debug("uri=" + uri);
			log.debug("method=" + method);
			log.debug("headser=" + header);
			log.debug("params=" + params);
		}
		
		if("/stop".equals(uri)) {

            /*
             * Create a new thread to run shutdown since we do not want this to
             * block on the queryService.
             */
			final Thread t = new Thread(new Runnable() {
				public void run() {
					System.err.println("Will shutdown.");
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
			
//			// Shutdown.
//			shutdown();

            /*
             * Note: Client probably might not see this response since the
             * shutdown thread may have already terminated the httpd service.
             */
			return new Response(HTTP_OK, MIME_TEXT_PLAIN, "Shutting down.");

		}
		
		return new Response(HTTP_NOTIMPLEMENTED, MIME_TEXT_PLAIN,
				"SPARQL POST QUERY not implemented.");

	}
	
    /**
	 * Accepts SPARQL queries.
	 */
	@Override
	public Response doGet(final String uri, final String method,
			final Properties header,
			final LinkedHashMap<String, Vector<String>> params) throws Exception {

		if (log.isDebugEnabled()) {
			log.debug("uri=" + uri);
			log.debug("method=" + method);
			log.debug("headser=" + header);
			log.debug("params=" + params);
		}

		if (uri == null || uri.length() == 0) {

			return doQuery(uri, method, header, params);

		}
		
		if("/status".equals(uri)) {
			
            // @todo Could list the known namespaces.
			return doStatus(uri, method, header, params);

        }

        if (uri.startsWith("/namespace/")) {

            // @todo allow status query against any namespace.
            return doQuery(uri, method, header, params);
            
        }

		return new Response(HTTP_NOTFOUND, MIME_TEXT_PLAIN, uri);
		
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
     * Return the timestamp which will be used to execute the query.
     * 
     * @todo the configured timestamp should only be used for the default
     *       namespace (or it should be configured for each graph explicitly, or
     *       we should bundle the (namespace,timestamp) together as a single
     *       object).
     * 
     * @todo use path for the timestamp or acquire read lock when the server
     *       starts against a specific namespace?
     */
    private long getTimestamp(final String uri,
            final LinkedHashMap<String, Vector<String>> params) {

        return config.timestamp;
        
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
	 * @todo Report on the average query latency, average concurrency of query
	 *       evaluation, etc.
	 */
	public Response doStatus(final String uri, final String method,
			final Properties header,
			final LinkedHashMap<String, Vector<String>> params) throws Exception {

		// SPARQL queries accepted by the SPARQL end point.
        final boolean showQueries = params.get("showQueries") != null;

        // IRunningQuery objects currently running on the query controller.
        final boolean showRunningQueries = params.get("showRunningQueries") != null;

        // Information about the KB (stats, properties).
        final boolean showKBInfo = params.get("showKBInfo") != null;

        // bigdata namespaces known to the index manager.
        final boolean showNamespaces = params.get("showNamespaces") != null;

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
            sb.append(getKBInfo(getNamespace(uri), getTimestamp(uri, params)));

        }
		
		if (indexManager instanceof IJournal) {

	        /*
	         * Stuff which is specific to a local/embedded database.
	         */

			final AbstractJournal jnl = (AbstractJournal) indexManager;

            sb.append("file\t= " + jnl.getFile() + "\n");

            sb.append("BufferMode\t= "
                    + jnl.getBufferStrategy().getBufferMode() + "\n");

            sb.append("nextOffset\t= " + jnl.getRootBlockView().getNextOffset()
                    + "\n");

			if (LRUNexus.INSTANCE != null) {

				sb.append(LRUNexus.Options.CLASS + "="
						+ LRUNexus.INSTANCE.toString().getClass() + "\n");

				sb.append(LRUNexus.INSTANCE.toString() + "\n");

			} else {
				
				sb.append("LRUNexus is disabled.");
				
			}

			// show the disk access details.
			sb.append(jnl.getBufferStrategy().getCounters().toString()+"\n");

		}
		
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
	public Response doQuery(final String uri, final String method,
			final Properties header,
			final LinkedHashMap<String, Vector<String>> params) throws Exception {

        final String namespace = getNamespace(uri);

        final long timestamp = getTimestamp(uri, params);
	    
		final String queryStr = params.get("query").get(0);

		if (queryStr == null) {

			return new Response(HTTP_BADREQUEST, MIME_TEXT_PLAIN,
					"Specify query using ?query=....");

		}

		if (log.isDebugEnabled())
			log.debug("query: " + queryStr);
		
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
		final InputStream is = new PipedInputStream(os,Bytes.kilobyte32*1); // note: default is 1k.
		final FutureTask<Void> ft = new FutureTask<Void>(getQueryTask(
                namespace, timestamp, queryStr, os));
		try {

			// Choose an appropriate MIME type.
			final String mimeType;
			final QueryType queryType = QueryType.fromQuery(queryStr);
			switch(queryType) {
			case DESCRIBE:
			case CONSTRUCT:
				mimeType = MIME_RDF_XML;
				break;
			case ASK:
			case SELECT:
				mimeType = MIME_SPARQL_RESULTS_XML;
				break;
			default:
				throw new RuntimeException("Unknown query type: "+queryType);
			}

			// Begin executing the query (asynchronous).
			queryService.execute(ft);
			
			// Setup the response.
			final Response r = new Response(HTTP_OK, mimeType + "; charset='"
					+ charset + "'", is);

			/*
			 * Sets the cache behavior.
			 */
			// r.addHeader("Cache-Control",
			// "max-age=60, must-revalidate, public");
			// to disable caching.
			// r.addHeader("Cache-Control", "no-cache");

			return r;

		} catch (Throwable e) {

			throw launderThrowable(e, os);

		}

	}

	static private RuntimeException launderThrowable(final Throwable t,
			final OutputStream os) throws Exception {
		if (os != null) {
			try {
				/*
				 * Write the stack trace onto the output stream. This will show
				 * up in the client's response. This code path will only be
				 * taken if we have already begun writing the response.
				 * Otherwise, an HTTP error status will be used instead.
				 */
				t.printStackTrace(new PrintWriter(os));
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
		try {
			// log an error for the service.
			log.error(t, t);
		} finally {
			// ignore any problems here.
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
    private Callable<Void> getQueryTask(final String namespace,
            final long timestamp, final String queryStr,
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

		switch (queryType) {
		case ASK:
			/*
			 * FIXME handle ASK.
			 */
			break;
		case DESCRIBE:
		case CONSTRUCT:
			return new GraphQueryTask(namespace, timestamp, queryStr, os);
		case SELECT:
			return new TupleQueryTask(namespace, timestamp, queryStr, os);
		}

		throw new RuntimeException("Unknown query type: " + queryType);

	}

    /**
     * Note: A read-only connection.
     * 
     * @param namespace
     * @param timestamp
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

        /** A pipe used to incrementally deliver the results to the client. */
        private final PipedOutputStream os;

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
                final PipedOutputStream os) {

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.queryStr = queryStr;
            this.os = os;

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
        abstract protected void doQuery(SailRepositoryConnection cxn,
                OutputStream os) throws Exception;

        final public Void call() throws Exception {
            final Long queryId = Long.valueOf(queryIdFactory.incrementAndGet());
            final SailRepositoryConnection cxn = getQueryConnection(namespace,
                    timestamp);
            final long begin = System.nanoTime();
            try {
                queries.put(queryId, new RunningQuery(queryId.longValue(),
                        queryStr, begin));
                try {
                	doQuery(cxn, os);
                } catch(Throwable t) {
                	/*
                	 * Log the query and the exception together.
                	 */
					log.error(t.getLocalizedMessage() + ":\n" + queryStr, t);
                }
                os.flush();
                return null;
            } catch (Throwable t) {
                // launder and rethrow the exception.
                throw launderThrowable(t, os);
            } finally {
                queries.remove(queryId);
                try {
                    os.close();
                } catch (Throwable t) {
                    log.error(t, t);
                }
                try {
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
                final String queryStr, final PipedOutputStream os) {

            super(namespace, timestamp, queryStr, os);

		}

        protected void doQuery(final SailRepositoryConnection cxn,
                final OutputStream os) throws Exception {

            final TupleQuery query = cxn.prepareTupleQuery(
                    QueryLanguage.SPARQL, queryStr);
            
            query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(os)));
            
        }
        
//		public Void call() throws Exception {
//			final Long queryId = Long.valueOf(queryIdFactory.incrementAndGet());
//			final SailRepositoryConnection cxn = getQueryConnection();
//			try {
//				final long begin = System.nanoTime();
//				queries.put(queryId, new RunningQuery(queryId.longValue(), queryStr, begin));
//				final TupleQuery query = cxn.prepareTupleQuery(
//						QueryLanguage.SPARQL, queryStr);
//				query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(os)));
//				os.close();
//				return null;
//			} catch (Throwable t) {
//				// launder and rethrow the exception.
//				throw launderThrowable(t,os);
//			} finally {
//				try {
//					cxn.close();
//				} finally {
//					queries.remove(queryId);
//				}
//			}
//		}

	}

	/**
	 * Executes a graph query.
	 */
	private class GraphQueryTask extends AbstractQueryTask {

        public GraphQueryTask(final String namespace, final long timestamp,
                final String queryStr, final PipedOutputStream os) {

            super(namespace,timestamp,queryStr,os);

		}

//		public Void call() throws Exception {
//			final Long queryId = Long.valueOf(queryIdFactory.incrementAndGet());
//			final SailRepositoryConnection cxn = getQueryConnection();
//			try {
//				final long begin = System.nanoTime();
//				queries.put(queryId, new RunningQuery(queryId.longValue(), queryStr, begin));
//				final BigdataSailGraphQuery query = (BigdataSailGraphQuery) cxn
//						.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
//				query.evaluate(new RDFXMLWriter(os));
//				os.close();
//				return null;
//			} catch (Throwable t) {
//				throw launderThrowable(t, os);
//			} finally {
//				try {
//					cxn.close();
//				} finally {
//					queries.remove(queryId);
//				}
//			}
//		}

        @Override
        protected void doQuery(final SailRepositoryConnection cxn,
                final OutputStream os) throws Exception {

            final BigdataSailGraphQuery query = (BigdataSailGraphQuery) cxn
                    .prepareGraphQuery(QueryLanguage.SPARQL, queryStr);

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
    private static class Config {

    	/**
    	 * When true, suppress various things otherwise written on stdout.
    	 */
    	public boolean quiet = false;
    	
		/**
		 * The port on which the server will answer requests.
		 */
		public int port = 80;
		
		/**
		 * The default namespace.
		 */
		public String namespace;
    	
		/**
		 * The default timestamp used to query the default namespace.
		 */
		public long timestamp;
		
    	/**
		 * The #of threads to use to handle SPARQL queries -or- ZERO (0) for an
		 * unbounded pool.
		 */
    	public int queryThreadPoolSize = 8;
    	
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
	 *            <dd>The port on which the service will respond.</dd>
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

			config.timestamp = txs.newTx(ITx.READ_COMMITTED);

			if (!config.quiet) {

				System.out.println("tx: " + config.timestamp);

			}

			// start the server.
			server = new NanoSparqlServer(config, indexManager);

            /*
             * Install a shutdown hook so that the master will cancel any
             * running clients if it is interrupted (normal kill will trigger
             * this hook).
             */
            {
                
                final NanoSparqlServer tmp = server;
                
                Runtime.getRuntime().addShutdownHook(new Thread() {

                    public void run() {

                        tmp.shutdownNow();

                        System.err.println("Caught signal, shutting down: "
                                + new Date());

                    }

                });

            }

			if (!config.quiet) {

				System.out.println("Service is running: http://localhost:"
						+ config.port);
            	
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
            if (txs != null) {
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
