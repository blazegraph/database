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
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.AbstractOperation;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultWriter;
import org.openrdf.query.resultio.BooleanQueryResultWriterRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.NullOutputStream;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailBooleanQuery;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.BigdataSailUpdate;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryType;
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

    /**
     * URL Query parameter used to request the explanation of a query rather
     * than its results.
     */
    protected static final String EXPLAIN = "explain";
    
    /**
     * URL Query parameter used to request the "analytic" query hints.
     */
    protected static final String ANALYTIC = "analytic";
    
    /**
     * URL query parameter used to specify a URI in the default graph for SPARQL
     * query (but not for SPARQL update).
     */
    protected static final String DEFAULT_GRAPH_URI = "default-graph-uri";

    /**
     * URL query parameter used to specify a URI in the set of named graphs for
     * SPARQL query (but not for SPARQL update).
     */
    protected static final String NAMED_GRAPH_URI = "named-graph-uri";
    
    /**
     * URL query parameter used to specify a URI in the default graph for SPARQL
     * UPDATE.
     */
    protected static final String USING_GRAPH_URI = "using-graph-uri";

    /**
     * URL query parameter used to specify a URI in the set of named graphs for
     * SPARQL UPDATE.
     */
    protected static final String USING_NAMED_GRAPH_URI = "using-named-graph-uri";

	private final SparqlEndpointConfig m_config;

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
     * Return the effective boolean value of a URL query parameter such as
     * "analytic". If the URL query parameter was not given, then the effective
     * boolean value is the <i>defaultValue</o>. If a URL query parameter which
     * is given without an explicit value, such as <code>&analytic</code>, then
     * it will be reported as <code>true</code>.
     * 
     * @param s
     *            The value of the URL query parameter.
     * @param defaultValue
     *            The default value to return if the parameter was not given.
     * 
     * @return The effective boolean value.
     */
    protected static Boolean getEffectiveBooleanValue(String s,
            final Boolean defaultValue) {

        if (s == null)
            return defaultValue;

        s = s.trim();

        if (s.length() == 0) {

            return true;

        }

        return Boolean.valueOf(s);

    }

	/**
     * Abstract base class for running queries handles the timing, pipe,
     * reporting, obtains the connection, and provides the finally {} semantics
     * for each type of query task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public abstract class AbstractQueryTask implements Callable<Void> {
        
        /** The namespace against which the query will be run. */
        private final String namespace;

        /**
         * The timestamp of the view for that namespace against which the query
         * will be run.
         */
        public final long timestamp;

        /**
         * The baseURI is set from the effective request URI.
         */
        protected final String baseURI;

        /**
         * The {@link ASTContainer} provides access to the original SPARQL
         * query, the query model, the query plan, etc.
         */
        protected final ASTContainer astContainer;

        /**
         * <code>true</code> iff this is a SPARQL UPDATE request.
         */
        protected final boolean update;

        /**
         * A symbolic constant indicating the type of query.
         */
        protected final QueryType queryType;
        
        /**
         * The negotiated MIME type to be used for the query response (this
         * does not include the charset encoding) -or- <code>null</code> if
         * this is a SPARQL UPDATE request.
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
         * negotiated {@link #mimeType} -or- <code>null</code> if this is a
         * SPARQL UPDATE request
         */
        protected final String fileExt;
        
        /** The request. */
        protected final HttpServletRequest req;
        
        /** Where to write the response. */
        protected final OutputStream os;

//		/**
//		 * Set to the timestamp as reported by {@link System#nanoTime()} when
//		 * the query begins to execute.
//		 */
//        final AtomicLong beginTime = new AtomicLong();
        
        /**
         * The queryId as assigned by the SPARQL end point (rather than the
         * {@link QueryEngine}).
         */
        protected final Long queryId;

		/**
		 * The queryId used by the {@link QueryEngine}. If the application has
		 * not specified this using {@link QueryHints#QUERYID} then this is
		 * assigned and set on the query using {@link QueryHints#QUERYID}. This
		 * decision can not be made until we parse the query so the behavior is
		 * handled by the subclasses.
		 */
        volatile protected UUID queryId2;

        /**
         * The parsed query. It will be one of the {@link BigdataSailQuery}
         * implementations or {@link BigdataSailUpdate}. They all extend
         * {@link AbstractOperation}.
         * <p>
         * Note: This field is made visible by the volatile write on
         * {@link #queryId2}.
         */
        protected AbstractOperation sailQueryOrUpdate;
        
        /**
         * When true, provide an "explanation" for the query (query plan, query
         * evaluation statistics) rather than the results of the query.
         */
        final boolean explain;
        
        /**
         * When true, enable the "analytic" query hints. MAY be
         * <code>null</code>, in which case we do not set the query hint.
         */
        final Boolean analytic;
        
        /**
         * 
         * @param namespace
         *            The namespace against which the query will be run.
         * @param timestamp
         *            The timestamp of the view for that namespace against which
         *            the query will be run.
         * @param baseURI
         *            The base URI.
         * @param astContainer
         *            The container with all the information about the submitted
         *            query, including the original SPARQL query, the parse
         *            tree, etc.
         * @param queryType
         *            The {@link QueryType}.
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
                final String baseURI,
                final ASTContainer astContainer,//
                final QueryType queryType,//
                final String mimeType,//
                final Charset charset,//
                final String fileExt,//
                final HttpServletRequest req,//
                final OutputStream os//
        ) {

            if (namespace == null)
                throw new IllegalArgumentException();
            if (baseURI == null)
                throw new IllegalArgumentException();
            if (astContainer == null)
                throw new IllegalArgumentException();
            if (queryType == null)
                throw new IllegalArgumentException();
            if (mimeType == null)
                throw new IllegalArgumentException();
            if (fileExt == null)
                throw new IllegalArgumentException();
            if (req == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.baseURI = baseURI;
            this.astContainer = astContainer;
            this.update = false;
            this.queryType = queryType;
            this.mimeType = mimeType;
            this.charset = charset;
            this.fileExt = fileExt;
            this.req = req;
            this.explain = req.getParameter(EXPLAIN) != null;
            this.analytic = getEffectiveBooleanValue(
                    req.getParameter(ANALYTIC), QueryHints.DEFAULT_ANALYTIC);
            this.os = os;
            this.queryId = Long.valueOf(m_queryIdFactory.incrementAndGet());

        }

        /**
         * 
         * @param namespace
         *            The namespace against which the query will be run.
         * @param timestamp
         *            The timestamp of the view for that namespace against which
         *            the query will be run.
         * @param baseURI
         *            The base URI.
         * @param astContainer
         *            The container with all the information about the submitted
         *            query, including the original SPARQL query, the parse
         *            tree, etc.
         * @param req
         *            The request.
         * @param os
         *            Where to write the data for the query result.
         */
        protected AbstractQueryTask(//
                final String namespace,//
                final long timestamp, //
                final String baseURI,
                final ASTContainer astContainer,//
//                final QueryType queryType,//
//                final String mimeType,//
//                final Charset charset,//
//                final String fileExt,//
                final HttpServletRequest req,//
                final OutputStream os//
        ) {

            if (namespace == null)
                throw new IllegalArgumentException();
            if (baseURI == null)
                throw new IllegalArgumentException();
            if (astContainer == null)
                throw new IllegalArgumentException();
            if (req == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.baseURI = baseURI;
            this.astContainer = astContainer;
            this.update = true;
            this.queryType = null;
            this.mimeType = null;
            this.charset = null;
            this.fileExt = null;
            this.req = req;
            this.explain = req.getParameter(EXPLAIN) != null;
            this.analytic = getEffectiveBooleanValue(
                    req.getParameter(ANALYTIC), QueryHints.DEFAULT_ANALYTIC);
            this.os = os;
            this.queryId = Long.valueOf(m_queryIdFactory.incrementAndGet());

        }

        /**
         * If the {@link HttpServletRequest} included one or more of
         * <ul>
         * <li>{@value BigdataRDFContext#DEFAULT_GRAPH_URI}</li>
         * <li>{@value BigdataRDFContext#NAMED_GRAPH_URI}</li>
         * <li>{@value BigdataRDFContext#USING_GRAPH_URI}</li>
         * <li>{@value BigdataRDFContext#USING_NAMED_GRAPH_URI}</li>
         * </ul>
         * then the {@link Dataset} for the query is replaced by the
         * {@link Dataset} constructed from those protocol parameters (the
         * parameters which are recognized are different for query and SPARQL
         * update).
         * 
         * @param queryOrUpdate
         *            The query.
         */
        protected void overrideDataset(final AbstractOperation queryOrUpdate) {
            
            final String[] defaultGraphURIs = req
                    .getParameterValues(update ? USING_GRAPH_URI
                            : DEFAULT_GRAPH_URI);

            final String[] namedGraphURIs = req
                    .getParameterValues(update ? USING_NAMED_GRAPH_URI
                            : NAMED_GRAPH_URI);

            if (defaultGraphURIs != null || namedGraphURIs != null) {

                final DatasetImpl dataset = new DatasetImpl();

                if (defaultGraphURIs != null)
                    for (String graphURI : defaultGraphURIs)
                        dataset.addDefaultGraph(new URIImpl(graphURI));

                if (namedGraphURIs != null)
                    for (String graphURI : namedGraphURIs)
                        dataset.addNamedGraph(new URIImpl(graphURI));

                queryOrUpdate.setDataset(dataset);

            }

        }

        /**
         * 
		 * <p>
		 * Note: This is also responsible for noticing the time at which the
		 * query begins to execute and storing the {@link RunningQuery} in the
		 * {@link #m_queries} map.
		 * 
         * @param The connection.
         */
        final AbstractQuery setupQuery(final BigdataSailRepositoryConnection cxn) {

            // Note the begin time for the query.
            final long begin =  System.nanoTime();
            
            final AbstractQuery query = newQuery(cxn);

        	// Figure out the UUID under which the query will execute.
            final UUID queryId2 = setQueryId(((BigdataSailQuery) query)
                    .getASTContainer());
            
            // Override query if data set protocol parameters were used.
			overrideDataset(query);

            if (analytic != null) {

                // Turn analytic query on/off as requested.
//                astContainer.getOriginalAST().setQueryHint(QueryHints.ANALYTIC,
//                        analytic.toString());
                astContainer.setQueryHint(QueryHints.ANALYTIC,
                        analytic.toString());
                
            }

			// Set the query object.
			this.sailQueryOrUpdate = query;
			
			// Set the IRunningQuery's UUID (volatile write!) 
			this.queryId2 = queryId2;
			
			// Stuff it in the map of running queries.
            m_queries.put(queryId, new RunningQuery(queryId.longValue(),
                    queryId2, begin, this));

            return query;
            
        }

        /**
         * 
         * <p>
         * Note: This is also responsible for noticing the time at which the
         * query begins to execute and storing the {@link RunningQuery} in the
         * {@link #m_queries} map.
         * 
         * @param The connection.
         */
        final BigdataSailUpdate setupUpdate(
                final BigdataSailRepositoryConnection cxn) {

            // Note the begin time for the query.
            final long begin =  System.nanoTime();
            
            final BigdataSailUpdate update = new BigdataSailUpdate(astContainer,
                    cxn);

            // Figure out the UUID under which the query will execute.
            final UUID queryId2 = setQueryId(((BigdataSailUpdate) update)
                    .getASTContainer());
            
            // Override query if data set protocol parameters were used.
            overrideDataset(update);

            if (analytic != null) {

                // Turn analytic query on/off as requested.
//                astContainer.getOriginalAST().setQueryHint(QueryHints.ANALYTIC,
//                        analytic.toString());
                astContainer.setQueryHint(QueryHints.ANALYTIC,
                        analytic.toString());
                
            }

            // Set the query object.
            this.sailQueryOrUpdate = update;
            
            // Set the IRunningQuery's UUID (volatile write!) 
            this.queryId2 = queryId2;
            
            // Stuff it in the map of running queries.
            m_queries.put(queryId, new RunningQuery(queryId.longValue(),
                    queryId2, begin, this));

            return update;
            
        }

        /**
         * Wrap the {@link ParsedQuery} as a {@link SailQuery}.
         * <p>
         * Note: This is achieved without reparsing the query.
         * 
         * @param cxn
         *            The connection.
         * 
         * @return The query.
         */
        private AbstractQuery newQuery(final BigdataSailRepositoryConnection cxn) {

//            final ASTContainer astContainer = ((BigdataParsedQuery) parsedQuery)
//                    .getASTContainer();

//            final QueryType queryType = ((BigdataParsedQuery) parsedQuery)
//                    .getQueryType();

            switch (queryType) {
            case SELECT:
                return new BigdataSailTupleQuery(astContainer, cxn);
            case DESCRIBE:
            case CONSTRUCT:
                return new BigdataSailGraphQuery(astContainer, cxn);
            case ASK: {
                return new BigdataSailBooleanQuery(astContainer, cxn);
            }
            default:
                throw new RuntimeException("Unknown query type: " + queryType);
            }

        }

        /**
         * Determines the {@link UUID} which will be associated with the
         * {@link IRunningQuery}. If {@link QueryHints#QUERYID} has already been
         * used by the application to specify the {@link UUID} then that
         * {@link UUID} is noted. Otherwise, a random {@link UUID} is generated
         * and assigned to the query by binding it on the query hints.
         * <p>
         * Note: The ability to provide metadata from the {@link ASTContainer}
         * in the {@link StatusServlet} or the "EXPLAIN" page depends on the
         * ability to cross walk the queryIds as established by this method.
         * 
         * @param query
         *            The query.
         * 
         * @return The {@link UUID} which will be associated with the
         *         {@link IRunningQuery}.
         */
		protected UUID setQueryId(final ASTContainer astContainer) {
			assert queryId2 == null; // precondition.
            // Figure out the effective UUID under which the query will run.
            final String queryIdStr = astContainer.getQueryHint(
                    QueryHints.QUERYID);
            if (queryIdStr == null) {
                queryId2 = UUID.randomUUID();
                astContainer.setQueryHint(QueryHints.QUERYID,
                        queryId2.toString());
			} else {
				queryId2 = UUID.fromString(queryIdStr);
			}
            return queryId2;
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
			BigdataSailRepositoryConnection cxn = null;
            try {
                cxn = getQueryConnection(namespace, timestamp);
                if(log.isTraceEnabled())
                    log.trace("Query running...");
//                try {
    			if(explain) {
					/*
					 * The data goes to a bit bucket and we send an
					 * "explanation" of the query evaluation back to the caller.
					 * 
					 * Note: The trick is how to get hold of the IRunningQuery
					 * object. It is created deep within the Sail when we
					 * finally submit a query plan to the query engine. We have
					 * the queryId (on queryId2), so we can look up the
					 * IRunningQuery in [m_queries] while it is running, but
					 * once it is terminated the IRunningQuery will have been
					 * cleared from the internal map maintained by the
					 * QueryEngine, at which point we can not longer find it.
					 */
    				doQuery(cxn, new NullOutputStream());
    			} else {
    				doQuery(cxn, os);
                    os.flush();
                    os.close();
    			}
            	if(log.isTraceEnabled())
            	    log.trace("Query done.");
//                } catch(Throwable t) {
//                	/*
//                	 * Log the query and the exception together.
//                	 */
//					log.error(t.getLocalizedMessage() + ":\n" + queryStr, t);
//                }
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
                final String baseURI,
                final ASTContainer astContainer, final QueryType queryType,
                final BooleanQueryResultFormat format,
                final HttpServletRequest req, final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer,
                    queryType, format.getDefaultMIMEType(),
                    format.getCharset(), format.getDefaultFileExtension(), req,
                    os);

        }

        protected void doQuery(final BigdataSailRepositoryConnection cxn,
                final OutputStream os) throws Exception {

            final BigdataSailBooleanQuery query = (BigdataSailBooleanQuery) setupQuery(cxn);
            
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
                final String baseURI,
                final ASTContainer astContainer, final QueryType queryType,
                final TupleQueryResultFormat format,
                final HttpServletRequest req,
                final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer,
                    queryType, format.getDefaultMIMEType(),
                    format.getCharset(), format.getDefaultFileExtension(), req,
                    os);

		}

		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) setupQuery(cxn);
			
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
                final String baseURI,
                final ASTContainer astContainer, final QueryType queryType,
                final RDFFormat format, final HttpServletRequest req,
                final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer,
                    queryType, format.getDefaultMIMEType(),
                    format.getCharset(), format.getDefaultFileExtension(), req,
                    os);

        }

		@Override
		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

            final BigdataSailGraphQuery query = (BigdataSailGraphQuery) setupQuery(cxn);
            
            /*
             * FIXME An error thrown here (such as if format is null and we do
             * not check it) will cause the response to hang, at least for the
             * test suite. Look into this further and make the error handling
             * bullet proof!
             * 
             * This may be related to queryId2. That should be imposed on the
             * IRunningQuery via QueryHints.QUERYID such that the QueryEngine
             * assigns that UUID to the query. We can then correlate the queryId
             * to the IRunningQuery, which is important for some of the status
             * pages. This will also let us INTERRUPT the IRunningQuery if there
             * is an error during evaluation, which might be necessary. For
             * example, if the client dies while the query is running. Look at
             * the old NSS code and see what it was doing and whether this was
             * logic was lost of simply never implemented.
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
     * Executes a SPARQL UPDATE.
     */
    private class UpdateTask extends AbstractQueryTask {

        public UpdateTask(final String namespace, final long timestamp,
                final String baseURI, final ASTContainer astContainer,
                final HttpServletRequest req, final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer,
//                    null,//queryType
//                    null,//format.getDefaultMIMEType()
//                    null,//format.getCharset(), 
//                    null,//format.getDefaultFileExtension(), 
                    req,//
                    os//
                    );

        }

        protected void doQuery(final BigdataSailRepositoryConnection cxn,
                final OutputStream os) throws Exception {

            final BigdataSailUpdate update = setupUpdate(cxn);

            update.execute();

        }

    }

    /**
     * Return the task which will execute the SPARQL Query -or- SPARQL UPDATE.
     * <p>
     * Note: The {@link OutputStream} is passed in rather than the
     * {@link HttpServletResponse} in order to permit operations such as
     * "DELETE WITH QUERY" where this method is used in a context which writes
     * onto an internal pipe rather than onto the {@link HttpServletResponse}.
     * 
     * @param namespace
     *            The namespace associated with the {@link AbstractTripleStore}
     *            view.
     * @param timestamp
     *            The timestamp associated with the {@link AbstractTripleStore}
     *            view.
     * @param queryStr
     *            The query.
     * @param acceptOverride
     *            Override the Accept header (optional). This is used by UPDATE
     *            and DELETE so they can control the {@link RDFFormat} of the
     *            materialized query results.
     * @param req
     *            The request.
     * @param os
     *            Where to write the results.
     * @param update
     *            <code>true</code> iff this is a SPARQL UPDATE request.
     * 
     * @return The task.
     * 
     * @throws MalformedQueryException
     */
    public AbstractQueryTask getQueryTask(//
            final String namespace,//
            final long timestamp,//
            final String queryStr,//
            final String acceptOverride,//
            final HttpServletRequest req,//
            final OutputStream os,//
            final boolean update//
            ) throws MalformedQueryException {

        /*
         * Setup the baseURI for this request. It will be set to the requestURI.
         */
        final String baseURI = req.getRequestURL().toString();

        if(update) {

            /*
             * Parse the query so we can figure out how it will need to be executed.
             * 
             * Note: This goes through some pains to make sure that we parse the
             * query exactly once in order to minimize the resources associated with
             * the query parser.
             */
            final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(
                    getTripleStore(namespace, timestamp)).parseUpdate2(
                    queryStr, baseURI);

            if (log.isDebugEnabled())
                log.debug(astContainer.toString());

            return new UpdateTask(namespace, timestamp, baseURI, astContainer,
                    req, os);

        }
        
        /*
         * Parse the query so we can figure out how it will need to be executed.
         * 
         * Note: This goes through some pains to make sure that we parse the
         * query exactly once in order to minimize the resources associated with
         * the query parser.
         */
        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(
                getTripleStore(namespace, timestamp)).parseQuery2(queryStr,
                baseURI);

        if (log.isDebugEnabled())
            log.debug(astContainer.toString());

        final QueryType queryType = astContainer.getOriginalAST()
                .getQueryType();
        
		/*
		 * When true, provide an "explanation" for the query (query plan, query
		 * evaluation statistics) rather than the results of the query.
		 */
		final boolean explain = req.getParameter(EXPLAIN) != null;

        /*
         * CONNEG for the MIME type.
         * 
         * Note: An attempt to CONNEG for a MIME type which can not be used with
         * a give type of query will result in a response using a default MIME
         * Type for that query.
         */
        final String acceptStr = explain ? "text/html"
                : acceptOverride != null ? acceptOverride : req.getHeader("Accept");

        final ConnegUtil util = new ConnegUtil(acceptStr);
        
        switch (queryType) {
        case ASK: {

            final BooleanQueryResultFormat format = util
                    .getBooleanQueryResultFormat(BooleanQueryResultFormat.SPARQL);

            return new AskQueryTask(namespace, timestamp, baseURI,
                    astContainer, queryType, format, req, os);

        }
        case DESCRIBE:
        case CONSTRUCT: {

            final RDFFormat format = util.getRDFFormat(RDFFormat.RDFXML);

            return new GraphQueryTask(namespace, timestamp, baseURI,
                    astContainer, queryType, format, req, os);

        }
        case SELECT: {

            final TupleQueryResultFormat format = util
                    .getTupleQueryResultFormat(TupleQueryResultFormat.SPARQL);

            return new TupleQueryTask(namespace, timestamp, baseURI,
                    astContainer, queryType, format, req, os);

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

		/**
		 * The task executing the query.
		 */
		final AbstractQueryTask queryTask;
		
//		/** The query. */
//		final String query;
		
		/** The timestamp when the query was accepted (ns). */
		final long begin;

		public RunningQuery(final long queryId, final UUID queryId2,
				final long begin,
				final AbstractQueryTask queryTask) {

			this.queryId = queryId;

			this.queryId2 = queryId2;
			
//			this.query = query;

			this.begin = begin;
			
			this.queryTask = queryTask;

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
     */
    public BigdataSailRepositoryConnection getQueryConnection(
            final String namespace, final long timestamp)
            throws RepositoryException {

        /*
         * Note: [timestamp] will be a read-only tx view of the triple store if
         * a READ_LOCK was specified when the NanoSparqlServer was started
         * (unless the query explicitly overrides the timestamp of the view on
         * which it will operate).
         */
        final AbstractTripleStore tripleStore = getTripleStore(namespace,
                timestamp);
        
        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        if (TimestampUtility.isReadOnly(timestamp)) {

            return (BigdataSailRepositoryConnection) repo
                    .getReadOnlyConnection(timestamp);

        }
        
        // Read-write connection.
        final BigdataSailRepositoryConnection conn = repo.getConnection();
        
        conn.setAutoCommit(false);
        
        return conn;

    }

    /**
     * Return a read-only view of the {@link AbstractTripleStore} for the given
     * namespace will read from the commit point associated with the given
     * timestamp.
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
    public AbstractTripleStore getTripleStore(final String namespace,
            final long timestamp) {
        
//        if (timestamp == ITx.UNISOLATED)
//            throw new IllegalArgumentException("UNISOLATED reads disallowed.");

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null) {

            throw new RuntimeException("Not found: namespace=" + namespace
                    + ", timestamp=" + TimestampUtility.toString(timestamp));

        }

        return tripleStore;
        
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
        @SuppressWarnings("unchecked")
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

            if (className == null) {
                // Skip deleted triple store entry.
                continue;
            }

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
