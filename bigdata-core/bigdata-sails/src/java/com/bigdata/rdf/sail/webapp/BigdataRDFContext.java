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

import info.aduna.xml.XMLWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
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
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.counters.CAT;
import com.bigdata.io.NullOutputStream;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataBaseContext;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSailBooleanQuery;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailQuery;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.BigdataSailUpdate;
import com.bigdata.rdf.sail.ISPARQLUpdateListener;
import com.bigdata.rdf.sail.SPARQLUpdateEvent;
import com.bigdata.rdf.sail.SPARQLUpdateEvent.DeleteInsertWhereStats;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.webapp.XMLBuilder.Node;
import com.bigdata.rdf.sail.webapp.client.StringUtil;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.RelationSchema;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

import info.aduna.xml.XMLWriter;

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
     * Optional value for the {@link #EXPLAIN} URL query parameter that may be
     * used to request more detail in the "EXPLAIN" of a query.
     */
    protected static final String EXPLAIN_DETAILS = "details";
    
    /**
     * URL Query parameter used to request the "analytic" query hints. MAY be
     * <code>null</code>, in which case we do not set
     * {@link QueryHints#ANALYTIC} query hint.
     */
    protected static final String ANALYTIC = "analytic";
    
    /**
     * URL Query parameter used to request the use of the Runtime Query
     * Optimizer.
     */
    protected static final String RTO = "RTO";
    
    /**
     * URL Query parameter used to request an XHTML response for SPARQL
     * QUERY or SPARQL UPDATE.  For SPARQL QUERY, this provides an XHTML
     * table view of the solutions.  For SPARQL UPDATE, this provides an
     * incremental progress report on the UPDATE request.
     */
    protected static final String XHTML = "xhtml";
    
    /**
     * URL Query parameter used to specify an XSL style sheet to be associated
     * with the response in combination with the {@link #XHTML} URL query
     * parameter.
     */
    protected static final String XSL_STYLESHEET = "xsl-stylesheet";
    
    /**
     * The default XSL style sheet.
     * 
     * @see #XSL_STYLESHEET
     */
    protected static final String DEFAULT_XSL_STYLESHEET = BigdataStatics
            .getContextPath() + "/html/result-to-html.xsl";
    
    /**
     * URL Query parameter used to request an incremental XHTML representation
     * reporting on the progress of a SPARQL UPDATE.
     * <p>
     * Note: When this option is specified, the SPARQL UPDATE will use an HTTP
     * status code of 200 (Ok) even if the UPDATE fails. In the event of an
     * UPDATE failure, the stack trace will be formatted into the response.
     */
    protected static final String MONITOR = "monitor";
    
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
    * URL query parameter used to specify the URI(s) from which data will be
    * loaded for INSERT (POST-WITH-URIs.
    * 
    * @see InsertServlet
    */
    protected static final String URI = "uri";

    /**
    * URL query parameter used to specify the default context(s) for INSERT
    * (POST-WITH-URIs, POST-WITH-BODY).
    * 
    * @see InsertServlet
    */
    protected static final String CONTEXT_URI = "context-uri";
    
    /**
     * URL query parameter used to specify a URI in the set of named graphs for
     * SPARQL UPDATE.
     */
    protected static final String USING_NAMED_GRAPH_URI = "using-named-graph-uri";

    /**
     * URL query parameter used to specify a non-default KB namespace (as an
     * alternative to having it in the URL path). The path takes precendence
     * over this query parameter.
     * 
     * @see BigdataRDFServlet#getNamespace(HttpServletRequest)
     */
    protected static final String NAMESPACE = "namespace";
    
    /**
     * HTTP header may be used to specify the timeout for a query.
     * 
     * @see http://trac.blazegraph.com/ticket/914 (Set timeout on remote query)
     */
    static public final String HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS = "X-BIGDATA-MAX-QUERY-MILLIS";
    
    /**
     * HTTP header may be used to echo back the query.
     * 
     */
    static public final String HTTP_HEADER_ECHO_BACK_QUERY = "X-ECHO-BACK-QUERY";
    
    /**
     * The name of the parameter/attribute that contains maxQueryTime (milliseconds)
     * for remote queries execution.
     * <p>
     * Note: This is not the openrdf parameter (in SECONDS), but higher resolution milliseconds value.
     * The {@link #HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS} and the
     * {@link ConfigParams#QUERY_TIMEOUT} both override this value. This is done
     * to make it possible to guarantee that a timeout is imposed either by the
     * server (web.xml) or by an http proxy. Application timeouts may be
     * specified using this query parameter.
     */
    static final String MAX_QUERY_TIME_MILLIS = "maxQueryTimeMillis";
    
    /**
     * Specifies a maximum query execution time, 
     * in whole seconds. The value should be an integer. 
     * A setting of 0 or a negative number indicates 
     * unlimited query time (the default).
     */
    static final String TIMEOUT = "timeout";

    /**
     * The name of the parameter/attribute that contains baseURI for remote queries execution.
     */
    static final String BASE_URI = "baseURI";

    private final SparqlEndpointConfig m_config;

    /**
     * A thread pool for running accepted queries against the
     * {@link QueryEngine}. The number of queries that will be processed
     * concurrently is determined by this thread pool.
     * 
     * @see SparqlEndpointConfig#queryThreadPoolSize
     */
    /*package*/final ExecutorService queryService;
	
	private final ScheduledFuture<?> m_queueStatsFuture;
	private final ThreadPoolExecutorBaseStatisticsTask m_queueSampleTask;

    /**
     * The currently executing queries (does not include queries where a client
     * has established a connection but the query is not running because the
     * {@link #queryService} is blocking).
     * <p>
     * Note: This includes both SPARQL QUERY and SPARQL UPDATE requests.
     * However, the {@link AbstractQueryTask#queryId2} might not yet be bound
     * since it is not set until the request begins to execute. See
     * {@link AbstractQueryTask#setQueryId(ASTContainer)}.
     */
    private final ConcurrentHashMap<Long/* queryId */, RunningQuery> m_queries = new ConcurrentHashMap<Long, RunningQuery>();
    
    /**
     * The currently executing QUERY and UPDATE requests.
     * <p>
     * Note: This does not include requests where a client has established a
     * connection to the SPARQL end point but the request is not executing
     * because the {@link #queryService} is blocking).
     * <p>
     * Note: This collection was introduced because the SPARQL UPDATE requests
     * are not executed on the {@link QueryEngine} and hence we can not use
     * {@link QueryEngine#getRunningQuery(UUID)} to resolve the {@link Future}
     * of an {@link UpdateTask}.
     */
    private final ConcurrentHashMap<UUID/* queryId2 */, RunningQuery> m_queries2 = new ConcurrentHashMap<UUID, RunningQuery>();

    /**
     * Class units a task and its future.
     */
    static class TaskAndFutureTask<T> {

         public final AbstractRestApiTask<T> task;
         public final FutureTask<T> ft;
         public final long beginNanos;
         public UUID taskUuid;
         private final AtomicLong elapsedNanos = new AtomicLong(-1L);

         TaskAndFutureTask(final AbstractRestApiTask<T> task, final FutureTask<T> ft, final long beginNanos) {
             this.task = task;
             this.ft = ft;
             this.beginNanos = beginNanos;
         }
         
         

         /**
          * Hook must be invoked when the task is done executing.
          */
         void done() {
             elapsedNanos.set(System.nanoTime() - beginNanos);
         }
         
         /**
         * The elapsed nanoseconds that the task has been executing. The clock
         * stops once {@link #done()} is called.
         */
         long getElapsedNanos() {
             
            final long elapsedNanos = this.elapsedNanos.get();

            if (elapsedNanos == -1L) {

                return System.nanoTime() - beginNanos;

            }

            return elapsedNanos;

         }
         
         /**
 		 * Convenience method to return com.bigdata.rdf.sail.model.RunningQuery from a BigdataRDFContext
 		 * running query. 
 		 * 
 		 * There is a current difference between the embedded and the REST model in that
 		 * the embedded model uses an arbitrary string as an External ID, but the 
 		 * REST version uses a timestamp.  The timestamp is tightly coupled to the current
 		 * workbench.
 		 * 
 		 * TODO:  This needs to be refactored into unified model between the embedded and REST clients for tasks.
 		 * 
 		 * @return 
 		 */
 		public com.bigdata.rdf.sail.model.RunningQuery getModelRunningQuery() {
 		
 			final com.bigdata.rdf.sail.model.RunningQuery modelQuery;
 			
 			final boolean isUpdateQuery = false;
 			
 			modelQuery = new com.bigdata.rdf.sail.model.RunningQuery(
 					Long.toString(this.beginNanos), this.task.uuid, this.beginNanos,
 					isUpdateQuery);
 			
 			return modelQuery;
 		}
 		
 		public long getMutationCount() {
 			return task.getMutationCount();
 		}	

    }

    /**
     * A mapping from the given (or assigned) {@link UUID} to the
     * {@link AbstractRestApiTask}.
     * <p>
     * Note: This partly duplicates information already captured by some other
     * collections. However it is the only source for this information for tasks
     * other than SPARQL QUERY or SPARQL UPDATE.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
     *      operations should be cancelable from both REST API and workbench
     *      </a>
     */
    private final ConcurrentHashMap<UUID/* RestAPITask */, TaskAndFutureTask<?>> m_restTasks = new ConcurrentHashMap<UUID, TaskAndFutureTask<?>>();

    /**
     * Return the {@link RunningQuery} for a currently executing SPARQL QUERY or
     * UPDATE request.
     * 
     * @param queryId2
     *            The {@link UUID} for the request.
     *            
     * @return The {@link RunningQuery} iff it was found.
     */
    RunningQuery getQueryById(final UUID queryId2) {

        return m_queries2.get(queryId2);
        
    }
    
    /**
     * Factory for the query identifiers.
     */
    private final AtomicLong m_queryIdFactory = new AtomicLong();
    
    /**
     * The currently executing queries (does not include queries where a client
     * has established a connection but the query is not running because the
     * {@link #queryService} is blocking).
     * 
     * @see #m_queries
     */
    final Map<Long, RunningQuery> getQueries() {

        return m_queries;
        
    }
    
    final public AtomicLong getQueryIdFactory() {
    
        return m_queryIdFactory;
        
    }

    /**
     * Return the {@link AbstractRestApiTask} for a currently executing request.
     * 
     * @param uuid
     *            The {@link UUID} for the request.
     * 
     * @return The {@link AbstractRestApiTask} iff it was found.
     */
    TaskAndFutureTask<?> getTaskById(final UUID uuid) {

        return m_restTasks.get(uuid);
        
    }

    /**
     * Register a task and the associated {@link FutureTask}.
     * 
     * @param task
     *            The task.
     * @param ft
     *            The {@link FutureTask} (which is used to cancel the task).
     */
    <T> void addTask(final AbstractRestApiTask<T> task, final FutureTask<T> ft) {

        m_restTasks.put(task.uuid, new TaskAndFutureTask<T>(task, ft, System.nanoTime()));
        
    }
    
    /**
     * Remove a task (the task should be known to be done).
     * 
     * @param uuid
     *            The task {@link UUID}.
     */
    void removeTask(final UUID uuid) {

        final TaskAndFutureTask<?> task = m_restTasks.remove(uuid);

        if (task != null) {

            // Notify the task that it is done executing.
            task.done();
            
        }

    }
    
    /**
     * A mapping from the given (or assigned) {@link UUID} to the
     * {@link AbstractRestApiTask}.
     * <p>
     * Note: This partly duplicates information already captured by some other
     * collections. However it is the only source for this information for tasks
     * other than SPARQL QUERY or SPARQL UPDATE.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1254" > All REST API
     *      operations should be cancelable from both REST API and workbench
     *      </a>
     */
    final Map<UUID/* RestAPITask */, TaskAndFutureTask<?>> getTasks() {

        return m_restTasks;
        
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
     * FIXME GROUP COMMIT: Shutdown should abort open transactions (including
     * queries and updates). This should be addressed when we handle group commit
     * since that provides us with a means to recognize and interrupt each
     * running {@link AbstractRestApiTask}.
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
     * Return the effective baseURI for the request. This may be set using the
     * {@value #BASE_URI} URL query parameter. If it is not set, it defaults to
     * the request URL.
     * 
     * @param req
     *            The request.
     * @param resp
     *            The response.
     * 
     * @return The effective baseURI and never null.
     */
    static public String getBaseURI(final HttpServletRequest req, final HttpServletResponse resp) {

        String baseURI = req.getParameter(BASE_URI);

        if (baseURI == null) {

            baseURI = req.getRequestURL().toString();

        }

        return baseURI;
	    
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
     * Return the effective string value of a URL query parameter. If the URL
     * query parameter was not given, or if it was given without an explicit
     * value, then the effective string value is the <i>defaultValue</o>.
     * 
     * @param s
     *            The value of the URL query parameter.
     * @param defaultValue
     *            The default value to return if the parameter was not given or
     *            given without an explicit value.
     * 
     * @return The effective value.
     */
    protected static String getEffectiveStringValue(String s,
            final String defaultValue) {

        if (s == null)
            return defaultValue;

        s = s.trim();

        if (s.length() == 0) {

            return defaultValue;

        }

        return s;

    }
    
    /**
     * Invoked if {@link #EXPLAIN} is found as a URL request parameter to
     * see whether it exists with {@link #EXPLAIN_DETAILS} as a value. We
     * have to check each value since there might be more than one.
     * 
     * @param req
     *            The request.
     * @return
     */
    static private boolean isExplainDetails(final HttpServletRequest req) {

        final String[] vals = req.getParameterValues(EXPLAIN);

        if (vals == null) {

            return false;

        }

        for (String val : vals) {

            if (val.equals(EXPLAIN_DETAILS))
                return true;

        }

        return false;

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
        
    	/** The connection used to isolate the query or update request. */
    	private final BigdataSailRepositoryConnection cxn;
    	
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
         * Controls returning inferred triples.
         * 
         * @see BLZG-1207 (getStatements ignored includeInferred)
         */
        protected final boolean includeInferred;

        /**
         * Bindings provided for query execution
         */
        protected final Map<String,Value> bindings;
        
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

        /** The response. */
        protected final HttpServletResponse resp;

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
         * handled by the subclasses. This also means that {@link #queryId2} is
         * NOT available until the {@link AbstractQueryTask} begins to execute.
         * <p>
         * Note: Even though a SPARQL UPDATE request does not execute on the
         * {@link QueryEngine}, a {@link UUID} will also be bound for the SPARQL
         * UPDATE request. This provides us with a consistent identifier that
         * can be used by clients and in the XHTML UI on the status page to
         * refer to a SPARQL UPDATE request.
         * 
         * @see AbstractQueryTask#setQueryId(ASTContainer)
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
         * The {@link Future} of the {@link UpdateTask} and <code>null</code> if
         * this is not a SPARQL UPDATE or if the {@link UpdateTask} has not
         * begun execution.
         */
        volatile protected Future<Void> updateFuture;
        
        /**
         * When <code>true</code>, provide an "explanation" for the query (query
         * plan, query evaluation statistics) rather than the results of the
         * query.
         */
        final boolean explain;

        /**
         * When <code>true</code>, provide an additional level of detail for the
         * query explanation.
         */
        final boolean explainDetails;

        /**
         * When <code>true</code>, enable the "analytic" query hints. 
         */
        final boolean analytic;

        /**
         * When <code>true</code>, enable the Runtime Query Optimizer.
         */
        final boolean rto;
        
        /**
         * When <code>true</code>, provide an view of the XHTML representation
         * of the solutions or graph result (SPARQL QUERY)
         * 
         * @see BigdataRDFContext#XHTML
         */
        final boolean xhtml;
        
        /**
         * When <code>true</code>, provide an incremental XHTML representation
         * reporting on the progress of a SPARQL UPDATE.
         * <p>
         * Note: When <code>true</code>, the SPARQL UPDATE will use an HTTP
         * status code of 200 (Ok) even if the UPDATE fails. In the event of an
         * UPDATE failure, the stack trace will be formatted into the response.
         * 
         * @see BigdataRDFContext#MONITOR
         */
        final boolean monitor;
        
        /**
         * The timstamp (in nanoseconds) when the task obtains its connection
         * and begins to execute. 
         */
        private volatile long beginNanos = 0L;
        
        /**
         * The timstamp (in nanoseconds) when the task finishes its execution.
         */
        private volatile long endNanos = 0L;

        
        /**
         * Return the elapsed execution time in milliseconds. This will be ZERO
         * (0) until the task begins to execute.
         * <p>
         * Note: This is used to report the elapsed time for the execution of
         * SPARQL UPDATE requests. Since SPARQL UPDATE requests are not run on
         * the {@link QueryEngine}, {@link IRunningQuery#getElapsed()} can not
         * be used for UPDATEs. It could also be used for QUERYs, but those are
         * run on the {@link QueryEngine} and {@link IRunningQuery#getElapsed()}
         * may be used instead.
         */
        public long getElapsedExecutionMillis() {

            if (beginNanos == 0L) {
                // Not yet executing.
                return 0L;
            }
            long now = endNanos;
            if (now == 0L) {
                // Not yet done executing.
                now = System.nanoTime();
            }
            // Elasped execution time (wall clock).
            final long elapsed = now - beginNanos;
            // Convert to milliseconds.
            return TimeUnit.NANOSECONDS.toMillis(elapsed);
        }

		/**
		 * Version for SPARQL QUERY.
		 * 
		 * @param cxn
		 *            The connection used to isolate the query or update
		 *            request.
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
        		final BigdataSailRepositoryConnection cxn,//
                final String namespace,//
                final long timestamp, //
                final String baseURI, //
                final boolean includeInferred, //
                final Map<String, Value> bindings, //
                final ASTContainer astContainer,//
                final QueryType queryType,//
                final String mimeType,//
                final Charset charset,//
                final String fileExt,//
                final HttpServletRequest req,//
                final HttpServletResponse resp,//
                final OutputStream os//
        ) {

            if (cxn == null)
                throw new IllegalArgumentException();
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
            if (resp == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.cxn = cxn;
            this.namespace = namespace;
            this.timestamp = timestamp;
            this.baseURI = baseURI;
            this.includeInferred = includeInferred;
            this.bindings = bindings;
            this.astContainer = astContainer;
            this.update = false;
            this.queryType = queryType;
            this.mimeType = mimeType;
            this.charset = charset;
            this.fileExt = fileExt;
            this.req = req;
            this.resp = resp;
            this.explain = req.getParameter(EXPLAIN) != null;
            this.explainDetails = explain && isExplainDetails(req);
            this.analytic = getEffectiveBooleanValue(
                    req.getParameter(ANALYTIC), QueryHints.DEFAULT_ANALYTIC);
            this.rto = getEffectiveBooleanValue(req.getParameter(RTO),
                    QueryHints.DEFAULT_OPTIMIZER
                            .equals(QueryOptimizerEnum.Runtime));
            this.xhtml = getEffectiveBooleanValue(req.getParameter(XHTML),
                    false);
            this.monitor = getEffectiveBooleanValue(req.getParameter(MONITOR),
                    false);
            this.os = os;
            this.queryId = Long.valueOf(m_queryIdFactory.incrementAndGet());

        }

        /**
         * Version for SPARQL UPDATE.
         * 
         * @param namespace
         *            The namespace against which the query will be run.
         * @param timestamp
         *            The timestamp of the view for that namespace against which
         *            the query will be run.
         * @param baseURI
         *            The base URI.
         * @param includeInferred
         *            when <code>true</code> inferences will be included in the
         *            visited access paths.
         * @param astContainer
         *            The container with all the information about the submitted
         *            query, including the original SPARQL query, the parse
         *            tree, etc.
         * @param req
         *            The request.
         * @param resp
         *            The response.
         * @param os
         *            Where to write the data for the query result.
         */
        protected AbstractQueryTask(//
        		final BigdataSailRepositoryConnection cxn,//
                final String namespace,//
                final long timestamp, //
                final String baseURI, //
                final boolean includeInferred, //
                final Map<String, Value> bindings, //
                final ASTContainer astContainer,//
//                final QueryType queryType,//
//                final String mimeType,//
//                final Charset charset,//
//                final String fileExt,//
                final HttpServletRequest req,//
                final HttpServletResponse resp,//
                final OutputStream os//
        ) {

            if (cxn == null)
                throw new IllegalArgumentException();
            if (namespace == null)
                throw new IllegalArgumentException();
            if (baseURI == null)
                throw new IllegalArgumentException();
            if (astContainer == null)
                throw new IllegalArgumentException();
            if (req == null)
                throw new IllegalArgumentException();
            if (resp == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.cxn = cxn;
            this.namespace = namespace;
            this.timestamp = timestamp;
            this.baseURI = baseURI;
            this.includeInferred = includeInferred;
            this.bindings = bindings;
            this.astContainer = astContainer;
            this.update = true;
            this.queryType = null;
            this.mimeType = null;
            this.charset = Charset.forName("UTF-8");
            this.fileExt = null;
            this.req = req;
            this.resp = resp;
            this.explain = req.getParameter(EXPLAIN) != null;
            this.explainDetails = explain && isExplainDetails(req);
            this.analytic = getEffectiveBooleanValue(
                    req.getParameter(ANALYTIC), QueryHints.DEFAULT_ANALYTIC);
            this.rto = getEffectiveBooleanValue(req.getParameter(RTO),
                    QueryHints.DEFAULT_OPTIMIZER
                            .equals(QueryOptimizerEnum.Runtime));
            this.xhtml = getEffectiveBooleanValue(req.getParameter(XHTML),
                    false);
            this.monitor = getEffectiveBooleanValue(req.getParameter(MONITOR),
                    false);
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
        
        protected void setBindings(final AbstractOperation queryOrUpdate) {
            for (Entry<String, Value> binding: bindings.entrySet()) {
                queryOrUpdate.setBinding(binding.getKey(), binding.getValue());
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
            final long begin = System.nanoTime();

            final AbstractQuery query = newQuery(cxn);

            // Figure out the UUID under which the query will execute.
            final UUID queryId2 = setQueryId(((BigdataSailQuery) query)
                    .getASTContainer());
            
            // Override query if data set protocol parameters were used.
			overrideDataset(query);

			// Set bindings if protocol parameters were used.
			setBindings(query);
			
			query.setIncludeInferred(includeInferred);
			
            if (analytic) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.ANALYTIC, "true");

            }

            if (rto) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.OPTIMIZER,
                        QueryOptimizerEnum.Runtime.toString());
                
            }

			// Set the query object.
			this.sailQueryOrUpdate = query;
			
			// Set the IRunningQuery's UUID (volatile write!) 
			this.queryId2 = queryId2;
			
            final RunningQuery r = new RunningQuery(queryId.longValue(),
                    queryId2, begin, this);

            // Stuff it in the maps of running queries.
            m_queries.put(queryId, r);
            m_queries2.put(queryId2, r);

            return query;
            
        }

        /**
         * 
         * <p>
         * Note: This is also responsible for noticing the time at which the
         * query begins to execute and storing the {@link RunningQuery} in the
         * {@link #m_queries} map.
         * 
         * @param cxn
         *            The connection.
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

			// Set bindings if protocol parameters were used.
			setBindings(update);

            if (analytic) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.ANALYTIC, "true");

            }

            if (rto) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.OPTIMIZER,
                        QueryOptimizerEnum.Runtime.toString());
                
            }

            // Set the query object.
            this.sailQueryOrUpdate = update;
            
            /*
             * Make a note of the UUID associated with this UPDATE request
             * (volatile write!)
             * 
             * Note: While the UPDATE request does not directly execute on the
             * QueryEngine, each request UPDATE is assigned a UUID. The UUID is
             * either assigned by a query hint specified with the SPARQL UPDATE
             * request or generated automatically. In either case, it becomes
             * bound on the ASTContainer as a query hint.
             */
            this.queryId2 = queryId2;

            final RunningQuery r = new RunningQuery(queryId.longValue(),
                    queryId2, begin, this);
            
            // Stuff it in the maps of running queries.
            m_queries.put(queryId, r);
            m_queries2.put(queryId2, r);

            /**
             * Handle data races in CANCEL of an UPDATE operation whose
             * cancellation was requested before it began to execute.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/899"> REST API Query
             *      Cancellation </a>
             */
            {

                final QueryEngine queryEngine = QueryEngineFactory.getInstance()
                        .getQueryController(getIndexManager());

                if (queryEngine.pendingCancel(queryId2)) {

                    /*
                     * There is a pending CANCEL for this UPDATE request, so
                     * cancel it now.
                     */
                    updateFuture.cancel(true/* mayInterruptIfRunning */);

                }

            }
            
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
         * 
         * @see http://trac.blazegraph.com/ticket/914 (Set timeout on remote query)
         */
        private AbstractQuery newQuery(final BigdataSailRepositoryConnection cxn) {

            /*
             * Establish the query timeout. This may be set in web.xml, which
             * overrides all queries and sets a maximum allowed time for query
             * execution. This may also be set either via setMaxQuery() or
             * setMaxQueryMillis() which set a HTTP header (in milliseconds).
             */

        	final long queryTimeoutMillis = getQueryTimeout(req, getConfig().queryTimeout);

            if (queryTimeoutMillis > 0) {

                /*
                 * If we have a timeout, then it is applied to the AST. The
                 * timeout will be in milliseconds.
                 */
                
                final QueryRoot originalQuery = astContainer.getOriginalAST();

                originalQuery.setTimeout(queryTimeoutMillis);

            }
            
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
         *         {@link IRunningQuery} and never <code>null</code>.
         */
		protected UUID setQueryId(final ASTContainer astContainer) {
			assert queryId2 == null; // precondition.
            // Figure out the effective UUID under which the query will run.
            final String queryIdStr = astContainer.getQueryHint(
                    QueryHints.QUERYID);
            if (queryIdStr == null) {
                // Not specified, so generate and set on query hint.
                queryId2 = UUID.randomUUID();
                astContainer.setQueryHint(QueryHints.QUERYID,
                        queryId2.toString());
			} else {
			    // Specified by a query hint.
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

//        /**
//         * Task for executing a SPARQL QUERY or SPARQL UPDATE.
//         * <p>
//         * See {@link AbstractQueryTask#update} to decide whether this task is a
//         * QUERY or an UPDATE.
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson</a>
//         */
//        private class SparqlRestApiTask implements Callable<Void> {//extends AbstractRestApiTask<Void> {
//
////            public SparqlRestApiTask(final HttpServletRequest req,
////                    final HttpServletResponse resp, final String namespace,
////                    final long timestamp) {
////
////                super(req, resp, namespace, timestamp);
////                
////            }
//
////            @Override
////            public boolean isReadOnly() {
////             
////                // Read-only unless SPARQL UPDATE.
////                return !AbstractQueryTask.this.update;
////                
////            }
//
//            @Override
//            public Void call() throws Exception {
////                BigdataSailRepositoryConnection cxn = null;
////                boolean success = false;
//                try {
//                    // Note: Will be UPDATE connection if UPDATE request!!!
////                    cxn = getQueryConnection();//namespace, timestamp);
//                    if(log.isTraceEnabled())
//                        log.trace("Query running...");
//                    beginNanos = System.nanoTime();
//                    if (explain && !update) {
//                        /*
//                         * The data goes to a bit bucket and we send an
//                         * "explanation" of the query evaluation back to the caller.
//                         * 
//                         * Note: The trick is how to get hold of the IRunningQuery
//                         * object. It is created deep within the Sail when we
//                         * finally submit a query plan to the query engine. We have
//                         * the queryId (on queryId2), so we can look up the
//                         * IRunningQuery in [m_queries] while it is running, but
//                         * once it is terminated the IRunningQuery will have been
//                         * cleared from the internal map maintained by the
//                         * QueryEngine, at which point we can not longer find it.
//                         * 
//                         * Note: We can't do this for UPDATE since it would have a
//                         * side-effect anyway. The way to "EXPLAIN" an UPDATE is to
//                         * break it down into the component QUERY bits and execute
//                         * those.
//                         */
//                        doQuery(cxn, new NullOutputStream());
////                        success = true;
//                    } else {
//                        doQuery(cxn, os);
////                        success = true;
//                        os.flush();
//                        os.close();
//                    }
//                    if (log.isTraceEnabled())
//                        log.trace("Query done.");
//                    return null;
//                } finally {
//                    endNanos = System.nanoTime();
//                    m_queries.remove(queryId);
//                    if (queryId2 != null) m_queries2.remove(queryId2);
//                }
//            }
//            
//        } // class SparqlRestApiTask
        
        @Override
        final public Void call() throws Exception {

			/*
			 * Note: We are already inside of an AbstractApiTask.submitApiTask()
			 * invocation made by doSparqlQuery() or doSparqlUpdate(). 
			 */
        	return innerCall();

        } // call()

        private Void innerCall() throws Exception {
//                BigdataSailRepositoryConnection cxn = null;
//                boolean success = false;
            try {
                // Note: Will be UPDATE connection if UPDATE request!!!
//                    cxn = getQueryConnection();//namespace, timestamp);
                if(log.isTraceEnabled())
                    log.trace("Query running...");
                beginNanos = System.nanoTime();
                if (explain && !update) {
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
                     * 
                     * Note: We can't do this for UPDATE since it would have a
                     * side-effect anyway. The way to "EXPLAIN" an UPDATE is to
                     * break it down into the component QUERY bits and execute
                     * those.
                     */
                    doQuery(cxn, new NullOutputStream());
//                        success = true;
                } else {
                    doQuery(cxn, os);
//                        success = true;
                  /*
                   * GROUP_COMMIT: For mutation requests, calling flush() on the
                   * output stream unblocks the client and allows it to proceed
                   * BEFORE the write set of a mutation has been melded into a
                   * group commit. This is only a problem for UPDATE requests.
                   * 
                   * The correct way to handle this is to allow the servlet
                   * container to close the output stream. That way the close
                   * occurs only after the group commit and when the control has
                   * been returned to the servlet container layer.
                   * 
                   * There are some REST API methods (DELETE-WITH-QUERY,
                   * UPDATE-WITH-QUERY) that reenter the API using a
                   * PipedInputStream / PipedOutputStream to run a query (against
                   * the last commit time) and pipe the results into a parser that
                   * then executes a mutation without requiring the results to be
                   * fully buffered. In order for those operations to not deadlock
                   * we MUST flush() and close() the PipedOutputStream here (at
                   * last for now - it looks like we probably need to execute those
                   * REST API methods differently in order to support group commit
                   * since reading from the lastCommitTime does NOT provide the
                   * proper visibility guarantees when there could already be
                   * multiple write sets buffered for the necessary indices by
                   * other mutation tasks within the current commit group.)
                   */
                     if (os instanceof PipedOutputStream) {
                     os.flush();
                     os.close();
                  }
                }
                if (log.isTraceEnabled())
                    log.trace("Query done.");
                return null;
            } finally {
                endNanos = System.nanoTime();
                m_queries.remove(queryId);
                if (queryId2 != null) m_queries2.remove(queryId2);
            }
        } // innerCall()
        
    } // class AbstractQueryTask

    /**
     * Executes a ASK query.
     */
    private class AskQueryTask extends AbstractQueryTask {

        public AskQueryTask(final BigdataSailRepositoryConnection cxn, 
        		final String namespace, final long timestamp,
                final String baseURI, final boolean includeInferred,
                final Map<String, Value> bindings,
                final ASTContainer astContainer, final QueryType queryType,
                final BooleanQueryResultFormat format,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(cxn, namespace, timestamp, baseURI, includeInferred, bindings, astContainer, queryType,
                    format.getDefaultMIMEType(), format.getCharset(), format
                            .getDefaultFileExtension(), req, resp, os);

        }

        @Override
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

        public TupleQueryTask(final BigdataSailRepositoryConnection cxn,
        		final String namespace, final long timestamp,
                final String baseURI, final boolean includeInferred,
                final Map<String, Value> bindings,
                final ASTContainer astContainer,
                final QueryType queryType, final String mimeType,
                final Charset charset, final String fileExt,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(cxn, namespace, timestamp, baseURI, includeInferred, bindings, astContainer, queryType,
                    mimeType, charset, fileExt, req, resp, os);

		}

        @Override
		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) setupQuery(cxn);

            final TupleQueryResultWriter w;


            if (xhtml) {
            
                /*
                 * Override the XMLWriter to ensure that the XSL style sheet is
                 * declared in the generated XML document. This will tell the
                 * browser that it should style the result.
                 * 
                 * Note: The Content-Type header also needs to be overridden in
                 * order to have the browser apply the style sheet.
                 */
                
                final String stylesheet = getEffectiveStringValue(
                        req.getParameter(XSL_STYLESHEET),
                        DEFAULT_XSL_STYLESHEET);
                
                final XMLWriter xmlWriter = new MyXMLWriter(os, stylesheet);
                
                w = new SPARQLResultsXMLWriter(xmlWriter);
                
            } else {

                // Note: getQueryTask() verifies that format will be non-null.
                final TupleQueryResultFormat format = TupleQueryResultWriterRegistry
                        .getInstance().getFileFormatForMIMEType(mimeType);

                w = TupleQueryResultWriterRegistry.getInstance().get(format)
                        .getWriter(os);
                
            }

            query.evaluate(w);

		}

	}
    
    private static class MyXMLWriter extends XMLWriter {

        final private String stylesheet;
        
        public MyXMLWriter(final OutputStream outputStream,
                final String stylesheet) {
            
            super(outputStream);
            
            this.stylesheet = stylesheet;
            
        }

        @Override
        public void startDocument() throws IOException {

            super.startDocument();
            
            _writeLn("<?xml-stylesheet type=\"text/xsl\" href=\"" + stylesheet
                    + "\" ?>");

        }
        
    }

	/**
	 * Executes a graph query.
	 */
    private class GraphQueryTask extends AbstractQueryTask {

        public GraphQueryTask(final BigdataSailRepositoryConnection cxn,
                final String namespace, final long timestamp,
                final String baseURI, final boolean includeInferred,
                final Map<String, Value> bindings,
                final ASTContainer astContainer,
                final QueryType queryType, final RDFFormat format,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(cxn, namespace, timestamp, baseURI, includeInferred, bindings, astContainer, queryType,
                    format.getDefaultMIMEType(), format.getCharset(), format
                            .getDefaultFileExtension(), req, resp, os);

        }

		@Override
		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

            final BigdataSailGraphQuery query = (BigdataSailGraphQuery) setupQuery(cxn);

            // Note: getQueryTask() verifies that format will be non-null.
            final RDFFormat format = RDFWriterRegistry.getInstance()
                    .getFileFormatForMIMEType(mimeType);

            final RDFWriter w = RDFWriterRegistry.getInstance().get(format)
                    .getWriter(os);

         query.evaluate(w);
         
        }

	}

	UpdateTask getUpdateTask(final BigdataSailRepositoryConnection cxn,
			final String namespace, final long timestamp, final String baseURI,
			final Map<String, Value> bindings,
			final ASTContainer astContainer, final HttpServletRequest req,
			final HttpServletResponse resp, final OutputStream os) {

		return new UpdateTask(cxn, namespace, timestamp, baseURI, bindings, astContainer,
				req, resp, os);

	}

    /**
     * Executes a SPARQL UPDATE.
     */
    class UpdateTask extends AbstractQueryTask {

        /**
         * The timestamp for the commit point associated with the update and
         * <code>-1</code> if the commit point has not yet been assigned.
         */
        public final AtomicLong commitTime = new AtomicLong(-1);
        
        private boolean echoBack = false;
        private final CAT mutationCount = new CAT();
        
        public UpdateTask(final BigdataSailRepositoryConnection cxn, 
        		final String namespace, final long timestamp,
                final String baseURI, final Map<String, Value> bindings,
                final ASTContainer astContainer,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

        	// SPARQL Query parameter includeInferred set to true, because inferred triples
        	// should always be updated automatically upon original triples changed 
            super(cxn, namespace, timestamp, baseURI, /* includeInferred = */ true, bindings, astContainer,
                    req,//
                    resp,//
                    os//
                    );
            /*
             * Setup a change listener. It will notice the #of mutations.
             */

            cxn.addChangeLog(new IChangeLog(){
            
                @Override
                public void changeEvent(final IChangeRecord record) {
                    mutationCount.increment();
                }
                
                @Override
                public void transactionBegin() {
                }
                
                @Override
                public void transactionPrepare() {
                }
                
                @Override
                public void transactionCommited(long commitTime) {
                }
                
                @Override
                public void transactionAborted() {
                }
                
                @Override
                public void close() {
                }

            });

        }

        /**
         * {@inheritDoc}
         * <p>
         * This executes the SPARQL UPDATE and formats the HTTP response.
         */
        @Override
        protected void doQuery(final BigdataSailRepositoryConnection cxn,
                final OutputStream os) throws Exception {
            
          

            // Prepare the UPDATE request.
            final BigdataSailUpdate update = setupUpdate(cxn);

            final SparqlUpdateResponseWriter listener;
            final ByteArrayOutputStream baos;
            

            if(req.getHeader(HTTP_HEADER_ECHO_BACK_QUERY) != null) {
                
                echoBack = Boolean.parseBoolean(req.getHeader(HTTP_HEADER_ECHO_BACK_QUERY));
                
            }

            if (monitor) {

                /*
                 * Establish a listener that will log the process onto an XHTML
                 * document that will be delivered (flushed) incrementally to
                 * the client. The status code for the response will always be
                 * 200 (Ok). If there is an error, then that error will be
                 * reported in the XHTML response but NOT in the status code.
                 */
                
                // Do not buffer the response.
                baos = null;
                
                // Always sending an OK with a response entity.
                resp.setStatus(BigdataServlet.HTTP_OK);

				/**
				 * Note: Content Type header is required. See <a href=
				 * "http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1"
				 * >RFC2616</a>
				 * 
				 * Note: This needs to be written before we write on the stream
				 * and after we decide on the status code. Since this code path
				 * handles the "monitor" mode, we are writing it out immediately
				 * and the response will be an XHTML document.
				 */
				resp.setContentType("text/html; charset=" + charset.name());

            /*
             * Note: Setting this to true is causing an EofException when the
             * jetty server attempts to write on the client (where the client in
             * this instance was Chrome). Given that flushing the http response
             * commits the response, it seems incorrect that we would ever do
             * this. We probably need to look at other mechanisms for providing
             * liveness for the SPARQL UPDATE "monitor" option, such as HTTP 1.1
             * streaming connections.
             * 
             * See #1133 (SPARQL UPDATE "MONITOR" LIVENESS)
             */
				final boolean flushEachEvent = false;
				
                // This will write the response entity.
                listener = new SparqlUpdateResponseWriter(resp, os, charset,
                        true /* reportLoadProgress */, flushEachEvent,
                        mutationCount, echoBack);

            } else {

                /*
                 * The listener logs the progress report (with the incremental
                 * load events) onto an xml document model but DOES NOT write
                 * anything on the servlet response. If there is an error, the
                 * HTTP status code will reflect that error. Otherwise we send
                 * back the XML document with a 200 (Ok) status code.
                 * 
                 * Note: This code path supports REST clients that expect the
                 * status code to reflect the *outcome* of the SPARQL UPDATE
                 * request. We MUST NOT write the XML document onto the response
                 * incrementally since the servlet response can become committed
                 * and we will be unable to change the status code from Ok (200)
                 * if an error occurs.
                 */

                // buffer the response here.
                baos = new ByteArrayOutputStream();

				/*
				 * Note: Do NOT set the ContentType yet. This action needs to be
				 * deferred until we decide that a normal response (vs an
				 * exception) will be delivered.
				 */
                
                listener = new SparqlUpdateResponseWriter(resp, baos, charset,
                        false/* reportLoadProgress */, false/* flushEachEvent */,
                        mutationCount, echoBack);

            }

            /*
             * Run the update.
             */
            {

                // Setup the SPARQL UPDATE listener.
                cxn.getSailConnection().addListener(listener);

                // Execute the SPARQL UPDATE.
                this.commitTime.set(update.execute2());

                // Write out the response.
                listener.commit(this.commitTime.get());

                // Flush the listener (close document elements, etc).
                listener.flush();

            }

            if (baos != null) {

                /*
                 * Since we buffered the response, we have to send it out now.
                 */
                
                // Send an OK with a response entity.
                resp.setStatus(BigdataServlet.HTTP_OK);

				/**
				 * Note: Content Type header is required. See <a href=
				 * "http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1"
				 * >RFC2616</a>
				 * 
				 * Note: This needs to be written before we write on the stream
				 * and after we decide on the status code. Since this code path
				 * defers the status code until we know whether or not the
				 * SPARQL UPDATE was atomically committed, we write it out now
				 * and then serialize the response document.
				 */
				resp.setContentType("text/html; charset=" + charset.name());

				// Copy the document into the response.
                baos.flush();
                os.write(baos.toByteArray());

                /*
                 * DO NOT FLUSH THE RESPONSE HERE.  IT WILL COMMIT THE RESPONSE
                 * TO THE CLIENT BEFORE THE GROUP COMMMIT !!!
                 * 
                 * @see #566
                 */
//                // Flush the response.
//                os.flush();

            }

        }

		public long getMutationCount() {
			return this.mutationCount.get();
		}

    }

    /**
     * Writes the SPARQL UPDATE response document onto the caller's
     * {@link OutputStream}. Depending on the use case, the stream will either
     * write directly onto the servlet response or it will be buffered until the
     * UPDATE request is finished.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/597">
     *      SPARQL UPDATE Listener </a>
     */
    private static class SparqlUpdateResponseWriter implements
            ISPARQLUpdateListener {

        private final long begin;
        private final HttpServletResponse resp;
        private final OutputStream os;
        private final Writer w;
        private final HTMLBuilder doc;
        private final Charset charset;
        private final XMLBuilder.Node body;
        private final boolean reportLoadProgress;
        private final boolean flushEachEvent;
        private final CAT mutationCount;
        private final boolean echoBack;
        
        /**
         * Used to correlate incremental LOAD progress messages.
         */
        private volatile Update lastOp = null;

        /**
         * 
         * 
         * @param os
         *            The {@link OutputStream}.
         * @param charset
         *            The character set.
         * @param reportLoadProgress
         *            When <code>true</code>, the incremental load progress will
         *            be included in the document. Note that this only makes
         *            sense when the document will be delivered incrementally to
         *            the client rather than "at-once" after the completion of
         *            the UPDATE operation.
         * @param flushEachEvent
         *            When <code>true</code>, each the {@link Writer} will be
         *            flushed after each logged event in order to ensure timely
         *            delivery to the client.
         * @param mutationCount
         *            A counter that is updated as mutations are applied.
         * @throws IOException
         */
        public SparqlUpdateResponseWriter(final HttpServletResponse resp,
                final OutputStream os, final Charset charset,
                final boolean reportLoadProgress, final boolean flushEachEvent,
                final CAT mutationCount, final boolean echoBack)
                throws IOException {

            if (resp == null)
                throw new IllegalArgumentException();
            
            if (os == null)
                throw new IllegalArgumentException();
            
            this.resp = resp;
            
//            /** Content Type header is required: Note: This should be handled when we make the decision to incrementally evict vs wait until the UPDATE completes and then write the status code (monitor vs non-monitor).
//            http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1
//            */
//            resp.setContentType("text/html; charset="+charset.name());
            
            this.os = os;
            
            this.charset = charset;

            this.w = new OutputStreamWriter(os, charset);

            this.doc = new HTMLBuilder(charset.name(), w);

            this.reportLoadProgress = reportLoadProgress;
            
            this.flushEachEvent = flushEachEvent;
        
            this.mutationCount = mutationCount;
            
            this.begin = System.nanoTime();
            
            this.body = writeSparqlUpdateResponseHeader();
            
            this.echoBack = echoBack;

        }

        /**
         * Write the header of the SPARQL UPDATE response.
         * 
         * @return The body.
         * 
         * @throws IOException
         */
        private XMLBuilder.Node writeSparqlUpdateResponseHeader()
                throws IOException {

            XMLBuilder.Node current = doc.root("html");
            		
            addHtmlHeader(current, charset.name());

            return current;
            
        }
        

        @Override
        public void updateEvent(final SPARQLUpdateEvent e) {

            try {

                // Total elapsed milliseconds from start of update request.
                final long totalElapsedMillis = TimeUnit.NANOSECONDS
                        .toMillis(System.nanoTime() - begin);

                // Elapsed milliseconds for this update operation.
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(e
                        .getElapsedNanos());
            
                if (e instanceof SPARQLUpdateEvent.LoadProgress) {
                
                    if (reportLoadProgress) {

                        /*
                         * Incremental progress on LOAD.
                         */
                        
                        final SPARQLUpdateEvent.LoadProgress tmp = (SPARQLUpdateEvent.LoadProgress) e;
                        
                        final long parsed = tmp.getParsedCount();
                        
                        final Update thisOp = e.getUpdate();

                        if (thisOp != lastOp) {

                            /*
                             * This is the first incremental load progress
                             * report for this LOAD operation.
                             */
                            lastOp = thisOp;

                            // Write out the LOAD operation.
                            body.node("pre").text(thisOp.toString())//
                                    .close();

                        }
                        
                        body.node("br")
                                .text("totalElapsed=" + totalElapsedMillis
                                        + "ms, elapsed=" + elapsedMillis
                                        + "ms, parsed=" + parsed + ", tps="
                                        + tmp.triplesPerSecond() + ", done="
                                        + tmp.isDone()).close();

                    }

                } else if (e.getCause() != null) {

                    /*
                     * An exception occurred when processing some update
                     * operation.
                     */
                    
                    final Throwable t = e.getCause();
                    
                    final StringWriter w = new StringWriter();
                    
                    final PrintWriter pw = new PrintWriter(w);
                    
                    t.printStackTrace(pw);
                    
                    pw.flush();
                    pw.close();
                    
                    body.node("p").text("ABORT").close()//
                    .node("pre").text(e.getUpdate().toString()).close()//
                    .node("pre").text(w.toString()).close()//
                    .node("p").text("totalElapsed=" + totalElapsedMillis
                            + "ms, elapsed=" + elapsedMillis + "ms")
                    .close();

                    // horizontal line after each operation.
                    body.node("hr").close();

                } else {

                    /*
                     * End of some UPDATE operation.
                     */

                    if (lastOp == e.getUpdate()) {
                        /*
                         * The end of a LOAD operation for which we reported the
                         * incremental progress. In this case, the LOAD
                         * operation was already written onto the response
                         * document, including the final report from the end of
                         * the parser run. So, all we have to do here is clear
                         * the reference.
                         */
                        lastOp = null;
//                        body.node("p")
//                                //
////                                .node("pre")
////                                .text(e.getUpdate().toString())
////                                .close()
//                                //
//                                .text("totalElapsed=" + totalElapsedMillis
//                                        + "ms, elapsed=" + elapsedMillis + "ms")//
//                                .close();
                    } else {
                    
						/*
						 * Report statistics for the UPDATE operation.
						 */
                    	
						/*
						 * Note: will be null unless DELETE/INSERT WHERE
						 * operation.
						 * 
						 * @see BLZG-1446 (Provide detailed statistics on
						 * execution performance inside of SPARQL UPDATE
						 * requests).
						 */
                    	final DeleteInsertWhereStats deleteInsertWhereStats = e.getDeleteInsertWhereStats();
                    	
                        if(echoBack) {
                        	
                            body.node("pre")
                                .text(e.getUpdate().toString())
                                .close();
                        }
                                //
                        body.node("p")
                                .text("totalElapsed=" + totalElapsedMillis
                                        + "ms, elapsed=" + elapsedMillis
                                        + "ms, connFlush="+TimeUnit.NANOSECONDS.toMillis(e.getConnectionFlushNanos())//
                                        + "ms, batchResolve="+TimeUnit.NANOSECONDS.toMillis(e.getBatchResolveNanos())//
										+ (deleteInsertWhereStats == null ? ""
												: ", whereClause=" + TimeUnit.NANOSECONDS.toMillis(deleteInsertWhereStats.whereNanos.get())
														+ "ms, deleteClause=" + TimeUnit.NANOSECONDS.toMillis(deleteInsertWhereStats.deleteNanos.get())
														+ "ms, insertClause=" + TimeUnit.NANOSECONDS.toMillis(deleteInsertWhereStats.whereNanos.get())
														+ "ms"))//
                                .close();
                   }
                    
                    // horizontal line after each operation.
                    body.node("hr").close();

                }

                if (flushEachEvent) {

               /*
                * Flush the response for each event so the client (presumably a
                * human operator) can see the progress log update "live".
                * 
                * Note: flushing the response is problematic and leads to an
                * EofException. This has been disabled, but that causes liveness
                * problems with the SPARQL UPDATE "monitor" option.
                * 
                * See #1133 (SPARQL UPDATE "MONITOR" LIVENESS)
                */
    
                    w.flush();
                    
                    os.flush();
                    
                    /*
                     * Note: appears to be necessary for incremental writes.
                     */
                    resp.flushBuffer();
                    
                }

            } catch (IOException e1) {
            
                throw new RuntimeException(e1);
                
            }

        }

        /**
         * Write the commit time into the document.
         * 
         * @param commitTime
         *            The commit time.
         */
        public void commit(final long commitTime) throws IOException {

            // Total elapsed milliseconds from start of update request.
            final long totalElapsedMillis = TimeUnit.NANOSECONDS
                    .toMillis(System.nanoTime() - begin);

            body.node("p")
                    .text("COMMIT: totalElapsed=" + totalElapsedMillis
                            + "ms, commitTime=" + commitTime
                            + ", mutationCount=" + mutationCount.get())//
                    .close();

        }

        public void flush() throws IOException {

            doc.closeAll(body);

            w.flush();

            w.close();
            
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
     *            The query (for log messages).
     * @param baseURI
     *            The baseURI (since BLZG-2039).
     * @param astContainer
     *            The ASTContainer for the already parsed query (since BLZG-2039).
     * @param includeInferred
	 * @param acceptOverride
	 *            Override the Accept header (optional). This is used by UPDATE
	 *            and DELETE so they can control the {@link RDFFormat} of the
	 *            materialized query results.
	 * @param req
	 *            The request.
     * @param resp
     *            The response.
     * @param os
     *            Where to write the results. Note: This is NOT always the
     *            OutputStream associated with the response! For
     *            DELETE-WITH-QUERY and UPDATE-WITH-QUERY this is a
     *            PipedOutputStream.
	 * 
	 * @return The task.
	 * 
	 * @throws IOException
	 */
    public AbstractQueryTask getQueryTask(//
    		final BigdataSailRepositoryConnection cxn,//
            final String namespace,//
            final long timestamp,//
            final String queryStr,//
            final String baseURI,// See BLZG-2039
            final ASTContainer astContainer,// See BLZG-2039
            final boolean includeInferred, //
            final Map<String, Value> bindings, //
            final String acceptOverride,//
            final HttpServletRequest req,//
            final HttpServletResponse resp,//
            final OutputStream os//
//            final boolean update//
            ) throws MalformedQueryException, IOException {

        if (cxn == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();
        
        if (queryStr == null)
            throw new IllegalArgumentException();
        
        if (baseURI == null)
            throw new IllegalArgumentException();
        
        if (astContainer == null)
            throw new IllegalArgumentException();
        
        if (log.isDebugEnabled())
            log.debug(astContainer.toString());

        final QueryType queryType = astContainer.getOriginalAST()
                .getQueryType();
        
		/*
		 * When true, provide an "explanation" for the query (query plan, query
		 * evaluation statistics) rather than the results of the query.
		 */
		final boolean explain = req.getParameter(EXPLAIN) != null;

		final boolean xhtml = req.getParameter(XHTML) != null;

        /*
         * CONNEG for the MIME type.
         * 
         * Note: An attempt to CONNEG for a MIME type which can not be used with
         * a given type of query will result in a response using a default MIME
         * Type for that query.
         */
        final String acceptStr;
        if (explain) {
            acceptStr = BigdataServlet.MIME_TEXT_HTML;
        } else if (acceptOverride != null) {
            acceptStr = acceptOverride;
        } else if (xhtml) {
            switch (queryType) {
            case ASK:
                /*
                 * TODO This is just sending back text/plain. If we want to keep
                 * to the XHTML semantics, then we should send back XML with an
                 * XSL style sheet.
                 */
                acceptStr = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
                break;
            case SELECT:
                /*
                 * We will send back an XML document with an XSLT style sheet
                 * declaration. The Content-Type needs to be application/xml in
                 * order for the browser to interpret the style sheet
                 * declaration.
                 */
                // Generate XML solutions so we can apply XSLT transform.
                acceptStr = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
                break;
            case DESCRIBE:
            case CONSTRUCT:
                /* Generate RDF/XML so we can apply XSLT transform.
                 * 
                 * TODO This should be sending back RDFs or using a lens.
                 */
                acceptStr = RDFFormat.RDFXML.getDefaultMIMEType();
                break;
            default:
                throw new AssertionError("QueryType=" + queryType);
            }
        } else {
            // Use whatever was specified by the client.
        	final List<String> acceptHeaders = Collections.list(req.getHeaders("Accept"));
            acceptStr = ConnegUtil.getMimeTypeForQueryParameterQueryRequest(
            		req.getParameter(BigdataRDFServlet.OUTPUT_FORMAT_QUERY_PARAMETER), 
            		acceptHeaders.toArray(new String[acceptHeaders.size()]));
        }

        // Do conneg.
        final ConnegUtil util = new ConnegUtil(acceptStr);

        switch (queryType) {
        case ASK: {

            final BooleanQueryResultFormat format = util
                    .getBooleanQueryResultFormat(BooleanQueryResultFormat.SPARQL);

            return new AskQueryTask(cxn, namespace, timestamp, baseURI, includeInferred, bindings,
                    astContainer, queryType, format, req, resp, os);

        }
        case DESCRIBE:
        case CONSTRUCT: {

            final RDFFormat format = util.getRDFFormat(RDFFormat.RDFXML);

            return new GraphQueryTask(cxn, namespace, timestamp, baseURI, includeInferred, bindings,
                    astContainer, queryType, format, req, resp, os);

        }
        case SELECT: {

            final TupleQueryResultFormat format = util
                    .getTupleQueryResultFormat(TupleQueryResultFormat.SPARQL);

            final String mimeType;
            final Charset charset;
            final String fileExt;
            if(xhtml) {
                /*
                 * Override as application/xml so the browser will interpret the
                 * XSL style sheet directive.
                 */
                mimeType = BigdataServlet.MIME_APPLICATION_XML;
                charset = Charset.forName(BigdataRDFServlet.UTF8);
                fileExt = "xml";
            } else {
                mimeType = format.getDefaultMIMEType();
                charset = format.getCharset();
                fileExt = format.getDefaultFileExtension();
            }
            return new TupleQueryTask(cxn, namespace, timestamp, baseURI, includeInferred, bindings,
                    astContainer, queryType, mimeType, charset, fileExt, req,
                    resp, os);

        }
        } // switch(queryType)

        throw new RuntimeException("Unknown query type: " + queryType);

    }

	/**
     * Metadata about running {@link AbstractQueryTask}s (this includes both
     * queries and update requests).
     */
	static class RunningQuery {

		/**
		 * The unique identifier for this query as assigned by the SPARQL 
		 * end point (rather than the {@link QueryEngine}).
		 */
		final long queryId;

        /**
         * The unique identifier for this query for the {@link QueryEngine}
         * (non-<code>null</code>).
         * 
         * @see QueryEngine#getRunningQuery(UUID)
         */
        final UUID queryId2;

        /**
         * The task executing the query (non-<code>null</code>).
         */
		final AbstractQueryTask queryTask;
		
//		/** The query. */
//		final String query;
		
		/** The timestamp when the query was accepted (ns). */
		final long begin;

		public RunningQuery(final long queryId, final UUID queryId2,
				final long begin,
				final AbstractQueryTask queryTask) {

            if (queryId2 == null)
                throw new IllegalArgumentException();
            
            if (queryTask == null)
                throw new IllegalArgumentException();

			this.queryId = queryId;

			this.queryId2 = queryId2;
			
//			this.query = query;

			this.begin = begin;
			
			this.queryTask = queryTask;

		}
		
		/**
		 * Convenience method to return com.bigdata.rdf.sail.model.RunningQuery from a BigdataRDFContext
		 * running query. 
		 * 
		 * There is a current difference between the embedded and the REST model in that
		 * the embedded model uses an arbitrary string as an External ID, but the 
		 * REST version uses a timestamp.  The timestap is tightly coupled to the current
		 * workbench.
		 * 
		 * @return 
		 */
		public com.bigdata.rdf.sail.model.RunningQuery getModelRunningQuery() {
		
			final com.bigdata.rdf.sail.model.RunningQuery modelQuery;
			
			final boolean isUpdateQuery = queryTask instanceof UpdateTask?true:false;
			
			modelQuery = new com.bigdata.rdf.sail.model.RunningQuery(
					Long.toString(this.queryId), this.queryId2, this.begin,
					isUpdateQuery);
			
			return modelQuery;
		}

	}

//    /**
//     * Return a connection transaction, which may be read-only or support
//     * update. When the timestamp is associated with a historical commit point,
//     * this will be a read-only connection. When it is associated with the
//     * {@link ITx#UNISOLATED} view or a read-write transaction, this will be a
//     * mutable connection.
//     * 
//     * @param namespace
//     *            The namespace.
//     * @param timestamp
//     *            The timestamp.
//     * 
//     * @throws RepositoryException
//     */
//    public BigdataSailRepositoryConnection getQueryConnection(
//            final String namespace, final long timestamp)
//            throws RepositoryException {
//
//        /*
//         * Note: [timestamp] will be a read-only tx view of the triple store if
//         * a READ_LOCK was specified when the NanoSparqlServer was started
//         * (unless the query explicitly overrides the timestamp of the view on
//         * which it will operate).
//         */
//        final AbstractTripleStore tripleStore = getTripleStore(namespace,
//                timestamp);
//        
//        if (tripleStore == null) {
//
//            throw new DatasetNotFoundException("Not found: namespace="
//                    + namespace + ", timestamp="
//                    + TimestampUtility.toString(timestamp));
//
//        }
//        
//        // Wrap with SAIL.
//        final BigdataSail sail = new BigdataSail(tripleStore);
//
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//
//        repo.initialize();
//
//        if (TimestampUtility.isReadOnly(timestamp)) {
//
//            return (BigdataSailRepositoryConnection) repo
//                    .getReadOnlyConnection(timestamp);
//
//        }
//        
//        // Read-write connection.
//        final BigdataSailRepositoryConnection conn = repo.getConnection();
//        
//        conn.setAutoCommit(false);
//        
//        return conn;
//
//    }

    /**
     * Return a read-only view of the {@link AbstractTripleStore} for the given
     * namespace will read from the commit point associated with the given
     * timestamp.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            A timestamp -or- a tx identifier.
     * 
     * @return The {@link AbstractTripleStore} -or- <code>null</code> if none is
     *         found for that namespace and timestamp.
     * 
     *         FIXME GROUP_COMMIT: Review all callers. They are suspect. The
     *         code will sometimes resolve the KB as of the timestamp, but,
     *         given that the default is to read against the lastCommitTime,
     *         that does NOT prevent a concurrent destroy or create of a KB that
     *         invalidates such a pre-condition test. The main reason for such
     *         pre-condition tests is to provide nice HTTP status code responses
     *         when an identified namespace does (or does not) exist. The better
     *         way to handle this is by pushing the pre-condition test down into
     *         the {@link AbstractRestApiTask} and then throwning out an appropriate
     *         marked exception that gets correctly converted into an HTTP
     *         BAD_REQUEST message rather than sending back a stack trace.
     */
    public AbstractTripleStore getTripleStore(final String namespace,
            final long timestamp) {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, timestamp);

        return tripleStore;
        
    }

    /**
     * Return a list of the namespaces for the {@link AbstractTripleStore}s
     * registered against the bigdata instance.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/867"> NSS concurrency
     *      problem with list namespaces and create namespace </a>
     */
    /*package*/ List<String> getNamespaces(final long timestamp) {
        
        final long tx = newTx(timestamp);
        
		try {
			
		    return getNamespacesTx(tx);
			
		} finally {
			
		    abortTx(tx);
		    
        }

    }

	public List<String> getNamespacesTx(long tx) {

        if (tx == ITx.READ_COMMITTED && getIndexManager() instanceof IBigdataFederation) {

			// Use the last commit point for the federation *only*.
            tx = getIndexManager().getLastCommitTime();

        }

		final SparseRowStore grs = getIndexManager().getGlobalRowStore(tx);

		if (grs == null) {

			log.warn("No GRS @ tx="
					+ TimestampUtility.toString(tx));

			// Empty.
			return Collections.emptyList();

		}

		return grs.getNamespaces(tx);
	}
    
	/**
	 * Obtain a new transaction to protect operations against the specified view
	 * of the database. This uses the transaction mechanisms to prevent
	 * recycling during operations NOT OTHERWISE PROTECTED by a
	 * {@link BigdataSailConnection} for what would otherwise amount to dirty
	 * reads. This is especially critical for reads on the global row store
	 * since it can not be protected by the {@link BigdataSailConnection} for
	 * cases where the KB instance does not yet exist. The presence of such a tx
	 * does NOT prevent concurrent commits. It only prevents recycling during
	 * such commits (and even then only on the RWStore backend).
	 * 
	 * @param timestamp
	 *            The timestamp for the desired view.
	 * 
	 * @return The transaction identifier -or- <code>timestamp</code> if the
	 *         {@link IIndexManager} is not a {@link Journal}.
	 * 
	 * @see ITransactionService#newTx(long)
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/867"> NSS concurrency
	 *      problem with list namespaces and create namespace </a>
	 */
    public long newTx(final long timestamp) {

        long tx = timestamp; // use dirty reads unless Journal.

        if (getIndexManager() instanceof IJournal) {
            
            final ITransactionService txs = ((IJournal) getIndexManager())
                  .getLocalTransactionManager().getTransactionService();

            try {
                tx = txs.newTx(timestamp);
            } catch (IOException e) {
                // Note: Local operation.  Will not throw IOException.
                throw new RuntimeException(e);
            }

        }

        return tx;
    }

	/**
	 * Abort a transaction obtained by {@link #newTx(long)}. This decements the
	 * native active transaction counter for the RWStore. Once that counter
	 * reaches zero, recycling will occur the next time an unisolated mutation
	 * goes through a commit on the journal.
	 * 
	 * @param tx
	 *            The transaction identifier.
	 */
	public void abortTx(final long tx) {

	    if (getIndexManager() instanceof IJournal) {

			final ITransactionService txs = ((IJournal) getIndexManager())
					.getLocalTransactionManager().getTransactionService();

			try {
				txs.abort(tx);
			} catch (IOException e) {
				// Note: Local operation. Will not throw IOException.
				throw new RuntimeException(e);
			}

		}

	}
	
   /**
    * Commit a transaction obtained by {@link #newTx(long)}
    * 
    * @param tx
    *           The transaction identifier.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
    *      transactions in the REST API</a>
    */
   public void commitTx(final long tx) {

      if (getIndexManager() instanceof IJournal) {

         final ITransactionService txs = ((IJournal) getIndexManager())
               .getLocalTransactionManager().getTransactionService();

         try {
            txs.commit(tx);
         } catch (IOException e) {
            // Note: Local operation. Will not throw IOException.
            throw new RuntimeException(e);
         }

      }

   }

	/**
	 * Utility method to consolidate header into a single location.
	 * 
	 * The post-condition is that the current node is open for writing on the
	 * body element.
	 * 
	 * @param current
	 * @throws IOException
	 */
	public static void addHtmlHeader(Node current, String charset)
			throws IOException {

		current = current.node("head");
		current.node("meta").attr("http-equiv", "Content-Type")
				.attr("content", "text/html;charset=" + charset).close();
		current.node("title").textNoEncode("blazegraph&trade; by SYSTAP")
				.close();
		current = current.close();// close the head.

		// open the body
		current = current.node("body");

	}
	
	/**
	 * Utility method to calculate a query timeout parameter value.
	 * Timeout could be set either via a HTTP header 
	 * {@link BigdataBaseContext#HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS} or
	 * via one of the request parameters 
	 * {@link BigdataBaseContext#MAX_QUERY_TIME_MILLIS} or
	 * {@link BigdataBaseContext#TIMEOUT}
	 * The method will return the minimum value in milliseconds 
	 * if several values are specified in request.
	 *  
	 * @param req
	 * @param queryTimeoutMillis
	 * 
	 */
	
	static long getQueryTimeout(final HttpServletRequest req, long queryTimeoutMillis) {
		
		{
            final String s = req
                    .getHeader(HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS);
            if (s != null) {
                final long tmp = StringUtil.toLong(s);
                if (tmp > 0 && // != -1L && //
                        (queryTimeoutMillis == 0/* noLimit */
                        || //
                        tmp < queryTimeoutMillis/* shorterLimit */)//
                ) {
                    // Set based on the http header value.
                    queryTimeoutMillis = tmp;
                }
            }
        }

        {
            final String s = req
                    .getParameter(MAX_QUERY_TIME_MILLIS);
            if (s != null) {
                /*
                 * The maxQueryTimeMillis parameter was specified (0 implies no timeout).
                 */
                final long tmp = StringUtil.toLong(s);
                if (tmp > 0 && // != -1L && //
                        (queryTimeoutMillis == 0/* noLimit */
                        || //
                        tmp < queryTimeoutMillis/* shorterLimit */)//
                ) {
                    /*
                     * Either we do not already have a timeout from the http
                     * header or the web.xml configuration (which implies no
                     * timeout) or the query parameter value is less than the current
                     * timeout. In both cases, we use the query parameter timeout
                     * instead.
                     */
                    queryTimeoutMillis = tmp;
                }
            } 
        }

        {	
	
        	final String s = req.getParameter(TIMEOUT);

        	if (s != null) {
                /*
                 * The timeout parameter was specified (0 implies no timeout).
                 */
                final long tmp = StringUtil.toLong(s) * 1000L;
                if (tmp > 0 && // != -1L && //
                        (queryTimeoutMillis == 0/* noLimit */
                        || //
                        tmp < queryTimeoutMillis/* shorterLimit */)//
                ) {
                    /*
                     * The timeout parameter value is less than the current
                     * timeout. 
                     */
                    queryTimeoutMillis = tmp;
                }
            } 
        }
		
		
		return queryTimeoutMillis;
		
	}

}
